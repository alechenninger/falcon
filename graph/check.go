package graph

import (
	"context"
	"fmt"

	"github.com/alechenninger/falcon/schema"
)

// VisitedKey tracks nodes visited during graph traversal for cycle detection.
type VisitedKey struct {
	ObjectType schema.TypeName
	ObjectID   schema.ID
	Relation   schema.RelationName
}

// Check is the core walk algorithm. It's a standalone function that takes
// Graph for recursion and MultiversionUsersets for data access.
// The observer parameter is used for observability; pass NoOpCheckObserver{} if not needed.
func Check(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	observer CheckObserver,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// Create probe for this check
	ctx, probe := observer.CheckStarted(ctx, subjectType, subjectID, objectType, objectID, relation)
	defer probe.End()

	// Validate inputs
	sch := usersets.Schema()
	ot, ok := sch.Types[objectType]
	if !ok {
		err := fmt.Errorf("unknown object type: %s", objectType)
		probe.Error(err)
		return false, window, err
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		err := fmt.Errorf("unknown relation %s on type %s", relation, objectType)
		probe.Error(err)
		return false, window, err
	}

	// Convert visited slice to map for O(1) lookups
	visitedMap := make(map[VisitedKey]bool, len(visited))
	for _, v := range visited {
		visitedMap[v] = true
	}

	found, resultWindow, err := checkRelation(ctx, graph, usersets, probe, subjectType, subjectID, objectType, objectID, rel, visitedMap, window)
	if err != nil {
		probe.Error(err)
	} else {
		probe.Result(found, resultWindow)
	}
	return found, resultWindow, err
}

// checkRelation evaluates a relation definition against the graph.
func checkRelation(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	probe CheckProbe,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	rel *schema.Relation,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	probe.RelationEntered(objectType, objectID, rel.Name)

	key := VisitedKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		probe.CycleDetected(key)
		return false, window, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// Evaluate each userset in the union
	// Track the tightest window across all "not found" results
	resultMin := window.Min()
	resultMax := window.Max()

	for i, us := range rel.Usersets {
		probe.UsersetChecking(&us)
		ok, newWindow, err := checkUserset(ctx, graph, usersets, probe, subjectType, subjectID, objectType, objectID, rel, us, visited, window)
		if err != nil {
			probe.Error(err)
			return false, window, err
		}
		if ok {
			probe.UnionBranchFound(i)
			return true, newWindow, nil
		}
		// For "not found", track tightest window (highest min)
		if newWindow.Min() > resultMin {
			resultMin = newWindow.Min()
		}
		if newWindow.Max() < resultMax {
			resultMax = newWindow.Max()
		}
	}

	// Like all dependent merges, if the new window is invalid,
	// we need to wait & retry or abort.

	return false, NewSnapshotWindow(resultMin, resultMax), nil
}

// checkUserset evaluates a single userset definition.
func checkUserset(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	probe CheckProbe,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	rel *schema.Relation,
	us schema.Userset,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	switch {
	case len(us.This) > 0:
		// Direct tuple membership for the current relation (including userset subjects)
		return checkDirectAndUserset(ctx, graph, usersets, probe, subjectType, subjectID, objectType, objectID, rel.Name, us.This, visited, window)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object (no routing needed - same object)
		sch := usersets.Schema()
		ot := sch.Types[objectType]
		computedRel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			err := fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
			probe.Error(err)
			return false, window, err
		}
		return checkRelation(ctx, graph, usersets, probe, subjectType, subjectID, objectType, objectID, computedRel, visited, window)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return checkArrow(ctx, graph, usersets, probe, subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited, window)

	default:
		err := fmt.Errorf("invalid userset: no operation specified")
		probe.Error(err)
		return false, window, err
	}
}

// checkDirectAndUserset checks both direct tuple membership and userset tuple membership.
func checkDirectAndUserset(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	probe CheckProbe,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	targetTypes []schema.SubjectRef,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	// Check direct membership first
	probe.DirectLookup(objectType, objectID, relation, subjectType)
	found, directWindow := usersets.ContainsDirectWithin(objectType, objectID, relation, subjectType, subjectID, window)
	probe.DirectLookupResult(found, directWindow)
	if found {
		return true, directWindow, nil
	}

	// Build checks for userset subjects with independent windows per type
	// Each type reads data from the original window, getting its own narrowed window
	var checks []RelationCheck

	for _, ref := range targetTypes {
		// TODO: we can know what the type of set is in this set,
		// and if it's not the same as the subjectType we can skip the check

		if ref.Relation == "" {
			continue // Skip direct subjects
		}

		// Read data for this type from original window - each type gets independent narrowing
		bitmap, typeWindow := usersets.GetSubjectBitmapWithin(
			objectType, objectID, relation, ref.Type, ref.Relation, window)
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		checks = append(checks, RelationCheck{
			ObjectType: ref.Type,
			ObjectIDs:  bitmap,
			Relation:   ref.Relation,
			Window:     typeWindow,
		})
	}

	if len(checks) == 0 {
		// Use directWindow; even though false, this "false" result is based on this window.
		return false, directWindow, nil
	}

	// Check if subject is in the union of all userset subjects
	probe.RecursiveCheck(subjectType, subjectID, objectType, objectID, relation, len(visited)+1)
	result, err := graph.CheckUnion(ctx, subjectType, subjectID, checks, visitedMapToSlice(visited))
	if err != nil {
		probe.Error(err)
		return false, window, err
	}

	// Use DependentSets to determine which specific subjects mattered,
	// then look up when those subjects were added to our local usersets.
	resultMin := result.Window.Min()

	for _, dep := range result.DependentSets {
		// Find the check that corresponds to this dependent set
		for _, chk := range checks {
			if chk.ObjectType == dep.ObjectType && chk.Relation == dep.Relation {
				// dep.ObjectIDs is nil for "not found" (all from input),
				// or a specific bitmap for "found"
				objectsToCheck := dep.ObjectIDs
				if objectsToCheck == nil {
					objectsToCheck = chk.ObjectIDs
				}

				// Look up when each of these subjects was added to our local userset
				iter := objectsToCheck.Iterator()
				for iter.HasNext() {
					objID := schema.ID(iter.Next())
					_, tupleWindow := usersets.ContainsUsersetSubjectWithin(
						objectType, objectID, relation,
						dep.ObjectType, objID, dep.Relation,
						window)
					if tupleWindow.Min() > resultMin {
						resultMin = tupleWindow.Min()
					}
				}
				break
			}
		}
	}

	// Also consider direct check window if the answer is negative
	// With negative unions, all sets are interdependent
	// On positive, we only need a single match, so just use the result min

	if !result.Found && directWindow.Min() > resultMin {
		resultMin = directWindow.Min()
	}

	// TODO: if the window is invalid, we would need to wait & retry or abort

	return result.Found, NewSnapshotWindow(resultMin, result.Window.Max()), nil
}

// checkArrow evaluates a tuple-to-userset (arrow) operation.
func checkArrow(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	probe CheckProbe,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	arrow *schema.TupleToUserset,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	probe.ArrowTraversal(arrow.TuplesetRelation, arrow.ComputedUsersetRelation)

	sch := usersets.Schema()

	// Get the target type from the schema
	ot := sch.Types[objectType]
	tuplesetRel, ok := ot.Relations[arrow.TuplesetRelation]
	if !ok {
		err := fmt.Errorf("unknown tupleset relation %s on type %s", arrow.TuplesetRelation, objectType)
		probe.Error(err)
		return false, window, err
	}

	// Determine the target types from the tupleset relation's Direct userset
	targetTypes := tuplesetRel.DirectTargetTypes()
	if targetTypes == nil {
		err := fmt.Errorf("relation %s has no Direct userset with target types", arrow.TuplesetRelation)
		probe.Error(err)
		return false, window, err
	}

	// Build checks for each target type with independent windows
	// Each type reads data from the original window, getting its own narrowed window
	var checks []RelationCheck

	for _, ref := range targetTypes {
		// TODO: we can know what the type of set is in this set,
		// and if it's not the same as the subjectType we can skip the check

		// Skip userset references for tupleset relations - they should be direct
		if ref.Relation != "" {
			continue
		}

		// Verify the target type has the computed relation in the schema
		targetOT, ok := sch.Types[ref.Type]
		if !ok {
			continue
		}
		if _, ok := targetOT.Relations[arrow.ComputedUsersetRelation]; !ok {
			continue
		}

		// Read data for this type from original window - each type gets independent narrowing
		bitmap, typeWindow := usersets.GetSubjectBitmapWithin(
			objectType, objectID, arrow.TuplesetRelation, ref.Type, "", window)
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		checks = append(checks, RelationCheck{
			ObjectType: ref.Type,
			ObjectIDs:  bitmap,
			Relation:   arrow.ComputedUsersetRelation,
			Window:     typeWindow,
		})
	}

	if len(checks) == 0 {
		// No targets found - return with initial window constrained to replicated time
		return false, window, nil
	}

	// Check if subject has relation on any of the target objects
	probe.RecursiveCheck(subjectType, subjectID, "", 0, arrow.ComputedUsersetRelation, len(visited)+1)
	result, err := graph.CheckUnion(ctx, subjectType, subjectID, checks, visitedMapToSlice(visited))
	if err != nil {
		probe.Error(err)
		return false, window, err
	}

	// Use DependentSets to determine which specific targets mattered,
	// then look up when those tuples were added to our local tupleset.
	resultMin := result.Window.Min()

	for _, dep := range result.DependentSets {
		// Find the check that corresponds to this dependent set
		for _, chk := range checks {
			if chk.ObjectType == dep.ObjectType && chk.Relation == dep.Relation {
				// dep.ObjectIDs is nil for "not found" (all from input),
				// or a specific bitmap for "found"
				objectsToCheck := dep.ObjectIDs
				if objectsToCheck == nil {
					objectsToCheck = chk.ObjectIDs
				}

				// Look up when each of these targets was added to our local tupleset
				// For arrows, these are direct subjects (not usersets)
				iter := objectsToCheck.Iterator()
				for iter.HasNext() {
					targetID := schema.ID(iter.Next())
					_, tupleWindow := usersets.ContainsDirectWithin(
						objectType, objectID, arrow.TuplesetRelation,
						dep.ObjectType, targetID,
						window)
					if tupleWindow.Min() > resultMin {
						resultMin = tupleWindow.Min()
					}
				}
				break
			}
		}
	}

	return result.Found, NewSnapshotWindow(resultMin, result.Window.Max()), nil
}

// visitedMapToSlice converts visited map to slice for interface calls.
func visitedMapToSlice(visited map[VisitedKey]bool) []VisitedKey {
	result := make([]VisitedKey, 0, len(visited))
	for k := range visited {
		result = append(result, k)
	}
	return result
}
