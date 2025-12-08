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

// check is the core walk algorithm. It's a standalone function that takes
// Graph for recursion and MultiversionUsersets for data access.
func check(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// Validate inputs
	sch := usersets.Schema()
	ot, ok := sch.Types[objectType]
	if !ok {
		return false, window, fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return false, window, fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	// Convert visited slice to map for O(1) lookups
	visitedMap := make(map[VisitedKey]bool, len(visited))
	for _, v := range visited {
		visitedMap[v] = true
	}

	return checkRelation(ctx, graph, usersets, subjectType, subjectID, objectType, objectID, rel, visitedMap, window)
}

// checkRelation evaluates a relation definition against the graph.
func checkRelation(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	rel *schema.Relation,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	key := VisitedKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		return false, window, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// Evaluate each userset in the union
	for _, us := range rel.Usersets {
		ok, newWindow, err := checkUserset(ctx, graph, usersets, subjectType, subjectID, objectType, objectID, rel, us, visited, window)
		if err != nil {
			return false, window, err
		}
		window = newWindow
		if ok {
			return true, window, nil
		}
	}

	return false, window, nil
}

// checkUserset evaluates a single userset definition.
func checkUserset(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
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
		return checkDirectAndUserset(ctx, graph, usersets, subjectType, subjectID, objectType, objectID, rel.Name, us.This, visited, window)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object (no routing needed - same object)
		sch := usersets.Schema()
		ot := sch.Types[objectType]
		computedRel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			return false, window, fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
		}
		return checkRelation(ctx, graph, usersets, subjectType, subjectID, objectType, objectID, computedRel, visited, window)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return checkArrow(ctx, graph, usersets, subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited, window)

	default:
		return false, window, fmt.Errorf("invalid userset: no operation specified")
	}
}

// checkDirectAndUserset checks both direct tuple membership and userset tuple membership.
func checkDirectAndUserset(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	targetTypes []schema.SubjectRef,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	// Check direct membership first
	found, _, newWindow := usersets.ContainsDirectWithin(objectType, objectID, relation, subjectType, subjectID, window)
	window = newWindow
	if found {
		return true, window, nil
	}

	// Check userset subjects via Graph recursion
	// e.g., document:100#viewer@group:1#member means "members of group 1 are viewers"
	// So we need to check if subjectType:subjectID is in group:1#member
	var foundErr error
	found, window = usersets.ForEachUsersetSubjectWithin(objectType, objectID, relation, targetTypes, window,
		func(usSubjectType schema.TypeName, usSubjectRelation schema.RelationName, usSubjectID schema.ID, w SnapshotWindow) (bool, SnapshotWindow) {
			// Check if subjectType:subjectID is in usSubjectType:usSubjectID#usSubjectRelation
			// This recurses through the Graph interface (may be local or distributed)
			visitedSlice := visitedMapToSlice(visited)
			ok, newW, err := graph.Check(ctx, subjectType, subjectID, usSubjectType, usSubjectID, usSubjectRelation, w, visitedSlice)
			if err != nil {
				foundErr = err
				return true, newW // stop
			}
			return ok, newW // stop if found
		})

	if foundErr != nil {
		return false, window, foundErr
	}
	return found, window, nil
}

// checkArrow evaluates a tuple-to-userset (arrow) operation.
func checkArrow(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	arrow *schema.TupleToUserset,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	sch := usersets.Schema()

	// Get the target type from the schema
	ot := sch.Types[objectType]
	tuplesetRel, ok := ot.Relations[arrow.TuplesetRelation]
	if !ok {
		return false, window, fmt.Errorf("unknown tupleset relation %s on type %s", arrow.TuplesetRelation, objectType)
	}

	// Determine the target types from the tupleset relation's Direct userset
	targetTypes := tuplesetRel.DirectTargetTypes()
	if targetTypes == nil {
		return false, window, fmt.Errorf("relation %s has no Direct userset with target types", arrow.TuplesetRelation)
	}

	// Check each target type
	for _, ref := range targetTypes {
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

		// Get the bitmap of target objects
		bitmap, _, newWindow := usersets.GetSubjectBitmapWithin(objectType, objectID, arrow.TuplesetRelation, ref.Type, "", window)
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}
		window = newWindow

		// Batch check all target objects via Graph interface
		visitedSlice := visitedMapToSlice(visited)
		ok, window, err := graph.BatchCheck(ctx, subjectType, subjectID, ref.Type, bitmap, arrow.ComputedUsersetRelation, window, visitedSlice)
		if err != nil {
			return false, window, err
		}
		if ok {
			return true, window, nil
		}
	}

	return false, window, nil
}

// visitedMapToSlice converts visited map to slice for interface calls.
func visitedMapToSlice(visited map[VisitedKey]bool) []VisitedKey {
	result := make([]VisitedKey, 0, len(visited))
	for k := range visited {
		result = append(result, k)
	}
	return result
}
