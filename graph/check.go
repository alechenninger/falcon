package graph

import (
	"context"
	"fmt"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// Check determines if the given subject has the specified relation on the
// object. It evaluates the relation according to the schema, which may involve:
//   - Direct tuple membership
//   - Userset tuple membership (e.g., group members)
//   - Union with other relations (computed usersets)
//   - Arrow traversal to other objects (tuple to userset)
//
// Returns (allowed, usedTime, error) where usedTime is the effective time of the
// snapshot that was used to answer the query. This can be used by callers
// to understand the consistency of the result.
func (g *Graph) Check(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, store.StoreTime, error) {
	return g.CheckCtx(context.Background(), subjectType, subjectID, objectType, objectID, relation)
}

// CheckCtx is like Check but accepts a context for cancellation and tracing.
func (g *Graph) CheckCtx(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, store.StoreTime, error) {
	// Start with max = replicated time, min = 0 (unknown)
	window := NewSnapshotWindow(0, g.replicatedTime.Load())
	ok, resultWindow, err := g.CheckAt(ctx, subjectType, subjectID, objectType, objectID, relation, &window)
	return ok, resultWindow.Max(), err
}

// CheckAt is like Check but allows specifying an initial snapshot window.
// This is used for distributed queries where the window may already be
// constrained by other shards.
//
// If window is nil, uses a fresh window starting at the current replicated time.
// Returns the narrowed window that was actually used for the query.
func (g *Graph) CheckAt(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, window *SnapshotWindow) (bool, SnapshotWindow, error) {
	return g.CheckAtWithVisited(ctx, subjectType, subjectID, objectType, objectID, relation, window, nil)
}

// CheckAtWithVisited is like CheckAt but accepts a list of already-visited nodes
// for cycle detection. This is used when receiving distributed queries that have
// already traversed other shards.
func (g *Graph) CheckAtWithVisited(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, window *SnapshotWindow, visited []VisitedKey) (bool, SnapshotWindow, error) {
	// Initialize window if not provided
	if window == nil {
		w := NewSnapshotWindow(0, g.replicatedTime.Load())
		window = &w
	}

	// Validate inputs
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return false, *window, fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return false, *window, fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	// Convert visited slice to map for O(1) lookups
	visitedMap := make(map[VisitedKey]bool, len(visited))
	for _, v := range visited {
		visitedMap[v] = true
	}

	return g.checkRelation(ctx, subjectType, subjectID, objectType, objectID, rel, visitedMap, *window)
}

// routeCheckRelation checks a relation on a potentially remote object.
// If a router is configured and returns a remote client, the check is dispatched
// via RPC. Otherwise, it falls back to local evaluation.
//
// This is used by checkArrow and checkDirectAndUserset when they need to
// evaluate relations on different objects (which may live on different shards).
func (g *Graph) routeCheckRelation(
	ctx context.Context,
	subjectType schema.TypeName,
	subjectID schema.ID,
	objectType schema.TypeName,
	objectID schema.ID,
	relation schema.RelationName,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	// If no router configured, evaluate locally (preserving visited map)
	if g.router == nil {
		return g.checkRelationByName(ctx, subjectType, subjectID, objectType, objectID, relation, visited, window)
	}

	// Route to get the appropriate client
	client, err := g.router.Route(ctx, objectType, objectID)
	if err != nil {
		return false, window, fmt.Errorf("routing failed: %w", err)
	}

	// Convert visited map to slice for the client call
	visitedSlice := make([]VisitedKey, 0, len(visited))
	for k := range visited {
		visitedSlice = append(visitedSlice, k)
	}

	// Dispatch to the client (local or remote)
	// The client receives the full visited list for cycle detection.
	return client.CheckRelation(ctx, subjectType, subjectID, objectType, objectID, relation, window, visitedSlice)
}

// checkRelationByName is like checkRelation but takes a relation name instead of object.
// It looks up the relation from the schema.
func (g *Graph) checkRelationByName(
	ctx context.Context,
	subjectType schema.TypeName,
	subjectID schema.ID,
	objectType schema.TypeName,
	objectID schema.ID,
	relation schema.RelationName,
	visited map[VisitedKey]bool,
	window SnapshotWindow,
) (bool, SnapshotWindow, error) {
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return false, window, fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return false, window, fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}
	return g.checkRelation(ctx, subjectType, subjectID, objectType, objectID, rel, visited, window)
}

// checkRelation evaluates a relation definition against the graph.
func (g *Graph) checkRelation(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, visited map[VisitedKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	key := VisitedKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		return false, window, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// Evaluate each userset in the union
	for _, us := range rel.Usersets {
		ok, newWindow, err := g.checkUserset(ctx, subjectType, subjectID, objectType, objectID, rel, us, visited, window)
		if err != nil {
			return false, window, err
		}
		// Always use the narrowed window going forward
		window = newWindow
		if ok {
			return true, window, nil
		}
	}

	return false, window, nil
}

// checkUserset evaluates a single userset definition.
func (g *Graph) checkUserset(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, us schema.Userset, visited map[VisitedKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	switch {
	case len(us.This) > 0:
		// Direct tuple membership for the current relation (including userset subjects)
		return g.checkDirectAndUserset(ctx, subjectType, subjectID, objectType, objectID, rel.Name, us.This, visited, window)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object (no routing needed - same object)
		ot := g.schema.Types[objectType]
		computedRel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			return false, window, fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
		}
		return g.checkRelation(ctx, subjectType, subjectID, objectType, objectID, computedRel, visited, window)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return g.checkArrow(ctx, subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited, window)

	default:
		return false, window, fmt.Errorf("invalid userset: no operation specified")
	}
}

// checkDirectAndUserset checks both direct tuple membership and userset tuple membership.
//
// For direct: checks if subject_type:subject_id is directly in the relation
// For userset: finds tuples like object#relation@other_type:other_id#other_relation
// and recursively checks if subject is in that userset.
//
// targetTypes specifies the allowed subject types from the Direct userset.
func (g *Graph) checkDirectAndUserset(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, targetTypes []schema.SubjectRef, visited map[VisitedKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	// Check direct membership first
	ok, newWindow := g.checkDirect(subjectType, subjectID, objectType, objectID, relation, window)
	window = newWindow
	if ok {
		return true, window, nil
	}

	// Check userset subjects
	// e.g., document:100#viewer@group:1#member means "members of group 1 are viewers"
	// So we need to check if subjectType:subjectID is in group:1#member
	var foundErr error
	found, window := g.forEachUsersetSubjectWithin(objectType, objectID, relation, targetTypes, window, func(usSubjectType schema.TypeName, usSubjectRelation schema.RelationName, usSubjectID schema.ID, w SnapshotWindow) (bool, SnapshotWindow) {
		// Check if subjectType:subjectID is in usSubjectType:usSubjectID#usSubjectRelation
		// e.g., is user:alice in group:1#member?
		// This may route to a different shard if the userset object is remote.
		ok, newW, err := g.routeCheckRelation(ctx, subjectType, subjectID, usSubjectType, usSubjectID, usSubjectRelation, visited, w)
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

// checkDirect checks if the subject has a direct tuple for the relation.
// Returns (found, narrowedWindow).
func (g *Graph) checkDirect(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, window SnapshotWindow) (bool, SnapshotWindow) {
	return g.containsSubjectWithin(objectType, objectID, relation, subjectType, "", subjectID, window)
}

// checkArrow evaluates a tuple-to-userset (arrow) operation.
// It follows the tupleset relation to find target objects, then checks the
// computed userset relation on each target.
func (g *Graph) checkArrow(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, arrow *schema.TupleToUserset, visited map[VisitedKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	// Get the target type from the schema
	ot := g.schema.Types[objectType]
	tuplesetRel, ok := ot.Relations[arrow.TuplesetRelation]
	if !ok {
		return false, window, fmt.Errorf("unknown tupleset relation %s on type %s", arrow.TuplesetRelation, objectType)
	}

	// Determine the target types from the tupleset relation's Direct userset
	targetTypes := tuplesetRel.DirectTargetTypes()
	if targetTypes == nil {
		return false, window, fmt.Errorf("relation %s has no Direct userset with target types", arrow.TuplesetRelation)
	}

	// For each target type, get the targets and check the relation
	// Note: tupleset relations (like "parent") typically have direct subjects
	// (Relation=""), pointing to objects rather than usersets
	for _, ref := range targetTypes {
		// Skip userset references for tupleset relations - they should be direct
		if ref.Relation != "" {
			continue
		}

		// Verify the target type has the computed relation in the schema
		targetOT, ok := g.schema.Types[ref.Type]
		if !ok {
			continue // Skip unknown target types
		}
		if _, ok := targetOT.Relations[arrow.ComputedUsersetRelation]; !ok {
			continue // This target type doesn't have the relation, try next
		}

		// Iterate over targets and check the relation
		// The targets may be on different shards, so we route each check.
		var foundErr error
		found, newWindow := g.forEachSubjectWithin(objectType, objectID, arrow.TuplesetRelation, ref.Type, "", window, func(targetID schema.ID, w SnapshotWindow) (bool, SnapshotWindow) {
			ok, newW, err := g.routeCheckRelation(ctx, subjectType, subjectID, ref.Type, targetID, arrow.ComputedUsersetRelation, visited, w)
			if err != nil {
				foundErr = err
				return true, newW // stop
			}
			return ok, newW // stop if found
		})
		window = newWindow

		if foundErr != nil {
			return false, window, foundErr
		}
		if found {
			return true, window, nil
		}
	}

	return false, window, nil
}

// containsSubjectWithin checks if a subject is in the set within the given snapshot window.
// It picks the latest state <= window.Max and narrows the window accordingly.
// Returns (found, narrowedWindow).
func (g *Graph) containsSubjectWithin(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName, subjectID schema.ID, window SnapshotWindow) (bool, SnapshotWindow) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	vs, ok := g.tuples[key]
	if !ok {
		return false, window
	}

	found, stateTime := vs.ContainsWithin(subjectID, window.Max())
	if stateTime == 0 {
		return false, window
	}
	return found, window.NarrowMin(stateTime)
}

// forEachSubjectWithin iterates over subject IDs within the given snapshot window.
// For each subject, fn is called with the current window; fn returns (stop, newWindow).
// The window narrows as we examine tuples.
// Returns (stopped, finalWindow).
func (g *Graph) forEachSubjectWithin(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName, window SnapshotWindow, fn func(schema.ID, SnapshotWindow) (bool, SnapshotWindow)) (bool, SnapshotWindow) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	vs, ok := g.tuples[key]
	if !ok {
		return false, window
	}

	snapshot, stateTime := vs.SnapshotWithin(window.Max())
	if stateTime == 0 {
		return false, window
	}
	window = window.NarrowMin(stateTime)

	it := snapshot.Iterator()
	for it.HasNext() {
		stop, newWindow := fn(schema.ID(it.Next()), window)
		window = newWindow
		if stop {
			return true, window
		}
	}
	return false, window
}

// forEachUsersetSubjectWithin iterates over userset subjects within the given snapshot window.
// For each (subjectType, subjectRelation, subjectID) tuple, fn is called with the current window.
// The window narrows as we examine tuples.
// Returns (stopped, finalWindow).
//
// targetTypes specifies the allowed subject types from the Direct userset.
func (g *Graph) forEachUsersetSubjectWithin(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, targetTypes []schema.SubjectRef, window SnapshotWindow, fn func(schema.TypeName, schema.RelationName, schema.ID, SnapshotWindow) (bool, SnapshotWindow)) (bool, SnapshotWindow) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// For each allowed subject reference (type + relation), do a direct lookup
	for _, ref := range targetTypes {
		// Skip direct subjects (no relation) - those are handled separately
		if ref.Relation == "" {
			continue
		}

		key := tupleKey{
			ObjectType:      objectType,
			ObjectID:        objectID,
			Relation:        relation,
			SubjectType:     ref.Type,
			SubjectRelation: ref.Relation,
		}

		vs, ok := g.tuples[key]
		if !ok {
			continue
		}

		snapshot, stateTime := vs.SnapshotWithin(window.Max())
		if stateTime == 0 || snapshot.IsEmpty() {
			continue
		}
		window = window.NarrowMin(stateTime)

		it := snapshot.Iterator()
		for it.HasNext() {
			stop, newWindow := fn(ref.Type, ref.Relation, schema.ID(it.Next()), window)
			window = newWindow
			if stop {
				return true, window
			}
		}
	}

	return false, window
}
