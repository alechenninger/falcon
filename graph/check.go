package graph

import (
	"fmt"

	"github.com/alechenninger/falcon/schema"
)

// Check determines if the given subject has the specified relation on the
// object. It evaluates the relation according to the schema, which may involve:
//   - Direct tuple membership
//   - Userset tuple membership (e.g., group members)
//   - Union with other relations (computed usersets)
//   - Arrow traversal to other objects (tuple to userset)
//
// Returns (allowed, usedLSN, error) where usedLSN is the effective LSN of the
// snapshot that was used to answer the query. This can be used by callers
// to understand the consistency of the result.
func (g *Graph) Check(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, LSN, error) {
	// Start with max = replicated LSN, min = 0 (unknown)
	window := NewSnapshotWindow(0, g.replicatedLSN.Load())
	ok, resultWindow, err := g.CheckAt(subjectType, subjectID, objectType, objectID, relation, &window)
	return ok, resultWindow.Max(), err
}

// CheckAt is like Check but allows specifying an initial snapshot window.
// This is used for distributed queries where the window may already be
// constrained by other shards.
//
// If window is nil, uses a fresh window starting at the current replicated LSN.
// Returns the narrowed window that was actually used for the query.
func (g *Graph) CheckAt(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, window *SnapshotWindow) (bool, SnapshotWindow, error) {
	// Initialize window if not provided
	if window == nil {
		w := NewSnapshotWindow(0, g.replicatedLSN.Load())
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

	// Track visited (object, relation) pairs to prevent infinite recursion
	visited := make(map[visitKey]bool)

	return g.checkRelation(subjectType, subjectID, objectType, objectID, rel, visited, *window)
}

// visitKey tracks visited nodes to prevent infinite recursion in cyclic graphs.
type visitKey struct {
	objectType schema.TypeName
	objectID   schema.ID
	relation   schema.RelationName
}

// checkRelation evaluates a relation definition against the graph.
func (g *Graph) checkRelation(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, visited map[visitKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	key := visitKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		return false, window, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// Evaluate each userset in the union
	for _, us := range rel.Usersets {
		ok, newWindow, err := g.checkUserset(subjectType, subjectID, objectType, objectID, rel, us, visited, window)
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
func (g *Graph) checkUserset(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, us schema.Userset, visited map[visitKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
	switch {
	case len(us.This) > 0:
		// Direct tuple membership for the current relation (including userset subjects)
		return g.checkDirectAndUserset(subjectType, subjectID, objectType, objectID, rel.Name, us.This, visited, window)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object
		ot := g.schema.Types[objectType]
		computedRel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			return false, window, fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
		}
		return g.checkRelation(subjectType, subjectID, objectType, objectID, computedRel, visited, window)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return g.checkArrow(subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited, window)

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
func (g *Graph) checkDirectAndUserset(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, targetTypes []schema.SubjectRef, visited map[visitKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
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
		usOT, ok := g.schema.Types[usSubjectType]
		if !ok {
			return false, w // continue
		}
		usRel, ok := usOT.Relations[usSubjectRelation]
		if !ok {
			return false, w // continue
		}

		// Check if subjectType:subjectID is in usSubjectType:usSubjectID#usSubjectRelation
		// e.g., is user:alice in group:1#member?
		ok, newW, err := g.checkRelation(subjectType, subjectID, usSubjectType, usSubjectID, usRel, visited, w)
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
func (g *Graph) checkArrow(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, arrow *schema.TupleToUserset, visited map[visitKey]bool, window SnapshotWindow) (bool, SnapshotWindow, error) {
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

		targetOT, ok := g.schema.Types[ref.Type]
		if !ok {
			continue // Skip unknown target types
		}

		targetRel, ok := targetOT.Relations[arrow.ComputedUsersetRelation]
		if !ok {
			continue // This target type doesn't have the relation, try next
		}

		// Iterate over targets and check the relation
		var foundErr error
		found, newWindow := g.forEachSubjectWithin(objectType, objectID, arrow.TuplesetRelation, ref.Type, "", window, func(targetID schema.ID, w SnapshotWindow) (bool, SnapshotWindow) {
			ok, newW, err := g.checkRelation(subjectType, subjectID, ref.Type, targetID, targetRel, visited, w)
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

	// Pick the latest usable state within the window
	stateLSN := vs.StateLSNWithin(window.Max())
	if stateLSN == 0 {
		// No state available within window
		return false, window
	}

	// Narrow the window: our min is now at least this state's LSN
	window = window.NarrowMin(stateLSN)

	// Check membership at this state
	if stateLSN == vs.HeadLSN() {
		return vs.Contains(subjectID), window
	}
	return vs.ContainsAt(subjectID, stateLSN), window
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

	// Pick the latest usable state within the window
	stateLSN := vs.StateLSNWithin(window.Max())
	if stateLSN == 0 {
		return false, window
	}

	// Narrow the window
	window = window.NarrowMin(stateLSN)

	// Get the appropriate bitmap
	var it interface {
		HasNext() bool
		Next() uint32
	}
	if stateLSN == vs.HeadLSN() {
		it = vs.Head().Iterator()
	} else {
		snapshot := vs.SnapshotAt(stateLSN)
		it = snapshot.Iterator()
	}

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

		// Pick the latest usable state within the window
		stateLSN := vs.StateLSNWithin(window.Max())
		if stateLSN == 0 {
			continue
		}

		// Narrow the window
		window = window.NarrowMin(stateLSN)

		// Get the appropriate bitmap
		var it interface {
			HasNext() bool
			Next() uint32
		}
		if stateLSN == vs.HeadLSN() {
			if vs.IsEmpty() {
				continue
			}
			it = vs.Head().Iterator()
		} else {
			snapshot := vs.SnapshotAt(stateLSN)
			if snapshot.IsEmpty() {
				continue
			}
			it = snapshot.Iterator()
		}

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
