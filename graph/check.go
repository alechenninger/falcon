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
// Returns true if the subject has the relation, false otherwise.
// This uses the current (HEAD) state of the graph.
func (g *Graph) Check(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, error) {
	return g.CheckAt(subjectType, subjectID, objectType, objectID, relation, nil)
}

// CheckAt is like Check but evaluates at a specific LSN for MVCC snapshot reads.
// If lsn is nil, uses the current (HEAD) state.
func (g *Graph) CheckAt(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, lsn *LSN) (bool, error) {
	// Validate inputs
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return false, fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return false, fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	// Track visited (object, relation) pairs to prevent infinite recursion
	visited := make(map[visitKey]bool)

	return g.checkRelation(subjectType, subjectID, objectType, objectID, rel, visited, lsn)
}

// visitKey tracks visited nodes to prevent infinite recursion in cyclic graphs.
type visitKey struct {
	objectType schema.TypeName
	objectID   schema.ID
	relation   schema.RelationName
}

// checkRelation evaluates a relation definition against the graph.
func (g *Graph) checkRelation(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, visited map[visitKey]bool, lsn *LSN) (bool, error) {
	key := visitKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		return false, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// Evaluate each userset in the union
	for _, us := range rel.Usersets {
		ok, err := g.checkUserset(subjectType, subjectID, objectType, objectID, rel, us, visited, lsn)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

// checkUserset evaluates a single userset definition.
func (g *Graph) checkUserset(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, rel *schema.Relation, us schema.Userset, visited map[visitKey]bool, lsn *LSN) (bool, error) {
	switch {
	case len(us.This) > 0:
		// Direct tuple membership for the current relation (including userset subjects)
		return g.checkDirectAndUserset(subjectType, subjectID, objectType, objectID, rel.Name, us.This, visited, lsn)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object
		ot := g.schema.Types[objectType]
		computedRel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			return false, fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
		}
		return g.checkRelation(subjectType, subjectID, objectType, objectID, computedRel, visited, lsn)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return g.checkArrow(subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited, lsn)

	default:
		return false, fmt.Errorf("invalid userset: no operation specified")
	}
}

// checkDirectAndUserset checks both direct tuple membership and userset tuple membership.
//
// For direct: checks if subject_type:subject_id is directly in the relation
// For userset: finds tuples like object#relation@other_type:other_id#other_relation
// and recursively checks if subject is in that userset.
//
// targetTypes specifies the allowed subject types from the Direct userset.
func (g *Graph) checkDirectAndUserset(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, targetTypes []schema.SubjectRef, visited map[visitKey]bool, lsn *LSN) (bool, error) {
	// Check direct membership first
	if g.checkDirect(subjectType, subjectID, objectType, objectID, relation, lsn) {
		return true, nil
	}

	// Check userset subjects
	// e.g., document:100#viewer@group:1#member means "members of group 1 are viewers"
	// So we need to check if subjectType:subjectID is in group:1#member
	var foundErr error
	found := g.forEachUsersetSubjectAt(objectType, objectID, relation, targetTypes, lsn, func(usSubjectType schema.TypeName, usSubjectRelation schema.RelationName, usSubjectID schema.ID) bool {
		usOT, ok := g.schema.Types[usSubjectType]
		if !ok {
			return false // continue
		}
		usRel, ok := usOT.Relations[usSubjectRelation]
		if !ok {
			return false // continue
		}

		// Check if subjectType:subjectID is in usSubjectType:usSubjectID#usSubjectRelation
		// e.g., is user:alice in group:1#member?
		ok, err := g.checkRelation(subjectType, subjectID, usSubjectType, usSubjectID, usRel, visited, lsn)
		if err != nil {
			foundErr = err
			return true // stop
		}
		return ok // stop if found
	})

	if foundErr != nil {
		return false, foundErr
	}
	return found, nil
}

// checkDirect checks if the subject has a direct tuple for the relation.
func (g *Graph) checkDirect(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, lsn *LSN) bool {
	return g.containsSubjectAt(objectType, objectID, relation, subjectType, "", subjectID, lsn)
}

// checkArrow evaluates a tuple-to-userset (arrow) operation.
// It follows the tupleset relation to find target objects, then checks the
// computed userset relation on each target.
func (g *Graph) checkArrow(subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, arrow *schema.TupleToUserset, visited map[visitKey]bool, lsn *LSN) (bool, error) {
	// Get the target type from the schema
	ot := g.schema.Types[objectType]
	tuplesetRel, ok := ot.Relations[arrow.TuplesetRelation]
	if !ok {
		return false, fmt.Errorf("unknown tupleset relation %s on type %s", arrow.TuplesetRelation, objectType)
	}

	// Determine the target types from the tupleset relation's Direct userset
	targetTypes := tuplesetRel.DirectTargetTypes()
	if targetTypes == nil {
		return false, fmt.Errorf("relation %s has no Direct userset with target types", arrow.TuplesetRelation)
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
		found := g.forEachSubjectAt(objectType, objectID, arrow.TuplesetRelation, ref.Type, "", lsn, func(targetID schema.ID) bool {
			ok, err := g.checkRelation(subjectType, subjectID, ref.Type, targetID, targetRel, visited, lsn)
			if err != nil {
				foundErr = err
				return true // stop
			}
			return ok // stop if found
		})

		if foundErr != nil {
			return false, foundErr
		}
		if found {
			return true, nil
		}
	}

	return false, nil
}

// containsSubjectAt checks if a subject is in the set at the given LSN.
// If lsn is nil, uses HEAD.
func (g *Graph) containsSubjectAt(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName, subjectID schema.ID, lsn *LSN) bool {
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
		return false
	}

	if lsn == nil {
		return vs.Contains(subjectID)
	}
	return vs.ContainsAt(subjectID, *lsn)
}

// forEachSubjectAt iterates over subject IDs at the given LSN, calling fn for each.
// If fn returns true, iteration stops and forEachSubjectAt returns true.
// If lsn is nil, uses HEAD.
func (g *Graph) forEachSubjectAt(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName, lsn *LSN, fn func(schema.ID) bool) bool {
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
		return false
	}

	if lsn == nil {
		it := vs.Head().Iterator()
		for it.HasNext() {
			if fn(schema.ID(it.Next())) {
				return true
			}
		}
	} else {
		// For historical queries, we need to use SnapshotAt
		snapshot := vs.SnapshotAt(*lsn)
		it := snapshot.Iterator()
		for it.HasNext() {
			if fn(schema.ID(it.Next())) {
				return true
			}
		}
	}
	return false
}

// forEachUsersetSubjectAt iterates over userset subjects at the given LSN.
// For each (subjectType, subjectRelation, subjectID) tuple, fn is called.
// If fn returns true, iteration stops and forEachUsersetSubjectAt returns true.
// If lsn is nil, uses HEAD.
//
// targetTypes specifies the allowed subject types from the Direct userset.
func (g *Graph) forEachUsersetSubjectAt(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, targetTypes []schema.SubjectRef, lsn *LSN, fn func(schema.TypeName, schema.RelationName, schema.ID) bool) bool {
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

		if lsn == nil {
			if vs.IsEmpty() {
				continue
			}
			it := vs.Head().Iterator()
			for it.HasNext() {
				if fn(ref.Type, ref.Relation, schema.ID(it.Next())) {
					return true
				}
			}
		} else {
			snapshot := vs.SnapshotAt(*lsn)
			if snapshot.IsEmpty() {
				continue
			}
			it := snapshot.Iterator()
			for it.HasNext() {
				if fn(ref.Type, ref.Relation, schema.ID(it.Next())) {
					return true
				}
			}
		}
	}

	return false
}
