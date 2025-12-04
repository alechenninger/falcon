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
func (g *Graph) Check(subjectType string, subjectID uint32, objectType string, objectID uint32, relation string) (bool, error) {
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

	return g.checkRelation(subjectType, subjectID, objectType, objectID, rel, visited)
}

// visitKey tracks visited nodes to prevent infinite recursion in cyclic graphs.
type visitKey struct {
	objectType string
	objectID   uint32
	relation   string
}

// checkRelation evaluates a relation definition against the graph.
func (g *Graph) checkRelation(subjectType string, subjectID uint32, objectType string, objectID uint32, rel *schema.Relation, visited map[visitKey]bool) (bool, error) {
	key := visitKey{objectType, objectID, rel.Name}
	if visited[key] {
		// Already visiting this node - cycle detected, return false to avoid infinite loop
		return false, nil
	}
	visited[key] = true
	defer func() { visited[key] = false }()

	// If no usersets defined, check direct and userset membership
	if len(rel.Usersets) == 0 {
		return g.checkDirectAndUserset(subjectType, subjectID, objectType, objectID, rel.Name, visited)
	}

	// Evaluate each userset in the union
	for _, us := range rel.Usersets {
		ok, err := g.checkUserset(subjectType, subjectID, objectType, objectID, rel.Name, us, visited)
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
func (g *Graph) checkUserset(subjectType string, subjectID uint32, objectType string, objectID uint32, relationName string, us schema.Userset, visited map[visitKey]bool) (bool, error) {
	switch {
	case us.This:
		// Direct tuple membership for the current relation (including userset subjects)
		return g.checkDirectAndUserset(subjectType, subjectID, objectType, objectID, relationName, visited)

	case us.ComputedRelation != "":
		// Reference to another relation on the same object
		ot := g.schema.Types[objectType]
		rel, ok := ot.Relations[us.ComputedRelation]
		if !ok {
			return false, fmt.Errorf("unknown computed relation %s on type %s", us.ComputedRelation, objectType)
		}
		return g.checkRelation(subjectType, subjectID, objectType, objectID, rel, visited)

	case us.TupleToUserset != nil:
		// Arrow traversal: follow relation to find targets, then check relation on targets
		return g.checkArrow(subjectType, subjectID, objectType, objectID, us.TupleToUserset, visited)

	default:
		return false, fmt.Errorf("invalid userset: no operation specified")
	}
}

// checkDirectAndUserset checks both direct tuple membership and userset tuple membership.
//
// For direct: checks if subject_type:subject_id is directly in the relation
// For userset: finds tuples like object#relation@other_type:other_id#other_relation
// and recursively checks if subject is in that userset.
func (g *Graph) checkDirectAndUserset(subjectType string, subjectID uint32, objectType string, objectID uint32, relation string, visited map[visitKey]bool) (bool, error) {
	// Check direct membership first
	if g.checkDirect(subjectType, subjectID, objectType, objectID, relation) {
		return true, nil
	}

	// Check userset subjects
	// e.g., document:100#viewer@group:1#member means "members of group 1 are viewers"
	// So we need to check if subjectType:subjectID is in group:1#member
	usersets := g.GetUsersetSubjects(objectType, objectID, relation)
	for _, us := range usersets {
		// For each userset tuple (e.g., @group:1#member), check if the subject
		// satisfies that userset
		for _, usSubjectID := range us.SubjectIDs {
			// Check if subjectType:subjectID is in us.SubjectType:usSubjectID#us.SubjectRelation
			// e.g., is user:alice in group:1#member?
			usOT, ok := g.schema.Types[us.SubjectType]
			if !ok {
				continue
			}
			usRel, ok := usOT.Relations[us.SubjectRelation]
			if !ok {
				continue
			}

			ok, err := g.checkRelation(subjectType, subjectID, us.SubjectType, usSubjectID, usRel, visited)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
	}

	return false, nil
}

// checkDirect checks if the subject has a direct tuple for the relation.
func (g *Graph) checkDirect(subjectType string, subjectID uint32, objectType string, objectID uint32, relation string) bool {
	bm := g.GetDirectSubjects(objectType, objectID, relation, subjectType)
	if bm == nil {
		return false
	}
	return bm.Contains(subjectID)
}

// checkArrow evaluates a tuple-to-userset (arrow) operation.
// It follows the tupleset relation to find target objects, then checks the
// computed userset relation on each target.
func (g *Graph) checkArrow(subjectType string, subjectID uint32, objectType string, objectID uint32, arrow *schema.TupleToUserset, visited map[visitKey]bool) (bool, error) {
	// Get the target type from the schema
	ot := g.schema.Types[objectType]
	tuplesetRel, ok := ot.Relations[arrow.TuplesetRelation]
	if !ok {
		return false, fmt.Errorf("unknown tupleset relation %s on type %s", arrow.TuplesetRelation, objectType)
	}

	// Determine the target types from the relation's TargetTypes
	if len(tuplesetRel.TargetTypes) == 0 {
		return false, fmt.Errorf("relation %s has no target types defined", arrow.TuplesetRelation)
	}

	// For each target type, get the targets and check the relation
	// Note: tupleset relations (like "parent") typically have direct subjects
	// (Relation=""), pointing to objects rather than usersets
	for _, ref := range tuplesetRel.TargetTypes {
		// Skip userset references for tupleset relations - they should be direct
		if ref.Relation != "" {
			continue
		}

		targets := g.GetDirectObjects(objectType, objectID, arrow.TuplesetRelation, ref.Type)
		if len(targets) == 0 {
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

		// Check the relation on each target object
		for _, targetID := range targets {
			ok, err := g.checkRelation(subjectType, subjectID, ref.Type, targetID, targetRel, visited)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
	}

	return false, nil
}
