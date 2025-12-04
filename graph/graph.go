// Package graph implements the in-memory tuple store for the authorization
// graph using roaring bitmaps for efficient set operations.
package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// tupleKey uniquely identifies a set of subjects for a given object, relation,
// subject type, and subject relation.
type tupleKey struct {
	ObjectType      string
	ObjectID        uint32
	Relation        string
	SubjectType     string
	SubjectRelation string // Empty for direct subjects
}

// Graph stores the authorization tuples using roaring bitmaps. Each tuple
// (object_type, object_id, relation, subject_type, subject_relation) maps to
// a bitmap of subject IDs.
//
// Currently all subjects are stored as uint32 IDs. External ID mapping will
// be added in a later level.
//
// If a Store is provided, writes are persisted before being applied to memory.
type Graph struct {
	schema *schema.Schema
	store  store.Store // optional; nil = in-memory only

	mu     sync.RWMutex
	tuples map[tupleKey]*roaring.Bitmap
}

// New creates a new Graph with the given schema. The graph is in-memory only;
// use WithStore to add persistence.
func New(s *schema.Schema) *Graph {
	return &Graph{
		schema: s,
		tuples: make(map[tupleKey]*roaring.Bitmap),
	}
}

// WithStore returns a copy of the Graph configured to persist to the given Store.
// If store is nil, returns the original Graph unchanged.
func (g *Graph) WithStore(s store.Store) *Graph {
	if s == nil {
		return g
	}
	return &Graph{
		schema: g.schema,
		store:  s,
		tuples: g.tuples,
	}
}

// Schema returns the schema for this graph.
func (g *Graph) Schema() *schema.Schema {
	return g.schema
}

// AddTuple adds a tuple to the graph.
//
// For direct subjects (e.g., document:100#viewer@user:1):
//
//	AddTuple(ctx, "document", 100, "viewer", "user", 1, "")
//
// For userset subjects (e.g., document:100#viewer@group:1#member):
//
//	AddTuple(ctx, "document", 100, "viewer", "group", 1, "member")
//
// If a Store is configured, the tuple is persisted before being added to memory.
func (g *Graph) AddTuple(ctx context.Context, objectType string, objectID uint32, relation string, subjectType string, subjectID uint32, subjectRelation string) error {
	if err := g.validateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	// Persist first if we have a store
	if g.store != nil {
		if err := g.store.WriteTuple(ctx, store.Tuple{
			ObjectType:      objectType,
			ObjectID:        objectID,
			Relation:        relation,
			SubjectType:     subjectType,
			SubjectID:       subjectID,
			SubjectRelation: subjectRelation,
		}); err != nil {
			return fmt.Errorf("failed to persist tuple: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	bm, ok := g.tuples[key]
	if !ok {
		bm = roaring.New()
		g.tuples[key] = bm
	}
	bm.Add(subjectID)
	return nil
}

// RemoveTuple removes a tuple from the graph.
//
// If a Store is configured, the tuple is deleted from the store before being
// removed from memory.
func (g *Graph) RemoveTuple(ctx context.Context, objectType string, objectID uint32, relation string, subjectType string, subjectID uint32, subjectRelation string) error {
	if err := g.validateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	// Delete from store first if we have one
	if g.store != nil {
		if err := g.store.DeleteTuple(ctx, store.Tuple{
			ObjectType:      objectType,
			ObjectID:        objectID,
			Relation:        relation,
			SubjectType:     subjectType,
			SubjectID:       subjectID,
			SubjectRelation: subjectRelation,
		}); err != nil {
			return fmt.Errorf("failed to delete tuple from store: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	if bm, ok := g.tuples[key]; ok {
		bm.Remove(subjectID)
	}
	return nil
}

// Hydrate loads all tuples from the Store into memory. This should be called
// on startup before serving requests.
//
// Returns an error if no Store is configured.
func (g *Graph) Hydrate(ctx context.Context) error {
	if g.store == nil {
		return fmt.Errorf("no store configured")
	}

	tuples, err := g.store.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load tuples: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for _, t := range tuples {
		key := tupleKey{
			ObjectType:      t.ObjectType,
			ObjectID:        t.ObjectID,
			Relation:        t.Relation,
			SubjectType:     t.SubjectType,
			SubjectRelation: t.SubjectRelation,
		}

		bm, ok := g.tuples[key]
		if !ok {
			bm = roaring.New()
			g.tuples[key] = bm
		}
		bm.Add(t.SubjectID)
	}

	return nil
}

// GetDirectSubjects returns the bitmap of subjects of the given type that have
// a direct tuple (subjectRelation="") for the given object and relation.
// Returns nil if no tuples exist.
//
// The returned bitmap should not be modified by the caller.
func (g *Graph) GetDirectSubjects(objectType string, objectID uint32, relation string, subjectType string) *roaring.Bitmap {
	return g.GetSubjects(objectType, objectID, relation, subjectType, "")
}

// GetSubjects returns the bitmap of subjects for the given tuple key.
// Returns nil if no tuples exist.
//
// The returned bitmap should not be modified by the caller.
func (g *Graph) GetSubjects(objectType string, objectID uint32, relation string, subjectType string, subjectRelation string) *roaring.Bitmap {
	g.mu.RLock()
	defer g.mu.RUnlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	return g.tuples[key]
}

// GetDirectObjects returns all object IDs of the given subject type that have a direct
// tuple (subjectRelation="") for the given relation. This is useful for arrow traversals
// where we need to find all objects referenced by a relation (e.g., all parent folders).
func (g *Graph) GetDirectObjects(objectType string, objectID uint32, relation string, subjectType string) []uint32 {
	bm := g.GetSubjects(objectType, objectID, relation, subjectType, "")
	if bm == nil || bm.IsEmpty() {
		return nil
	}
	return bm.ToArray()
}

// UsersetSubject represents a userset subject tuple for a given object/relation.
type UsersetSubject struct {
	SubjectType     string
	SubjectRelation string
	SubjectIDs      []uint32
}

// GetUsersetSubjects returns all userset subject tuples for the given object
// and relation. This uses the schema's TargetTypes to determine exactly which
// (subjectType, subjectRelation) combinations are allowed, doing direct map
// lookups instead of iterating over all tuples.
//
// Complexity: O(number of TargetTypes) instead of O(total tuples).
func (g *Graph) GetUsersetSubjects(objectType string, objectID uint32, relation string) []UsersetSubject {
	// Get the relation definition to find allowed target types
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return nil
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return nil
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var results []UsersetSubject

	// For each allowed subject reference (type + relation), do a direct lookup
	for _, ref := range rel.TargetTypes {
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

		if bm, ok := g.tuples[key]; ok && !bm.IsEmpty() {
			results = append(results, UsersetSubject{
				SubjectType:     ref.Type,
				SubjectRelation: ref.Relation,
				SubjectIDs:      bm.ToArray(),
			})
		}
	}

	return results
}

// validateTuple checks that the object type, relation, and subject reference
// are valid according to the schema.
func (g *Graph) validateTuple(objectType, relation, subjectType, subjectRelation string) error {
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	// Check that the subject type + relation is in TargetTypes
	for _, ref := range rel.TargetTypes {
		if ref.Type == subjectType && ref.Relation == subjectRelation {
			return nil
		}
	}

	if subjectRelation == "" {
		return fmt.Errorf("subject type %s is not allowed for %s#%s", subjectType, objectType, relation)
	}
	return fmt.Errorf("subject %s#%s is not allowed for %s#%s", subjectType, subjectRelation, objectType, relation)
}
