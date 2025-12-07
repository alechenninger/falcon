// Package graph implements the in-memory tuple store for the authorization
// graph using roaring bitmaps for efficient set operations.
package graph

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// tupleKey uniquely identifies a set of subjects for a given object, relation,
// subject type, and subject relation.
type tupleKey struct {
	ObjectType      schema.TypeName
	ObjectID        schema.ID
	Relation        schema.RelationName
	SubjectType     schema.TypeName
	SubjectRelation schema.RelationName // Empty for direct subjects
}

// Graph stores the authorization tuples using roaring bitmaps with MVCC support.
// Each tuple (object_type, object_id, relation, subject_type, subject_relation)
// maps to a versioned set of subject IDs.
//
// Currently all subjects are stored as uint32 IDs. External ID mapping will
// be added in a later level.
//
// State is updated exclusively via the ChangeStream - writes go to the Store
// and are applied to memory only when received from the stream.
type Graph struct {
	schema   *schema.Schema
	store    store.Store   // optional; nil = in-memory only (for testing without persistence)
	observer GraphObserver // optional; defaults to NoOpGraphObserver

	mu     sync.RWMutex
	tuples map[tupleKey]*versionedSet

	// replicatedLSN is the latest LSN we've seen from the change stream.
	// This represents the point in the log that we know our in-memory state
	// is up to date with.
	replicatedLSN atomic.Uint64
}

// New creates a new Graph with the given schema.
func New(s *schema.Schema) *Graph {
	return &Graph{
		schema:   s,
		observer: NoOpGraphObserver{},
		tuples:   make(map[tupleKey]*versionedSet),
	}
}

// WithStore returns a copy of the Graph configured to persist to the given Store.
// If store is nil, returns the original Graph unchanged.
func (g *Graph) WithStore(s store.Store) *Graph {
	if s == nil {
		return g
	}
	return &Graph{
		schema:   g.schema,
		store:    s,
		observer: g.observer,
		tuples:   g.tuples,
	}
}

// WithObserver returns a copy of the Graph configured to use the given observer.
// The observer is called at key points during graph operations for instrumentation.
func (g *Graph) WithObserver(obs GraphObserver) *Graph {
	if obs == nil {
		obs = NoOpGraphObserver{}
	}
	return &Graph{
		schema:   g.schema,
		store:    g.store,
		observer: obs,
		tuples:   g.tuples,
	}
}

// Schema returns the schema for this graph.
func (g *Graph) Schema() *schema.Schema {
	return g.schema
}

// ReplicatedLSN returns the latest LSN that has been applied to the in-memory state.
func (g *Graph) ReplicatedLSN() LSN {
	return g.replicatedLSN.Load()
}

// Subscribe starts consuming changes from the given ChangeStream.
// This should be called after initial hydration to receive ongoing updates.
// The function blocks until the context is canceled or an error occurs.
func (g *Graph) Subscribe(ctx context.Context, stream store.ChangeStream) error {
	afterLSN := g.replicatedLSN.Load()
	changes, errCh := stream.Subscribe(ctx, afterLSN)

	// Signal that we're ready to receive changes
	g.observer.SubscribeReady(ctx)

	for {
		select {
		case change, ok := <-changes:
			if !ok {
				return nil // Channel closed
			}
			g.applyChange(ctx, change)
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("change stream error: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyChange applies a single change from the ChangeStream to the in-memory state.
// This is called internally by Subscribe.
func (g *Graph) applyChange(ctx context.Context, change store.Change) {
	_, probe := g.observer.ApplyChangeStarted(ctx, change)
	defer probe.End()

	t := change.Tuple
	switch change.Op {
	case store.OpInsert:
		g.applyAdd(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.LSN)
	case store.OpDelete:
		g.applyRemove(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.LSN)
	}
	g.replicatedLSN.Store(change.LSN)
	probe.Applied(change.LSN)
}

// applyAdd adds a subject to the versioned set for the given tuple key.
func (g *Graph) applyAdd(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, lsn LSN) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	vs, ok := g.tuples[key]
	if !ok {
		vs = newVersionedSet(lsn)
		g.tuples[key] = vs
	}
	vs.Add(subjectID, lsn)
}

// applyRemove removes a subject from the versioned set for the given tuple key.
func (g *Graph) applyRemove(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, lsn LSN) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	if vs, ok := g.tuples[key]; ok {
		vs.Remove(subjectID, lsn)
	}
}

// Hydrate loads all tuples from the Store into memory. This should be called
// on startup before subscribing to the ChangeStream.
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

		vs, ok := g.tuples[key]
		if !ok {
			vs = newVersionedSet(0)
			g.tuples[key] = vs
		}
		vs.Add(t.SubjectID, 0)
	}

	return nil
}

// GetDirectSubjects returns the bitmap of subjects of the given type that have
// a direct tuple (subjectRelation="") for the given object and relation.
// Returns nil if no tuples exist.
//
// The returned bitmap should not be modified by the caller.
func (g *Graph) GetDirectSubjects(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName) *roaring.Bitmap {
	return g.GetSubjects(objectType, objectID, relation, subjectType, "")
}

// GetSubjects returns the bitmap of subjects for the given tuple key.
// Returns nil if no tuples exist.
//
// The returned bitmap should not be modified by the caller.
func (g *Graph) GetSubjects(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) *roaring.Bitmap {
	g.mu.RLock()
	defer g.mu.RUnlock()

	key := tupleKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	if vs, ok := g.tuples[key]; ok {
		return vs.Head()
	}
	return nil
}

// UsersetSubject represents a userset subject tuple for a given object/relation.
type UsersetSubject struct {
	SubjectType     schema.TypeName
	SubjectRelation schema.RelationName
	SubjectIDs      *roaring.Bitmap
}

// GetUsersetSubjects returns all userset subject tuples for the given object
// and relation. This uses the Direct userset's TargetTypes to determine exactly
// which (subjectType, subjectRelation) combinations are allowed, doing direct
// map lookups instead of iterating over all tuples.
//
// Complexity: O(number of TargetTypes) instead of O(total tuples).
func (g *Graph) GetUsersetSubjects(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) []UsersetSubject {
	// Get the relation definition to find allowed target types
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return nil
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return nil
	}

	targetTypes := rel.DirectTargetTypes()
	if targetTypes == nil {
		return nil
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	var results []UsersetSubject

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

		if vs, ok := g.tuples[key]; ok && !vs.IsEmpty() {
			results = append(results, UsersetSubject{
				SubjectType:     ref.Type,
				SubjectRelation: ref.Relation,
				SubjectIDs:      vs.Head(),
			})
		}
	}

	return results
}

// ValidateTuple checks that the object type, relation, and subject reference
// are valid according to the schema. Only relations with a Direct userset
// can have tuples written to them.
func (g *Graph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	ot, ok := g.schema.Types[objectType]
	if !ok {
		return fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	// Check that the subject type + relation is in the Direct userset's TargetTypes
	targetTypes := rel.DirectTargetTypes()
	if targetTypes == nil {
		return fmt.Errorf("relation %s#%s does not allow direct tuples", objectType, relation)
	}

	for _, ref := range targetTypes {
		if ref.Type == subjectType && ref.Relation == subjectRelation {
			return nil
		}
	}

	if subjectRelation == "" {
		return fmt.Errorf("subject type %s is not allowed for %s#%s", subjectType, objectType, relation)
	}
	return fmt.Errorf("subject %s#%s is not allowed for %s#%s", subjectType, subjectRelation, objectType, relation)
}

// TruncateHistory removes undo entries older than the given LSN from all
// versioned sets. This is used for garbage collection.
func (g *Graph) TruncateHistory(minLSN LSN) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, vs := range g.tuples {
		vs.Truncate(minLSN)
	}
}
