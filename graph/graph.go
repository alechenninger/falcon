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
	schema     *schema.Schema
	store      store.Store   // optional; nil = in-memory only (for testing without persistence)
	observer   GraphObserver // optional; defaults to NoOpGraphObserver
	router     Router        // optional; for distributed check dispatch (single-object routing)
	dispatcher *Dispatcher   // optional; for batch dispatch (scatter-gather)

	mu     sync.RWMutex
	tuples map[tupleKey]*versionedSet

	// replicatedTime is the latest time we've seen from the change stream.
	// This represents the point in the log that we know our in-memory state
	// is up to date with. This is a pointer so that graphs derived via
	// With* methods share the same atomic value.
	replicatedTime *store.AtomicStoreTime
}

// New creates a new Graph with the given schema.
func New(s *schema.Schema) *Graph {
	return &Graph{
		schema:         s,
		observer:       NoOpGraphObserver{},
		tuples:         make(map[tupleKey]*versionedSet),
		replicatedTime: &store.AtomicStoreTime{},
	}
}

// WithStore returns a copy of the Graph configured to persist to the given Store.
// If store is nil, returns the original Graph unchanged.
func (g *Graph) WithStore(s store.Store) *Graph {
	if s == nil {
		return g
	}
	return &Graph{
		schema:         g.schema,
		store:          s,
		observer:       g.observer,
		router:         g.router,
		dispatcher:     g.dispatcher,
		tuples:         g.tuples,
		replicatedTime: g.replicatedTime,
	}
}

// WithObserver returns a copy of the Graph configured to use the given observer.
// The observer is called at key points during graph operations for instrumentation.
func (g *Graph) WithObserver(obs GraphObserver) *Graph {
	if obs == nil {
		obs = NoOpGraphObserver{}
	}
	return &Graph{
		schema:         g.schema,
		store:          g.store,
		observer:       obs,
		router:         g.router,
		dispatcher:     g.dispatcher,
		tuples:         g.tuples,
		replicatedTime: g.replicatedTime,
	}
}

// WithRouter returns a copy of the Graph configured to use the given router
// for dispatching cross-object checks. This is used in distributed deployments
// where different objects may live on different shards.
//
// If router is nil, all checks are performed locally (single-node mode).
func (g *Graph) WithRouter(r Router) *Graph {
	return &Graph{
		schema:         g.schema,
		store:          g.store,
		observer:       g.observer,
		router:         r,
		dispatcher:     g.dispatcher,
		tuples:         g.tuples,
		replicatedTime: g.replicatedTime,
	}
}

// WithDispatcher returns a copy of the Graph configured to use the given dispatcher
// for batch check dispatch. The dispatcher handles scatter-gather for checking
// relations across multiple objects in parallel.
//
// If dispatcher is nil, batch checks fall back to serial local evaluation.
func (g *Graph) WithDispatcher(d *Dispatcher) *Graph {
	return &Graph{
		schema:         g.schema,
		store:          g.store,
		observer:       g.observer,
		router:         g.router,
		dispatcher:     d,
		tuples:         g.tuples,
		replicatedTime: g.replicatedTime,
	}
}

// Schema returns the schema for this graph.
func (g *Graph) Schema() *schema.Schema {
	return g.schema
}

// ReplicatedTime returns the latest time that has been applied to the in-memory state.
func (g *Graph) ReplicatedTime() store.StoreTime {
	return g.replicatedTime.Load()
}

// Subscribe starts consuming changes from the given ChangeStream.
// This should be called after initial hydration to receive ongoing updates.
// The function blocks until the context is canceled or an error occurs.
func (g *Graph) Subscribe(ctx context.Context, stream store.ChangeStream) error {
	afterTime := g.replicatedTime.Load()
	changes, errCh := stream.Subscribe(ctx, afterTime)

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
		g.applyAdd(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.Time)
	case store.OpDelete:
		g.applyRemove(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.Time)
	}
	g.replicatedTime.Store(change.Time)
	probe.Applied(change.Time)
}

// applyAdd adds a subject to the versioned set for the given tuple key.
func (g *Graph) applyAdd(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, t store.StoreTime) {
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
		vs = newVersionedSet(t)
		g.tuples[key] = vs
	}
	vs.Add(subjectID, t)
}

// applyRemove removes a subject from the versioned set for the given tuple key.
func (g *Graph) applyRemove(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, t store.StoreTime) {
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
		vs.Remove(subjectID, t)
	}
}

// Hydrate loads all tuples from the Router (or Store) into memory.
// This should be called on startup before subscribing to the ChangeStream.
//
// If a Router is configured, Hydrate uses Router.LoadAll which filters tuples
// to only those owned by this node. Otherwise, it falls back to Store.LoadAll.
//
// Returns an error if neither Router nor Store is configured.
func (g *Graph) Hydrate(ctx context.Context) error {
	var iter store.TupleIterator
	var err error

	if g.router != nil {
		iter, err = g.router.LoadAll(ctx)
	} else if g.store != nil {
		iter, err = g.store.LoadAll(ctx)
	} else {
		return fmt.Errorf("no router or store configured")
	}

	if err != nil {
		return fmt.Errorf("failed to load tuples: %w", err)
	}
	defer iter.Close()

	g.mu.Lock()
	defer g.mu.Unlock()

	for iter.Next() {
		t := iter.Tuple()
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

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to iterate tuples: %w", err)
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

// TruncateHistory removes undo entries older than the given time from all
// versioned sets. This is used for garbage collection.
func (g *Graph) TruncateHistory(minTime store.StoreTime) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, vs := range g.tuples {
		vs.Truncate(minTime)
	}
}
