package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestGraph wraps a LocalGraph with a MemoryStore for testing.
// It provides synchronization via SignalingObserver to wait for
// writes to be applied.
type TestGraph struct {
	*LocalGraph
	store    *store.MemoryStore
	observer *SignalingObserver
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTestGraph creates a LocalGraph with a MemoryStore for testing.
// Call Close() when done to stop the subscription.
func NewTestGraph(s *schema.Schema) *TestGraph {
	ctx, cancel := context.WithCancel(context.Background())
	ms := store.NewMemoryStore()
	observer := NewSignalingObserver()

	g := NewLocalGraph(s, ms, ms).WithUsersetsObserver(observer)

	tg := &TestGraph{
		LocalGraph: g,
		store:      ms,
		observer:   observer,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start the graph (hydrate + subscribe) in background
	go func() {
		_ = g.Start(ctx)
	}()

	// Wait for the subscription to be ready before returning
	observer.WaitReady()

	return tg
}

// WriteTuple validates and writes a tuple, waiting for it to be replicated.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) WriteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	if err := tg.LocalGraph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	s := tg.Schema()
	if err := tg.store.WriteTuple(ctx, store.Tuple{
		ObjectType:      s.GetTypeID(objectType),
		ObjectID:        objectID,
		Relation:        s.GetRelationID(objectType, relation),
		SubjectType:     s.GetTypeID(subjectType),
		SubjectID:       subjectID,
		SubjectRelation: s.GetRelationID(subjectType, subjectRelation),
	}); err != nil {
		return err
	}

	// Wait for this write to be applied via the observer
	t, _ := tg.store.CurrentTime(ctx)
	tg.observer.WaitForTime(t)
	return nil
}

// DeleteTuple validates and removes a tuple, waiting for it to be replicated.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) DeleteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	if err := tg.LocalGraph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	s := tg.Schema()
	if err := tg.store.DeleteTuple(ctx, store.Tuple{
		ObjectType:      s.GetTypeID(objectType),
		ObjectID:        objectID,
		Relation:        s.GetRelationID(objectType, relation),
		SubjectType:     s.GetTypeID(subjectType),
		SubjectID:       subjectID,
		SubjectRelation: s.GetRelationID(subjectType, subjectRelation),
	}); err != nil {
		return err
	}

	// Wait for this delete to be applied via the observer
	t, _ := tg.store.CurrentTime(ctx)
	tg.observer.WaitForTime(t)
	return nil
}

// Store returns the underlying MemoryStore.
func (tg *TestGraph) Store() *store.MemoryStore {
	return tg.store
}

// Check is a convenience wrapper that calls Check with MaxSnapshotWindow and nil visited.
// Most tests don't care about the snapshot window or cycle detection setup.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) Check(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, store.StoreTime, error) {
	s := tg.Schema()
	ok, window, err := tg.LocalGraph.Check(ctx,
		s.GetTypeID(subjectType), subjectID,
		s.GetTypeID(objectType), objectID,
		s.GetRelationID(objectType, relation),
		MaxSnapshotWindow, nil)
	return ok, window.Max(), err
}

// CheckAt is a test helper that checks with a specific snapshot window.
// This is used for MVCC tests that need to verify historical state.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) CheckAt(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, window *SnapshotWindow) (bool, SnapshotWindow, error) {
	s := tg.Schema()
	return tg.LocalGraph.Check(ctx,
		s.GetTypeID(subjectType), subjectID,
		s.GetTypeID(objectType), objectID,
		s.GetRelationID(objectType, relation),
		*window, nil)
}

// TruncateHistory is a test helper for MVCC garbage collection tests.
func (tg *TestGraph) TruncateHistory(minTime store.StoreTime) {
	tg.LocalGraph.usersets.TruncateHistory(minTime)
}

// Close stops the subscription.
func (tg *TestGraph) Close() {
	tg.cancel()
}
