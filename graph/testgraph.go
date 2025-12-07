package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestGraph wraps a Graph with a MemoryStore for testing.
// It uses the observer pattern for synchronization - writes to the store
// are applied via the Graph's subscription, and the SignalingObserver
// allows waiting for changes to be applied.
type TestGraph struct {
	*Graph
	store    *store.MemoryStore
	observer *SignalingObserver
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTestGraph creates a Graph subscribed to a MemoryStore.
// Call Close() when done to stop the subscription.
func NewTestGraph(s *schema.Schema) *TestGraph {
	ctx, cancel := context.WithCancel(context.Background())
	ms := store.NewMemoryStore()
	observer := NewSignalingObserver()

	g := New(s).WithObserver(observer)

	tg := &TestGraph{
		Graph:    g,
		store:    ms,
		observer: observer,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start subscription in a goroutine using the standard Graph.Subscribe
	go func() {
		_ = g.Subscribe(ctx, ms)
	}()

	// Wait for the subscription to be ready before returning
	observer.WaitReady()

	return tg
}

// WriteTuple validates and writes a tuple, waiting for it to be replicated.
func (tg *TestGraph) WriteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	if err := tg.Graph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	if err := tg.store.WriteTuple(ctx, store.Tuple{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
	}); err != nil {
		return err
	}

	// Wait for this write to be applied via the observer
	t, _ := tg.store.CurrentTime(ctx)
	tg.observer.WaitForTime(t)
	return nil
}

// DeleteTuple validates and removes a tuple, waiting for it to be replicated.
func (tg *TestGraph) DeleteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	if err := tg.Graph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	if err := tg.store.DeleteTuple(ctx, store.Tuple{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
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

// Close stops the subscription.
func (tg *TestGraph) Close() {
	tg.cancel()
}
