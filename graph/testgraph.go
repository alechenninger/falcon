package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestGraph wraps a Graph with a MemoryStore and LocalRouter for testing.
// It mirrors the production setup where the Graph subscribes to a Router,
// which wraps the underlying Store and ChangeStream.
//
// The observer pattern is used for synchronization - writes to the store
// are applied via the Graph's subscription to the Router, and the
// SignalingObserver allows waiting for changes to be applied.
type TestGraph struct {
	*Graph
	store    *store.MemoryStore
	router   *LocalRouter
	observer *SignalingObserver
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTestGraph creates a Graph with a full production-like setup:
// MemoryStore -> LocalRouter -> Graph subscription.
// Call Close() when done to stop the subscription.
func NewTestGraph(s *schema.Schema) *TestGraph {
	ctx, cancel := context.WithCancel(context.Background())
	ms := store.NewMemoryStore()
	observer := NewSignalingObserver()

	g := New(s).WithObserver(observer)

	// Create LocalGraphClient and Router - MemoryStore serves as both Store and ChangeStream
	client := NewLocalGraphClient(g)
	router := NewLocalRouter(client, ms, ms)

	// Wire the Graph with the Router
	g = g.WithRouter(router)

	tg := &TestGraph{
		Graph:    g,
		store:    ms,
		router:   router,
		observer: observer,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Subscribe to the Router (not directly to MemoryStore) - mirrors production
	go func() {
		_ = g.Subscribe(ctx, router)
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

// Router returns the LocalRouter.
func (tg *TestGraph) Router() *LocalRouter {
	return tg.router
}

// Close stops the subscription.
func (tg *TestGraph) Close() {
	tg.cancel()
}
