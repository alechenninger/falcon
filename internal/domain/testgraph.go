package domain

import (
	"context"

	
)

// TestStore is an interface for stores that can be used with TestGraph.
// It combines Store and ChangeStream with CurrentTime for test synchronization.
type TestStore interface {
	Store
	ChangeStream
	CurrentTime(ctx context.Context) (StoreTime, error)
}

// TestGraph wraps a LocalGraph with a store for testing.
// It provides synchronization via SignalingObserver to wait for
// writes to be applied.
type TestGraph struct {
	*LocalGraph
	store    TestStore
	observer *SignalingObserver
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTestGraph creates a LocalGraph with the provided store for testing.
// Call Close() when done to stop the subscription.
func NewTestGraph(s *Schema, store TestStore) *TestGraph {
	ctx, cancel := context.WithCancel(context.Background())
	observer := NewSignalingObserver()

	g := NewLocalGraph(s, store, store)
	g.SetUsersetsObserver(observer)

	tg := &TestGraph{
		LocalGraph: g,
		store:      store,
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
func (tg *TestGraph) WriteTuple(ctx context.Context, objectType TypeName, objectID ID, relation RelationName, subjectType TypeName, subjectID ID, subjectRelation RelationName) error {
	if err := tg.LocalGraph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	s := tg.Schema()
	tuple := Tuple{
		ObjectType:      s.GetTypeID(objectType),
		ObjectID:        objectID,
		Relation:        s.GetRelationID(objectType, relation),
		SubjectType:     s.GetTypeID(subjectType),
		SubjectID:       subjectID,
		SubjectRelation: s.GetRelationID(subjectType, subjectRelation),
	}

	tx, err := tg.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Wait for this write to be applied via the observer
	t, _ := tg.store.CurrentTime(ctx)
	tg.observer.WaitForTime(t)
	return nil
}

// DeleteTuple validates and removes a tuple, waiting for it to be replicated.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) DeleteTuple(ctx context.Context, objectType TypeName, objectID ID, relation RelationName, subjectType TypeName, subjectID ID, subjectRelation RelationName) error {
	if err := tg.LocalGraph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
		return err
	}

	s := tg.Schema()
	tuple := Tuple{
		ObjectType:      s.GetTypeID(objectType),
		ObjectID:        objectID,
		Relation:        s.GetRelationID(objectType, relation),
		SubjectType:     s.GetTypeID(subjectType),
		SubjectID:       subjectID,
		SubjectRelation: s.GetRelationID(subjectType, subjectRelation),
	}

	tx, err := tg.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.Write(ctx, []Mutation{{Op: OpDelete, Tuple: tuple}}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Wait for this delete to be applied via the observer
	t, _ := tg.store.CurrentTime(ctx)
	tg.observer.WaitForTime(t)
	return nil
}

// Store returns the underlying store.
func (tg *TestGraph) Store() TestStore {
	return tg.store
}

// Check is a convenience wrapper that calls Check with MaxSnapshotWindow and nil visited.
// Most tests don't care about the snapshot window or cycle detection setup.
// Takes string names for convenience and converts to IDs internally.
func (tg *TestGraph) Check(ctx context.Context, subjectType TypeName, subjectID ID, objectType TypeName, objectID ID, relation RelationName) (bool, StoreTime, error) {
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
func (tg *TestGraph) CheckAt(ctx context.Context, subjectType TypeName, subjectID ID, objectType TypeName, objectID ID, relation RelationName, window *SnapshotWindow) (bool, SnapshotWindow, error) {
	s := tg.Schema()
	return tg.LocalGraph.Check(ctx,
		s.GetTypeID(subjectType), subjectID,
		s.GetTypeID(objectType), objectID,
		s.GetRelationID(objectType, relation),
		*window, nil)
}

// Close stops the subscription.
func (tg *TestGraph) Close() {
	tg.cancel()
}
