package graph

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestGraph wraps a Graph with a MemoryStore for testing.
// It uses the same subscription pattern as production but provides
// synchronous writes by waiting for replication.
type TestGraph struct {
	*Graph
	store  *store.MemoryStore
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	cond    *sync.Cond
	lastLSN store.LSN
}

// NewTestGraph creates a Graph subscribed to a MemoryStore.
// Call Close() when done to stop the subscription.
func NewTestGraph(s *schema.Schema) *TestGraph {
	ctx, cancel := context.WithCancel(context.Background())
	ms := store.NewMemoryStore()

	// Subscribe BEFORE starting the goroutine to avoid race
	changes, errCh := ms.Subscribe(ctx, 0)

	tg := &TestGraph{
		Graph:  New(s),
		store:  ms,
		ctx:    ctx,
		cancel: cancel,
	}
	tg.cond = sync.NewCond(&tg.mu)

	// Start subscription goroutine
	go tg.runSubscription(changes, errCh)

	return tg
}

func (tg *TestGraph) runSubscription(changes <-chan store.Change, errCh <-chan error) {
	for {
		select {
		case change, ok := <-changes:
			if !ok {
				return
			}
			tg.Graph.ApplyChange(change)

			// Update lastLSN and signal waiters
			tg.mu.Lock()
			tg.lastLSN = change.LSN
			tg.cond.Broadcast()
			tg.mu.Unlock()

		case <-errCh:
			return
		case <-tg.ctx.Done():
			return
		}
	}
}

// waitForLSN blocks until the graph has applied at least the given LSN.
func (tg *TestGraph) waitForLSN(lsn store.LSN) {
	tg.mu.Lock()
	defer tg.mu.Unlock()

	for tg.lastLSN < lsn {
		tg.cond.Wait()
	}
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

	// Wait for this write to be applied
	lsn, _ := tg.store.CurrentLSN(ctx)
	tg.waitForLSN(lsn)
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

	// Wait for this delete to be applied
	lsn, _ := tg.store.CurrentLSN(ctx)
	tg.waitForLSN(lsn)
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
