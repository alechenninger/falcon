package store

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/schema"
)

// MemoryStore implements Store and ChangeStream for testing.
// Writes immediately emit changes to subscribers with sequential LSNs.
type MemoryStore struct {
	mu          sync.RWMutex
	tuples      map[tupleKey]struct{}
	nextLSN     LSN
	subscribers []chan Change
}

// tupleKey is the map key for deduplication.
type tupleKey struct {
	ObjectType      schema.TypeName
	ObjectID        schema.ID
	Relation        schema.RelationName
	SubjectType     schema.TypeName
	SubjectID       schema.ID
	SubjectRelation schema.RelationName
}

func toKey(t Tuple) tupleKey {
	return tupleKey{
		ObjectType:      t.ObjectType,
		ObjectID:        t.ObjectID,
		Relation:        t.Relation,
		SubjectType:     t.SubjectType,
		SubjectID:       t.SubjectID,
		SubjectRelation: t.SubjectRelation,
	}
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tuples:  make(map[tupleKey]struct{}),
		nextLSN: 1, // Start at 1 so 0 means "from beginning"
	}
}

// WriteTuple adds a tuple to the store and emits a change to subscribers.
func (s *MemoryStore) WriteTuple(ctx context.Context, t Tuple) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := toKey(t)
	if _, exists := s.tuples[key]; exists {
		// Already exists, no-op
		return nil
	}

	lsn := s.nextLSN
	s.nextLSN++
	s.tuples[key] = struct{}{}

	change := Change{LSN: lsn, Op: OpInsert, Tuple: t}
	for _, ch := range s.subscribers {
		select {
		case ch <- change:
		default:
			// Subscriber not keeping up, drop the change
		}
	}

	return nil
}

// DeleteTuple removes a tuple from the store and emits a change to subscribers.
func (s *MemoryStore) DeleteTuple(ctx context.Context, t Tuple) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := toKey(t)
	if _, exists := s.tuples[key]; !exists {
		// Doesn't exist, no-op
		return nil
	}

	lsn := s.nextLSN
	s.nextLSN++
	delete(s.tuples, key)

	change := Change{LSN: lsn, Op: OpDelete, Tuple: t}
	for _, ch := range s.subscribers {
		select {
		case ch <- change:
		default:
		}
	}

	return nil
}

// LoadAll returns all tuples currently in the store.
func (s *MemoryStore) LoadAll(ctx context.Context) ([]Tuple, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Tuple, 0, len(s.tuples))
	for key := range s.tuples {
		result = append(result, keyToTuple(key))
	}
	return result, nil
}

func keyToTuple(k tupleKey) Tuple {
	return Tuple(k)
}

// Close is a no-op for the in-memory store.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscriber channels
	for _, ch := range s.subscribers {
		close(ch)
	}
	s.subscribers = nil
	return nil
}

// Subscribe returns a channel that receives changes after the given LSN.
// The channel is closed when Close() is called or the context is canceled.
func (s *MemoryStore) Subscribe(ctx context.Context, afterLSN LSN) (<-chan Change, <-chan error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Buffer some changes to avoid blocking writers
	ch := make(chan Change, 100)
	errCh := make(chan error, 1)
	s.subscribers = append(s.subscribers, ch)

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		// Remove this subscriber
		for i, sub := range s.subscribers {
			if sub == ch {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}()

	return ch, errCh
}

// CurrentLSN returns the latest LSN (the next LSN minus 1).
func (s *MemoryStore) CurrentLSN(ctx context.Context) (LSN, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nextLSN == 1 {
		return 0, nil
	}
	return s.nextLSN - 1, nil
}

// Compile-time interface checks
var (
	_ Store        = (*MemoryStore)(nil)
	_ ChangeStream = (*MemoryStore)(nil)
)
