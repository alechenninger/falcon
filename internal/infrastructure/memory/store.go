package memory

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/internal/domain"
)

// Store implements domain.Store and domain.ChangeStream for testing.
// Writes immediately emit changes to subscribers with sequential times.
type Store struct {
	mu          sync.RWMutex
	tuples      map[tupleKey]struct{}
	nextTime    domain.StoreTime
	subscribers []chan domain.Change
}

// tupleKey is the map key for deduplication.
type tupleKey struct {
	ObjectType      domain.TypeID
	ObjectID        domain.ID
	Relation        domain.RelationID
	SubjectType     domain.TypeID
	SubjectID       domain.ID
	SubjectRelation domain.RelationID
}

func toKey(t domain.Tuple) tupleKey {
	return tupleKey{
		ObjectType:      t.ObjectType,
		ObjectID:        t.ObjectID,
		Relation:        t.Relation,
		SubjectType:     t.SubjectType,
		SubjectID:       t.SubjectID,
		SubjectRelation: t.SubjectRelation,
	}
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		tuples:   make(map[tupleKey]struct{}),
		nextTime: 1, // Start at 1 so 0 means "from beginning"
	}
}

// WriteTuple adds a tuple to the store and emits a change to subscribers.
func (s *Store) WriteTuple(ctx context.Context, t domain.Tuple) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := toKey(t)
	if _, exists := s.tuples[key]; exists {
		// Already exists, no-op
		return nil
	}

	time := s.nextTime
	s.nextTime++
	s.tuples[key] = struct{}{}

	change := domain.Change{Time: time, Op: domain.OpInsert, Tuple: t}
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
func (s *Store) DeleteTuple(ctx context.Context, t domain.Tuple) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := toKey(t)
	if _, exists := s.tuples[key]; !exists {
		// Doesn't exist, no-op
		return nil
	}

	time := s.nextTime
	s.nextTime++
	delete(s.tuples, key)

	change := domain.Change{Time: time, Op: domain.OpDelete, Tuple: t}
	for _, ch := range s.subscribers {
		select {
		case ch <- change:
		default:
		}
	}

	return nil
}

// LoadAll returns an iterator over all tuples currently in the store.
func (s *Store) LoadAll(ctx context.Context) (domain.TupleIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]domain.Tuple, 0, len(s.tuples))
	for key := range s.tuples {
		result = append(result, keyToTuple(key))
	}
	return domain.NewSliceIterator(result), nil
}

func keyToTuple(k tupleKey) domain.Tuple {
	return domain.Tuple{
		ObjectType:      k.ObjectType,
		ObjectID:        k.ObjectID,
		Relation:        k.Relation,
		SubjectType:     k.SubjectType,
		SubjectID:       k.SubjectID,
		SubjectRelation: k.SubjectRelation,
	}
}

// Close is a no-op for the in-memory store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscriber channels
	for _, ch := range s.subscribers {
		close(ch)
	}
	s.subscribers = nil
	return nil
}

// Subscribe returns a channel that receives changes after the given time.
// The channel is closed when Close() is called or the context is canceled.
func (s *Store) Subscribe(ctx context.Context, after domain.StoreTime) (<-chan domain.Change, <-chan error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Buffer some changes to avoid blocking writers
	ch := make(chan domain.Change, 100)
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

// CurrentTime returns the latest time (the next time minus 1).
func (s *Store) CurrentTime(ctx context.Context) (domain.StoreTime, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nextTime == 1 {
		return 0, nil
	}
	return s.nextTime - 1, nil
}

// Compile-time interface checks
var (
	_ domain.Store        = (*Store)(nil)
	_ domain.ChangeStream = (*Store)(nil)
)
