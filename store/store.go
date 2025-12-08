// Package store defines the persistence interface for the authorization graph.
package store

import (
	"context"
	"sync/atomic"

	"github.com/alechenninger/falcon/schema"
)

// StoreTime represents a timestamp encoded as uint64.
// Stores must encode their native format in an order-preserving way.
// Supported: Postgres LSN, Oracle SCN, MariaDB GTID, SQL Server Change Tracking.
//
// Use native operators (<, >, ==) for comparisons. Use [StoreTime.Difference] and
// [StoreTime.Less] for type-safe conversions between StoreTime and StoreDelta.
type StoreTime uint64

// Difference returns the delta (t - other) as a [StoreDelta].
// Panics if the delta exceeds uint32 max value.
func (t StoreTime) Difference(other StoreTime) StoreDelta {
	delta := uint64(t - other)
	if delta > uint64(^uint32(0)) {
		panic("StoreTime.Difference: delta overflow (exceeds uint32)")
	}
	return StoreDelta(delta)
}

// Less returns t - d as a new StoreTime.
func (t StoreTime) Less(d StoreDelta) StoreTime {
	return t - StoreTime(d)
}

// StoreDelta represents the distance between two [StoreTime] values.
// Use native + operator for adding deltas.
type StoreDelta uint32

// AtomicStoreTime provides atomic operations on [StoreTime].
type AtomicStoreTime struct {
	v atomic.Uint64
}

// Load atomically loads and returns the stored StoreTime.
func (a *AtomicStoreTime) Load() StoreTime {
	return StoreTime(a.v.Load())
}

// Store atomically stores t.
func (a *AtomicStoreTime) Store(t StoreTime) {
	a.v.Store(uint64(t))
}

// Tuple represents a single authorization tuple.
//
// For direct subjects: object_type:object_id#relation@subject_type:subject_id
// Example: document:100#viewer@user:1
//
// For userset subjects: object_type:object_id#relation@subject_type:subject_id#subject_relation
// Example: document:100#viewer@group:1#member (all members of group 1 are viewers)
type Tuple struct {
	ObjectType      schema.TypeName
	ObjectID        schema.ID
	Relation        schema.RelationName
	SubjectType     schema.TypeName
	SubjectID       schema.ID
	SubjectRelation schema.RelationName // Optional: empty for direct subjects, set for userset subjects
}

// ChangeOp represents the type of change (insert or delete).
type ChangeOp int

const (
	// OpInsert indicates a tuple was inserted.
	OpInsert ChangeOp = iota
	// OpDelete indicates a tuple was deleted.
	OpDelete
)

// Change represents a tuple change with its timestamp.
type Change struct {
	Time  StoreTime
	Op    ChangeOp
	Tuple Tuple
}

// Store defines the persistence interface for authorization tuples.
// Implementations handle durable storage of tuples (e.g., Postgres).
//
// Note: Writes do not return timestamps. The timestamp is only available via
// the ChangeStream, which delivers changes in WAL order with their timestamps.
type Store interface {
	// WriteTuple persists a tuple to the store. If the tuple already exists,
	// this is a no-op.
	WriteTuple(ctx context.Context, t Tuple) error

	// DeleteTuple removes a tuple from the store. If the tuple doesn't exist,
	// this is a no-op.
	DeleteTuple(ctx context.Context, t Tuple) error

	// LoadAll returns an iterator over all tuples in the store.
	// This is used to hydrate the in-memory graph on startup.
	// The caller must call Close on the returned iterator.
	LoadAll(ctx context.Context) (TupleIterator, error)

	// Close releases any resources held by the store.
	Close() error
}

// ChangeStream emits ordered tuple changes from the store.
// Implementations tail the WAL (Postgres) or emit changes directly (in-memory).
type ChangeStream interface {
	// Subscribe returns a channel of changes starting after the given time.
	// Pass 0 to get all changes from the beginning.
	// The channel is closed when the context is canceled or an error occurs.
	Subscribe(ctx context.Context, after StoreTime) (<-chan Change, <-chan error)

	// CurrentTime returns the current time of the store (latest committed change).
	CurrentTime(ctx context.Context) (StoreTime, error)
}

// TupleIterator provides cursor-style iteration over tuples.
// Callers must call Close when done to release resources.
//
// Usage:
//
//	iter, err := store.LoadAll(ctx)
//	if err != nil { ... }
//	defer iter.Close()
//	for iter.Next() {
//	    tuple := iter.Tuple()
//	    // process tuple
//	}
//	if err := iter.Err(); err != nil { ... }
type TupleIterator interface {
	// Next advances to the next tuple. Returns true if there is a tuple
	// available, false when iteration is complete or an error occurred.
	Next() bool

	// Tuple returns the current tuple. Only valid after Next returns true.
	Tuple() Tuple

	// Err returns any error encountered during iteration.
	// Should be checked after Next returns false.
	Err() error

	// Close releases resources held by the iterator.
	Close() error
}

// SliceIterator wraps a slice of tuples as a TupleIterator.
// Useful for testing or in-memory implementations.
type SliceIterator struct {
	tuples []Tuple
	idx    int
}

// NewSliceIterator creates a TupleIterator from a slice.
func NewSliceIterator(tuples []Tuple) *SliceIterator {
	return &SliceIterator{tuples: tuples, idx: -1}
}

// Next advances to the next tuple.
func (s *SliceIterator) Next() bool {
	s.idx++
	return s.idx < len(s.tuples)
}

// Tuple returns the current tuple.
func (s *SliceIterator) Tuple() Tuple {
	return s.tuples[s.idx]
}

// Err always returns nil for SliceIterator.
func (s *SliceIterator) Err() error {
	return nil
}

// Close is a no-op for SliceIterator.
func (s *SliceIterator) Close() error {
	return nil
}
