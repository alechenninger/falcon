package domain

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrIDNotFound is returned when an external ID has no mapping to an internal ID.
var ErrIDNotFound = errors.New("external ID not found")

// ExternalID is the application-level identifier for objects and subjects.
// These are strings that get mapped to internal compact numeric IDs ([ID]).
// Examples: "doc-abc", "alice", "550e8400-e29b-41d4-a716-446655440000"
type ExternalID string

// ObjectRef identifies an object by its type and external ID.
// An external ID is only meaningful in the context of its object type;
// the same external ID string may refer to different objects in different types.
type ObjectRef struct {
	Type TypeID
	ID   ExternalID
}

// NoObject is a zero ObjectRef, used when no object reference is needed.
// For example, pass NoObject as the root when an object is its own shard root.
var NoObject ObjectRef

// IsZero returns true if this is a zero-value ObjectRef (equivalent to NoObject).
func (r ObjectRef) IsZero() bool {
	return r.Type == 0 && r.ID == ""
}

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

// Tuple represents a single authorization tuple using compact IDs.
//
// For direct subjects: object_type:object_id#relation@subject_type:subject_id
// Example: document:100#viewer@user:1
//
// For userset subjects: object_type:object_id#relation@subject_type:subject_id#subject_relation
// Example: document:100#viewer@group:1#member (all members of group 1 are viewers)
type Tuple struct {
	ObjectType      TypeID
	ObjectID        ID
	Relation        RelationID
	SubjectType     TypeID
	SubjectID       ID
	SubjectRelation RelationID // Optional: NoRelation for direct subjects
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

// Mutation represents a single tuple change.
type Mutation struct {
	Op    ChangeOp // OpInsert or OpDelete
	Tuple Tuple
}

// TupleField identifies a field of a Tuple for use in predicates.
type TupleField int

const (
	FieldObjectType TupleField = iota
	FieldObjectID
	FieldRelation
	FieldSubjectType
	FieldSubjectID
	FieldSubjectRelation
)

// CompareOp specifies how to compare a field value.
type CompareOp int

const (
	// OpEq matches if the field equals the value.
	OpEq CompareOp = iota
	// OpNeq matches if the field does not equal the value.
	OpNeq
	// OpLt matches if the field is less than the value (lexicographic for strings).
	OpLt
	// OpLte matches if the field is less than or equal to the value.
	OpLte
	// OpGt matches if the field is greater than the value.
	OpGt
	// OpGte matches if the field is greater than or equal to the value.
	OpGte
	// OpStartsWith matches if the field value starts with the given prefix.
	// Only valid for string fields (ObjectID, SubjectID).
	OpStartsWith
)

// TuplePredicate defines a condition over tuples that can be evaluated in SQL.
// Predicates form a tree structure with boolean combinators (And, Or, Not)
// and leaf nodes that compare tuple fields to values.
type TuplePredicate interface {
	// isTuplePredicate is a marker method to ensure only predicate types
	// implement this interface.
	isTuplePredicate()
}

// FieldPredicate compares a tuple field to a value.
// For ObjectType/SubjectType fields, Value should be a TypeID.
// For Relation/SubjectRelation fields, Value should be a RelationID.
// For ObjectID/SubjectID fields, Value should be an [ObjectRef] containing
// both the type and external ID. The Store implementation joins with the
// ID mapping table for ID comparisons.
type FieldPredicate struct {
	Field TupleField
	Op    CompareOp
	Value any
}

func (FieldPredicate) isTuplePredicate() {}

// AndPredicate matches if all child predicates match.
type AndPredicate struct {
	Predicates []TuplePredicate
}

func (AndPredicate) isTuplePredicate() {}

// OrPredicate matches if any child predicate matches.
type OrPredicate struct {
	Predicates []TuplePredicate
}

func (OrPredicate) isTuplePredicate() {}

// NotPredicate inverts the result of its child predicate.
type NotPredicate struct {
	Predicate TuplePredicate
}

func (NotPredicate) isTuplePredicate() {}

// Predicate constructors for convenient usage.

// Eq creates a predicate that matches if the field equals the value.
func Eq(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpEq, Value: value}
}

// Neq creates a predicate that matches if the field does not equal the value.
func Neq(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpNeq, Value: value}
}

// Lt creates a predicate that matches if the field is less than the value.
func Lt(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpLt, Value: value}
}

// Lte creates a predicate that matches if the field is less than or equal to the value.
func Lte(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpLte, Value: value}
}

// Gt creates a predicate that matches if the field is greater than the value.
func Gt(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpGt, Value: value}
}

// Gte creates a predicate that matches if the field is greater than or equal to the value.
func Gte(field TupleField, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpGte, Value: value}
}

// StartsWith creates a predicate that matches if the field value starts with the prefix.
// Only valid for ObjectID and SubjectID fields.
// The ObjectRef.ID is used as the prefix to match against, and the type must also match.
func StartsWith(field TupleField, ref ObjectRef) FieldPredicate {
	return FieldPredicate{Field: field, Op: OpStartsWith, Value: ref}
}

// And creates a predicate that matches if all predicates match.
func And(predicates ...TuplePredicate) AndPredicate {
	return AndPredicate{Predicates: predicates}
}

// Or creates a predicate that matches if any predicate matches.
func Or(predicates ...TuplePredicate) OrPredicate {
	return OrPredicate{Predicates: predicates}
}

// Not creates a predicate that inverts the result.
func Not(predicate TuplePredicate) NotPredicate {
	return NotPredicate{Predicate: predicate}
}

// Store defines the persistence interface for authorization tuples.
// Transaction boundaries are controlled externally via Begin/Commit/Rollback.
// Preconditions are evaluated by querying tuples, not by the store itself.
type Store interface {
	// Begin starts a new transaction.
	// The returned Tx must be committed or rolled back.
	Begin(ctx context.Context) (Tx, error)

	// LoadAll returns an iterator over all tuples in the store.
	// This is used to hydrate the in-memory graph on startup.
	// The caller must call Close on the returned iterator.
	LoadAll(ctx context.Context) (TupleIterator, error)

	// Close releases any resources held by the store.
	Close() error
}

// Tx represents a database transaction.
// All operations within a transaction see a consistent snapshot and are
// committed atomically. The caller controls transaction boundaries.
type Tx interface {
	// ID Resolution

	// GetID returns the internal ID for an object reference.
	// Returns [ErrIDNotFound] if the external ID is not mapped.
	GetID(ctx context.Context, ref ObjectRef) (ID, error)

	// GetOrProvisionID returns the internal ID for an object reference, creating
	// a new mapping if one does not exist. This is idempotent within the transaction.
	//
	// The root parameter specifies the shard root for this object. Pass NoObject
	// if the object is its own root. The root is used to determine which shard
	// the object belongs to and may be encoded in the high bits of the ID.
	GetOrProvisionID(ctx context.Context, ref ObjectRef, root ObjectRef) (ID, error)

	// GetRef returns the external object reference for an internal ID.
	// Returns [ErrIDNotFound] if the internal ID has no mapping.
	// Used for reverse queries (list_objects, list_subjects).
	GetRef(ctx context.Context, typeID TypeID, id ID) (ObjectRef, error)

	// Tuple Operations

	// Write applies mutations within this transaction.
	// Tuples use internal IDs (resolved via GetID/ProvisionID).
	// Can be called multiple times before commit.
	Write(ctx context.Context, mutations []Mutation) error

	// Contains checks if any tuple matches the predicate within this transaction's view.
	// Used to evaluate preconditions before writing.
	// Returns true if at least one matching tuple exists.
	// For ObjectID/SubjectID fields, predicate values are [ObjectRef];
	// the implementation joins with the ID mapping table internally.
	Contains(ctx context.Context, predicate TuplePredicate) (bool, error)

	// Transaction Control

	// Commit commits the transaction.
	// After commit, the Tx should not be used.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction.
	// After rollback, the Tx should not be used.
	// Rollback is safe to call multiple times or after commit (no-op).
	Rollback(ctx context.Context) error
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
