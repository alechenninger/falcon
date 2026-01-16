// Package store defines the persistence interface for the authorization graph.
//
// Deprecated: Use github.com/alechenninger/falcon/internal/domain instead.
// This package re-exports types from domain for backward compatibility.
package store

import (
	"github.com/alechenninger/falcon/internal/domain"
)

// Re-export types from domain for backward compatibility.
type (
	// StoreTime represents a timestamp encoded as uint64.
	// Deprecated: Use domain.StoreTime instead.
	StoreTime = domain.StoreTime

	// StoreDelta represents the distance between two StoreTime values.
	// Deprecated: Use domain.StoreDelta instead.
	StoreDelta = domain.StoreDelta

	// AtomicStoreTime provides atomic operations on StoreTime.
	// Deprecated: Use domain.AtomicStoreTime instead.
	AtomicStoreTime = domain.AtomicStoreTime

	// Tuple represents a single authorization tuple using compact IDs.
	// Deprecated: Use domain.Tuple instead.
	Tuple = domain.Tuple

	// ChangeOp represents the type of change (insert or delete).
	// Deprecated: Use domain.ChangeOp instead.
	ChangeOp = domain.ChangeOp

	// Change represents a tuple change with its timestamp.
	// Deprecated: Use domain.Change instead.
	Change = domain.Change

	// Store defines the persistence interface for authorization tuples.
	// Deprecated: Use domain.Store instead.
	Store = domain.Store

	// ChangeStream emits ordered tuple changes from the store.
	// Deprecated: Use domain.ChangeStream instead.
	ChangeStream = domain.ChangeStream

	// TupleIterator provides cursor-style iteration over tuples.
	// Deprecated: Use domain.TupleIterator instead.
	TupleIterator = domain.TupleIterator

	// SliceIterator wraps a slice of tuples as a TupleIterator.
	// Deprecated: Use domain.SliceIterator instead.
	SliceIterator = domain.SliceIterator
)

// Re-export constants from domain.
const (
	// OpInsert indicates a tuple was inserted.
	OpInsert = domain.OpInsert
	// OpDelete indicates a tuple was deleted.
	OpDelete = domain.OpDelete
)

// NewSliceIterator creates a TupleIterator from a slice.
// Deprecated: Use domain.NewSliceIterator instead.
func NewSliceIterator(tuples []Tuple) *SliceIterator {
	return domain.NewSliceIterator(tuples)
}
