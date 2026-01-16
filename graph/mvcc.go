// Package graph implements the in-memory tuple store with MVCC support.
package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
)

// Re-export types from domain for backward compatibility.
type (
	// VersionedSet stores a set of IDs with MVCC support via undo chains.
	// Deprecated: Use domain.VersionedSet instead.
	VersionedSet = domain.VersionedSet
)

// NewVersionedSet creates a new versioned set starting at the given time.
// Deprecated: Use domain.NewVersionedSet instead.
func NewVersionedSet(t domain.StoreTime) *VersionedSet {
	return domain.NewVersionedSet(t)
}
