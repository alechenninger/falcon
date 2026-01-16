package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/schema"
)

// Re-export types from domain for backward compatibility.
type (
	// MultiversionUsersets stores authorization tuples as compressed subject sets.
	// Deprecated: Use domain.MultiversionUsersets instead.
	MultiversionUsersets = domain.MultiversionUsersets
)

// NewMultiversionUsersets creates a new MultiversionUsersets with the given schema.
// Deprecated: Use domain.NewMultiversionUsersets instead.
func NewMultiversionUsersets(s *schema.Schema) *MultiversionUsersets {
	return domain.NewMultiversionUsersets(s)
}
