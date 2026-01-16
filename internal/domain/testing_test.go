package domain_test

import (
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/internal/infrastructure/memory"
	
)

// newTestGraph creates a TestGraph with a memory store for testing.
// This is a convenience wrapper that creates the memory store automatically.
func newTestGraph(s *domain.Schema) *domain.TestGraph {
	return domain.NewTestGraph(s, memory.NewStore())
}
