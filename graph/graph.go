// Package graph implements the in-memory authorization graph with roaring
// bitmaps for efficient set operations and MVCC for snapshot isolation.
//
// Deprecated: Use github.com/alechenninger/falcon/internal/domain instead.
// This package re-exports types from domain for backward compatibility.
package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/schema"
)

// Re-export types from domain for backward compatibility.
type (
	// RelationCheck represents a check against a single object type's relation.
	// Deprecated: Use domain.RelationCheck instead.
	RelationCheck = domain.RelationCheck

	// DependentSet identifies objects that were relevant to a check result.
	// Deprecated: Use domain.DependentSet instead.
	DependentSet = domain.DependentSet

	// CheckResult represents the outcome of a check with provenance information.
	// Deprecated: Use domain.CheckResult instead.
	CheckResult = domain.CheckResult

	// VisitedKey tracks nodes visited during graph traversal for cycle detection.
	// Deprecated: Use domain.VisitedKey instead.
	VisitedKey = domain.VisitedKey

	// Graph provides core authorization check operations.
	// Deprecated: Use domain.Graph instead.
	Graph = domain.Graph

	// GraphService extends Graph with lifecycle management.
	// Deprecated: Use domain.GraphService instead.
	GraphService = domain.GraphService

	// LocalGraph is a single-node Graph implementation.
	// Deprecated: Use domain.LocalGraph instead.
	LocalGraph = domain.LocalGraph

	// BenchGraph wraps a LocalGraph for benchmarking with direct population.
	// Deprecated: Use domain.BenchGraph instead.
	BenchGraph = domain.BenchGraph
)

// NewLocalGraph creates a new LocalGraph.
// Deprecated: Use domain.NewLocalGraph instead.
func NewLocalGraph(s *schema.Schema, stream domain.ChangeStream, st domain.Store) *LocalGraph {
	return domain.NewLocalGraph(s, stream, st)
}

// NewBenchGraph creates a graph for benchmarking with direct population.
// Deprecated: Use domain.NewBenchGraph instead.
func NewBenchGraph(s *schema.Schema) *BenchGraph {
	return domain.NewBenchGraph(s)
}
