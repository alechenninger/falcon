package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/schema"
)

// Re-export types from domain for backward compatibility.
type (
	// ShardID identifies a shard in a distributed graph.
	// Deprecated: Use domain.ShardID instead.
	ShardID = domain.ShardID

	// Router determines which shard owns a given object.
	// Deprecated: Use domain.Router instead.
	Router = domain.Router

	// ShardedGraph is a distributed Graph implementation.
	// Deprecated: Use domain.ShardedGraph instead.
	ShardedGraph = domain.ShardedGraph
)

// NewShardedGraph creates a new ShardedGraph.
// Deprecated: Use domain.NewShardedGraph instead.
func NewShardedGraph(
	localShardID ShardID,
	s *schema.Schema,
	router Router,
	shards map[ShardID]Graph,
	stream domain.ChangeStream,
	st domain.Store,
) *ShardedGraph {
	return domain.NewShardedGraph(localShardID, s, router, shards, stream, st)
}
