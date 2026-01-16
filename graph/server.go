package graph

import (
	transportgrpc "github.com/alechenninger/falcon/internal/transport/grpc"
)

// GraphServer implements the gRPC GraphServiceServer interface.
// Deprecated: Use transportgrpc.Server instead.
type GraphServer = transportgrpc.Server

// NewGraphServer creates a new GraphServer wrapping the given Graph.
// Deprecated: Use transportgrpc.NewServer instead.
func NewGraphServer(g Graph) *GraphServer {
	return transportgrpc.NewServer(g)
}
