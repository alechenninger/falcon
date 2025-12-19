package graph

import (
	"context"

	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
)

// GraphServer implements the gRPC GraphServiceServer interface by delegating
// to an underlying Graph implementation.
type GraphServer struct {
	graphpb.UnimplementedGraphServiceServer
	graph  Graph
	schema *schema.Schema
}

// NewGraphServer creates a new GraphServer wrapping the given Graph.
func NewGraphServer(g Graph) *GraphServer {
	return &GraphServer{
		graph:  g,
		schema: g.Schema(),
	}
}

// Check implements graphpb.GraphServiceServer.
func (s *GraphServer) Check(ctx context.Context, req *graphpb.CheckRequest) (*graphpb.CheckResponse, error) {
	window := snapshotWindowFromProto(req.Window)
	visited := visitedKeysFromProto(req.Visited)

	allowed, resultWindow, err := s.graph.Check(ctx,
		schema.TypeID(req.SubjectTypeId),
		schema.ID(req.SubjectId),
		schema.TypeID(req.ObjectTypeId),
		schema.ID(req.ObjectId),
		schema.RelationID(req.RelationId),
		window,
		visited,
	)
	if err != nil {
		return nil, err
	}

	return &graphpb.CheckResponse{
		Allowed: allowed,
		Window:  snapshotWindowToProto(resultWindow),
	}, nil
}

// CheckUnion implements graphpb.GraphServiceServer.
func (s *GraphServer) CheckUnion(ctx context.Context, req *graphpb.CheckUnionRequest) (*graphpb.CheckUnionResponse, error) {
	checks, err := relationChecksFromProto(req.Checks)
	if err != nil {
		return nil, err
	}
	visited := visitedKeysFromProto(req.Visited)

	result, err := s.graph.CheckUnion(ctx,
		schema.TypeID(req.SubjectTypeId),
		schema.ID(req.SubjectId),
		checks,
		visited,
	)
	if err != nil {
		return nil, err
	}

	return &graphpb.CheckUnionResponse{
		Allowed:       result.Found,
		Window:        snapshotWindowToProto(result.Window),
		DependentSets: dependentSetsToProto(result.DependentSets),
	}, nil
}

// Compile-time interface check
var _ graphpb.GraphServiceServer = (*GraphServer)(nil)
