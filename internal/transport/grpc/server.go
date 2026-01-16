package grpc

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
	infragrpc "github.com/alechenninger/falcon/internal/infrastructure/grpc"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

// Server implements the gRPC GraphServiceServer interface by delegating
// to an underlying domain.Graph implementation.
type Server struct {
	graphpb.UnimplementedGraphServiceServer
	graph  domain.Graph
	schema *domain.Schema
}

// NewServer creates a new Server wrapping the given Graph.
func NewServer(g domain.Graph) *Server {
	return &Server{
		graph:  g,
		schema: g.Schema(),
	}
}

// Check implements graphpb.GraphServiceServer.
func (s *Server) Check(ctx context.Context, req *graphpb.CheckRequest) (*graphpb.CheckResponse, error) {
	window := infragrpc.SnapshotWindowFromProto(req.Window)
	visited := infragrpc.VisitedKeysFromProto(req.Visited)

	allowed, resultWindow, err := s.graph.Check(ctx,
		domain.TypeID(req.SubjectTypeId),
		domain.ID(req.SubjectId),
		domain.TypeID(req.ObjectTypeId),
		domain.ID(req.ObjectId),
		domain.RelationID(req.RelationId),
		window,
		visited,
	)
	if err != nil {
		return nil, err
	}

	return &graphpb.CheckResponse{
		Allowed: allowed,
		Window:  infragrpc.SnapshotWindowToProto(resultWindow),
	}, nil
}

// CheckUnion implements graphpb.GraphServiceServer.
func (s *Server) CheckUnion(ctx context.Context, req *graphpb.CheckUnionRequest) (*graphpb.CheckUnionResponse, error) {
	checks, err := infragrpc.RelationChecksFromProto(req.Checks)
	if err != nil {
		return nil, err
	}
	visited := infragrpc.VisitedKeysFromProto(req.Visited)

	result, err := s.graph.CheckUnion(ctx,
		domain.TypeID(req.SubjectTypeId),
		domain.ID(req.SubjectId),
		checks,
		visited,
	)
	if err != nil {
		return nil, err
	}

	return &graphpb.CheckUnionResponse{
		Allowed:       result.Found,
		Window:        infragrpc.SnapshotWindowToProto(result.Window),
		DependentSets: infragrpc.DependentSetsToProto(result.DependentSets),
	}, nil
}

// Compile-time interface check
var _ graphpb.GraphServiceServer = (*Server)(nil)
