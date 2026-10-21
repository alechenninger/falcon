package grpc

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
	infragrpc "github.com/alechenninger/falcon/internal/infrastructure/grpc"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	subjectType, err := infragrpc.TypeIDFromProto(req.SubjectTypeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "subject_type_id: %v", err)
	}
	objectType, err := infragrpc.TypeIDFromProto(req.ObjectTypeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "object_type_id: %v", err)
	}
	relation, err := infragrpc.RelationIDFromProto(req.RelationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "relation_id: %v", err)
	}
	window := infragrpc.SnapshotWindowFromProto(req.Window)
	visited, err := infragrpc.VisitedKeysFromProto(req.Visited)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "visited: %v", err)
	}

	allowed, resultWindow, err := s.graph.Check(ctx,
		subjectType,
		domain.ID(req.SubjectId),
		objectType,
		domain.ID(req.ObjectId),
		relation,
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
	subjectType, err := infragrpc.TypeIDFromProto(req.SubjectTypeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "subject_type_id: %v", err)
	}
	checks, err := infragrpc.RelationChecksFromProto(req.Checks)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "checks: %v", err)
	}
	visited, err := infragrpc.VisitedKeysFromProto(req.Visited)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "visited: %v", err)
	}

	result, err := s.graph.CheckUnion(ctx,
		subjectType,
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
