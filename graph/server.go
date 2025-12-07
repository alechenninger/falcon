package graph

import (
	"context"

	pb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// Server implements the GraphService gRPC service.
// It delegates to a local Graph for check operations.
type Server struct {
	pb.UnimplementedGraphServiceServer
	graph *Graph
}

// NewServer creates a new GraphService server for the given Graph.
func NewServer(g *Graph) *Server {
	return &Server{graph: g}
}

// CheckRelation implements GraphService.CheckRelation.
// It checks if a subject has a relation on an object within a snapshot window.
func (s *Server) CheckRelation(ctx context.Context, req *pb.CheckRelationRequest) (*pb.CheckRelationResponse, error) {
	window := SnapshotWindow{}
	if req.Window != nil {
		window = NewSnapshotWindow(store.StoreTime(req.Window.Min), store.StoreTime(req.Window.Max))
	} else {
		window = NewSnapshotWindow(0, s.graph.ReplicatedTime())
	}

	// Convert visited from proto format
	visited := make([]VisitedKey, len(req.Visited))
	for i, v := range req.Visited {
		visited[i] = VisitedKey{
			ObjectType: schema.TypeName(v.ObjectType),
			ObjectID:   schema.ID(v.ObjectId),
			Relation:   schema.RelationName(v.Relation),
		}
	}

	allowed, resultWindow, err := s.graph.CheckAtWithVisited(
		ctx,
		schema.TypeName(req.SubjectType),
		schema.ID(req.SubjectId),
		schema.TypeName(req.ObjectType),
		schema.ID(req.ObjectId),
		schema.RelationName(req.Relation),
		&window,
		visited,
	)
	if err != nil {
		return nil, err
	}

	return &pb.CheckRelationResponse{
		Allowed: allowed,
		Window: &pb.SnapshotWindow{
			Min: uint64(resultWindow.Min()),
			Max: uint64(resultWindow.Max()),
		},
	}, nil
}

