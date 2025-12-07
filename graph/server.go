package graph

import (
	"context"

	"github.com/RoaringBitmap/roaring"
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
	visited := visitedFromProto(req.Visited)

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

// BatchCheckRelation implements GraphService.BatchCheckRelation.
// It checks if a subject has a relation on ANY of the given objects.
func (s *Server) BatchCheckRelation(ctx context.Context, req *pb.BatchCheckRelationRequest) (*pb.BatchCheckRelationResponse, error) {
	window := SnapshotWindow{}
	if req.Window != nil {
		window = NewSnapshotWindow(store.StoreTime(req.Window.Min), store.StoreTime(req.Window.Max))
	} else {
		window = NewSnapshotWindow(0, s.graph.ReplicatedTime())
	}

	// Convert visited from proto format
	visited := visitedFromProto(req.Visited)

	// Convert ObjectSets from proto format, deserializing bitmaps
	objects := make([]ObjectSet, len(req.Objects))
	for i, pbObj := range req.Objects {
		bitmap := roaring.New()
		if len(pbObj.ObjectIds) > 0 {
			if _, err := bitmap.FromBuffer(pbObj.ObjectIds); err != nil {
				return nil, err
			}
		}
		objects[i] = ObjectSet{
			Type: schema.TypeName(pbObj.ObjectType),
			IDs:  bitmap,
		}
	}

	// Create a LocalGraphClient to handle the batch check locally
	localClient := NewLocalGraphClient(s.graph)
	check := RelationCheck{
		SubjectType: schema.TypeName(req.SubjectType),
		SubjectID:   schema.ID(req.SubjectId),
		Objects:     objects,
		Relation:    schema.RelationName(req.Relation),
	}

	allowed, resultWindow, err := localClient.BatchCheckRelation(ctx, check, window, visited)
	if err != nil {
		return nil, err
	}

	return &pb.BatchCheckRelationResponse{
		Allowed: allowed,
		Window: &pb.SnapshotWindow{
			Min: uint64(resultWindow.Min()),
			Max: uint64(resultWindow.Max()),
		},
	}, nil
}

// visitedFromProto converts proto VisitedNode slice to VisitedKey slice.
func visitedFromProto(pbVisited []*pb.VisitedNode) []VisitedKey {
	visited := make([]VisitedKey, len(pbVisited))
	for i, v := range pbVisited {
		visited[i] = VisitedKey{
			ObjectType: schema.TypeName(v.ObjectType),
			ObjectID:   schema.ID(v.ObjectId),
			Relation:   schema.RelationName(v.Relation),
		}
	}
	return visited
}

