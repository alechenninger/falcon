package graph

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring"
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
	visited := s.visitedKeysFromProto(req.Visited)

	// Convert IDs to names
	subjectType := s.schema.TypeByID(schema.TypeID(req.SubjectTypeId))
	if subjectType == nil {
		return nil, fmt.Errorf("unknown subject type ID: %d", req.SubjectTypeId)
	}
	objectType := s.schema.TypeByID(schema.TypeID(req.ObjectTypeId))
	if objectType == nil {
		return nil, fmt.Errorf("unknown object type ID: %d", req.ObjectTypeId)
	}
	relation := objectType.RelationByID(schema.RelationID(req.RelationId))
	if relation == nil {
		return nil, fmt.Errorf("unknown relation ID: %d on type %s", req.RelationId, objectType.Name)
	}

	allowed, resultWindow, err := s.graph.Check(ctx,
		subjectType.Name,
		schema.ID(req.SubjectId),
		objectType.Name,
		schema.ID(req.ObjectId),
		relation.Name,
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
	checks, err := s.relationChecksFromProto(req.Checks)
	if err != nil {
		return nil, err
	}
	visited := s.visitedKeysFromProto(req.Visited)

	// Convert subject type ID to name
	subjectType := s.schema.TypeByID(schema.TypeID(req.SubjectTypeId))
	if subjectType == nil {
		return nil, fmt.Errorf("unknown subject type ID: %d", req.SubjectTypeId)
	}

	result, err := s.graph.CheckUnion(ctx,
		subjectType.Name,
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
		DependentSets: s.dependentSetsToProto(result.DependentSets),
	}, nil
}

// visitedKeysFromProto converts proto VisitedNodes to a slice of VisitedKey.
func (s *GraphServer) visitedKeysFromProto(visited []*graphpb.VisitedNode) []VisitedKey {
	if visited == nil {
		return nil
	}
	result := make([]VisitedKey, len(visited))
	for i, v := range visited {
		objType := s.schema.TypeByID(schema.TypeID(v.ObjectTypeId))
		var objTypeName schema.TypeName
		var relName schema.RelationName
		if objType != nil {
			objTypeName = objType.Name
			if rel := objType.RelationByID(schema.RelationID(v.RelationId)); rel != nil {
				relName = rel.Name
			}
		}
		result[i] = VisitedKey{
			ObjectType: objTypeName,
			ObjectID:   schema.ID(v.ObjectId),
			Relation:   relName,
		}
	}
	return result
}

// relationChecksFromProto converts proto RelationChecks to a slice of RelationCheck.
func (s *GraphServer) relationChecksFromProto(checks []*graphpb.RelationCheck) ([]RelationCheck, error) {
	result := make([]RelationCheck, len(checks))
	for i, c := range checks {
		bitmap := roaring.New()
		if len(c.ObjectIds) > 0 {
			if _, err := bitmap.FromBuffer(c.ObjectIds); err != nil {
				return nil, err
			}
		}
		objType := s.schema.TypeByID(schema.TypeID(c.ObjectTypeId))
		if objType == nil {
			return nil, fmt.Errorf("unknown object type ID: %d", c.ObjectTypeId)
		}
		var relName schema.RelationName
		if rel := objType.RelationByID(schema.RelationID(c.RelationId)); rel != nil {
			relName = rel.Name
		}
		result[i] = RelationCheck{
			ObjectType: objType.Name,
			ObjectIDs:  bitmap,
			Relation:   relName,
			Window:     snapshotWindowFromProto(c.Window),
		}
	}
	return result, nil
}

// dependentSetsToProto converts DependentSets to proto representation.
func (s *GraphServer) dependentSetsToProto(sets []DependentSet) []*graphpb.DependentSet {
	if sets == nil {
		return nil
	}
	result := make([]*graphpb.DependentSet, len(sets))
	for i, set := range sets {
		var objectIDs []byte
		if set.ObjectIDs != nil {
			objectIDs, _ = set.ObjectIDs.ToBytes()
		}
		result[i] = &graphpb.DependentSet{
			ObjectTypeId: uint32(s.schema.GetTypeID(set.ObjectType)),
			RelationId:   uint32(s.schema.GetRelationID(set.ObjectType, set.Relation)),
			ObjectIds:    objectIDs,
		}
	}
	return result
}

// Compile-time interface check
var _ graphpb.GraphServiceServer = (*GraphServer)(nil)
