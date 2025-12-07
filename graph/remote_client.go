package graph

import (
	"context"

	pb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// RemoteGraphClient implements GraphClient using gRPC to call a remote node.
type RemoteGraphClient struct {
	client pb.GraphServiceClient
}

// NewRemoteGraphClient creates a RemoteGraphClient with the given gRPC client.
func NewRemoteGraphClient(client pb.GraphServiceClient) *RemoteGraphClient {
	return &RemoteGraphClient{client: client}
}

// CheckRelation dispatches to the remote node via gRPC.
func (c *RemoteGraphClient) CheckRelation(
	ctx context.Context,
	subjectType schema.TypeName,
	subjectID schema.ID,
	objectType schema.TypeName,
	objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// Convert visited to proto format
	pbVisited := visitedToProto(visited)

	resp, err := c.client.CheckRelation(ctx, &pb.CheckRelationRequest{
		SubjectType: string(subjectType),
		SubjectId:   uint32(subjectID),
		ObjectType:  string(objectType),
		ObjectId:    uint32(objectID),
		Relation:    string(relation),
		Window: &pb.SnapshotWindow{
			Min: uint64(window.Min()),
			Max: uint64(window.Max()),
		},
		Visited: pbVisited,
	})
	if err != nil {
		return false, window, err
	}

	resultWindow := window
	if resp.Window != nil {
		resultWindow = NewSnapshotWindow(store.StoreTime(resp.Window.Min), store.StoreTime(resp.Window.Max))
	}

	return resp.Allowed, resultWindow, nil
}

// BatchCheckRelation dispatches a batch check to the remote node via gRPC.
// It serializes the bitmaps to bytes for transmission.
func (c *RemoteGraphClient) BatchCheckRelation(
	ctx context.Context,
	check RelationCheck,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// Convert ObjectSets to proto format with serialized bitmaps
	pbObjects := make([]*pb.ObjectSet, len(check.Objects))
	for i, objSet := range check.Objects {
		// Serialize the roaring bitmap to bytes
		bitmapBytes, err := objSet.IDs.ToBytes()
		if err != nil {
			return false, window, err
		}
		pbObjects[i] = &pb.ObjectSet{
			ObjectType: string(objSet.Type),
			ObjectIds:  bitmapBytes,
		}
	}

	resp, err := c.client.BatchCheckRelation(ctx, &pb.BatchCheckRelationRequest{
		SubjectType: string(check.SubjectType),
		SubjectId:   uint32(check.SubjectID),
		Objects:     pbObjects,
		Relation:    string(check.Relation),
		Window: &pb.SnapshotWindow{
			Min: uint64(window.Min()),
			Max: uint64(window.Max()),
		},
		Visited: visitedToProto(visited),
	})
	if err != nil {
		return false, window, err
	}

	resultWindow := window
	if resp.Window != nil {
		resultWindow = NewSnapshotWindow(store.StoreTime(resp.Window.Min), store.StoreTime(resp.Window.Max))
	}

	return resp.Allowed, resultWindow, nil
}

// visitedToProto converts a slice of VisitedKey to proto format.
func visitedToProto(visited []VisitedKey) []*pb.VisitedNode {
	pbVisited := make([]*pb.VisitedNode, len(visited))
	for i, v := range visited {
		pbVisited[i] = &pb.VisitedNode{
			ObjectType: string(v.ObjectType),
			ObjectId:   uint32(v.ObjectID),
			Relation:   string(v.Relation),
		}
	}
	return pbVisited
}
