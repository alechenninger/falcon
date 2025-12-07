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
	pbVisited := make([]*pb.VisitedNode, len(visited))
	for i, v := range visited {
		pbVisited[i] = &pb.VisitedNode{
			ObjectType: string(v.ObjectType),
			ObjectId:   uint32(v.ObjectID),
			Relation:   string(v.Relation),
		}
	}

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

