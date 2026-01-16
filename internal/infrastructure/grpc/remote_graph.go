package grpc

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
)

// RemoteGraph implements the domain.Graph interface by delegating to a remote
// gRPC GraphService. It is used by ShardedGraph to communicate with
// remote shards.
//
// RemoteGraph does NOT implement domain.GraphService (no Start method) since
// it doesn't manage local state - it's a pure client.
type RemoteGraph struct {
	client graphpb.GraphServiceClient
	schema *schema.Schema
}

// NewRemoteGraph creates a new RemoteGraph that delegates to the given gRPC client.
// The schema is required for the Schema() method.
func NewRemoteGraph(client graphpb.GraphServiceClient, s *schema.Schema) *RemoteGraph {
	return &RemoteGraph{
		client: client,
		schema: s,
	}
}

// Check delegates to the remote GraphService.Check RPC.
func (g *RemoteGraph) Check(ctx context.Context,
	subjectType schema.TypeID, subjectID schema.ID,
	objectType schema.TypeID, objectID schema.ID,
	relation schema.RelationID,
	window domain.SnapshotWindow, visited []domain.VisitedKey,
) (bool, domain.SnapshotWindow, error) {
	req := &graphpb.CheckRequest{
		SubjectTypeId: uint32(subjectType),
		SubjectId:     uint32(subjectID),
		ObjectTypeId:  uint32(objectType),
		ObjectId:      uint32(objectID),
		RelationId:    uint32(relation),
		Window:        SnapshotWindowToProto(window),
		Visited:       VisitedKeysToProto(visited),
	}

	resp, err := g.client.Check(ctx, req)
	if err != nil {
		return false, window, err
	}

	return resp.Allowed, SnapshotWindowFromProto(resp.Window), nil
}

// CheckUnion delegates to the remote GraphService.CheckUnion RPC.
func (g *RemoteGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeID, subjectID schema.ID,
	checks []domain.RelationCheck,
	visited []domain.VisitedKey,
) (domain.CheckResult, error) {
	if len(checks) == 0 {
		return domain.CheckResult{}, nil
	}

	req := &graphpb.CheckUnionRequest{
		SubjectTypeId: uint32(subjectType),
		SubjectId:     uint32(subjectID),
		Checks:        RelationChecksToProto(checks),
		Visited:       VisitedKeysToProto(visited),
	}

	resp, err := g.client.CheckUnion(ctx, req)
	if err != nil {
		return domain.CheckResult{}, err
	}

	return checkResultFromProto(resp), nil
}

// checkResultFromProto converts a proto CheckUnionResponse to domain.CheckResult.
func checkResultFromProto(resp *graphpb.CheckUnionResponse) domain.CheckResult {
	return domain.CheckResult{
		Found:         resp.Allowed,
		DependentSets: DependentSetsFromProto(resp.DependentSets),
		Window:        SnapshotWindowFromProto(resp.Window),
	}
}

// Schema returns the authorization schema.
func (g *RemoteGraph) Schema() *schema.Schema {
	return g.schema
}

// Compile-time interface check
var _ domain.Graph = (*RemoteGraph)(nil)
