package graph

import (
	"context"
	"math"

	"github.com/RoaringBitmap/roaring"
	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// RemoteGraph implements the Graph interface by delegating to a remote
// gRPC GraphService. It is used by ShardedGraph to communicate with
// remote shards.
//
// RemoteGraph does NOT implement GraphService (no Start method) since
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
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow, visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	req := &graphpb.CheckRequest{
		SubjectType: string(subjectType),
		SubjectId:   uint32(subjectID),
		ObjectType:  string(objectType),
		ObjectId:    uint32(objectID),
		Relation:    string(relation),
		Window:      snapshotWindowToProto(window),
		Visited:     visitedKeysToProto(visited),
	}

	resp, err := g.client.Check(ctx, req)
	if err != nil {
		return false, window, err
	}

	return resp.Allowed, snapshotWindowFromProto(resp.Window), nil
}

// CheckUnion delegates to the remote GraphService.CheckUnion RPC.
func (g *RemoteGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []RelationCheck,
	visited []VisitedKey,
) (CheckResult, error) {
	if len(checks) == 0 {
		return CheckResult{}, nil
	}

	req := &graphpb.CheckUnionRequest{
		SubjectType: string(subjectType),
		SubjectId:   uint32(subjectID),
		Checks:      relationChecksToProto(checks),
		Visited:     visitedKeysToProto(visited),
	}

	resp, err := g.client.CheckUnion(ctx, req)
	if err != nil {
		return CheckResult{}, err
	}

	return checkResultFromProto(resp), nil
}

// checkResultFromProto converts a proto CheckUnionResponse to CheckResult.
func checkResultFromProto(resp *graphpb.CheckUnionResponse) CheckResult {
	return CheckResult{
		Found:         resp.Allowed,
		DependentSets: dependentSetsFromProto(resp.DependentSets),
		Window:        snapshotWindowFromProto(resp.Window),
	}
}

// dependentSetsToProto converts DependentSets to proto representation.
func dependentSetsToProto(sets []DependentSet) []*graphpb.DependentSet {
	if sets == nil {
		return nil
	}
	result := make([]*graphpb.DependentSet, len(sets))
	for i, s := range sets {
		var objectIDs []byte
		if s.ObjectIDs != nil {
			objectIDs, _ = s.ObjectIDs.ToBytes()
		}
		result[i] = &graphpb.DependentSet{
			ObjectType: string(s.ObjectType),
			Relation:   string(s.Relation),
			ObjectIds:  objectIDs,
		}
	}
	return result
}

// dependentSetsFromProto converts proto DependentSets to Go type.
func dependentSetsFromProto(sets []*graphpb.DependentSet) []DependentSet {
	if sets == nil {
		return nil
	}
	result := make([]DependentSet, len(sets))
	for i, s := range sets {
		var bitmap *roaring.Bitmap
		if len(s.ObjectIds) > 0 {
			bitmap = roaring.New()
			bitmap.FromBuffer(s.ObjectIds)
		}
		result[i] = DependentSet{
			ObjectType: schema.TypeName(s.ObjectType),
			Relation:   schema.RelationName(s.Relation),
			ObjectIDs:  bitmap,
		}
	}
	return result
}

// Schema returns the authorization schema.
func (g *RemoteGraph) Schema() *schema.Schema {
	return g.schema
}

// Compile-time interface check
var _ Graph = (*RemoteGraph)(nil)

// snapshotWindowToProto converts a SnapshotWindow to its proto representation.
func snapshotWindowToProto(w SnapshotWindow) *graphpb.SnapshotWindow {
	return &graphpb.SnapshotWindow{
		Min: uint64(w.Min()),
		Max: uint64(w.Max()),
	}
}

// snapshotWindowFromProto converts a proto SnapshotWindow to the Go type.
// Handles the special case of MaxSnapshotWindow (min=0, max=MaxUint64).
func snapshotWindowFromProto(w *graphpb.SnapshotWindow) SnapshotWindow {
	if w == nil {
		return SnapshotWindow{}
	}
	// Detect MaxSnapshotWindow: min=0 and max=MaxUint64
	// Can't use NewSnapshotWindow for this because delta would overflow
	if w.Min == 0 && w.Max == math.MaxUint64 {
		return MaxSnapshotWindow
	}
	return NewSnapshotWindow(store.StoreTime(w.Min), store.StoreTime(w.Max))
}

// visitedKeysToProto converts a slice of VisitedKey to proto representation.
func visitedKeysToProto(visited []VisitedKey) []*graphpb.VisitedNode {
	if visited == nil {
		return nil
	}
	result := make([]*graphpb.VisitedNode, len(visited))
	for i, v := range visited {
		result[i] = &graphpb.VisitedNode{
			ObjectType: string(v.ObjectType),
			ObjectId:   uint32(v.ObjectID),
			Relation:   string(v.Relation),
		}
	}
	return result
}

// visitedKeysFromProto converts proto VisitedNodes to a slice of VisitedKey.
func visitedKeysFromProto(visited []*graphpb.VisitedNode) []VisitedKey {
	if visited == nil {
		return nil
	}
	result := make([]VisitedKey, len(visited))
	for i, v := range visited {
		result[i] = VisitedKey{
			ObjectType: schema.TypeName(v.ObjectType),
			ObjectID:   schema.ID(v.ObjectId),
			Relation:   schema.RelationName(v.Relation),
		}
	}
	return result
}

// relationChecksToProto converts a slice of RelationCheck to proto representation.
func relationChecksToProto(checks []RelationCheck) []*graphpb.RelationCheck {
	result := make([]*graphpb.RelationCheck, len(checks))
	for i, c := range checks {
		var objectIDs []byte
		if c.ObjectIDs != nil {
			objectIDs, _ = c.ObjectIDs.ToBytes()
		}
		result[i] = &graphpb.RelationCheck{
			ObjectType: string(c.ObjectType),
			ObjectIds:  objectIDs,
			Relation:   string(c.Relation),
			Window:     snapshotWindowToProto(c.Window),
		}
	}
	return result
}

// relationChecksFromProto converts proto RelationChecks to a slice of RelationCheck.
func relationChecksFromProto(checks []*graphpb.RelationCheck) ([]RelationCheck, error) {
	result := make([]RelationCheck, len(checks))
	for i, c := range checks {
		bitmap := roaring.New()
		if len(c.ObjectIds) > 0 {
			if _, err := bitmap.FromBuffer(c.ObjectIds); err != nil {
				return nil, err
			}
		}
		result[i] = RelationCheck{
			ObjectType: schema.TypeName(c.ObjectType),
			ObjectIDs:  bitmap,
			Relation:   schema.RelationName(c.Relation),
			Window:     snapshotWindowFromProto(c.Window),
		}
	}
	return result, nil
}
