package graph

import (
	infragrpc "github.com/alechenninger/falcon/internal/infrastructure/grpc"
	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
)

// RemoteGraph implements the Graph interface by delegating to a remote
// gRPC GraphService.
// Deprecated: Use infragrpc.RemoteGraph instead.
type RemoteGraph = infragrpc.RemoteGraph

// NewRemoteGraph creates a new RemoteGraph that delegates to the given gRPC client.
// Deprecated: Use infragrpc.NewRemoteGraph instead.
func NewRemoteGraph(client graphpb.GraphServiceClient, s *schema.Schema) *RemoteGraph {
	return infragrpc.NewRemoteGraph(client, s)
}

// Re-export proto conversion functions for backward compatibility.
// These are used by server.go.

// snapshotWindowToProto converts a SnapshotWindow to its proto representation.
// Deprecated: Use infragrpc.SnapshotWindowToProto instead.
var snapshotWindowToProto = infragrpc.SnapshotWindowToProto

// snapshotWindowFromProto converts a proto SnapshotWindow to the Go type.
// Deprecated: Use infragrpc.SnapshotWindowFromProto instead.
var snapshotWindowFromProto = infragrpc.SnapshotWindowFromProto

// visitedKeysToProto converts a slice of VisitedKey to proto representation.
// Deprecated: Use infragrpc.VisitedKeysToProto instead.
var visitedKeysToProto = infragrpc.VisitedKeysToProto

// visitedKeysFromProto converts proto VisitedNodes to a slice of VisitedKey.
// Deprecated: Use infragrpc.VisitedKeysFromProto instead.
var visitedKeysFromProto = infragrpc.VisitedKeysFromProto

// relationChecksToProto converts a slice of RelationCheck to proto representation.
// Deprecated: Use infragrpc.RelationChecksToProto instead.
var relationChecksToProto = infragrpc.RelationChecksToProto

// relationChecksFromProto converts proto RelationChecks to a slice of RelationCheck.
// Deprecated: Use infragrpc.RelationChecksFromProto instead.
var relationChecksFromProto = infragrpc.RelationChecksFromProto

// dependentSetsToProto converts DependentSets to proto representation.
// Deprecated: Use infragrpc.DependentSetsToProto instead.
var dependentSetsToProto = infragrpc.DependentSetsToProto

// dependentSetsFromProto converts proto DependentSets to Go type.
// Deprecated: Use infragrpc.DependentSetsFromProto instead.
var dependentSetsFromProto = infragrpc.DependentSetsFromProto
