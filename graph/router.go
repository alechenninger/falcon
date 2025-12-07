package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
)

// Router determines which GraphClient handles a given object's tuples.
// It abstracts the sharding topology from the check algorithm.
//
// When the check algorithm needs to evaluate a relation on an object
// (e.g., after traversing an arrow), it calls Route to get the appropriate
// client. The Router returns either a LocalGraphClient (for local objects)
// or a RemoteGraphClient (for objects on other shards).
type Router interface {
	// Route returns a GraphClient for the node that owns the given object's tuples.
	// All relations for an object are on the same shard, so routing is per-object.
	Route(ctx context.Context, objectType schema.TypeName, objectID schema.ID) (GraphClient, error)
}

// LocalRouter always returns the same local client.
// This is used for single-node deployments or testing.
type LocalRouter struct {
	client GraphClient
}

// NewLocalRouter creates a LocalRouter that always returns the given client.
func NewLocalRouter(client GraphClient) *LocalRouter {
	return &LocalRouter{client: client}
}

// Route always returns the local client, ignoring the object type and ID.
func (r *LocalRouter) Route(ctx context.Context, objectType schema.TypeName, objectID schema.ID) (GraphClient, error) {
	return r.client, nil
}

