package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
)

// RoutedGroup represents objects grouped by destination with the client to call.
// Used by Router.GroupByDestination to batch requests to the same node.
type RoutedGroup struct {
	Client  GraphClient
	Objects []ObjectSet // Objects routed to this client (may be subsets if sharding splits by ID range)
}

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

	// GroupByDestination groups objects by their destination node.
	// This is used for scatter-gather: the caller passes all objects to check,
	// and the router groups them by destination, keeping bitmaps compressed.
	//
	// For single-node deployments, this returns a single group with all objects.
	// For sharded deployments, objects may be split across multiple groups,
	// and bitmaps may be split by ID range.
	GroupByDestination(ctx context.Context, objects []ObjectSet) ([]RoutedGroup, error)
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

// GroupByDestination returns a single group containing all objects.
// In single-node mode, everything goes to the same local client.
func (r *LocalRouter) GroupByDestination(ctx context.Context, objects []ObjectSet) ([]RoutedGroup, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	return []RoutedGroup{
		{
			Client:  r.client,
			Objects: objects,
		},
	}, nil
}
