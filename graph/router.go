package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// RoutedGroup represents objects grouped by destination with the client to call.
// Used by Router.GroupByDestination to batch requests to the same node.
type RoutedGroup struct {
	Client  GraphClient
	Objects []ObjectSet // Objects routed to this client (may be subsets if sharding splits by ID range)
}

// Router determines which GraphClient handles a given object's tuples and
// provides access to the change stream and tuple loading for hydration.
//
// The Router is the single gateway for all read operations in sharded mode:
// - Query routing: Route and GroupByDestination for dispatching checks
// - Change streaming: Subscribe and CurrentTime for receiving updates
// - Bulk loading: LoadAll for initial hydration
//
// In sharded deployments, the Router filters changes and tuples to only those
// owned by this node. In single-node mode (LocalRouter), everything passes through.
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

	// ChangeStream methods for receiving tuple changes.
	// In sharded mode, only changes for locally-owned objects are emitted.
	store.ChangeStream

	// LoadAll returns an iterator over tuples for locally-owned objects.
	// Used for initial hydration on startup.
	LoadAll(ctx context.Context) (store.TupleIterator, error)
}

// LocalRouter always returns the same local client and passes through all
// changes and tuples. This is used for single-node deployments or testing.
type LocalRouter struct {
	client GraphClient
	stream store.ChangeStream
	st     store.Store
}

// NewLocalRouter creates a LocalRouter that always returns the given client.
// The stream and store are used for Subscribe/CurrentTime and LoadAll respectively.
func NewLocalRouter(client GraphClient, stream store.ChangeStream, st store.Store) *LocalRouter {
	return &LocalRouter{
		client: client,
		stream: stream,
		st:     st,
	}
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

// Subscribe delegates to the underlying ChangeStream.
// In single-node mode, all changes pass through unfiltered.
func (r *LocalRouter) Subscribe(ctx context.Context, after store.StoreTime) (<-chan store.Change, <-chan error) {
	return r.stream.Subscribe(ctx, after)
}

// CurrentTime delegates to the underlying ChangeStream.
func (r *LocalRouter) CurrentTime(ctx context.Context) (store.StoreTime, error) {
	return r.stream.CurrentTime(ctx)
}

// LoadAll delegates to the underlying Store.
// In single-node mode, all tuples pass through unfiltered.
func (r *LocalRouter) LoadAll(ctx context.Context) (store.TupleIterator, error) {
	return r.st.LoadAll(ctx)
}

// Compile-time interface check
var _ Router = (*LocalRouter)(nil)
