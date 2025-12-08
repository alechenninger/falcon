// Package graph implements the in-memory authorization graph with roaring
// bitmaps for efficient set operations and MVCC for snapshot isolation.
package graph

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// Graph provides authorization check operations on the relationship graph.
// Implementations manage the underlying data and handle distributed dispatch.
type Graph interface {
	// Start hydrates from the store and subscribes to the change stream.
	// This blocks until the context is canceled or an error occurs.
	Start(ctx context.Context) error

	// Check determines if the subject has the relation on the object.
	// The window constrains what snapshot times can be used; pass MaxSnapshotWindow
	// for an unconstrained query. The visited slice tracks nodes for cycle
	// detection; pass nil for a fresh query.
	// Returns (allowed, narrowedWindow, error).
	Check(ctx context.Context,
		subjectType schema.TypeName, subjectID schema.ID,
		objectType schema.TypeName, objectID schema.ID,
		relation schema.RelationName,
		window SnapshotWindow, visited []VisitedKey,
	) (bool, SnapshotWindow, error)

	// BatchCheck checks if subject has relation on ANY of the given objects.
	// Short-circuits on first true. Objects are passed as a bitmap for efficiency.
	// Returns (allowed, narrowedWindow, error).
	BatchCheck(ctx context.Context,
		subjectType schema.TypeName, subjectID schema.ID,
		objectType schema.TypeName, objectIDs *roaring.Bitmap,
		relation schema.RelationName,
		window SnapshotWindow, visited []VisitedKey,
	) (bool, SnapshotWindow, error)

	// Schema returns the authorization schema.
	Schema() *schema.Schema
}

// LocalGraph is a single-node Graph implementation.
// It owns the MultiversionUsersets data and handles all checks locally.
type LocalGraph struct {
	usersets *MultiversionUsersets
	stream   store.ChangeStream
	st       store.Store
	observer GraphObserver
}

// NewLocalGraph creates a new LocalGraph.
func NewLocalGraph(s *schema.Schema, stream store.ChangeStream, st store.Store) *LocalGraph {
	return &LocalGraph{
		usersets: NewMultiversionUsersets(s),
		stream:   stream,
		st:       st,
		observer: NoOpGraphObserver{},
	}
}

// WithObserver returns a copy with the given observer for instrumentation.
func (g *LocalGraph) WithObserver(obs GraphObserver) *LocalGraph {
	if obs == nil {
		obs = NoOpGraphObserver{}
	}
	return &LocalGraph{
		usersets: g.usersets,
		stream:   g.stream,
		st:       g.st,
		observer: obs,
	}
}

// Start hydrates from store and subscribes to changes.
func (g *LocalGraph) Start(ctx context.Context) error {
	// Hydrate from store
	iter, err := g.st.LoadAll(ctx)
	if err != nil {
		return err
	}
	if err := g.usersets.Hydrate(iter); err != nil {
		iter.Close()
		return err
	}
	iter.Close()

	// Subscribe to changes
	return g.usersets.Subscribe(ctx, g.stream, g.observer)
}

// Schema returns the authorization schema.
func (g *LocalGraph) Schema() *schema.Schema {
	return g.usersets.Schema()
}

// Check determines if subject has relation on object.
func (g *LocalGraph) Check(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow, visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	return check(ctx, g, g.usersets,
		subjectType, subjectID, objectType, objectID, relation,
		window, visited)
}

// BatchCheck checks if subject has relation on ANY of the objects.
func (g *LocalGraph) BatchCheck(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectIDs *roaring.Bitmap,
	relation schema.RelationName,
	window SnapshotWindow, visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	if objectIDs == nil || objectIDs.IsEmpty() {
		return false, window, nil
	}

	iter := objectIDs.Iterator()
	for iter.HasNext() {
		objectID := schema.ID(iter.Next())
		ok, newWindow, err := check(ctx, g, g.usersets,
			subjectType, subjectID, objectType, objectID, relation,
			window, visited)
		if err != nil {
			return false, window, err
		}
		window = newWindow
		if ok {
			return true, window, nil
		}
	}

	return false, window, nil
}

// ValidateTuple checks if a tuple is valid according to the schema.
func (g *LocalGraph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *LocalGraph) ReplicatedTime() store.StoreTime {
	return g.usersets.ReplicatedTime()
}

// Compile-time interface check
var _ Graph = (*LocalGraph)(nil)
