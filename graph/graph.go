// Package graph implements the in-memory authorization graph with roaring
// bitmaps for efficient set operations and MVCC for snapshot isolation.
package graph

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// RelationCheck represents a check against a single object type's relation.
// Used by CheckUnion to batch multiple checks with independent windows.
type RelationCheck struct {
	ObjectType schema.TypeName
	ObjectIDs  *roaring.Bitmap
	Relation   schema.RelationName
	Window     SnapshotWindow // Window narrowed based on reading this type's data
}

// Graph provides core authorization check operations.
// This interface can be implemented by both local and remote graphs.
type Graph interface {
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

	// CheckUnion checks if subject is in the union of all the given usersets.
	// Returns true if subject has the relation on ANY of the objects across all checks.
	// Each RelationCheck has its own window, allowing independent narrowing per type.
	//
	// Return semantics:
	//   - Found: returns window from the successful check
	//   - Not found: returns tightest window across all checks (max of mins, min of maxes)
	CheckUnion(ctx context.Context,
		subjectType schema.TypeName, subjectID schema.ID,
		checks []RelationCheck,
		visited []VisitedKey,
	) (bool, SnapshotWindow, error)

	// Schema returns the authorization schema.
	Schema() *schema.Schema
}

// GraphService extends Graph with lifecycle management.
// Used for in-process graphs that need hydration and change subscription.
type GraphService interface {
	Graph

	// Start hydrates from the store and subscribes to the change stream.
	// This blocks until the context is canceled or an error occurs.
	Start(ctx context.Context) error
}

// LocalGraph is a single-node Graph implementation.
// It owns the MultiversionUsersets data and handles all checks locally.
type LocalGraph struct {
	usersets *MultiversionUsersets
	stream   store.ChangeStream
	st       store.Store
	observer UsersetsObserver
}

// NewLocalGraph creates a new LocalGraph.
func NewLocalGraph(s *schema.Schema, stream store.ChangeStream, st store.Store) *LocalGraph {
	return &LocalGraph{
		usersets: NewMultiversionUsersets(s),
		stream:   stream,
		st:       st,
		observer: NoOpUsersetsObserver{},
	}
}

// WithObserver returns a copy with the given observer for instrumentation.
func (g *LocalGraph) WithObserver(obs UsersetsObserver) *LocalGraph {
	if obs == nil {
		obs = NoOpUsersetsObserver{}
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

// CheckUnion checks if subject is in the union of all the given usersets.
func (g *LocalGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []RelationCheck,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	if len(checks) == 0 {
		return false, SnapshotWindow{}, nil
	}

	var tightestWindow SnapshotWindow
	first := true

	for _, chk := range checks {
		if chk.ObjectIDs == nil || chk.ObjectIDs.IsEmpty() {
			continue
		}

		iter := chk.ObjectIDs.Iterator()
		for iter.HasNext() {
			objectID := schema.ID(iter.Next())
			ok, resultWindow, err := check(ctx, g, g.usersets,
				subjectType, subjectID, chk.ObjectType, objectID, chk.Relation,
				chk.Window, visited)
			if err != nil {
				return false, chk.Window, err
			}
			if ok {
				return true, resultWindow, nil
			}
			// Track tightest window for "not found" case
			if first {
				tightestWindow = resultWindow
				first = false
			} else {
				tightestWindow = tightestWindow.Intersect(resultWindow)
			}
		}
	}

	if first {
		// No checks were performed (all empty)
		return false, SnapshotWindow{}, nil
	}
	return false, tightestWindow, nil
}

// ValidateTuple checks if a tuple is valid according to the schema.
func (g *LocalGraph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *LocalGraph) ReplicatedTime() store.StoreTime {
	return g.usersets.ReplicatedTime()
}

// Compile-time interface checks
var (
	_ Graph        = (*LocalGraph)(nil)
	_ GraphService = (*LocalGraph)(nil)
)
