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

// DependentSet identifies objects that were relevant to a check result.
type DependentSet struct {
	ObjectType schema.TypeName
	Relation   schema.RelationName

	// ObjectIDs identifies the specific objects that mattered.
	// If nil, means "all objects from the corresponding input check" (optimization).
	// If non-nil, the specific subset (e.g., single matching ID for union positive).
	ObjectIDs *roaring.Bitmap
}

// CheckResult represents the outcome of a check with provenance information.
type CheckResult struct {
	Found         bool
	DependentSets []DependentSet
	Window        SnapshotWindow // combined window for the result
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
	// The returned CheckResult includes:
	//   - Found: whether subject was found in any userset
	//   - DependentSets: which object sets were relevant to the decision
	//   - Window: combined snapshot window for the result
	CheckUnion(ctx context.Context,
		subjectType schema.TypeName, subjectID schema.ID,
		checks []RelationCheck,
		visited []VisitedKey,
	) (CheckResult, error)

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
	usersets      *MultiversionUsersets
	stream        store.ChangeStream
	st            store.Store
	observer      UsersetsObserver
	checkObserver CheckObserver
	localObserver LocalGraphObserver
}

// NewLocalGraph creates a new LocalGraph.
func NewLocalGraph(s *schema.Schema, stream store.ChangeStream, st store.Store) *LocalGraph {
	return &LocalGraph{
		usersets:      NewMultiversionUsersets(s),
		stream:        stream,
		st:            st,
		observer:      NoOpUsersetsObserver{},
		checkObserver: NoOpCheckObserver{},
		localObserver: NoOpLocalGraphObserver{},
	}
}

// WithUsersetsObserver returns a copy with the given UsersetsObserver for instrumentation.
func (g *LocalGraph) WithUsersetsObserver(obs UsersetsObserver) *LocalGraph {
	if obs == nil {
		obs = NoOpUsersetsObserver{}
	}
	return &LocalGraph{
		usersets:      g.usersets,
		stream:        g.stream,
		st:            g.st,
		observer:      obs,
		checkObserver: g.checkObserver,
		localObserver: g.localObserver,
	}
}

// WithCheckObserver returns a copy with the given CheckObserver for instrumentation.
func (g *LocalGraph) WithCheckObserver(obs CheckObserver) *LocalGraph {
	if obs == nil {
		obs = NoOpCheckObserver{}
	}
	return &LocalGraph{
		usersets:      g.usersets,
		stream:        g.stream,
		st:            g.st,
		observer:      g.observer,
		checkObserver: obs,
		localObserver: g.localObserver,
	}
}

// WithGraphObserver returns a copy with the given LocalGraphObserver for instrumentation.
func (g *LocalGraph) WithGraphObserver(obs LocalGraphObserver) *LocalGraph {
	if obs == nil {
		obs = NoOpLocalGraphObserver{}
	}
	return &LocalGraph{
		usersets:      g.usersets,
		stream:        g.stream,
		st:            g.st,
		observer:      g.observer,
		checkObserver: g.checkObserver,
		localObserver: obs,
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
	g.assertWindowWithinReplicated(window)

	// LocalGraphObserver for public API level
	ctx, localProbe := g.localObserver.CheckStarted(ctx, subjectType, subjectID, objectType, objectID, relation)
	defer localProbe.End()

	// Pass CheckObserver to check() - it creates its own probe internally
	found, resultWindow, err := Check(ctx, g, g.usersets, g.checkObserver,
		subjectType, subjectID, objectType, objectID, relation,
		window, visited)

	if err != nil {
		localProbe.Error(err)
	} else {
		localProbe.Result(found, resultWindow)
	}

	return found, resultWindow, err
}

// CheckUnion checks if subject is in the union of all the given usersets.
func (g *LocalGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []RelationCheck,
	visited []VisitedKey,
) (CheckResult, error) {
	// LocalGraphObserver for public API level
	ctx, localProbe := g.localObserver.CheckUnionStarted(ctx, subjectType, subjectID, len(checks))
	defer localProbe.End()

	if len(checks) == 0 {
		return CheckResult{}, nil
	}

	// Validate all windows are within replicated time
	for _, chk := range checks {
		g.assertWindowWithinReplicated(chk.Window)
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

			ok, resultWindow, err := Check(ctx, g, g.usersets, g.checkObserver,
				subjectType, subjectID, chk.ObjectType, objectID, chk.Relation,
				chk.Window, visited)
			if err != nil {
				localProbe.Error(err)
				return CheckResult{Window: chk.Window}, err
			}

			if ok {
				// Found: return single matching object
				matchBitmap := roaring.New()
				matchBitmap.Add(uint32(objectID))
				result := CheckResult{
					Found: true,
					DependentSets: []DependentSet{{
						ObjectType: chk.ObjectType,
						Relation:   chk.Relation,
						ObjectIDs:  matchBitmap,
					}},
					Window: resultWindow,
				}
				localProbe.Result(result)
				return result, nil
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
		result := CheckResult{}
		localProbe.Result(result)
		return result, nil
	}

	// Not found: all input checks were relevant (nil ObjectIDs = reference to input)
	dependentSets := make([]DependentSet, 0, len(checks))
	for _, chk := range checks {
		if chk.ObjectIDs != nil && !chk.ObjectIDs.IsEmpty() {
			dependentSets = append(dependentSets, DependentSet{
				ObjectType: chk.ObjectType,
				Relation:   chk.Relation,
				ObjectIDs:  nil, // nil = "all objects from input check"
			})
		}
	}
	result := CheckResult{
		Found:         false,
		DependentSets: dependentSets,
		Window:        tightestWindow,
	}
	localProbe.Result(result)
	return result, nil
}

// ValidateTuple checks if a tuple is valid according to the schema.
func (g *LocalGraph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *LocalGraph) ReplicatedTime() store.StoreTime {
	return g.usersets.ReplicatedTime()
}

// assertWindowWithinReplicated panics if window.Min() > replicatedTime.
// This ensures we never try to read data that hasn't been replicated yet.
func (g *LocalGraph) assertWindowWithinReplicated(window SnapshotWindow) {
	replicatedTime := g.usersets.ReplicatedTime()
	if window.Min() > replicatedTime {
		panic("check received min > replicated time - caller ahead of this replica")
	}
}

// Compile-time interface checks
var (
	_ Graph        = (*LocalGraph)(nil)
	_ GraphService = (*LocalGraph)(nil)
)
