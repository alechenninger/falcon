package domain

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	
)

// LocalGraph is a single-node Graph implementation.
// It owns the MultiversionUsersets data and handles all checks locally.
type LocalGraph struct {
	usersets      *MultiversionUsersets
	stream        ChangeStream
	st            Store
	observer      UsersetsObserver
	checkObserver CheckObserver
	localObserver LocalGraphObserver
}

// NewLocalGraph creates a new LocalGraph.
func NewLocalGraph(s *Schema, stream ChangeStream, st Store) *LocalGraph {
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

// SetUsersetsObserver sets the UsersetsObserver for instrumentation.
// This mutates the receiver in place (useful for test setup).
func (g *LocalGraph) SetUsersetsObserver(obs UsersetsObserver) {
	if obs == nil {
		obs = NoOpUsersetsObserver{}
	}
	g.observer = obs
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

// Schema returns the authorization 
func (g *LocalGraph) Schema() *Schema {
	return g.usersets.Schema()
}

// Check determines if subject has relation on object.
func (g *LocalGraph) Check(ctx context.Context,
	subjectType TypeID, subjectID ID,
	objectType TypeID, objectID ID,
	relation RelationID,
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
	subjectType TypeID, subjectID ID,
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
			objectID := ID(iter.Next())

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

// ValidateTuple checks if a tuple is valid according to the 
func (g *LocalGraph) ValidateTuple(objectType TypeName, relation RelationName, subjectType TypeName, subjectRelation RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *LocalGraph) ReplicatedTime() StoreTime {
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

// TruncateHistory removes undo entries older than the given time.
// This is used for garbage collection tests.
func (g *LocalGraph) TruncateHistory(minTime StoreTime) {
	g.usersets.TruncateHistory(minTime)
}

// Compile-time interface checks
var (
	_ Graph        = (*LocalGraph)(nil)
	_ GraphService = (*LocalGraph)(nil)
)
