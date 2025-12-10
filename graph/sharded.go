package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// ShardID identifies a shard in a distributed graph.
type ShardID string

// Router determines which shard owns a given object.
// This is called during Check and CheckUnion to route requests.
type Router func(objectType schema.TypeName, objectID schema.ID) ShardID

// ShardedGraph is a distributed Graph implementation that routes checks
// to the appropriate shard based on object ownership.
//
// It owns a MultiversionUsersets for local data and has references to
// other Graph instances for remote shards. The check algorithm calls
// back to ShardedGraph.CheckUnion for recursion, enabling cross-shard routing.
type ShardedGraph struct {
	localShardID    ShardID
	usersets        *MultiversionUsersets
	shards          map[ShardID]Graph // remote shards (does NOT include self)
	router          Router
	stream          store.ChangeStream
	st              store.Store
	observer        UsersetsObserver
	shardedObserver ShardedGraphObserver
	checkObserver   CheckObserver
}

// NewShardedGraph creates a new ShardedGraph.
//
// Parameters:
//   - localShardID: the ID of this shard
//   - s: the authorization schema
//   - router: function to determine which shard owns an object
//   - shards: map of remote shard IDs to their Graph instances (should NOT include self)
//   - stream: change stream for subscribing to updates
//   - st: store for initial hydration
func NewShardedGraph(
	localShardID ShardID,
	s *schema.Schema,
	router Router,
	shards map[ShardID]Graph,
	stream store.ChangeStream,
	st store.Store,
) *ShardedGraph {
	return &ShardedGraph{
		localShardID:    localShardID,
		usersets:        NewMultiversionUsersets(s),
		shards:          shards,
		router:          router,
		stream:          stream,
		st:              st,
		observer:        NoOpUsersetsObserver{},
		shardedObserver: NoOpShardedGraphObserver{},
		checkObserver:   NoOpCheckObserver{},
	}
}

// WithUsersetsObserver returns a copy with the given UsersetsObserver for instrumentation.
func (g *ShardedGraph) WithUsersetsObserver(obs UsersetsObserver) *ShardedGraph {
	if obs == nil {
		obs = NoOpUsersetsObserver{}
	}
	return &ShardedGraph{
		localShardID:    g.localShardID,
		usersets:        g.usersets,
		shards:          g.shards,
		router:          g.router,
		stream:          g.stream,
		st:              g.st,
		observer:        obs,
		shardedObserver: g.shardedObserver,
		checkObserver:   g.checkObserver,
	}
}

// WithGraphObserver returns a copy with the given ShardedGraphObserver for instrumentation.
func (g *ShardedGraph) WithGraphObserver(obs ShardedGraphObserver) *ShardedGraph {
	if obs == nil {
		obs = NoOpShardedGraphObserver{}
	}
	return &ShardedGraph{
		localShardID:    g.localShardID,
		usersets:        g.usersets,
		shards:          g.shards,
		router:          g.router,
		stream:          g.stream,
		st:              g.st,
		observer:        g.observer,
		shardedObserver: obs,
		checkObserver:   g.checkObserver,
	}
}

// WithCheckObserver returns a copy with the given CheckObserver for instrumentation.
func (g *ShardedGraph) WithCheckObserver(obs CheckObserver) *ShardedGraph {
	if obs == nil {
		obs = NoOpCheckObserver{}
	}
	return &ShardedGraph{
		localShardID:    g.localShardID,
		usersets:        g.usersets,
		shards:          g.shards,
		router:          g.router,
		stream:          g.stream,
		st:              g.st,
		observer:        g.observer,
		shardedObserver: g.shardedObserver,
		checkObserver:   obs,
	}
}

// Start hydrates from store and subscribes to changes.
// Only tuples for objects owned by this shard are loaded and applied.
func (g *ShardedGraph) Start(ctx context.Context) error {
	// Hydrate from store - filter to only local tuples
	iter, err := g.st.LoadAll(ctx)
	if err != nil {
		return err
	}
	filteredIter := &filteringTupleIterator{
		inner:   iter,
		include: g.ownsObject,
	}
	if err := g.usersets.Hydrate(filteredIter); err != nil {
		iter.Close()
		return err
	}
	iter.Close()

	// Subscribe to changes - filter to only local changes
	filteredStream := &filteringChangeStream{
		inner:   g.stream,
		include: g.ownsObject,
	}
	return g.usersets.Subscribe(ctx, filteredStream, g.observer)
}

// ownsObject returns true if this shard owns the given object.
func (g *ShardedGraph) ownsObject(objectType schema.TypeName, objectID schema.ID) bool {
	return g.router(objectType, objectID) == g.localShardID
}

// Schema returns the authorization schema.
func (g *ShardedGraph) Schema() *schema.Schema {
	return g.usersets.Schema()
}

// Check determines if subject has relation on object.
// Routes to the appropriate shard based on the object's ownership.
func (g *ShardedGraph) Check(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow, visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	targetShard := g.router(objectType, objectID)

	if targetShard == g.localShardID {
		// Local check - validate window is within replicated time
		g.assertWindowWithinReplicated(window)

		return Check(ctx, g, g.usersets, g.checkObserver,
			subjectType, subjectID, objectType, objectID, relation,
			window, visited)
	}

	// Remote check - delegate to the appropriate shard
	remoteShard, ok := g.shards[targetShard]
	if !ok {
		// Unknown shard - this shouldn't happen with a properly configured router
		// For now, fall back to local (in production this would be an error)
		return Check(ctx, g, g.usersets, g.checkObserver,
			subjectType, subjectID, objectType, objectID, relation,
			window, visited)
	}

	return remoteShard.Check(ctx, subjectType, subjectID, objectType, objectID, relation, window, visited)
}

// checkUnionResult holds the result of a remote CheckUnion call.
type checkUnionResult struct {
	shardID ShardID
	result  CheckResult
	err     error
}

// CheckUnion checks if subject is in the union of all the given usersets.
// Groups checks by shard and dispatches accordingly.
// Remote shard checks run in parallel and cancel when any returns true.
// If some shards error but none return true, returns an inconclusive error.
func (g *ShardedGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []RelationCheck,
	visited []VisitedKey,
) (CheckResult, error) {
	ctx, probe := g.shardedObserver.CheckUnionStarted(ctx, subjectType, subjectID)
	defer probe.End()

	if len(checks) == 0 {
		probe.Empty()
		return CheckResult{}, nil
	}

	// Partition checks by shard
	// For each RelationCheck, we need to split the bitmap by shard
	localChecks := make([]RelationCheck, 0)
	remoteChecks := make(map[ShardID][]RelationCheck)

	for _, chk := range checks {
		if chk.ObjectIDs == nil || chk.ObjectIDs.IsEmpty() {
			continue
		}

		// Partition this check's object IDs by shard
		localBitmap := roaring.New()
		remoteBitmaps := make(map[ShardID]*roaring.Bitmap)

		iter := chk.ObjectIDs.Iterator()
		for iter.HasNext() {
			objectID := schema.ID(iter.Next())
			targetShard := g.router(chk.ObjectType, objectID)

			if targetShard == g.localShardID {
				localBitmap.Add(uint32(objectID))
			} else {
				if remoteBitmaps[targetShard] == nil {
					remoteBitmaps[targetShard] = roaring.New()
				}
				remoteBitmaps[targetShard].Add(uint32(objectID))
			}
		}

		// Add local check if any local objects
		if !localBitmap.IsEmpty() {
			localChecks = append(localChecks, RelationCheck{
				ObjectType: chk.ObjectType,
				ObjectIDs:  localBitmap,
				Relation:   chk.Relation,
				Window:     chk.Window,
			})
		}

		// Add remote checks
		for shardID, bitmap := range remoteBitmaps {
			remoteChecks[shardID] = append(remoteChecks[shardID], RelationCheck{
				ObjectType: chk.ObjectType,
				ObjectIDs:  bitmap,
				Relation:   chk.Relation,
				Window:     chk.Window,
			})
		}
	}

	var tightestWindow SnapshotWindow
	first := true

	// Validate windows for local checks are within replicated time
	for _, chk := range localChecks {
		g.assertWindowWithinReplicated(chk.Window)
	}

	// Process local checks first (fast path - no network)
	for _, chk := range localChecks {
		iter := chk.ObjectIDs.Iterator()
		for iter.HasNext() {
			objectID := schema.ID(iter.Next())

			ok, resultWindow, err := Check(ctx, g, g.usersets, g.checkObserver,
				subjectType, subjectID, chk.ObjectType, objectID, chk.Relation,
				chk.Window, visited)
			if err != nil {
				return CheckResult{Window: chk.Window}, err
			}

			if ok {
				probe.Result(true, resultWindow)
				// Found: return single matching object
				matchBitmap := roaring.New()
				matchBitmap.Add(uint32(objectID))
				return CheckResult{
					Found: true,
					DependentSets: []DependentSet{{
						ObjectType: chk.ObjectType,
						Relation:   chk.Relation,
						ObjectIDs:  matchBitmap,
					}},
					Window: resultWindow,
				}, nil
			}
			// Track tightest window for "not found" case
			if first {
				tightestWindow = resultWindow
				first = false
			} else {
				// TODO: We would need to defer intersection to avoid creating an invalid window
				// when there may be a true result coming.
				// If all false, then we need to wait & retry to avoid invalid window.
				tightestWindow = tightestWindow.Intersect(resultWindow)
			}
		}
	}

	// Process remote checks in parallel
	if len(remoteChecks) == 0 {
		if first {
			probe.Empty()
			return CheckResult{}, nil
		}
		probe.Result(false, tightestWindow)
		return buildNotFoundResult(checks, tightestWindow), nil
	}

	// Create a cancellable context for remote checks
	remoteCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Channel to collect results from remote shards
	results := make(chan checkUnionResult, len(remoteChecks))

	// Launch remote checks in parallel
	var wg sync.WaitGroup
	for shardID, shardChecks := range remoteChecks {
		remoteShard, ok := g.shards[shardID]
		if !ok {
			probe.UnknownShard(shardID)
			continue
		}

		wg.Add(1)
		probe.RemoteShardDispatched(shardID)
		go func(sid ShardID, shard Graph, checks []RelationCheck) {
			defer wg.Done()

			result, err := shard.CheckUnion(remoteCtx, subjectType, subjectID, checks, visited)

			// Check if context was cancelled before sending
			select {
			case <-remoteCtx.Done():
				// Context cancelled, don't send result
				return
			default:
			}

			results <- checkUnionResult{
				shardID: sid,
				result:  result,
				err:     err,
			}
		}(shardID, remoteShard, shardChecks)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Track shards that failed
	var failedShards []ShardID

	// Collect results
	for result := range results {
		if result.err != nil {
			// Report error via observer but continue processing other shards
			probe.RemoteShardError(result.shardID, result.err)
			failedShards = append(failedShards, result.shardID)
			continue
		}
		probe.RemoteShardResult(result.shardID, result.result.Found, result.result.Window)
		if result.result.Found {
			// Found! Cancel remaining checks and return immediately
			cancel()
			probe.Result(true, result.result.Window)
			return result.result, nil
		}
		// Track tightest window for "not found" case
		if first {
			tightestWindow = result.result.Window
			first = false
		} else {
			tightestWindow = tightestWindow.Intersect(result.result.Window)
		}
	}

	// Check if we had any errors
	if len(failedShards) > 0 {
		probe.Inconclusive(failedShards)
		return CheckResult{}, fmt.Errorf("check inconclusive: shards failed: %v", failedShards)
	}

	if first {
		// No checks were performed (all empty)
		probe.Empty()
		return CheckResult{}, nil
	}
	probe.Result(false, tightestWindow)
	return buildNotFoundResult(checks, tightestWindow), nil
}

// buildNotFoundResult creates a CheckResult for the "not found" case,
// referencing all input checks as relevant.
func buildNotFoundResult(checks []RelationCheck, window SnapshotWindow) CheckResult {
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
	return CheckResult{
		Found:         false,
		DependentSets: dependentSets,
		Window:        window,
	}
}

// ValidateTuple checks if a tuple is valid according to the schema.
func (g *ShardedGraph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *ShardedGraph) ReplicatedTime() store.StoreTime {
	return g.usersets.ReplicatedTime()
}

// TODO: this is temporary; remove this after we have a proper hydration protocol.
// SetReplicatedTime sets the replicated time after hydration.
// Used for static/test scenarios where data is loaded without a change stream.
func (g *ShardedGraph) SetReplicatedTime(t store.StoreTime) {
	g.usersets.SetReplicatedTime(t)
}

// assertWindowWithinReplicated panics if window.Min() > replicatedTime.
// This ensures we never try to read data that hasn't been replicated yet.
func (g *ShardedGraph) assertWindowWithinReplicated(window SnapshotWindow) {
	replicatedTime := g.usersets.ReplicatedTime()
	if window.Min() > replicatedTime {
		panic("check received min > replicated time - caller ahead of this replica")
	}
}

// SetRemoteShard adds or updates a remote shard reference.
// This is used during initialization to wire up cross-shard references.
func (g *ShardedGraph) SetRemoteShard(id ShardID, shard Graph) {
	if g.shards == nil {
		g.shards = make(map[ShardID]Graph)
	}
	g.shards[id] = shard
}

// Compile-time interface checks
var (
	_ Graph        = (*ShardedGraph)(nil)
	_ GraphService = (*ShardedGraph)(nil)
)

// filteringTupleIterator wraps a TupleIterator and only yields tuples matching the filter.
type filteringTupleIterator struct {
	inner   store.TupleIterator
	include func(schema.TypeName, schema.ID) bool
	current store.Tuple
}

func (f *filteringTupleIterator) Next() bool {
	for f.inner.Next() {
		t := f.inner.Tuple()
		if f.include(t.ObjectType, t.ObjectID) {
			f.current = t
			return true
		}
	}
	return false
}

func (f *filteringTupleIterator) Tuple() store.Tuple {
	return f.current
}

func (f *filteringTupleIterator) Err() error {
	return f.inner.Err()
}

func (f *filteringTupleIterator) Close() error {
	return f.inner.Close()
}

// filteringChangeStream wraps a ChangeStream and filters tuple data from non-local changes.
// Non-local changes are transformed into empty changes (with zero-value tuple) so that
// replicatedTime still advances, but no data is applied.
type filteringChangeStream struct {
	inner   store.ChangeStream
	include func(schema.TypeName, schema.ID) bool
}

func (f *filteringChangeStream) Subscribe(ctx context.Context, after store.StoreTime) (<-chan store.Change, <-chan error) {
	innerChanges, innerErrs := f.inner.Subscribe(ctx, after)

	changes := make(chan store.Change, 100)
	errs := make(chan error, 1)

	go func() {
		defer close(changes)
		for {
			select {
			case change, ok := <-innerChanges:
				if !ok {
					return
				}
				// Transform non-local changes to empty changes (still advances time)
				if !f.include(change.Tuple.ObjectType, change.Tuple.ObjectID) {
					change.Tuple = store.Tuple{} // Clear tuple data
				}
				select {
				case changes <- change:
				case <-ctx.Done():
					return
				}
			case err := <-innerErrs:
				if err != nil {
					errs <- err
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return changes, errs
}

func (f *filteringChangeStream) CurrentTime(ctx context.Context) (store.StoreTime, error) {
	return f.inner.CurrentTime(ctx)
}

// Compile-time interface checks for filtering types
var (
	_ store.TupleIterator = (*filteringTupleIterator)(nil)
	_ store.ChangeStream  = (*filteringChangeStream)(nil)
)
