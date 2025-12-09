package graph

import (
	"context"
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
	localShardID ShardID
	usersets     *MultiversionUsersets
	shards       map[ShardID]Graph // remote shards (does NOT include self)
	router       Router
	stream       store.ChangeStream
	st           store.Store
	observer     GraphObserver
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
		localShardID: localShardID,
		usersets:     NewMultiversionUsersets(s),
		shards:       shards,
		router:       router,
		stream:       stream,
		st:           st,
		observer:     NoOpGraphObserver{},
	}
}

// WithObserver returns a copy with the given observer for instrumentation.
func (g *ShardedGraph) WithObserver(obs GraphObserver) *ShardedGraph {
	if obs == nil {
		obs = NoOpGraphObserver{}
	}
	return &ShardedGraph{
		localShardID: g.localShardID,
		usersets:     g.usersets,
		shards:       g.shards,
		router:       g.router,
		stream:       g.stream,
		st:           g.st,
		observer:     obs,
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
		// Local check - use our usersets but pass self as graph for recursion
		return check(ctx, g, g.usersets,
			subjectType, subjectID, objectType, objectID, relation,
			window, visited)
	}

	// Remote check - delegate to the appropriate shard
	remoteShard, ok := g.shards[targetShard]
	if !ok {
		// Unknown shard - this shouldn't happen with a properly configured router
		// For now, fall back to local (in production this would be an error)
		return check(ctx, g, g.usersets,
			subjectType, subjectID, objectType, objectID, relation,
			window, visited)
	}

	return remoteShard.Check(ctx, subjectType, subjectID, objectType, objectID, relation, window, visited)
}

// checkUnionResult holds the result of a remote CheckUnion call.
type checkUnionResult struct {
	shardID ShardID
	found   bool
	window  SnapshotWindow
	err     error
}

// CheckUnion checks if subject is in the union of all the given usersets.
// Groups checks by shard and dispatches accordingly.
// Remote shard checks run in parallel and cancel when any returns true.
func (g *ShardedGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []RelationCheck,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	if len(checks) == 0 {
		return false, SnapshotWindow{}, nil
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

	// Process local checks first (fast path - no network)
	for _, chk := range localChecks {
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

	// Process remote checks in parallel
	if len(remoteChecks) == 0 {
		if first {
			return false, SnapshotWindow{}, nil
		}
		return false, tightestWindow, nil
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
			// Unknown shard - skip (in production this would be an error)
			continue
		}

		wg.Add(1)
		go func(sid ShardID, shard Graph, checks []RelationCheck) {
			defer wg.Done()

			ok, resultWindow, err := shard.CheckUnion(remoteCtx, subjectType, subjectID, checks, visited)

			// Check if context was cancelled before sending
			select {
			case <-remoteCtx.Done():
				// Context cancelled, don't send result
				return
			default:
			}

			results <- checkUnionResult{
				shardID: sid,
				found:   ok,
				window:  resultWindow,
				err:     err,
			}
		}(shardID, remoteShard, shardChecks)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for result := range results {
		if result.err != nil {
			// Cancel remaining checks on error
			cancel()
			return false, SnapshotWindow{}, result.err
		}
		if result.found {
			// Found! Cancel remaining checks and return immediately
			cancel()
			return true, result.window, nil
		}
		// Track tightest window for "not found" case
		if first {
			tightestWindow = result.window
			first = false
		} else {
			tightestWindow = tightestWindow.Intersect(result.window)
		}
	}

	if first {
		// No checks were performed (all empty)
		return false, SnapshotWindow{}, nil
	}
	return false, tightestWindow, nil
}

// ValidateTuple checks if a tuple is valid according to the schema.
func (g *ShardedGraph) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	return g.usersets.ValidateTuple(objectType, relation, subjectType, subjectRelation)
}

// ReplicatedTime returns the current replicated time.
func (g *ShardedGraph) ReplicatedTime() store.StoreTime {
	return g.usersets.ReplicatedTime()
}

// SetRemoteShard adds or updates a remote shard reference.
// This is used during initialization to wire up cross-shard references.
func (g *ShardedGraph) SetRemoteShard(id ShardID, shard Graph) {
	if g.shards == nil {
		g.shards = make(map[ShardID]Graph)
	}
	g.shards[id] = shard
}

// Compile-time interface check
var _ Graph = (*ShardedGraph)(nil)

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
