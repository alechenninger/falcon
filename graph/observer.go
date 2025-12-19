package graph

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// UsersetKey identifies a userset for observation purposes using type/relation IDs.
type UsersetKey struct {
	ObjectType      schema.TypeID
	ObjectID        schema.ID
	Relation        schema.RelationID
	SubjectType     schema.TypeID
	SubjectRelation schema.RelationID
}

// UsersetsObserver is called at key points during MultiversionUsersets operations.
// Implementations should embed NoOpUsersetsObserver for forward compatibility
// with new methods added to this interface.
type UsersetsObserver interface {
	// SubscribeReady is called when Subscribe has established its connection
	// to the change stream and is ready to receive changes.
	SubscribeReady(ctx context.Context)

	// ApplyChangeStarted is called when a change begins processing.
	// Returns a potentially modified context and a probe to track the operation.
	ApplyChangeStarted(ctx context.Context, change store.Change) (context.Context, ApplyChangeProbe)

	// GetSubjectBitmapStarted is called when GetSubjectBitmapWithin begins.
	GetSubjectBitmapStarted(key UsersetKey, window SnapshotWindow) BitmapReadProbe

	// ContainsDirectStarted is called when ContainsDirectWithin begins.
	ContainsDirectStarted(key UsersetKey, subjectID schema.ID, window SnapshotWindow) ContainsReadProbe

	// ContainsUsersetSubjectStarted is called when ContainsUsersetSubjectWithin begins.
	ContainsUsersetSubjectStarted(key UsersetKey, subjectID schema.ID, window SnapshotWindow) ContainsReadProbe
}

// BitmapReadProbe tracks a GetSubjectBitmapWithin operation.
// Implementations should embed NoOpBitmapReadProbe for forward compatibility.
type BitmapReadProbe interface {
	// Result is called when the bitmap is found.
	Result(size int, window SnapshotWindow)

	// NotFound is called when no bitmap exists for the key.
	NotFound()

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// ContainsReadProbe tracks a ContainsDirectWithin or ContainsUsersetSubjectWithin operation.
// Implementations should embed NoOpContainsReadProbe for forward compatibility.
type ContainsReadProbe interface {
	// Result is called with the operation result.
	Result(found bool, window SnapshotWindow)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// ApplyChangeProbe tracks a single applyChange invocation.
// Implementations should embed NoOpApplyChangeProbe for forward compatibility.
type ApplyChangeProbe interface {
	// Applied is called when the change has been successfully applied at the given time.
	Applied(t store.StoreTime)

	// End signals the method is complete (for timing). Called via defer.
	End()
}

// NoOpUsersetsObserver is a no-op implementation of UsersetsObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpUsersetsObserver struct{}

// SubscribeReady does nothing.
func (NoOpUsersetsObserver) SubscribeReady(context.Context) {}

// ApplyChangeStarted returns the context unchanged and a no-op probe.
func (NoOpUsersetsObserver) ApplyChangeStarted(ctx context.Context, _ store.Change) (context.Context, ApplyChangeProbe) {
	return ctx, NoOpApplyChangeProbe{}
}

// GetSubjectBitmapStarted returns a no-op probe.
func (NoOpUsersetsObserver) GetSubjectBitmapStarted(_ UsersetKey, _ SnapshotWindow) BitmapReadProbe {
	return NoOpBitmapReadProbe{}
}

// ContainsDirectStarted returns a no-op probe.
func (NoOpUsersetsObserver) ContainsDirectStarted(_ UsersetKey, _ schema.ID, _ SnapshotWindow) ContainsReadProbe {
	return NoOpContainsReadProbe{}
}

// ContainsUsersetSubjectStarted returns a no-op probe.
func (NoOpUsersetsObserver) ContainsUsersetSubjectStarted(_ UsersetKey, _ schema.ID, _ SnapshotWindow) ContainsReadProbe {
	return NoOpContainsReadProbe{}
}

// NoOpBitmapReadProbe is a no-op implementation of BitmapReadProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpBitmapReadProbe struct{}

func (NoOpBitmapReadProbe) Result(int, SnapshotWindow) {}
func (NoOpBitmapReadProbe) NotFound()                  {}
func (NoOpBitmapReadProbe) End()                       {}

// NoOpContainsReadProbe is a no-op implementation of ContainsReadProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpContainsReadProbe struct{}

func (NoOpContainsReadProbe) Result(bool, SnapshotWindow) {}
func (NoOpContainsReadProbe) End()                        {}

// NoOpApplyChangeProbe is a no-op implementation of ApplyChangeProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpApplyChangeProbe struct{}

// Applied does nothing.
func (NoOpApplyChangeProbe) Applied(store.StoreTime) {}

// End does nothing.
func (NoOpApplyChangeProbe) End() {}

// ShardedGraphObserver is called at key points during ShardedGraph operations.
// Implementations should embed NoOpShardedGraphObserver for forward compatibility
// with new methods added to this interface.
type ShardedGraphObserver interface {
	// CheckStarted is called when Check begins.
	// Returns a potentially modified context and a probe to track the operation lifecycle.
	CheckStarted(ctx context.Context,
		subjectType schema.TypeID, subjectID schema.ID,
		objectType schema.TypeID, objectID schema.ID,
		relation schema.RelationID,
	) (context.Context, ShardedCheckProbe)

	// CheckUnionStarted is called when CheckUnion begins.
	// Returns a potentially modified context and a probe to track the operation lifecycle.
	CheckUnionStarted(ctx context.Context, subjectType schema.TypeID, subjectID schema.ID) (context.Context, CheckUnionProbe)
}

// ShardedCheckProbe tracks a ShardedGraph.Check invocation lifecycle.
// Implementations should embed NoOpShardedCheckProbe for forward compatibility.
type ShardedCheckProbe interface {
	// LocalCheck is called when routing to local shard.
	LocalCheck()

	// RemoteCheck is called when routing to a remote shard.
	RemoteCheck(shardID ShardID)

	// UnknownShard is called when a shard ID is not found (fallback to local).
	UnknownShard(shardID ShardID)

	// Result is called with the check result.
	Result(found bool, window SnapshotWindow)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the method is complete (for timing). Called via defer.
	End()
}

// CheckUnionProbe tracks a CheckUnion invocation lifecycle.
// Implementations should embed NoOpCheckUnionProbe for forward compatibility.
type CheckUnionProbe interface {
	// Empty is called when no checks were provided.
	Empty()

	// UnknownShard is called when a shard ID is not found in the shards map.
	UnknownShard(shardID ShardID)

	// RemoteShardDispatched is called when dispatching to a remote shard.
	RemoteShardDispatched(shardID ShardID)

	// RemoteShardResult is called when a remote shard returns successfully.
	RemoteShardResult(shardID ShardID, found bool, window SnapshotWindow)

	// RemoteShardError is called when a remote shard returns an error.
	RemoteShardError(shardID ShardID, err error)

	// Result is called when the overall check completes with a definitive result.
	Result(found bool, window SnapshotWindow)

	// Inconclusive is called when the check cannot determine result due to shard errors.
	Inconclusive(failedShards []ShardID)

	// End signals the method is complete (for timing). Called via defer.
	End()
}

// NoOpShardedGraphObserver is a no-op implementation of ShardedGraphObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpShardedGraphObserver struct{}

// CheckStarted returns the context unchanged and a no-op probe.
func (NoOpShardedGraphObserver) CheckStarted(ctx context.Context,
	_ schema.TypeID, _ schema.ID,
	_ schema.TypeID, _ schema.ID,
	_ schema.RelationID,
) (context.Context, ShardedCheckProbe) {
	return ctx, NoOpShardedCheckProbe{}
}

// CheckUnionStarted returns the context unchanged and a no-op probe.
func (NoOpShardedGraphObserver) CheckUnionStarted(ctx context.Context, _ schema.TypeID, _ schema.ID) (context.Context, CheckUnionProbe) {
	return ctx, NoOpCheckUnionProbe{}
}

// NoOpShardedCheckProbe is a no-op implementation of ShardedCheckProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpShardedCheckProbe struct{}

func (NoOpShardedCheckProbe) LocalCheck()                 {}
func (NoOpShardedCheckProbe) RemoteCheck(ShardID)         {}
func (NoOpShardedCheckProbe) UnknownShard(ShardID)        {}
func (NoOpShardedCheckProbe) Result(bool, SnapshotWindow) {}
func (NoOpShardedCheckProbe) Error(error)                 {}
func (NoOpShardedCheckProbe) End()                        {}

// NoOpCheckUnionProbe is a no-op implementation of CheckUnionProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpCheckUnionProbe struct{}

// Empty does nothing.
func (NoOpCheckUnionProbe) Empty() {}

// UnknownShard does nothing.
func (NoOpCheckUnionProbe) UnknownShard(ShardID) {}

// RemoteShardDispatched does nothing.
func (NoOpCheckUnionProbe) RemoteShardDispatched(ShardID) {}

// RemoteShardResult does nothing.
func (NoOpCheckUnionProbe) RemoteShardResult(ShardID, bool, SnapshotWindow) {}

// RemoteShardError does nothing.
func (NoOpCheckUnionProbe) RemoteShardError(ShardID, error) {}

// Result does nothing.
func (NoOpCheckUnionProbe) Result(bool, SnapshotWindow) {}

// Inconclusive does nothing.
func (NoOpCheckUnionProbe) Inconclusive([]ShardID) {}

// End does nothing.
func (NoOpCheckUnionProbe) End() {}

// SignalingObserver broadcasts when changes are applied.
// Used for test synchronization without exposing internal APIs.
type SignalingObserver struct {
	NoOpUsersetsObserver // Embed for forward compatibility

	mu       sync.Mutex
	cond     *sync.Cond
	lastTime store.StoreTime
	ready    chan struct{}
}

// NewSignalingObserver creates a new SignalingObserver.
func NewSignalingObserver() *SignalingObserver {
	so := &SignalingObserver{
		ready: make(chan struct{}),
	}
	so.cond = sync.NewCond(&so.mu)
	return so
}

// SubscribeReady signals that the subscription is ready to receive changes.
func (o *SignalingObserver) SubscribeReady(context.Context) {
	close(o.ready)
}

// WaitReady blocks until the subscription is ready to receive changes.
func (o *SignalingObserver) WaitReady() {
	<-o.ready
}

// ApplyChangeStarted returns a probe that signals when the change completes.
func (o *SignalingObserver) ApplyChangeStarted(ctx context.Context, _ store.Change) (context.Context, ApplyChangeProbe) {
	return ctx, &signalingProbe{
		NoOpApplyChangeProbe: NoOpApplyChangeProbe{},
		observer:             o,
	}
}

// WaitForTime blocks until a change with at least the given time has been applied.
func (o *SignalingObserver) WaitForTime(t store.StoreTime) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for o.lastTime < t {
		o.cond.Wait()
	}
}

// LastTime returns the time of the most recently applied change.
func (o *SignalingObserver) LastTime() store.StoreTime {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastTime
}

// signalingProbe signals the observer when the change is applied.
type signalingProbe struct {
	NoOpApplyChangeProbe // Embed for forward compatibility
	observer             *SignalingObserver
}

// Applied signals that the change has been applied at the given time.
func (p *signalingProbe) Applied(t store.StoreTime) {
	p.observer.mu.Lock()
	p.observer.lastTime = t
	p.observer.cond.Broadcast()
	p.observer.mu.Unlock()
}

// -----------------------------------------------------------------------------
// CheckObserver - instruments the recursive check algorithm (check.go)
// -----------------------------------------------------------------------------

// CheckObserver is called at key points during check algorithm execution.
// Implementations should embed NoOpCheckObserver for forward compatibility
// with new methods added to this interface.
type CheckObserver interface {
	// CheckStarted is called when a top-level check begins.
	// Returns a potentially modified context and a probe to track the operation.
	CheckStarted(ctx context.Context,
		subjectType schema.TypeID, subjectID schema.ID,
		objectType schema.TypeID, objectID schema.ID,
		relation schema.RelationID,
	) (context.Context, CheckProbe)
}

// CheckProbe tracks a single check invocation through the recursive algorithm.
// Implementations should embed NoOpCheckProbe for forward compatibility.
type CheckProbe interface {
	// RelationEntered is called when entering a relation check.
	RelationEntered(objectType schema.TypeID, objectID schema.ID, relation schema.RelationID)

	// CycleDetected is called when a cycle is detected and we return false.
	CycleDetected(key VisitedKey)

	// UsersetChecking is called before checking each userset variant in a union.
	UsersetChecking(userset *schema.Userset)

	// DirectLookup is called when checking direct membership.
	DirectLookup(objectType schema.TypeID, objectID schema.ID, relation schema.RelationID, subjectType schema.TypeID)

	// DirectLookupResult is called when a direct lookup completes.
	DirectLookupResult(found bool, window SnapshotWindow)

	// ArrowTraversal is called when following an arrow (computed userset).
	ArrowTraversal(tuplesetRelation, computedRelation schema.RelationName)

	// RecursiveCheck is called when making a recursive check call.
	RecursiveCheck(subjectType schema.TypeID, subjectID schema.ID,
		objectType schema.TypeID, objectID schema.ID,
		relation schema.RelationID, depth int)

	// UnionBranchFound is called when a union branch matches.
	UnionBranchFound(branchIndex int)

	// Result is called with the final check result.
	Result(found bool, window SnapshotWindow)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the check is complete (for timing). Called via defer.
	End()
}

// NoOpCheckObserver is a no-op implementation of CheckObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpCheckObserver struct{}

// CheckStarted returns the context unchanged and a no-op probe.
func (NoOpCheckObserver) CheckStarted(ctx context.Context,
	_ schema.TypeID, _ schema.ID,
	_ schema.TypeID, _ schema.ID,
	_ schema.RelationID,
) (context.Context, CheckProbe) {
	return ctx, NoOpCheckProbe{}
}

// NoOpCheckProbe is a no-op implementation of CheckProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpCheckProbe struct{}

func (NoOpCheckProbe) RelationEntered(schema.TypeID, schema.ID, schema.RelationID)             {}
func (NoOpCheckProbe) CycleDetected(VisitedKey)                                                {}
func (NoOpCheckProbe) UsersetChecking(*schema.Userset)                                         {}
func (NoOpCheckProbe) DirectLookup(schema.TypeID, schema.ID, schema.RelationID, schema.TypeID) {}
func (NoOpCheckProbe) DirectLookupResult(bool, SnapshotWindow)                                 {}
func (NoOpCheckProbe) ArrowTraversal(schema.RelationName, schema.RelationName)                 {}
func (NoOpCheckProbe) RecursiveCheck(schema.TypeID, schema.ID, schema.TypeID, schema.ID, schema.RelationID, int) {
}
func (NoOpCheckProbe) UnionBranchFound(int)        {}
func (NoOpCheckProbe) Result(bool, SnapshotWindow) {}
func (NoOpCheckProbe) Error(error)                 {}
func (NoOpCheckProbe) End()                        {}

// -----------------------------------------------------------------------------
// LocalGraphObserver - instruments LocalGraph operations (graph.go)
// -----------------------------------------------------------------------------

// LocalGraphObserver is called at key points during LocalGraph operations.
// Implementations should embed NoOpLocalGraphObserver for forward compatibility
// with new methods added to this interface.
type LocalGraphObserver interface {
	// CheckStarted is called when LocalGraph.Check begins.
	CheckStarted(ctx context.Context,
		subjectType schema.TypeID, subjectID schema.ID,
		objectType schema.TypeID, objectID schema.ID,
		relation schema.RelationID,
	) (context.Context, LocalCheckProbe)

	// CheckUnionStarted is called when LocalGraph.CheckUnion begins.
	CheckUnionStarted(ctx context.Context,
		subjectType schema.TypeID, subjectID schema.ID,
		numChecks int,
	) (context.Context, LocalCheckUnionProbe)
}

// LocalCheckProbe tracks a single LocalGraph.Check invocation.
// Implementations should embed NoOpLocalCheckProbe for forward compatibility.
type LocalCheckProbe interface {
	// Result is called with the check result.
	Result(found bool, window SnapshotWindow)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the check is complete (for timing). Called via defer.
	End()
}

// LocalCheckUnionProbe tracks a single LocalGraph.CheckUnion invocation.
// Implementations should embed NoOpLocalCheckUnionProbe for forward compatibility.
type LocalCheckUnionProbe interface {
	// BitmapLookup is called when looking up subjects in a userset.
	BitmapLookup(objectType schema.TypeID, objectID schema.ID, relation schema.RelationID,
		subjectType schema.TypeID, subjectRelation schema.RelationID)

	// BitmapLookupResult is called when a bitmap lookup completes.
	BitmapLookupResult(size int, window SnapshotWindow)

	// ContainsCheck is called when checking if a subject is in a set.
	ContainsCheck(objectType schema.TypeID, objectID schema.ID, relation schema.RelationID,
		subjectType schema.TypeID, subjectID schema.ID)

	// ContainsCheckResult is called when a contains check completes.
	ContainsCheckResult(found bool, window SnapshotWindow)

	// Result is called with the final CheckUnion result.
	Result(result CheckResult)

	// Error is called when an error occurs.
	Error(err error)

	// End signals the check is complete (for timing). Called via defer.
	End()
}

// NoOpLocalGraphObserver is a no-op implementation of LocalGraphObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpLocalGraphObserver struct{}

// CheckStarted returns the context unchanged and a no-op probe.
func (NoOpLocalGraphObserver) CheckStarted(ctx context.Context,
	_ schema.TypeID, _ schema.ID,
	_ schema.TypeID, _ schema.ID,
	_ schema.RelationID,
) (context.Context, LocalCheckProbe) {
	return ctx, NoOpLocalCheckProbe{}
}

// CheckUnionStarted returns the context unchanged and a no-op probe.
func (NoOpLocalGraphObserver) CheckUnionStarted(ctx context.Context,
	_ schema.TypeID, _ schema.ID,
	_ int,
) (context.Context, LocalCheckUnionProbe) {
	return ctx, NoOpLocalCheckUnionProbe{}
}

// NoOpLocalCheckProbe is a no-op implementation of LocalCheckProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpLocalCheckProbe struct{}

func (NoOpLocalCheckProbe) Result(bool, SnapshotWindow) {}
func (NoOpLocalCheckProbe) Error(error)                 {}
func (NoOpLocalCheckProbe) End()                        {}

// NoOpLocalCheckUnionProbe is a no-op implementation of LocalCheckUnionProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpLocalCheckUnionProbe struct{}

func (NoOpLocalCheckUnionProbe) BitmapLookup(schema.TypeID, schema.ID, schema.RelationID, schema.TypeID, schema.RelationID) {
}
func (NoOpLocalCheckUnionProbe) BitmapLookupResult(int, SnapshotWindow) {}
func (NoOpLocalCheckUnionProbe) ContainsCheck(schema.TypeID, schema.ID, schema.RelationID, schema.TypeID, schema.ID) {
}
func (NoOpLocalCheckUnionProbe) ContainsCheckResult(bool, SnapshotWindow) {}
func (NoOpLocalCheckUnionProbe) Result(CheckResult)                       {}
func (NoOpLocalCheckUnionProbe) Error(error)                              {}
func (NoOpLocalCheckUnionProbe) End()                                     {}

// -----------------------------------------------------------------------------
// MVCCObserver - instruments versioned set operations (mvcc.go)
// -----------------------------------------------------------------------------

// MVCCObserver is called at key points during versionedSet operations.
// Implementations should embed NoOpMVCCObserver for forward compatibility
// with new methods added to this interface.
type MVCCObserver interface {
	// ContainsWithinStarted is called when ContainsWithin begins.
	ContainsWithinStarted(id schema.ID, maxTime store.StoreTime) MVCCProbe

	// SnapshotWithinStarted is called when SnapshotWithin begins.
	SnapshotWithinStarted(maxTime store.StoreTime) MVCCProbe
}

// MVCCProbe tracks a single MVCC lookup operation.
// Implementations should embed NoOpMVCCProbe for forward compatibility.
type MVCCProbe interface {
	// HistoryDepth reports how many history entries were traversed.
	HistoryDepth(depth int)

	// UndoApplied is called when an undo entry is applied during time travel.
	UndoApplied(timeDelta uint32)

	// HeadUsed is called when the head state was used (no time travel needed).
	HeadUsed()

	// Result is called with the operation result.
	// For ContainsWithin: found indicates presence, stateTime is the oldest valid time.
	// For SnapshotWithin: found is always true if successful, stateTime is the snapshot time.
	Result(found bool, stateTime store.StoreTime)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// NoOpMVCCObserver is a no-op implementation of MVCCObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpMVCCObserver struct{}

// ContainsWithinStarted returns a no-op probe.
func (NoOpMVCCObserver) ContainsWithinStarted(_ schema.ID, _ store.StoreTime) MVCCProbe {
	return NoOpMVCCProbe{}
}

// SnapshotWithinStarted returns a no-op probe.
func (NoOpMVCCObserver) SnapshotWithinStarted(_ store.StoreTime) MVCCProbe {
	return NoOpMVCCProbe{}
}

// NoOpMVCCProbe is a no-op implementation of MVCCProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpMVCCProbe struct{}

func (NoOpMVCCProbe) HistoryDepth(int)             {}
func (NoOpMVCCProbe) UndoApplied(uint32)           {}
func (NoOpMVCCProbe) HeadUsed()                    {}
func (NoOpMVCCProbe) Result(bool, store.StoreTime) {}
func (NoOpMVCCProbe) End()                         {}
