package graph

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

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
	// CheckUnionStarted is called when CheckUnion begins.
	// Returns a potentially modified context and a probe to track the operation lifecycle.
	CheckUnionStarted(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID) (context.Context, CheckUnionProbe)
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

// CheckUnionStarted returns the context unchanged and a no-op probe.
func (NoOpShardedGraphObserver) CheckUnionStarted(ctx context.Context, _ schema.TypeName, _ schema.ID) (context.Context, CheckUnionProbe) {
	return ctx, NoOpCheckUnionProbe{}
}

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
