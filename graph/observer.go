package graph

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/store"
)

// GraphObserver is called at key points during Graph operations.
// Implementations should embed NoOpGraphObserver for forward compatibility
// with new methods added to this interface.
type GraphObserver interface {
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
	// Applied is called when the change has been successfully applied at the given LSN.
	Applied(lsn store.LSN)

	// End signals the method is complete (for timing). Called via defer.
	End()
}

// NoOpGraphObserver is a no-op implementation of GraphObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpGraphObserver struct{}

// SubscribeReady does nothing.
func (NoOpGraphObserver) SubscribeReady(context.Context) {}

// ApplyChangeStarted returns the context unchanged and a no-op probe.
func (NoOpGraphObserver) ApplyChangeStarted(ctx context.Context, _ store.Change) (context.Context, ApplyChangeProbe) {
	return ctx, NoOpApplyChangeProbe{}
}

// NoOpApplyChangeProbe is a no-op implementation of ApplyChangeProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpApplyChangeProbe struct{}

// Applied does nothing.
func (NoOpApplyChangeProbe) Applied(store.LSN) {}

// End does nothing.
func (NoOpApplyChangeProbe) End() {}

// SignalingObserver broadcasts when changes are applied.
// Used for test synchronization without exposing internal APIs.
type SignalingObserver struct {
	NoOpGraphObserver // Embed for forward compatibility

	mu      sync.Mutex
	cond    *sync.Cond
	lastLSN store.LSN
	ready   chan struct{}
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

// WaitForLSN blocks until a change with at least the given LSN has been applied.
func (o *SignalingObserver) WaitForLSN(lsn store.LSN) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for o.lastLSN < lsn {
		o.cond.Wait()
	}
}

// LastLSN returns the LSN of the most recently applied change.
func (o *SignalingObserver) LastLSN() store.LSN {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastLSN
}

// signalingProbe signals the observer when the change is applied.
type signalingProbe struct {
	NoOpApplyChangeProbe // Embed for forward compatibility
	observer             *SignalingObserver
}

// Applied signals that the change has been applied at the given LSN.
func (p *signalingProbe) Applied(lsn store.LSN) {
	p.observer.mu.Lock()
	p.observer.lastLSN = lsn
	p.observer.cond.Broadcast()
	p.observer.mu.Unlock()
}
