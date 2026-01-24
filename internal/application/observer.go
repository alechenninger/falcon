package application

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
)

// WriteObserver is called at key points during WriteService operations.
// Implementations should embed NoOpWriteObserver for forward compatibility
// with new methods added to this interface.
type WriteObserver interface {
	// WriteStarted is called when a Write operation begins.
	// Returns a potentially modified context and a probe to track the operation.
	WriteStarted(ctx context.Context, cmd WriteCommand) (context.Context, WriteProbe)
}

// WriteProbe tracks a single Write operation.
// Implementations should embed NoOpWriteProbe for forward compatibility.
type WriteProbe interface {
	// MutationCount records the number of mutations in the write command.
	MutationCount(count int)

	// HasPrecondition records whether the write has a precondition.
	HasPrecondition(has bool)

	// PreconditionEvaluated records the result of precondition evaluation.
	PreconditionEvaluated(satisfied bool)

	// MutationsApplied is called when the mutations have been successfully written.
	MutationsApplied()

	// Committed is called when the transaction has been committed.
	Committed()

	// Error records an error that occurred during the operation.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// NoOpWriteObserver is a no-op implementation of WriteObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpWriteObserver struct{}

// WriteStarted returns the context unchanged and a no-op probe.
func (NoOpWriteObserver) WriteStarted(ctx context.Context, _ WriteCommand) (context.Context, WriteProbe) {
	return ctx, NoOpWriteProbe{}
}

// NoOpWriteProbe is a no-op implementation of WriteProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpWriteProbe struct{}

// MutationCount does nothing.
func (NoOpWriteProbe) MutationCount(int) {}

// HasPrecondition does nothing.
func (NoOpWriteProbe) HasPrecondition(bool) {}

// PreconditionEvaluated does nothing.
func (NoOpWriteProbe) PreconditionEvaluated(bool) {}

// MutationsApplied does nothing.
func (NoOpWriteProbe) MutationsApplied() {}

// Committed does nothing.
func (NoOpWriteProbe) Committed() {}

// Error does nothing.
func (NoOpWriteProbe) Error(error) {}

// End does nothing.
func (NoOpWriteProbe) End() {}

// QueryObserver is called at key points during QueryService operations.
// Implementations should embed NoOpQueryObserver for forward compatibility
// with new methods added to this interface.
type QueryObserver interface {
	// CheckStarted is called when a Check operation begins.
	// Returns a potentially modified context and a probe to track the operation.
	CheckStarted(ctx context.Context, q CheckQuery) (context.Context, QueryCheckProbe)
}

// QueryCheckProbe tracks a single Check operation.
// Implementations should embed NoOpQueryCheckProbe for forward compatibility.
type QueryCheckProbe interface {
	// IdsResolved records the internal IDs after resolution.
	IdsResolved(objectID, subjectID domain.ID)

	// Result records the authorization result.
	Result(allowed bool)

	// Error records an error that occurred during the operation.
	Error(err error)

	// End signals the operation is complete (for timing). Called via defer.
	End()
}

// NoOpQueryObserver is a no-op implementation of QueryObserver.
// Embed this in custom observers for forward compatibility with new methods.
type NoOpQueryObserver struct{}

// CheckStarted returns the context unchanged and a no-op probe.
func (NoOpQueryObserver) CheckStarted(ctx context.Context, _ CheckQuery) (context.Context, QueryCheckProbe) {
	return ctx, NoOpQueryCheckProbe{}
}

// NoOpQueryCheckProbe is a no-op implementation of QueryCheckProbe.
// Embed this in custom probes for forward compatibility with new methods.
type NoOpQueryCheckProbe struct{}

// IdsResolved does nothing.
func (NoOpQueryCheckProbe) IdsResolved(domain.ID, domain.ID) {}

// Result does nothing.
func (NoOpQueryCheckProbe) Result(bool) {}

// Error does nothing.
func (NoOpQueryCheckProbe) Error(error) {}

// End does nothing.
func (NoOpQueryCheckProbe) End() {}
