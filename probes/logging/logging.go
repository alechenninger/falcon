// Package logging provides slog-based implementations of the graph observer interfaces.
//
// Deprecated: Use github.com/alechenninger/falcon/internal/application/observability instead.
package logging

import (
	"log/slog"

	"github.com/alechenninger/falcon/internal/application/observability"
)

// Re-export types from observability for backward compatibility.
type (
	// RequestIDKey is the context key for request IDs.
	// Deprecated: Use observability.RequestIDKey instead.
	RequestIDKey = observability.RequestIDKey

	// UsersetsObserver logs MultiversionUsersets operations.
	// Deprecated: Use observability.UsersetsObserver instead.
	UsersetsObserver = observability.UsersetsObserver

	// ShardedGraphObserver logs ShardedGraph operations.
	// Deprecated: Use observability.ShardedGraphObserver instead.
	ShardedGraphObserver = observability.ShardedGraphObserver

	// CheckObserver logs check algorithm operations.
	// Deprecated: Use observability.CheckObserver instead.
	CheckObserver = observability.CheckObserver

	// LocalGraphObserver logs LocalGraph operations.
	// Deprecated: Use observability.LocalGraphObserver instead.
	LocalGraphObserver = observability.LocalGraphObserver

	// MVCCObserver logs versionedSet MVCC operations.
	// Deprecated: Use observability.MVCCObserver instead.
	MVCCObserver = observability.MVCCObserver
)

// RequestIDFromContext extracts the request ID from the context, or returns empty string.
// Deprecated: Use observability.RequestIDFromContext instead.
var RequestIDFromContext = observability.RequestIDFromContext

// NewUsersetsObserver creates a new logging UsersetsObserver.
// Deprecated: Use observability.NewUsersetsObserver instead.
func NewUsersetsObserver(logger *slog.Logger) *UsersetsObserver {
	return observability.NewUsersetsObserver(logger)
}

// NewShardedGraphObserver creates a new logging ShardedGraphObserver.
// Deprecated: Use observability.NewShardedGraphObserver instead.
func NewShardedGraphObserver(logger *slog.Logger) *ShardedGraphObserver {
	return observability.NewShardedGraphObserver(logger)
}

// NewCheckObserver creates a new logging CheckObserver.
// Deprecated: Use observability.NewCheckObserver instead.
func NewCheckObserver(logger *slog.Logger) *CheckObserver {
	return observability.NewCheckObserver(logger)
}

// NewLocalGraphObserver creates a new logging LocalGraphObserver.
// Deprecated: Use observability.NewLocalGraphObserver instead.
func NewLocalGraphObserver(logger *slog.Logger) *LocalGraphObserver {
	return observability.NewLocalGraphObserver(logger)
}

// NewMVCCObserver creates a new logging MVCCObserver.
// Deprecated: Use observability.NewMVCCObserver instead.
func NewMVCCObserver(logger *slog.Logger) *MVCCObserver {
	return observability.NewMVCCObserver(logger)
}
