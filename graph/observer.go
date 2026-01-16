package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
)

// Re-export types from domain for backward compatibility.
type (
	// UsersetKey identifies a userset for observation purposes.
	// Deprecated: Use domain.UsersetKey instead.
	UsersetKey = domain.UsersetKey

	// UsersetsObserver is called at key points during MultiversionUsersets operations.
	// Deprecated: Use domain.UsersetsObserver instead.
	UsersetsObserver = domain.UsersetsObserver

	// BitmapReadProbe tracks a GetSubjectBitmapWithin operation.
	// Deprecated: Use domain.BitmapReadProbe instead.
	BitmapReadProbe = domain.BitmapReadProbe

	// ContainsReadProbe tracks a ContainsDirectWithin operation.
	// Deprecated: Use domain.ContainsReadProbe instead.
	ContainsReadProbe = domain.ContainsReadProbe

	// ApplyChangeProbe tracks a single applyChange invocation.
	// Deprecated: Use domain.ApplyChangeProbe instead.
	ApplyChangeProbe = domain.ApplyChangeProbe

	// NoOpUsersetsObserver is a no-op implementation of UsersetsObserver.
	// Deprecated: Use domain.NoOpUsersetsObserver instead.
	NoOpUsersetsObserver = domain.NoOpUsersetsObserver

	// NoOpBitmapReadProbe is a no-op implementation of BitmapReadProbe.
	// Deprecated: Use domain.NoOpBitmapReadProbe instead.
	NoOpBitmapReadProbe = domain.NoOpBitmapReadProbe

	// NoOpContainsReadProbe is a no-op implementation of ContainsReadProbe.
	// Deprecated: Use domain.NoOpContainsReadProbe instead.
	NoOpContainsReadProbe = domain.NoOpContainsReadProbe

	// NoOpApplyChangeProbe is a no-op implementation of ApplyChangeProbe.
	// Deprecated: Use domain.NoOpApplyChangeProbe instead.
	NoOpApplyChangeProbe = domain.NoOpApplyChangeProbe

	// ShardedGraphObserver is called at key points during ShardedGraph operations.
	// Deprecated: Use domain.ShardedGraphObserver instead.
	ShardedGraphObserver = domain.ShardedGraphObserver

	// ShardedCheckProbe tracks a ShardedGraph.Check invocation lifecycle.
	// Deprecated: Use domain.ShardedCheckProbe instead.
	ShardedCheckProbe = domain.ShardedCheckProbe

	// CheckUnionProbe tracks a CheckUnion invocation lifecycle.
	// Deprecated: Use domain.CheckUnionProbe instead.
	CheckUnionProbe = domain.CheckUnionProbe

	// NoOpShardedGraphObserver is a no-op implementation of ShardedGraphObserver.
	// Deprecated: Use domain.NoOpShardedGraphObserver instead.
	NoOpShardedGraphObserver = domain.NoOpShardedGraphObserver

	// NoOpShardedCheckProbe is a no-op implementation of ShardedCheckProbe.
	// Deprecated: Use domain.NoOpShardedCheckProbe instead.
	NoOpShardedCheckProbe = domain.NoOpShardedCheckProbe

	// NoOpCheckUnionProbe is a no-op implementation of CheckUnionProbe.
	// Deprecated: Use domain.NoOpCheckUnionProbe instead.
	NoOpCheckUnionProbe = domain.NoOpCheckUnionProbe

	// SignalingObserver broadcasts when changes are applied.
	// Deprecated: Use domain.SignalingObserver instead.
	SignalingObserver = domain.SignalingObserver

	// CheckObserver is called at key points during check algorithm execution.
	// Deprecated: Use domain.CheckObserver instead.
	CheckObserver = domain.CheckObserver

	// CheckProbe tracks a single check invocation.
	// Deprecated: Use domain.CheckProbe instead.
	CheckProbe = domain.CheckProbe

	// NoOpCheckObserver is a no-op implementation of CheckObserver.
	// Deprecated: Use domain.NoOpCheckObserver instead.
	NoOpCheckObserver = domain.NoOpCheckObserver

	// NoOpCheckProbe is a no-op implementation of CheckProbe.
	// Deprecated: Use domain.NoOpCheckProbe instead.
	NoOpCheckProbe = domain.NoOpCheckProbe

	// LocalGraphObserver is called at key points during LocalGraph operations.
	// Deprecated: Use domain.LocalGraphObserver instead.
	LocalGraphObserver = domain.LocalGraphObserver

	// LocalCheckProbe tracks a single LocalGraph.Check invocation.
	// Deprecated: Use domain.LocalCheckProbe instead.
	LocalCheckProbe = domain.LocalCheckProbe

	// LocalCheckUnionProbe tracks a single LocalGraph.CheckUnion invocation.
	// Deprecated: Use domain.LocalCheckUnionProbe instead.
	LocalCheckUnionProbe = domain.LocalCheckUnionProbe

	// NoOpLocalGraphObserver is a no-op implementation of LocalGraphObserver.
	// Deprecated: Use domain.NoOpLocalGraphObserver instead.
	NoOpLocalGraphObserver = domain.NoOpLocalGraphObserver

	// NoOpLocalCheckProbe is a no-op implementation of LocalCheckProbe.
	// Deprecated: Use domain.NoOpLocalCheckProbe instead.
	NoOpLocalCheckProbe = domain.NoOpLocalCheckProbe

	// NoOpLocalCheckUnionProbe is a no-op implementation of LocalCheckUnionProbe.
	// Deprecated: Use domain.NoOpLocalCheckUnionProbe instead.
	NoOpLocalCheckUnionProbe = domain.NoOpLocalCheckUnionProbe

	// MVCCObserver is called at key points during versionedSet operations.
	// Deprecated: Use domain.MVCCObserver instead.
	MVCCObserver = domain.MVCCObserver

	// MVCCProbe tracks a single MVCC lookup operation.
	// Deprecated: Use domain.MVCCProbe instead.
	MVCCProbe = domain.MVCCProbe

	// NoOpMVCCObserver is a no-op implementation of MVCCObserver.
	// Deprecated: Use domain.NoOpMVCCObserver instead.
	NoOpMVCCObserver = domain.NoOpMVCCObserver

	// NoOpMVCCProbe is a no-op implementation of MVCCProbe.
	// Deprecated: Use domain.NoOpMVCCProbe instead.
	NoOpMVCCProbe = domain.NoOpMVCCProbe
)

// NewSignalingObserver creates a new SignalingObserver.
// Deprecated: Use domain.NewSignalingObserver instead.
func NewSignalingObserver() *SignalingObserver {
	return domain.NewSignalingObserver()
}
