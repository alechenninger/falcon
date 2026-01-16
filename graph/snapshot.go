package graph

import (
	"github.com/alechenninger/falcon/internal/domain"
)

// Re-export types from domain for backward compatibility.
type (
	// SnapshotWindow represents the time range for a consistent snapshot read.
	// Deprecated: Use domain.SnapshotWindow instead.
	SnapshotWindow = domain.SnapshotWindow
)

// MaxSnapshotWindow is an unconstrained window spanning all time.
// Deprecated: Use domain.MaxSnapshotWindow instead.
var MaxSnapshotWindow = domain.MaxSnapshotWindow

// NewSnapshotWindow creates a new SnapshotWindow with the given min and max times.
// Deprecated: Use domain.NewSnapshotWindow instead.
func NewSnapshotWindow(min, max domain.StoreTime) SnapshotWindow {
	return domain.NewSnapshotWindow(min, max)
}
