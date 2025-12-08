package graph

import (
	"math"

	"github.com/alechenninger/falcon/store"
)

// MaxSnapshotWindow is an unconstrained window spanning all time (0 to MaxUint64).
// Use this as the starting window when you don't have constraints.
// The narrowing algorithm will constrain it based on actual data access.
// Safe to use directly since SnapshotWindow is a value type (copied on use).
//
// This is a sentinel value - Min() special-cases it to return 0.
var MaxSnapshotWindow = SnapshotWindow{
	minDelta: math.MaxUint32,
	max:      store.StoreTime(math.MaxUint64),
}

// isMaxWindow returns true if this is the MaxSnapshotWindow sentinel.
func (w SnapshotWindow) isMaxWindow() bool {
	return w.max == store.StoreTime(math.MaxUint64) && w.minDelta == math.MaxUint32
}

// SnapshotWindow represents the time range for a consistent snapshot read.
// As we traverse the graph, we narrow the window by raising Min when we
// pick states from tuples, ensuring all reads are from a consistent snapshot.
//
// Internally, Min is stored as a delta from Max to save memory (4 bytes vs 8).
// Since Min <= Max, the delta is always non-negative.
type SnapshotWindow struct {
	// minDelta is Max - Min. This compresses the Min value into 4 bytes.
	// Min() returns Max - minDelta.
	minDelta store.StoreDelta

	// max is the maximum time we can use. This starts as the replicated time
	// (what we know we're up to) and may decrease when a shard has a lower
	// replicated time (in distributed queries).
	max store.StoreTime
}

// NewSnapshotWindow creates a new SnapshotWindow with the given min and max times.
// Panics if min > max or if the delta exceeds uint32 range.
func NewSnapshotWindow(min, max store.StoreTime) SnapshotWindow {
	if min > max {
		panic("SnapshotWindow: min > max")
	}
	// Difference panics if delta exceeds uint32
	delta := max.Difference(min)
	return SnapshotWindow{minDelta: delta, max: max}
}

// Min returns the minimum time we've committed to. This is the highest state time
// we've used so far, meaning other tuple reads must be at least this fresh.
func (w SnapshotWindow) Min() store.StoreTime {
	// Special case: MaxSnapshotWindow represents [0, MaxUint64]
	if w.isMaxWindow() {
		return 0
	}
	return w.max.Less(w.minDelta)
}

// Max returns the maximum time we can use.
func (w SnapshotWindow) Max() store.StoreTime {
	return w.max
}

// NarrowMin returns a new window with Min raised to at least minTime.
// The window can only get narrower - Min only increases.
// Panics if the new min would exceed max - this indicates
// the shards have drifted too far apart to maintain consistency.
func (w SnapshotWindow) NarrowMin(minTime store.StoreTime) SnapshotWindow {
	currentMin := w.Min()
	if minTime > currentMin {
		if minTime > w.max {
			panic("SnapshotWindow: cannot narrow min above max - shards too far apart")
		}
		return NewSnapshotWindow(minTime, w.max)
	}
	return w
}

// NarrowMax returns a new window with Max lowered to at most maxTime.
// Used when a shard's replicated time is lower than our current max.
// Panics if the current Min would exceed the new max - this indicates
// the shards have drifted too far apart to maintain consistency.
func (w SnapshotWindow) NarrowMax(maxTime store.StoreTime) SnapshotWindow {
	if maxTime < w.max {
		min := w.Min()
		if min > maxTime {
			panic("SnapshotWindow: cannot narrow max below min - shards too far apart")
		}
		return NewSnapshotWindow(min, maxTime)
	}
	return w
}

// CanUse returns true if the given stateTime is usable within this window.
// A state is usable if it's <= Max (not newer than our ceiling).
func (w SnapshotWindow) CanUse(stateTime store.StoreTime) bool {
	return stateTime <= w.max
}

// IsValid returns true if the window is valid (Min <= Max).
// This is always true for properly constructed windows.
func (w SnapshotWindow) IsValid() bool {
	return w.Min() <= w.max
}
