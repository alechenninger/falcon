package domain

import (
	"fmt"
	"math"
)

// MaxSnapshotWindow is an unconstrained window spanning all time (0 to MaxUint64).
// Use this as the starting window when you don't have constraints.
// The narrowing algorithm will constrain it based on actual data access.
// Safe to use directly since SnapshotWindow is a value type (copied on use).
var MaxSnapshotWindow = SnapshotWindow{
	min: 0,
	max: StoreTime(math.MaxUint64),
}

// SnapshotWindow represents the time range for a consistent snapshot read.
// As we traverse the graph, we narrow the window by raising Min when we
// pick states from tuples, ensuring all reads are from a consistent snapshot.
// Used this way, the window represents the snapshots in which we would get the
// same answer. It ensures causal consistency
// (through constraints on what versions queries are limited to see),
// while also lazily assigning a specific, "effective" snapshot
// (by giving the range of what would still be causally consistent).
//
// Min and Max are stored as absolute times.
type SnapshotWindow struct {
	min StoreTime

	// max is the maximum time we can use. This starts as the replicated time
	// (what we know we're up to) and may decrease when a shard has a lower
	// replicated time (in distributed queries).
	max StoreTime
}

// NewSnapshotWindow creates a new SnapshotWindow with the given min and max times.
// Panics if min > max.
func NewSnapshotWindow(min, max StoreTime) SnapshotWindow {
	if min > max {
		panic("SnapshotWindow: min > max")
	}
	return SnapshotWindow{min: min, max: max}
}

// Min returns the minimum time we've committed to. This is the highest state time
// we've used so far, meaning other tuple reads must be at least this fresh.
func (w SnapshotWindow) Min() StoreTime {
	return w.min
}

// Max returns the maximum time we can use.
func (w SnapshotWindow) Max() StoreTime {
	return w.max
}

// NarrowMin returns a new window with Min raised to at least minTime.
// The window can only get narrower - Min only increases.
// Panics if the new min would exceed max - this indicates
// the shards have drifted too far apart to maintain consistency.
func (w SnapshotWindow) NarrowMin(minTime StoreTime) SnapshotWindow {
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
func (w SnapshotWindow) NarrowMax(maxTime StoreTime) SnapshotWindow {
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
func (w SnapshotWindow) CanUse(stateTime StoreTime) bool {
	return stateTime <= w.max
}

// IsValid returns true if the window is valid (Min <= Max).
// This is always true for properly constructed windows.
func (w SnapshotWindow) IsValid() bool {
	return w.Min() <= w.max
}

// String returns a string representation showing the actual Min and Max values.
func (w SnapshotWindow) String() string {
	return fmt.Sprintf("SnapshotWindow{Min: %d, Max: %d}", w.Min(), w.Max())
}

// Intersect returns the tightest window that satisfies both windows.
// This is max of mins and min of maxes.
// Panics if the resulting window would be invalid (min > max).
func (w SnapshotWindow) Intersect(other SnapshotWindow) SnapshotWindow {
	newMin := max(other.Min(), w.Min())
	newMax := min(other.Max(), w.Max())
	return NewSnapshotWindow(newMin, newMax)
}
