package graph

import "github.com/alechenninger/falcon/store"

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
	return w.max.Less(w.minDelta)
}

// Max returns the maximum time we can use.
func (w SnapshotWindow) Max() store.StoreTime {
	return w.max
}

// NarrowMin returns a new window with Min raised to at least minTime.
// The window can only get narrower - Min only increases.
func (w SnapshotWindow) NarrowMin(minTime store.StoreTime) SnapshotWindow {
	currentMin := w.Min()
	if minTime > currentMin {
		return NewSnapshotWindow(minTime, w.max)
	}
	return w
}

// NarrowMax returns a new window with Max lowered to at most maxTime.
// Used when a shard's replicated time is lower than our current max.
func (w SnapshotWindow) NarrowMax(maxTime store.StoreTime) SnapshotWindow {
	if maxTime < w.max {
		return NewSnapshotWindow(w.Min(), maxTime)
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
