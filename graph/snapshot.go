package graph

// SnapshotWindow represents the LSN range for a consistent snapshot read.
// As we traverse the graph, we narrow the window by raising Min when we
// pick states from tuples, ensuring all reads are from a consistent snapshot.
//
// Internally, Min is stored as a delta from Max to save memory (4 bytes vs 8).
// Since Min <= Max, the delta is always non-negative.
type SnapshotWindow struct {
	// minDelta is Max - Min. This compresses the Min value into 4 bytes.
	// Min() returns Max - minDelta.
	minDelta uint32

	// max is the maximum LSN we can use. This starts as the replicated LSN
	// (what we know we're up to) and may decrease when a shard has a lower
	// replicated LSN (in distributed queries).
	max LSN
}

// NewSnapshotWindow creates a new SnapshotWindow with the given min and max LSNs.
// Panics if min > max or if the delta exceeds uint32 range.
func NewSnapshotWindow(min, max LSN) SnapshotWindow {
	if min > max {
		panic("SnapshotWindow: min > max")
	}
	delta := max - min
	if delta > LSN(^uint32(0)) {
		panic("SnapshotWindow: delta overflow (window spans more than 4 billion LSNs)")
	}
	return SnapshotWindow{minDelta: uint32(delta), max: max}
}

// Min returns the minimum LSN we've committed to. This is the highest state LSN
// we've used so far, meaning other tuple reads must be at least this fresh.
func (w SnapshotWindow) Min() LSN {
	return w.max - LSN(w.minDelta)
}

// Max returns the maximum LSN we can use.
func (w SnapshotWindow) Max() LSN {
	return w.max
}

// NarrowMin returns a new window with Min raised to at least minLSN.
// The window can only get narrower - Min only increases.
func (w SnapshotWindow) NarrowMin(minLSN LSN) SnapshotWindow {
	currentMin := w.Min()
	if minLSN > currentMin {
		return NewSnapshotWindow(minLSN, w.max)
	}
	return w
}

// NarrowMax returns a new window with Max lowered to at most maxLSN.
// Used when a shard's replicated LSN is lower than our current max.
func (w SnapshotWindow) NarrowMax(maxLSN LSN) SnapshotWindow {
	if maxLSN < w.max {
		return NewSnapshotWindow(w.Min(), maxLSN)
	}
	return w
}

// CanUse returns true if the given stateLSN is usable within this window.
// A state is usable if it's <= Max (not newer than our ceiling).
func (w SnapshotWindow) CanUse(stateLSN LSN) bool {
	return stateLSN <= w.max
}

// IsValid returns true if the window is valid (Min <= Max).
// This is always true for properly constructed windows.
func (w SnapshotWindow) IsValid() bool {
	return w.Min() <= w.max
}
