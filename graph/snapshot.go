package graph

// SnapshotWindow represents the LSN range for a consistent snapshot read.
// As we traverse the graph, we narrow the window by raising Min when we
// pick states from tuples, ensuring all reads are from a consistent snapshot.
type SnapshotWindow struct {
	// Min is the minimum LSN we've committed to. This is the highest state LSN
	// we've used so far, meaning other tuple reads must be at least this fresh.
	// Starts at 0 (unknown) and increases as we examine tuples.
	Min LSN

	// Max is the maximum LSN we can use. This starts as the replicated LSN
	// (what we know we're up to) and may decrease when a shard has a lower
	// replicated LSN (in distributed queries).
	Max LSN
}

// NarrowMin returns a new window with Min raised to at least minLSN.
// The window can only get narrower - Min only increases.
func (w SnapshotWindow) NarrowMin(minLSN LSN) SnapshotWindow {
	if minLSN > w.Min {
		return SnapshotWindow{Min: minLSN, Max: w.Max}
	}
	return w
}

// NarrowMax returns a new window with Max lowered to at most maxLSN.
// Used when a shard's replicated LSN is lower than our current max.
func (w SnapshotWindow) NarrowMax(maxLSN LSN) SnapshotWindow {
	if maxLSN < w.Max {
		return SnapshotWindow{Min: w.Min, Max: maxLSN}
	}
	return w
}

// CanUse returns true if the given stateLSN is usable within this window.
// A state is usable if it's <= Max (not newer than our ceiling).
func (w SnapshotWindow) CanUse(stateLSN LSN) bool {
	return stateLSN <= w.Max
}

// IsValid returns true if the window is valid (Min <= Max).
func (w SnapshotWindow) IsValid() bool {
	return w.Min <= w.Max
}



