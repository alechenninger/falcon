// Package graph implements the in-memory tuple store with MVCC support.
package graph

import (
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// undoEntry records a change that can be undone to reconstruct historical state.
// In the history slice, timeDelta stores the time gap to the next (newer) entry.
type undoEntry struct {
	timeDelta store.StoreDelta // Delta to next entry's time (0 for headUndo, computed for history)
	added     []schema.ID      // IDs added at this time (undo = remove)
	removed   []schema.ID      // IDs removed at this time (undo = add back)
}

// versionedSet stores a bitmap with MVCC support via undo chains.
// HEAD is always the current state; historical reads walk the undo chain.
//
// Structure:
//   - headUndo: the most recent change (at headTime), or nil if no history
//   - history: older changes, oldest first, each with delta to the next newer entry
//
// To traverse from newest to oldest: process headUndo first, then history in reverse.
type versionedSet struct {
	mu       sync.RWMutex
	headTime store.StoreTime // Timestamp of the current head state
	head     *roaring.Bitmap // Always current state
	headUndo *undoEntry      // Most recent change (at headTime), nil if no history
	history  []undoEntry     // Older entries, oldest first
}

// newVersionedSet creates a new versioned set starting at the given time.
func newVersionedSet(t store.StoreTime) *versionedSet {
	return &versionedSet{
		headTime: t,
		head:     roaring.New(),
		headUndo: nil,
		history:  nil,
	}
}

// Add adds an ID to the set at the given time.
// If the ID already exists, this is a no-op.
func (v *versionedSet) Add(id schema.ID, t store.StoreTime) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.head.Contains(uint32(id)) {
		return // Already present, no change
	}

	v.head.Add(uint32(id))

	// Record undo: this was an add, so undo = remove
	v.recordUndo(t, []schema.ID{id}, nil)
}

// Remove removes an ID from the set at the given time.
// If the ID doesn't exist, this is a no-op.
func (v *versionedSet) Remove(id schema.ID, t store.StoreTime) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.head.Contains(uint32(id)) {
		return // Not present, no change
	}

	v.head.Remove(uint32(id))

	// Record undo: this was a remove, so undo = add back
	v.recordUndo(t, nil, []schema.ID{id})
}

// recordUndo records a new undo entry and maintains the delta chain.
// added contains IDs that were added (undo = remove them).
// removed contains IDs that were removed (undo = add them back).
// Must be called with v.mu held.
func (v *versionedSet) recordUndo(t store.StoreTime, added, removed []schema.ID) {
	if v.headUndo != nil {
		// Move current headUndo to history with its delta to the new head
		v.headUndo.timeDelta = t.Difference(v.headTime)
		v.history = append(v.history, *v.headUndo)
	}

	// New headUndo (timeDelta not used - it's always at headTime)
	v.headUndo = &undoEntry{added: added, removed: removed}
	v.headTime = t
}

// Contains checks if the ID is in the current (HEAD) state.
func (v *versionedSet) Contains(id schema.ID) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.head.Contains(uint32(id))
}

// Head returns the current HEAD bitmap. The caller must not modify it.
func (v *versionedSet) Head() *roaring.Bitmap {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.head
}

// HeadTime returns the timestamp of the current HEAD state.
func (v *versionedSet) HeadTime() store.StoreTime {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.headTime
}

// IsEmpty returns true if the HEAD bitmap is empty.
func (v *versionedSet) IsEmpty() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.head.IsEmpty()
}

// Truncate removes undo entries older than the given time.
// This is used for garbage collection - entries older than the oldest
// active query can be discarded.
func (v *versionedSet) Truncate(minTime store.StoreTime) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.headUndo == nil {
		return // No history to truncate
	}

	// Walk history from newest to oldest to find cutoff point
	// We keep entries where time >= minTime
	currentTime := v.headTime
	cutoff := 0 // Index of first entry to keep (oldest first)
	for i := len(v.history) - 1; i >= 0; i-- {
		currentTime = currentTime.Less(v.history[i].timeDelta)
		if currentTime < minTime {
			cutoff = i + 1 // Discard this and older entries
			break
		}
	}
	v.history = v.history[cutoff:]
}

// OldestTime returns the oldest time in the undo chain, or headTime if empty.
func (v *versionedSet) OldestTime() store.StoreTime {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.headUndo == nil {
		return v.headTime
	}

	if len(v.history) == 0 {
		return v.headTime // Only headUndo exists, it's at headTime
	}

	// Compute the oldest time by walking the chain
	currentTime := v.headTime
	for i := len(v.history) - 1; i >= 0; i-- {
		currentTime = currentTime.Less(v.history[i].timeDelta)
	}
	return currentTime
}

// ContainsWithin checks if the ID is in the set at the latest state <= maxTime.
// Returns (found, stateTime) where stateTime is the OLDEST time that gives
// the same answer, allowing for maximum snapshot window width.
// Returns (false, 0) if no state is available within the time bound.
func (v *versionedSet) ContainsWithin(id schema.ID, maxTime store.StoreTime) (bool, store.StoreTime) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// First, find the result at the latest state <= maxTime
	result, resultTime, historyIdx := v.findStateAtMax(id, maxTime)
	if resultTime == 0 {
		return false, 0 // No state available
	}

	// Now continue walking back to find the oldest time with the same answer.
	// historyIdx is the index we stopped at (or len(history) if we used head/headUndo).
	oldestTime := resultTime
	currentTime := resultTime

	// Determine the starting point for our backward search.
	// If we used head state (historyIdx == len(history)), check headUndo first.
	// Otherwise, we stopped at history[historyIdx], check if that entry changed our ID.
	if historyIdx == len(v.history) {
		// Used head state - check if headUndo changed our ID
		if v.headUndo != nil {
			if slices.Contains(v.headUndo.added, id) && result {
				return result, oldestTime // Added at headTime
			}
			if slices.Contains(v.headUndo.removed, id) && !result {
				return result, oldestTime // Removed at headTime
			}
		}
	} else {
		// Stopped at history[historyIdx] - this entry might be where state changed.
		// We returned the state BEFORE undoing this entry, but the entry might
		// record an earlier change that led to this state.
		// Actually, we need to check what happened BETWEEN historyIdx and headUndo.

		// Check if headUndo changed our ID (state at headTime)
		if v.headUndo != nil {
			if slices.Contains(v.headUndo.added, id) && result {
				return result, oldestTime // The current result came from headUndo
			}
			if slices.Contains(v.headUndo.removed, id) && !result {
				return result, oldestTime // The current result came from headUndo
			}
		}

		// Check entries from len-1 down to historyIdx (inclusive) for changes
		for i := len(v.history) - 1; i >= historyIdx; i-- {
			undo := &v.history[i]
			if slices.Contains(undo.added, id) && result {
				return result, oldestTime // This entry made it present
			}
			if slices.Contains(undo.removed, id) && !result {
				return result, oldestTime // This entry made it absent
			}
		}
	}

	// Continue from just before where we found our state
	startIdx := historyIdx - 1
	if historyIdx == len(v.history) {
		startIdx = len(v.history) - 1
	}

	for i := startIdx; i >= 0; i-- {
		undo := &v.history[i]
		entryTime := currentTime.Less(undo.timeDelta)

		// Check if this entry changed the state for our ID
		if slices.Contains(undo.added, id) && result {
			oldestTime = entryTime
			break
		}
		if slices.Contains(undo.removed, id) && !result {
			oldestTime = entryTime
			break
		}

		oldestTime = entryTime
		currentTime = entryTime
	}

	return result, oldestTime
}

// findStateAtMax finds the state of an ID at the latest time <= maxTime.
// Returns (result, stateTime, historyIdx) where historyIdx is the history
// index we stopped at (or len(history) if we used head state).
func (v *versionedSet) findStateAtMax(id schema.ID, maxTime store.StoreTime) (bool, store.StoreTime, int) {
	// If head is within bounds, use it
	if v.headTime <= maxTime {
		return v.head.Contains(uint32(id)), v.headTime, len(v.history)
	}

	// headTime > maxTime, need to find an older state
	if v.headUndo == nil {
		return false, 0, 0 // No history available
	}

	// Start with head state and undo changes
	result := v.head.Contains(uint32(id))

	// Undo headUndo first
	if slices.Contains(v.headUndo.added, id) {
		result = false
	}
	if slices.Contains(v.headUndo.removed, id) {
		result = true
	}

	// Walk history in reverse to find state <= maxTime
	currentTime := v.headTime
	for i := len(v.history) - 1; i >= 0; i-- {
		undo := &v.history[i]
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime <= maxTime {
			return result, currentTime, i
		}
		// Continue undoing
		if slices.Contains(undo.added, id) {
			result = false
		}
		if slices.Contains(undo.removed, id) {
			result = true
		}
	}

	// Walked entire history without finding a usable state
	return false, 0, 0
}

// SnapshotWithin returns a clone of the bitmap at the latest state <= maxTime.
// Returns (snapshot, stateTime) where stateTime is the time of the state used,
// or (nil, 0) if no state is available within the time bound.
func (v *versionedSet) SnapshotWithin(maxTime store.StoreTime) (*roaring.Bitmap, store.StoreTime) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// We CANNOT go back before the latest state within maxTime here,
	// because it would always change the answer.

	// If head is within bounds, use it
	if v.headTime <= maxTime {
		return v.head.Clone(), v.headTime
	}

	// headTime > maxTime, need to find an older state
	if v.headUndo == nil {
		return nil, 0 // No history available
	}

	// Start with head state and undo changes
	result := v.head.Clone()

	// Undo headUndo first
	for _, id := range v.headUndo.added {
		result.Remove(uint32(id))
	}
	for _, id := range v.headUndo.removed {
		result.Add(uint32(id))
	}

	// Walk history in reverse to find state <= maxTime
	currentTime := v.headTime
	for i := len(v.history) - 1; i >= 0; i-- {
		undo := &v.history[i]
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime <= maxTime {
			return result, currentTime
		}
		// Continue undoing
		for _, id := range undo.added {
			result.Remove(uint32(id))
		}
		for _, id := range undo.removed {
			result.Add(uint32(id))
		}
	}

	// Walked entire history without finding a usable state
	return nil, 0
}
