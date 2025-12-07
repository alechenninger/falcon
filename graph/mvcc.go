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

// ContainsAt checks if the ID was in the set at the given historical time.
// This walks the undo chain to reconstruct the historical state without cloning.
func (v *versionedSet) ContainsAt(id schema.ID, t store.StoreTime) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := v.head.Contains(uint32(id))

	// Early exit: if target time >= headTime, use current state
	if t >= v.headTime {
		return result
	}

	// Need to undo changes newer than target time
	// First check headUndo (at headTime)
	if v.headUndo != nil {
		// headTime > t, so we need to undo headUndo
		if slices.Contains(v.headUndo.added, id) {
			result = false
		}
		if slices.Contains(v.headUndo.removed, id) {
			result = true
		}

		// Walk history in reverse (newest to oldest)
		currentTime := v.headTime
		for i := len(v.history) - 1; i >= 0; i-- {
			undo := &v.history[i]
			currentTime = currentTime.Less(undo.timeDelta)
			if currentTime <= t {
				break // This change is at or before target, don't undo it
			}
			if slices.Contains(undo.added, id) {
				result = false
			}
			if slices.Contains(undo.removed, id) {
				result = true
			}
		}
	}

	return result
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

// SnapshotAt returns a clone of the bitmap as it was at the given time.
// This is expensive and should only be used when a full bitmap is needed
// (e.g., for GetSubjects at a historical time).
func (v *versionedSet) SnapshotAt(t store.StoreTime) *roaring.Bitmap {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := v.head.Clone()

	// Early exit: if target time >= headTime, use current state
	if t >= v.headTime {
		return result
	}

	// Need to undo changes newer than target time
	if v.headUndo != nil {
		// Undo headUndo (at headTime, which is > t)
		for _, id := range v.headUndo.added {
			result.Remove(uint32(id))
		}
		for _, id := range v.headUndo.removed {
			result.Add(uint32(id))
		}

		// Walk history in reverse (newest to oldest)
		currentTime := v.headTime
		for i := len(v.history) - 1; i >= 0; i-- {
			undo := &v.history[i]
			currentTime = currentTime.Less(undo.timeDelta)
			if currentTime <= t {
				break // This change is at or before target, don't undo it
			}
			for _, id := range undo.added {
				result.Remove(uint32(id))
			}
			for _, id := range undo.removed {
				result.Add(uint32(id))
			}
		}
	}

	return result
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

// StateTimeWithin returns the time of the latest state that is <= maxTime.
// This is the state we would use when reading within the given snapshot window.
//
// If headTime <= maxTime, returns headTime (we can use current state).
// Otherwise, walks the undo chain to find the latest usable state.
// Returns 0 if no state is available within the window.
func (v *versionedSet) StateTimeWithin(maxTime store.StoreTime) store.StoreTime {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// If head is within bounds, use it
	if v.headTime <= maxTime {
		return v.headTime
	}

	// headTime > maxTime, so we need to look at history
	if v.headUndo == nil {
		return 0 // No history available
	}

	// Walk history in reverse to find the latest state <= maxTime
	currentTime := v.headTime
	for i := len(v.history) - 1; i >= 0; i-- {
		currentTime = currentTime.Less(v.history[i].timeDelta)
		if currentTime <= maxTime {
			return currentTime
		}
	}

	// Walked entire history without finding a usable state
	return 0
}
