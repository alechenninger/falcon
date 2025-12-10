// Package graph implements the in-memory tuple store with MVCC support.
package graph

import (
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// inlineThreshold is the maximum number of elements stored inline before
// promoting to a roaring bitmap. Chosen to balance memory savings on small
// sets (like parent relations with 1 element) vs overhead.
const inlineThreshold = 3

// inlineMode indicates elements are stored in the inline array (count 0-3).
// bitmapMode indicates elements are stored in the head bitmap.
const bitmapMode = 255

// undoEntry records a change that can be undone to reconstruct historical state.
// In the history slice, timeDelta stores the time gap to the next (newer) entry.
type undoEntry struct {
	timeDelta store.StoreDelta // Delta to next entry's time (0 for headUndo, computed for history)
	added     []schema.ID      // IDs added at this time (undo = remove)
	removed   []schema.ID      // IDs removed at this time (undo = add back)
}

// versionedSet stores a set of IDs with MVCC support via undo chains.
// HEAD is always the current state; historical reads walk the undo chain.
//
// Small-set optimization: sets with <= inlineThreshold elements are stored
// inline in the inline array to avoid roaring bitmap allocation overhead.
// This saves ~80 bytes per small set (common for parent relations).
//
// Structure:
//   - headUndo: the most recent change (at headTime), or nil if no history
//   - history: older changes, oldest first, each with delta to the next newer entry
//
// To traverse from newest to oldest: process headUndo first, then history in reverse.
type versionedSet struct {
	mu       sync.RWMutex
	headTime store.StoreTime // Timestamp of the current head state

	// Small set storage (inline mode): count is 0-3, elements in inline[0:count]
	// Large set storage (bitmap mode): count is bitmapMode (255), elements in head
	inline [inlineThreshold]schema.ID
	count  uint8 // 0-3 = inline mode, bitmapMode = use bitmap

	head     *roaring.Bitmap // Only allocated when count == bitmapMode
	headUndo *undoEntry      // Most recent change (at headTime), nil if no history
	history  []undoEntry     // Older entries, oldest first
}

// newVersionedSet creates a new versioned set starting at the given time.
// The set starts empty in inline mode (no bitmap allocated).
func newVersionedSet(t store.StoreTime) *versionedSet {
	return &versionedSet{
		headTime: t,
		count:    0, // Inline mode, empty
		head:     nil,
		headUndo: nil,
		history:  nil,
	}
}

// isInlineMode returns true if the set is using inline storage.
func (v *versionedSet) isInlineMode() bool {
	return v.count != bitmapMode
}

// inlineContains checks if the ID is in the inline array.
// Must be called with at least a read lock held.
func (v *versionedSet) inlineContains(id schema.ID) bool {
	for i := uint8(0); i < v.count; i++ {
		if v.inline[i] == id {
			return true
		}
	}
	return false
}

// inlineAdd adds an ID to the inline array if space available.
// Returns true if added, false if already present or no space.
// Must be called with write lock held.
func (v *versionedSet) inlineAdd(id schema.ID) bool {
	// Check if already present
	for i := uint8(0); i < v.count; i++ {
		if v.inline[i] == id {
			return false // Already present
		}
	}
	// Check if space available
	if v.count >= inlineThreshold {
		return false // No space, need to promote
	}
	v.inline[v.count] = id
	v.count++
	return true
}

// inlineRemove removes an ID from the inline array.
// Returns true if removed, false if not present.
// Must be called with write lock held.
func (v *versionedSet) inlineRemove(id schema.ID) bool {
	for i := uint8(0); i < v.count; i++ {
		if v.inline[i] == id {
			// Shift remaining elements
			for j := i; j < v.count-1; j++ {
				v.inline[j] = v.inline[j+1]
			}
			v.count--
			return true
		}
	}
	return false
}

// promoteTobitmap converts from inline mode to bitmap mode.
// Must be called with write lock held.
func (v *versionedSet) promoteToBitmap() {
	if !v.isInlineMode() {
		return // Already in bitmap mode
	}
	v.head = roaring.New()
	for i := uint8(0); i < v.count; i++ {
		v.head.Add(uint32(v.inline[i]))
	}
	v.count = bitmapMode
}

// Add adds an ID to the set at the given time.
// If the ID already exists, this is a no-op.
func (v *versionedSet) Add(id schema.ID, t store.StoreTime) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.isInlineMode() {
		if v.inlineContains(id) {
			return // Already present
		}
		if !v.inlineAdd(id) {
			// No space, promote to bitmap and add
			v.promoteToBitmap()
			v.head.Add(uint32(id))
		}
	} else {
		if v.head.Contains(uint32(id)) {
			return // Already present
		}
		v.head.Add(uint32(id))
	}

	// Record undo: this was an add, so undo = remove
	v.recordUndo(t, []schema.ID{id}, nil)
}

// AddBulk adds an ID to the set without recording undo history.
// This is used during hydration when loading a snapshot - there's no need
// to time-travel before the snapshot, so we skip the undo chain overhead.
// The caller must hold no lock; this acquires the write lock.
// TODO: rethink this when we have a proper hydration protocol.
func (v *versionedSet) AddBulk(id schema.ID) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.isInlineMode() {
		if v.inlineContains(id) {
			return // Already present
		}
		if !v.inlineAdd(id) {
			// No space, promote to bitmap and add
			v.promoteToBitmap()
			v.head.Add(uint32(id))
		}
	} else {
		v.head.Add(uint32(id))
	}
}

// Remove removes an ID from the set at the given time.
// If the ID doesn't exist, this is a no-op.
func (v *versionedSet) Remove(id schema.ID, t store.StoreTime) {
	v.mu.Lock()
	defer v.mu.Unlock()

	var removed bool
	if v.isInlineMode() {
		removed = v.inlineRemove(id)
	} else {
		if !v.head.Contains(uint32(id)) {
			return // Not present
		}
		v.head.Remove(uint32(id))
		removed = true
	}

	if removed {
		// Record undo: this was a remove, so undo = add back
		v.recordUndo(t, nil, []schema.ID{id})
	}
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
	if v.isInlineMode() {
		return v.inlineContains(id)
	}
	return v.head.Contains(uint32(id))
}

// Head returns the current HEAD bitmap. The caller must not modify it.
// If in inline mode, promotes to bitmap mode first.
func (v *versionedSet) Head() *roaring.Bitmap {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.isInlineMode() {
		v.promoteToBitmap()
	}
	return v.head
}

// HeadTime returns the timestamp of the current HEAD state.
func (v *versionedSet) HeadTime() store.StoreTime {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.headTime
}

// IsEmpty returns true if the HEAD set is empty.
func (v *versionedSet) IsEmpty() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.isInlineMode() {
		return v.count == 0
	}
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
	return v.ContainsWithinObserved(id, maxTime, NoOpMVCCObserver{})
}

// ContainsWithinObserved is like ContainsWithin but with observability hooks.
func (v *versionedSet) ContainsWithinObserved(id schema.ID, maxTime store.StoreTime, obs MVCCObserver) (bool, store.StoreTime) {
	probe := obs.ContainsWithinStarted(id, maxTime)
	defer probe.End()

	// TODO: what should we do with truncated history, or "beginning of time"?
	// if a subject has never been in the set, do we return false, 0?
	// e.g. a folder -> group members is added at time 1
	// a user is added at time 2
	// we check a different user. is that causally consistent from 0?
	// If we truncate history, we don't know how far we can go back and still know
	// about a particular subject. We can only save when the userset was first created.
	// (before that we know the set was effectively empty)
	// But for this to be useful, we'd need to persist this.
	// Maybe we don't bother. It only matters when the first tuple is ever added.
	// Maybe we could track the last point at which we know the set goes from empty to non-empty,
	// and non-empty to empty.

	v.mu.RLock()
	defer v.mu.RUnlock()

	// First, find the result at the latest state <= maxTime
	result, resultTime, historyIdx := v.findStateAtMax(id, maxTime)

	// Track if we used head state directly
	if historyIdx == len(v.history) && resultTime == v.headTime {
		probe.HeadUsed()
	}

	// Now continue walking back to find the oldest time with the same answer.
	// Our causal dependency in this case depends not on the entire set, but on this specific subject.

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

	historyDepth := 0
	for i := startIdx; i >= 0; i-- {
		undo := &v.history[i]
		entryTime := currentTime.Less(undo.timeDelta)
		historyDepth++
		probe.UndoApplied(uint32(undo.timeDelta))

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

	probe.HistoryDepth(historyDepth)
	probe.Result(result, oldestTime)
	return result, oldestTime
}

// findStateAtMax finds the state of an ID at the latest time <= maxTime.
// Returns (result, stateTime, historyIdx) where historyIdx is the history
// index we stopped at (or len(history) if we used head state).
func (v *versionedSet) findStateAtMax(id schema.ID, maxTime store.StoreTime) (bool, store.StoreTime, int) {
	// If head is within bounds, use it
	if v.headTime <= maxTime {
		var contains bool
		if v.isInlineMode() {
			contains = v.inlineContains(id)
		} else {
			contains = v.head.Contains(uint32(id))
		}
		return contains, v.headTime, len(v.history)
	}

	// headTime > maxTime, need to find an older state
	if v.headUndo == nil {
		return false, 0, 0 // No history available
	}

	// Start with head state and undo changes
	var result bool
	if v.isInlineMode() {
		result = v.inlineContains(id)
	} else {
		result = v.head.Contains(uint32(id))
	}

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
	return v.SnapshotWithinObserved(maxTime, NoOpMVCCObserver{})
}

// SnapshotWithinObserved is like SnapshotWithin but with observability hooks.
func (v *versionedSet) SnapshotWithinObserved(maxTime store.StoreTime, obs MVCCObserver) (*roaring.Bitmap, store.StoreTime) {
	probe := obs.SnapshotWithinStarted(maxTime)
	defer probe.End()

	v.mu.RLock()
	defer v.mu.RUnlock()

	// We CANNOT go back before the latest state within maxTime here,
	// because it would always change the answer.

	// If head is within bounds, use it
	if v.headTime <= maxTime {
		probe.HeadUsed()
		probe.HistoryDepth(0)
		probe.Result(true, v.headTime)
		// Create bitmap from current state
		if v.isInlineMode() {
			result := roaring.New()
			for i := uint8(0); i < v.count; i++ {
				result.Add(uint32(v.inline[i]))
			}
			return result, v.headTime
		}
		return v.head.Clone(), v.headTime
	}

	// headTime > maxTime, need to find an older state
	if v.headUndo == nil {
		probe.HistoryDepth(0)
		probe.Result(false, 0)
		return nil, 0 // No history available
	}

	// Start with head state and undo changes
	var result *roaring.Bitmap
	if v.isInlineMode() {
		result = roaring.New()
		for i := uint8(0); i < v.count; i++ {
			result.Add(uint32(v.inline[i]))
		}
	} else {
		result = v.head.Clone()
	}

	// Undo headUndo first
	for _, id := range v.headUndo.added {
		result.Remove(uint32(id))
	}
	for _, id := range v.headUndo.removed {
		result.Add(uint32(id))
	}

	// Walk history in reverse to find state <= maxTime
	currentTime := v.headTime
	historyDepth := 0
	for i := len(v.history) - 1; i >= 0; i-- {
		undo := &v.history[i]
		currentTime = currentTime.Less(undo.timeDelta)
		historyDepth++
		probe.UndoApplied(uint32(undo.timeDelta))
		if currentTime <= maxTime {
			probe.HistoryDepth(historyDepth)
			probe.Result(true, currentTime)
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
	probe.HistoryDepth(historyDepth)
	probe.Result(false, 0)
	return nil, 0
}
