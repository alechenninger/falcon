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
// Timestamps are stored as chain deltas to save memory (4 bytes vs 8 bytes per entry):
//   - undos[0].timeDelta = headTime - undos[0].time
//   - undos[n].timeDelta = undos[n-1].time - undos[n].time (for n > 0)
//
// To compute an entry's timestamp, iterate from the front accumulating deltas.
type undoEntry struct {
	timeDelta store.StoreDelta // Delta from previous entry's time (or headTime for first entry)
	added     []schema.ID      // IDs added at this time (undo = remove)
	removed   []schema.ID      // IDs removed at this time (undo = add back)
}

// versionedSet stores a bitmap with MVCC support via undo chains.
// HEAD is always the current state; historical reads walk the undo chain.
type versionedSet struct {
	mu       sync.RWMutex
	headTime store.StoreTime // Timestamp of the current head state
	head     *roaring.Bitmap // Always current state
	undos    []undoEntry     // Newest first (reverse chronological order)
}

// newVersionedSet creates a new versioned set starting at the given time.
func newVersionedSet(t store.StoreTime) *versionedSet {
	return &versionedSet{
		headTime: t,
		head:     roaring.New(),
		undos:    nil,
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
	v.prependUndo(t, undoEntry{
		timeDelta: 0, // Will be set by prependUndo
		added:     []schema.ID{id},
	})
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
	v.prependUndo(t, undoEntry{
		timeDelta: 0, // Will be set by prependUndo
		removed:   []schema.ID{id},
	})
}

// prependUndo adds a new undo entry at the front and maintains the chain delta invariant.
// Must be called with v.mu held.
func (v *versionedSet) prependUndo(t store.StoreTime, entry undoEntry) {
	oldHeadTime := v.headTime

	// The new entry's delta is 0 (it's at headTime after we update it)
	entry.timeDelta = 0

	// Prepend the new entry
	v.undos = append([]undoEntry{entry}, v.undos...)

	// Update the second entry's delta (was first, now second)
	// Its time stays the same, but its delta is now relative to the new first entry
	if len(v.undos) > 1 {
		// oldFirstTime = oldHeadTime - oldFirst.timeDelta
		// newDelta = newFirstTime - oldFirstTime = t - (oldHeadTime - oldDelta)
		oldDelta := v.undos[1].timeDelta
		// Compute: t - oldHeadTime + oldDelta
		// This is equivalent to: t - (oldHeadTime - oldDelta)
		// The Less method panics if the result exceeds uint32
		newDelta := t.Difference(oldHeadTime.Less(oldDelta))
		v.undos[1].timeDelta = newDelta
	}

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

	// Walk undo chain, undoing changes newer than target time
	// Compute times incrementally using chain deltas
	currentTime := v.headTime
	for _, undo := range v.undos {
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime <= t {
			break // Reached target time, stop undoing
		}
		// Undo this change
		if slices.Contains(undo.added, id) {
			result = false // Was added after target, undo it
		}
		if slices.Contains(undo.removed, id) {
			result = true // Was removed after target, undo it
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

	// Find the first entry that's < minTime (oldest we need to keep)
	// Compute times incrementally using chain deltas
	cutoff := len(v.undos)
	currentTime := v.headTime
	for i, undo := range v.undos {
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime < minTime {
			cutoff = i
			break
		}
	}
	v.undos = v.undos[:cutoff]
}

// SnapshotAt returns a clone of the bitmap as it was at the given time.
// This is expensive and should only be used when a full bitmap is needed
// (e.g., for GetSubjects at a historical time).
func (v *versionedSet) SnapshotAt(t store.StoreTime) *roaring.Bitmap {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := v.head.Clone()

	// Walk undo chain, applying inverses for changes newer than target time
	// Compute times incrementally using chain deltas
	currentTime := v.headTime
	for _, undo := range v.undos {
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime <= t {
			break
		}
		// Undo this change
		for _, id := range undo.added {
			result.Remove(uint32(id))
		}
		for _, id := range undo.removed {
			result.Add(uint32(id))
		}
	}
	return result
}

// OldestTime returns the oldest time in the undo chain, or headTime if empty.
func (v *versionedSet) OldestTime() store.StoreTime {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.undos) == 0 {
		return v.headTime
	}

	// Compute the oldest time by walking the chain
	currentTime := v.headTime
	for _, undo := range v.undos {
		currentTime = currentTime.Less(undo.timeDelta)
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

	// Walk undo chain to find the latest state <= maxTime
	// Each undo entry represents a change at that time.
	// The state "before" that change existed at the previous entry's time
	// (or the oldest time if it's the last entry).
	// Compute times incrementally using chain deltas
	currentTime := v.headTime
	for i, undo := range v.undos {
		currentTime = currentTime.Less(undo.timeDelta)
		if currentTime <= maxTime {
			return currentTime
		}
		// If we've walked past maxTime, the usable state is the one
		// just before this change (if there is one)
		if i == len(v.undos)-1 {
			// This is the oldest change we have - we can't go further back
			// The state before this change is unknown/unavailable
			return 0
		}
	}

	return 0
}
