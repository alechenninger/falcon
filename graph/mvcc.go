// Package graph implements the in-memory tuple store with MVCC support.
package graph

import (
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
)

// LSN represents a Log Sequence Number from the WAL.
type LSN = uint64

// undoEntry records a change that can be undone to reconstruct historical state.
// LSNs are stored as chain deltas to save memory (4 bytes vs 8 bytes per entry):
//   - undos[0].lsnDelta = headLSN - undos[0].lsn
//   - undos[n].lsnDelta = undos[n-1].lsn - undos[n].lsn (for n > 0)
//
// To compute an entry's LSN, iterate from the front accumulating deltas.
type undoEntry struct {
	lsnDelta uint32      // Delta from previous entry's LSN (or headLSN for first entry)
	added    []schema.ID // IDs added at this LSN (undo = remove)
	removed  []schema.ID // IDs removed at this LSN (undo = add back)
}

// versionedSet stores a bitmap with MVCC support via undo chains.
// HEAD is always the current state; historical reads walk the undo chain.
type versionedSet struct {
	mu      sync.RWMutex
	headLSN LSN             // LSN of the current head state
	head    *roaring.Bitmap // Always current state
	undos   []undoEntry     // Newest first (reverse chronological order)
}

// newVersionedSet creates a new versioned set starting at the given LSN.
func newVersionedSet(lsn LSN) *versionedSet {
	return &versionedSet{
		headLSN: lsn,
		head:    roaring.New(),
		undos:   nil,
	}
}

// Add adds an ID to the set at the given LSN.
// If the ID already exists, this is a no-op.
func (v *versionedSet) Add(id schema.ID, lsn LSN) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.head.Contains(uint32(id)) {
		return // Already present, no change
	}

	v.head.Add(uint32(id))

	// Record undo: this was an add, so undo = remove
	v.prependUndo(lsn, undoEntry{
		lsnDelta: 0, // Will be set by prependUndo
		added:    []schema.ID{id},
	})
}

// Remove removes an ID from the set at the given LSN.
// If the ID doesn't exist, this is a no-op.
func (v *versionedSet) Remove(id schema.ID, lsn LSN) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.head.Contains(uint32(id)) {
		return // Not present, no change
	}

	v.head.Remove(uint32(id))

	// Record undo: this was a remove, so undo = add back
	v.prependUndo(lsn, undoEntry{
		lsnDelta: 0, // Will be set by prependUndo
		removed:  []schema.ID{id},
	})
}

// prependUndo adds a new undo entry at the front and maintains the chain delta invariant.
// Must be called with v.mu held.
func (v *versionedSet) prependUndo(lsn LSN, entry undoEntry) {
	oldHeadLSN := v.headLSN

	// The new entry's delta is 0 (it's at headLSN after we update it)
	entry.lsnDelta = 0

	// Prepend the new entry
	v.undos = append([]undoEntry{entry}, v.undos...)

	// Update the second entry's delta (was first, now second)
	// Its LSN stays the same, but its delta is now relative to the new first entry
	if len(v.undos) > 1 {
		// oldFirstLSN = oldHeadLSN - oldFirst.lsnDelta
		// newDelta = newFirstLSN - oldFirstLSN = lsn - (oldHeadLSN - oldDelta)
		oldDelta := v.undos[1].lsnDelta
		newDelta := lsn - oldHeadLSN + LSN(oldDelta)
		if newDelta > LSN(^uint32(0)) {
			panic("LSN delta overflow: undo chain spans more than 4 billion LSNs")
		}
		v.undos[1].lsnDelta = uint32(newDelta)
	}

	v.headLSN = lsn
}

// Contains checks if the ID is in the current (HEAD) state.
func (v *versionedSet) Contains(id schema.ID) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.head.Contains(uint32(id))
}

// ContainsAt checks if the ID was in the set at the given historical LSN.
// This walks the undo chain to reconstruct the historical state without cloning.
func (v *versionedSet) ContainsAt(id schema.ID, lsn LSN) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := v.head.Contains(uint32(id))

	// Walk undo chain, undoing changes newer than target LSN
	// Compute LSNs incrementally using chain deltas
	currentLSN := v.headLSN
	for _, undo := range v.undos {
		currentLSN -= LSN(undo.lsnDelta)
		if currentLSN <= lsn {
			break // Reached target LSN, stop undoing
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

// HeadLSN returns the LSN of the current HEAD state.
func (v *versionedSet) HeadLSN() LSN {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.headLSN
}

// IsEmpty returns true if the HEAD bitmap is empty.
func (v *versionedSet) IsEmpty() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.head.IsEmpty()
}

// Truncate removes undo entries older than the given LSN.
// This is used for garbage collection - entries older than the oldest
// active query can be discarded.
func (v *versionedSet) Truncate(minLSN LSN) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Find the first entry that's < minLSN (oldest we need to keep)
	// Compute LSNs incrementally using chain deltas
	cutoff := len(v.undos)
	currentLSN := v.headLSN
	for i, undo := range v.undos {
		currentLSN -= LSN(undo.lsnDelta)
		if currentLSN < minLSN {
			cutoff = i
			break
		}
	}
	v.undos = v.undos[:cutoff]
}

// SnapshotAt returns a clone of the bitmap as it was at the given LSN.
// This is expensive and should only be used when a full bitmap is needed
// (e.g., for GetSubjects at a historical LSN).
func (v *versionedSet) SnapshotAt(lsn LSN) *roaring.Bitmap {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := v.head.Clone()

	// Walk undo chain, applying inverses for changes newer than target LSN
	// Compute LSNs incrementally using chain deltas
	currentLSN := v.headLSN
	for _, undo := range v.undos {
		currentLSN -= LSN(undo.lsnDelta)
		if currentLSN <= lsn {
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

// OldestLSN returns the oldest LSN in the undo chain, or headLSN if empty.
func (v *versionedSet) OldestLSN() LSN {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.undos) == 0 {
		return v.headLSN
	}

	// Compute the oldest LSN by walking the chain
	currentLSN := v.headLSN
	for _, undo := range v.undos {
		currentLSN -= LSN(undo.lsnDelta)
	}
	return currentLSN
}

// StateLSNWithin returns the LSN of the latest state that is <= maxLSN.
// This is the state we would use when reading within the given snapshot window.
//
// If headLSN <= maxLSN, returns headLSN (we can use current state).
// Otherwise, walks the undo chain to find the latest usable state.
// Returns 0 if no state is available within the window.
func (v *versionedSet) StateLSNWithin(maxLSN LSN) LSN {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// If head is within bounds, use it
	if v.headLSN <= maxLSN {
		return v.headLSN
	}

	// Walk undo chain to find the latest state <= maxLSN
	// Each undo entry represents a change at that LSN.
	// The state "before" that change existed at the previous entry's LSN
	// (or the oldest LSN if it's the last entry).
	// Compute LSNs incrementally using chain deltas
	currentLSN := v.headLSN
	for i, undo := range v.undos {
		currentLSN -= LSN(undo.lsnDelta)
		if currentLSN <= maxLSN {
			return currentLSN
		}
		// If we've walked past maxLSN, the usable state is the one
		// just before this change (if there is one)
		if i == len(v.undos)-1 {
			// This is the oldest change we have - we can't go further back
			// The state before this change is unknown/unavailable
			return 0
		}
	}

	return 0
}
