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
type undoEntry struct {
	lsn     LSN         // LSN this change happened at
	added   []schema.ID // IDs added at this LSN (undo = remove)
	removed []schema.ID // IDs removed at this LSN (undo = add back)
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
	v.undos = append([]undoEntry{{
		lsn:   lsn,
		added: []schema.ID{id},
	}}, v.undos...)
	v.headLSN = lsn
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
	v.undos = append([]undoEntry{{
		lsn:     lsn,
		removed: []schema.ID{id},
	}}, v.undos...)
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
	for _, undo := range v.undos {
		if undo.lsn <= lsn {
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

	// Find the first entry that's <= minLSN (oldest we need to keep)
	cutoff := len(v.undos)
	for i, undo := range v.undos {
		if undo.lsn < minLSN {
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
	for _, undo := range v.undos {
		if undo.lsn <= lsn {
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
	return v.undos[len(v.undos)-1].lsn
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
	for i, undo := range v.undos {
		if undo.lsn <= maxLSN {
			return undo.lsn
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
