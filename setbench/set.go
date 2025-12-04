// Package falcon provides set membership implementations for benchmarking.
package falcon

import "github.com/RoaringBitmap/roaring"

// RoaringSet uses a roaring bitmap for membership checks, with a mapping
// from arbitrary string IDs to dense integers. This approach trades the overhead
// of maintaining a dictionary for the compression benefits of roaring bitmaps.
type RoaringSet struct {
	bitmap  *roaring.Bitmap
	idToIdx map[string]uint32
	nextIdx uint32
}

// NewRoaringSet creates a new RoaringSet.
func NewRoaringSet() *RoaringSet {
	return &RoaringSet{
		bitmap:  roaring.NewBitmap(),
		idToIdx: make(map[string]uint32),
	}
}

// NewRoaringSetWithCapacity creates a new RoaringSet with pre-allocated map capacity.
func NewRoaringSetWithCapacity(capacity int) *RoaringSet {
	return &RoaringSet{
		bitmap:  roaring.NewBitmap(),
		idToIdx: make(map[string]uint32, capacity),
	}
}

// Add inserts an ID into the set. Returns true if the ID was newly added.
func (r *RoaringSet) Add(id string) bool {
	if idx, exists := r.idToIdx[id]; exists {
		// Already in the mapping, check if in bitmap
		if r.bitmap.Contains(idx) {
			return false
		}
		r.bitmap.Add(idx)
		return true
	}

	// New ID: assign a dense index
	idx := r.nextIdx
	r.nextIdx++
	r.idToIdx[id] = idx
	r.bitmap.Add(idx)
	return true
}

// Contains checks if the ID is in the set.
func (r *RoaringSet) Contains(id string) bool {
	if idx, exists := r.idToIdx[id]; exists {
		return r.bitmap.Contains(idx)
	}
	return false
}

// Cardinality returns the number of elements in the set.
func (r *RoaringSet) Cardinality() uint64 {
	return r.bitmap.GetCardinality()
}

// PlainSet uses a simple Go map for membership checks.
// This is the baseline comparison for the roaring approach.
type PlainSet struct {
	members map[string]struct{}
}

// NewPlainSet creates a new PlainSet.
func NewPlainSet() *PlainSet {
	return &PlainSet{
		members: make(map[string]struct{}),
	}
}

// NewPlainSetWithCapacity creates a new PlainSet with pre-allocated capacity.
func NewPlainSetWithCapacity(capacity int) *PlainSet {
	return &PlainSet{
		members: make(map[string]struct{}, capacity),
	}
}

// Add inserts an ID into the set. Returns true if the ID was newly added.
func (p *PlainSet) Add(id string) bool {
	if _, exists := p.members[id]; exists {
		return false
	}
	p.members[id] = struct{}{}
	return true
}

// Contains checks if the ID is in the set.
func (p *PlainSet) Contains(id string) bool {
	_, exists := p.members[id]
	return exists
}

// Cardinality returns the number of elements in the set.
func (p *PlainSet) Cardinality() uint64 {
	return uint64(len(p.members))
}

// ============================================================================
// Shared Dictionary Approach (for ID reuse across multiple sets)
// ============================================================================

// IDDictionary provides a shared mapping from arbitrary string IDs to dense integers.
// This allows multiple roaring bitmaps to share the same ID space, amortizing
// the dictionary overhead across many sets.
type IDDictionary struct {
	idToIdx map[string]uint32
	nextIdx uint32
}

// NewIDDictionary creates a new shared ID dictionary.
func NewIDDictionary() *IDDictionary {
	return &IDDictionary{
		idToIdx: make(map[string]uint32),
	}
}

// NewIDDictionaryWithCapacity creates a new dictionary with pre-allocated capacity.
func NewIDDictionaryWithCapacity(capacity int) *IDDictionary {
	return &IDDictionary{
		idToIdx: make(map[string]uint32, capacity),
	}
}

// GetOrAssign returns the dense index for an ID, assigning one if needed.
func (d *IDDictionary) GetOrAssign(id string) uint32 {
	if idx, exists := d.idToIdx[id]; exists {
		return idx
	}
	idx := d.nextIdx
	d.nextIdx++
	d.idToIdx[id] = idx
	return idx
}

// Get returns the dense index for an ID, or (0, false) if not found.
func (d *IDDictionary) Get(id string) (uint32, bool) {
	idx, exists := d.idToIdx[id]
	return idx, exists
}

// Len returns the number of IDs in the dictionary.
func (d *IDDictionary) Len() int {
	return len(d.idToIdx)
}

// SharedRoaringSet is a roaring bitmap that uses a shared dictionary.
// Multiple SharedRoaringSets can share the same IDDictionary.
type SharedRoaringSet struct {
	bitmap *roaring.Bitmap
	dict   *IDDictionary
}

// NewSharedRoaringSet creates a new set using the provided shared dictionary.
func NewSharedRoaringSet(dict *IDDictionary) *SharedRoaringSet {
	return &SharedRoaringSet{
		bitmap: roaring.NewBitmap(),
		dict:   dict,
	}
}

// Add inserts an ID into the set (also adds to shared dictionary if new).
func (s *SharedRoaringSet) Add(id string) bool {
	idx := s.dict.GetOrAssign(id)
	if s.bitmap.Contains(idx) {
		return false
	}
	s.bitmap.Add(idx)
	return true
}

// Contains checks if the ID is in the set.
func (s *SharedRoaringSet) Contains(id string) bool {
	if idx, exists := s.dict.Get(id); exists {
		return s.bitmap.Contains(idx)
	}
	return false
}

// Cardinality returns the number of elements in the set.
func (s *SharedRoaringSet) Cardinality() uint64 {
	return s.bitmap.GetCardinality()
}

// Bitmap returns the underlying roaring bitmap (for memory measurements).
func (s *SharedRoaringSet) Bitmap() *roaring.Bitmap {
	return s.bitmap
}
