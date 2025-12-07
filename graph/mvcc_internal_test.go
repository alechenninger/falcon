package graph

import (
	"testing"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestVersionedSet_Add tests basic add operations.
func TestVersionedSet_Add(t *testing.T) {
	vs := newVersionedSet(0)

	// Add an ID
	vs.Add(1, 10)

	if !vs.Contains(1) {
		t.Error("expected ID 1 to be present after Add")
	}
	if vs.HeadTime() != 10 {
		t.Errorf("expected headTime=10, got %d", vs.HeadTime())
	}

	// Add same ID again (no-op)
	vs.Add(1, 20)
	if vs.HeadTime() != 10 {
		t.Errorf("expected headTime=10 (no change for duplicate add), got %d", vs.HeadTime())
	}

	// Add different ID
	vs.Add(2, 30)
	if !vs.Contains(2) {
		t.Error("expected ID 2 to be present after Add")
	}
	if vs.HeadTime() != 30 {
		t.Errorf("expected headTime=30, got %d", vs.HeadTime())
	}
}

// TestVersionedSet_Remove tests basic remove operations.
func TestVersionedSet_Remove(t *testing.T) {
	vs := newVersionedSet(0)

	// Add then remove
	vs.Add(1, 10)
	vs.Remove(1, 20)

	if vs.Contains(1) {
		t.Error("expected ID 1 to be absent after Remove")
	}
	if vs.HeadTime() != 20 {
		t.Errorf("expected headTime=20, got %d", vs.HeadTime())
	}

	// Remove non-existent ID (no-op)
	vs.Remove(999, 30)
	if vs.HeadTime() != 20 {
		t.Errorf("expected headTime=20 (no change for removing non-existent), got %d", vs.HeadTime())
	}
}

// TestVersionedSet_DeltaChainInvariants verifies the delta chain is computed correctly.
func TestVersionedSet_DeltaChainInvariants(t *testing.T) {
	vs := newVersionedSet(0)

	// Add entries at known times
	times := []store.StoreTime{100, 200, 350, 500, 750}
	for i, time := range times {
		vs.Add(schema.ID(i+1), time)
	}

	// Verify we can reconstruct all times correctly via OldestTime
	oldestTime := vs.OldestTime()
	if oldestTime != times[0] {
		t.Errorf("OldestTime() = %d, want %d", oldestTime, times[0])
	}

	// Verify HeadTime
	if vs.HeadTime() != times[len(times)-1] {
		t.Errorf("HeadTime() = %d, want %d", vs.HeadTime(), times[len(times)-1])
	}

	// Verify ContainsWithin works correctly for each time
	for i, time := range times {
		// ID should be present at its add time
		found, stateTime := vs.ContainsWithin(schema.ID(i+1), time)
		if !found || stateTime != time {
			t.Errorf("ID %d should be present at time %d (got found=%v, stateTime=%d)", i+1, time, found, stateTime)
		}
		// ID should not be present just before its add time
		if time > 0 {
			found, _ := vs.ContainsWithin(schema.ID(i+1), time-1)
			if found {
				t.Errorf("ID %d should NOT be present at time %d", i+1, time-1)
			}
		}
	}
}

// TestVersionedSet_Truncate tests that old undo entries are removed.
func TestVersionedSet_Truncate(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history at times 10, 20, 30, 40
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Add(3, 30)
	vs.Add(4, 40)

	// Before truncation, we can query at t=10
	found, stateTime := vs.ContainsWithin(1, 10)
	if !found || stateTime != 10 {
		t.Error("before truncation: ID 1 should be present at t=10")
	}

	// Truncate entries older than t=25
	vs.Truncate(25)

	// HEAD should still work
	if !vs.Contains(1) || !vs.Contains(2) || !vs.Contains(3) || !vs.Contains(4) {
		t.Error("after truncation: HEAD should still have all IDs")
	}

	// Query at t=30 should still work
	found, stateTime = vs.ContainsWithin(3, 30)
	if !found || stateTime != 30 {
		t.Error("after truncation: ID 3 should be present at t=30")
	}

	// The oldest time should now be >= 25
	if vs.OldestTime() < 25 {
		t.Errorf("after Truncate(25): OldestTime() = %d, expected >= 25", vs.OldestTime())
	}
}

// TestVersionedSet_EmptySet tests behavior with no changes.
func TestVersionedSet_EmptySet(t *testing.T) {
	vs := newVersionedSet(100)

	if vs.Contains(1) {
		t.Error("empty set should not contain any IDs")
	}
	if vs.HeadTime() != 100 {
		t.Errorf("HeadTime() = %d, want 100", vs.HeadTime())
	}
	if vs.OldestTime() != 100 {
		t.Errorf("OldestTime() = %d, want 100 (same as head for empty)", vs.OldestTime())
	}
	if !vs.IsEmpty() {
		t.Error("IsEmpty() should return true for empty set")
	}

	// ContainsWithin should return (false, 0) for time before headTime (no history)
	found, stateTime := vs.ContainsWithin(1, 50)
	if found || stateTime != 0 {
		t.Errorf("ContainsWithin(1, 50) = (%v, %d), want (false, 0)", found, stateTime)
	}

	// ContainsWithin should return (false, 100) for time >= headTime
	found, stateTime = vs.ContainsWithin(1, 150)
	if found || stateTime != 100 {
		t.Errorf("ContainsWithin(1, 150) = (%v, %d), want (false, 100)", found, stateTime)
	}

	// SnapshotWithin should return (nil, 0) for time before headTime
	snapshot, stateTime := vs.SnapshotWithin(50)
	if snapshot != nil || stateTime != 0 {
		t.Errorf("SnapshotWithin(50) should return (nil, 0), got stateTime=%d", stateTime)
	}

	// SnapshotWithin should return empty bitmap for time >= headTime
	snapshot, stateTime = vs.SnapshotWithin(150)
	if stateTime != 100 || !snapshot.IsEmpty() {
		t.Error("SnapshotWithin(150) should return empty bitmap at stateTime=100")
	}
}

// TestVersionedSet_AddRemoveAdd tests re-adding a removed ID.
func TestVersionedSet_AddRemoveAdd(t *testing.T) {
	vs := newVersionedSet(0)

	vs.Add(1, 10)    // Add
	vs.Remove(1, 20) // Remove
	vs.Add(1, 30)    // Add again

	// Current state should have ID 1
	if !vs.Contains(1) {
		t.Error("ID 1 should be present after add-remove-add")
	}

	// Historical queries using ContainsWithin
	tests := []struct {
		maxTime   store.StoreTime
		wantFound bool
		wantTime  store.StoreTime
	}{
		{10, true, 10},  // at add time
		{15, true, 10},  // between add and remove (uses state at t=10)
		{20, false, 20}, // at remove time
		{25, false, 20}, // between remove and re-add (uses state at t=20)
		{30, true, 30},  // at re-add time
	}

	for _, tt := range tests {
		found, stateTime := vs.ContainsWithin(1, tt.maxTime)
		if found != tt.wantFound {
			t.Errorf("ContainsWithin(1, %d) found = %v, want %v", tt.maxTime, found, tt.wantFound)
		}
		if stateTime != tt.wantTime {
			t.Errorf("ContainsWithin(1, %d) stateTime = %d, want %d", tt.maxTime, stateTime, tt.wantTime)
		}
	}
}

// TestVersionedSet_MultipleIDs tests operations with multiple IDs.
func TestVersionedSet_MultipleIDs(t *testing.T) {
	vs := newVersionedSet(0)

	// Interleaved operations on multiple IDs
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Remove(1, 30)
	vs.Add(3, 40)
	vs.Remove(2, 50)
	vs.Add(1, 60) // Re-add 1

	tests := []struct {
		maxTime  store.StoreTime
		expected []schema.ID
		absent   []schema.ID
	}{
		{5, nil, []schema.ID{1, 2, 3}}, // No state available
		{10, []schema.ID{1}, []schema.ID{2, 3}},
		{20, []schema.ID{1, 2}, []schema.ID{3}},
		{30, []schema.ID{2}, []schema.ID{1, 3}},
		{40, []schema.ID{2, 3}, []schema.ID{1}},
		{50, []schema.ID{3}, []schema.ID{1, 2}},
		{60, []schema.ID{1, 3}, []schema.ID{2}},
		{100, []schema.ID{1, 3}, []schema.ID{2}},
	}

	for _, tt := range tests {
		for _, id := range tt.expected {
			found, stateTime := vs.ContainsWithin(id, tt.maxTime)
			if !found {
				t.Errorf("at maxTime=%d: expected ID %d to be present (stateTime=%d)", tt.maxTime, id, stateTime)
			}
		}
		for _, id := range tt.absent {
			found, _ := vs.ContainsWithin(id, tt.maxTime)
			if found {
				t.Errorf("at maxTime=%d: expected ID %d to be absent", tt.maxTime, id)
			}
		}
	}
}

// TestVersionedSet_HeadUndoIsSet verifies that headUndo is always set after changes.
func TestVersionedSet_HeadUndoIsSet(t *testing.T) {
	vs := newVersionedSet(0)

	// Add multiple entries
	for i := 1; i <= 5; i++ {
		vs.Add(schema.ID(i), store.StoreTime(i*100))

		// After each add, headUndo should be set
		vs.mu.RLock()
		if vs.headUndo == nil {
			t.Errorf("after adding ID %d: headUndo should not be nil", i)
		}
		// History should have i-1 entries (all previous headUndos)
		if len(vs.history) != i-1 {
			t.Errorf("after adding ID %d: len(history) = %d, want %d",
				i, len(vs.history), i-1)
		}
		vs.mu.RUnlock()
	}
}

// TestVersionedSet_DeltaChainReconstruction verifies that we can reconstruct
// all historical times by walking the delta chain.
func TestVersionedSet_DeltaChainReconstruction(t *testing.T) {
	vs := newVersionedSet(0)

	// Add entries at specific times
	times := []store.StoreTime{100, 250, 400, 600, 1000}
	for i, time := range times {
		vs.Add(schema.ID(i+1), time)
	}

	// Walk the delta chain and verify we can reconstruct each time
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// headUndo is at headTime (times[4] = 1000)
	if vs.headTime != times[len(times)-1] {
		t.Errorf("headTime = %d, expected %d", vs.headTime, times[len(times)-1])
	}

	// Walk history in reverse (newest to oldest)
	currentTime := vs.headTime
	for i := len(vs.history) - 1; i >= 0; i-- {
		currentTime = currentTime.Less(vs.history[i].timeDelta)

		// The reconstructed time should match our expected times
		// history[i] corresponds to times[i] (oldest first)
		expectedTime := times[i]
		if currentTime != expectedTime {
			t.Errorf("history[%d]: reconstructed time = %d, expected %d",
				i, currentTime, expectedTime)
		}
	}
}

// TestVersionedSet_LargeDelta tests that large time gaps work correctly.
func TestVersionedSet_LargeDelta(t *testing.T) {
	vs := newVersionedSet(0)

	// Use times with large gaps (but still fitting in uint32 delta)
	vs.Add(1, 1_000_000)
	vs.Add(2, 2_000_000_000) // 2 billion gap

	found, stateTime := vs.ContainsWithin(1, 1_000_000)
	if !found || stateTime != 1_000_000 {
		t.Error("ID 1 should be present at t=1_000_000")
	}

	found, stateTime = vs.ContainsWithin(2, 2_000_000_000)
	if !found || stateTime != 2_000_000_000 {
		t.Error("ID 2 should be present at t=2_000_000_000")
	}

	// At t=1_500_000_000, state is at t=1_000_000, ID 2 not yet added
	found, stateTime = vs.ContainsWithin(2, 1_500_000_000)
	if found {
		t.Error("ID 2 should NOT be present at t=1_500_000_000")
	}
	if stateTime != 1_000_000 {
		t.Errorf("stateTime should be 1_000_000, got %d", stateTime)
	}
}

// TestVersionedSet_HeadTimeQuery tests querying at exactly headTime.
func TestVersionedSet_HeadTimeQuery(t *testing.T) {
	vs := newVersionedSet(0)

	vs.Add(1, 10)
	vs.Add(2, 20)

	// Query at exactly headTime should match HEAD state
	headTime := vs.HeadTime()

	found1, stateTime := vs.ContainsWithin(1, headTime)
	if !found1 || stateTime != headTime {
		t.Errorf("ContainsWithin(1, headTime) = (%v, %d), want (true, %d)", found1, stateTime, headTime)
	}

	found2, stateTime := vs.ContainsWithin(2, headTime)
	if !found2 || stateTime != headTime {
		t.Errorf("ContainsWithin(2, headTime) = (%v, %d), want (true, %d)", found2, stateTime, headTime)
	}

	snapshot, stateTime := vs.SnapshotWithin(headTime)
	if stateTime != headTime {
		t.Errorf("SnapshotWithin(headTime) stateTime = %d, want %d", stateTime, headTime)
	}
	if !snapshot.Contains(1) || !snapshot.Contains(2) {
		t.Error("SnapshotWithin(headTime) should match HEAD")
	}
}

// TestVersionedSet_QueryFutureTime tests querying at a time after headTime.
func TestVersionedSet_QueryFutureTime(t *testing.T) {
	vs := newVersionedSet(0)

	vs.Add(1, 10)
	vs.Add(2, 20)

	headTime := vs.HeadTime()
	futureTime := headTime + 1000

	// Query at future time should match HEAD state (stateTime = headTime)
	found1, stateTime := vs.ContainsWithin(1, futureTime)
	if !found1 || stateTime != headTime {
		t.Errorf("ContainsWithin(1, futureTime) = (%v, %d), want (true, %d)", found1, stateTime, headTime)
	}

	found2, stateTime := vs.ContainsWithin(2, futureTime)
	if !found2 || stateTime != headTime {
		t.Errorf("ContainsWithin(2, futureTime) = (%v, %d), want (true, %d)", found2, stateTime, headTime)
	}
}

// TestVersionedSet_ContainsWithin tests the combined query and state time lookup.
func TestVersionedSet_ContainsWithin(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history: add 1 at t=10, add 2 at t=20, remove 1 at t=30
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Remove(1, 30)

	tests := []struct {
		name          string
		id            schema.ID
		maxTime       store.StoreTime
		wantFound     bool
		wantStateTime store.StoreTime
	}{
		// max >= head: use head state
		{"id1 max=100", 1, 100, false, 30},
		{"id2 max=100", 2, 100, true, 30},
		{"id1 max=30", 1, 30, false, 30},
		{"id2 max=30", 2, 30, true, 30},

		// max between entries: find latest state <= max
		{"id1 max=25", 1, 25, true, 20},
		{"id2 max=25", 2, 25, true, 20},
		{"id1 max=15", 1, 15, true, 10},
		{"id2 max=15", 2, 15, false, 10},

		// max < oldest: no state available
		{"id1 max=5", 1, 5, false, 0},
		{"id2 max=5", 2, 5, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, stateTime := vs.ContainsWithin(tt.id, tt.maxTime)
			if found != tt.wantFound {
				t.Errorf("ContainsWithin(%d, %d) found = %v, want %v", tt.id, tt.maxTime, found, tt.wantFound)
			}
			if stateTime != tt.wantStateTime {
				t.Errorf("ContainsWithin(%d, %d) stateTime = %d, want %d", tt.id, tt.maxTime, stateTime, tt.wantStateTime)
			}
		})
	}
}

// TestVersionedSet_SnapshotWithin tests the combined snapshot and state time lookup.
func TestVersionedSet_SnapshotWithin(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history: add 1,2,3 at t=10,20,30, remove 1 at t=40
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Add(3, 30)
	vs.Remove(1, 40)

	tests := []struct {
		name          string
		maxTime       store.StoreTime
		wantIDs       []schema.ID
		wantStateTime store.StoreTime
	}{
		{"max=100", 100, []schema.ID{2, 3}, 40},
		{"max=40", 40, []schema.ID{2, 3}, 40},
		{"max=35", 35, []schema.ID{1, 2, 3}, 30},
		{"max=25", 25, []schema.ID{1, 2}, 20},
		{"max=15", 15, []schema.ID{1}, 10},
		{"max=5", 5, nil, 0}, // No state available
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot, stateTime := vs.SnapshotWithin(tt.maxTime)
			if stateTime != tt.wantStateTime {
				t.Errorf("SnapshotWithin(%d) stateTime = %d, want %d", tt.maxTime, stateTime, tt.wantStateTime)
			}
			if tt.wantStateTime == 0 {
				if snapshot != nil {
					t.Errorf("SnapshotWithin(%d) expected nil snapshot when stateTime=0", tt.maxTime)
				}
				return
			}
			for _, id := range tt.wantIDs {
				if !snapshot.Contains(uint32(id)) {
					t.Errorf("SnapshotWithin(%d) missing ID %d", tt.maxTime, id)
				}
			}
			if int(snapshot.GetCardinality()) != len(tt.wantIDs) {
				t.Errorf("SnapshotWithin(%d) has %d elements, want %d",
					tt.maxTime, snapshot.GetCardinality(), len(tt.wantIDs))
			}
		})
	}
}
