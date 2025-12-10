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

	// ID 1 was added at 10, unchanged since - oldest valid time is 10
	found1, stateTime := vs.ContainsWithin(1, headTime)
	if !found1 || stateTime != 10 {
		t.Errorf("ContainsWithin(1, headTime) = (%v, %d), want (true, 10)", found1, stateTime)
	}

	// ID 2 was added at 20 - oldest valid time is 20
	found2, stateTime := vs.ContainsWithin(2, headTime)
	if !found2 || stateTime != 20 {
		t.Errorf("ContainsWithin(2, headTime) = (%v, %d), want (true, 20)", found2, stateTime)
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

	futureTime := vs.HeadTime() + 1000

	// Query at future time should return oldest time where the answer is valid
	// ID 1 was added at 10, unchanged since
	found1, stateTime := vs.ContainsWithin(1, futureTime)
	if !found1 || stateTime != 10 {
		t.Errorf("ContainsWithin(1, futureTime) = (%v, %d), want (true, 10)", found1, stateTime)
	}

	// ID 2 was added at 20
	found2, stateTime := vs.ContainsWithin(2, futureTime)
	if !found2 || stateTime != 20 {
		t.Errorf("ContainsWithin(2, futureTime) = (%v, %d), want (true, 20)", found2, stateTime)
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
		// max >= head: returns oldest time where the answer is valid
		{"id1 max=100", 1, 100, false, 30}, // removed at 30, oldest "not found" time is 30
		{"id2 max=100", 2, 100, true, 20},  // added at 20, unchanged since, oldest "found" time is 20
		{"id1 max=30", 1, 30, false, 30},   // removed at 30
		{"id2 max=30", 2, 30, true, 20},    // added at 20, unchanged since

		// max between entries: find oldest time where the answer matches
		{"id1 max=25", 1, 25, true, 10},  // added at 10, oldest "found" time is 10
		{"id2 max=25", 2, 25, true, 20},  // added at 20, oldest "found" time is 20
		{"id1 max=15", 1, 15, true, 10},  // added at 10
		{"id2 max=15", 2, 15, false, 10}, // not added yet, oldest "not found" time is 10

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

// TestVersionedSet_SnapshotWithin_ReturnsLatestTime verifies that SnapshotWithin
// always returns the LATEST time <= maxTime, NOT the oldest. This is because
// snapshots return actual data, which would change if we went further back.
func TestVersionedSet_SnapshotWithin_ReturnsLatestTime(t *testing.T) {
	vs := newVersionedSet(0)

	// Add IDs at different times
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Add(3, 30)

	// Query at max=100 - should return time 30 (latest), not time 10 (oldest)
	snapshot, stateTime := vs.SnapshotWithin(100)
	if stateTime != 30 {
		t.Errorf("SnapshotWithin should return latest time 30, got %d", stateTime)
	}
	if snapshot.GetCardinality() != 3 {
		t.Errorf("expected 3 IDs in snapshot at time 30, got %d", snapshot.GetCardinality())
	}

	// Query at max=25 - should return time 20 (latest <= 25), not time 10
	snapshot, stateTime = vs.SnapshotWithin(25)
	if stateTime != 20 {
		t.Errorf("SnapshotWithin should return latest time 20, got %d", stateTime)
	}
	if snapshot.GetCardinality() != 2 {
		t.Errorf("expected 2 IDs in snapshot at time 20, got %d", snapshot.GetCardinality())
	}

	// Contrast with ContainsWithin for ID 1 - should return oldest time (10)
	// since ID 1 was added at 10 and unchanged
	found, containsTime := vs.ContainsWithin(1, 100)
	if !found || containsTime != 10 {
		t.Errorf("ContainsWithin should return oldest time 10 for ID 1, got %d", containsTime)
	}
}

// TestVersionedSet_ContainsWithin_OldestTime tests that ContainsWithin returns
// the oldest stateTime that gives the same answer, maximizing snapshot window width.
func TestVersionedSet_ContainsWithin_OldestTime(t *testing.T) {
	t.Run("always negative returns oldest snapshot", func(t *testing.T) {
		vs := newVersionedSet(0)

		// Add some IDs, but never add ID 99
		vs.Add(1, 10)
		vs.Add(2, 20)
		vs.Add(3, 30)

		// Query for ID 99 which was never added - should return oldest time
		found, stateTime := vs.ContainsWithin(99, 100)
		if found {
			t.Error("expected ID 99 to not be found")
		}
		// Should return oldest available time (10) since answer is always "not found"
		if stateTime != 10 {
			t.Errorf("expected oldest time 10, got %d", stateTime)
		}
	})

	t.Run("always positive returns oldest snapshot", func(t *testing.T) {
		vs := newVersionedSet(0)

		// Add ID 1 at time 10, then add unrelated IDs
		vs.Add(1, 10)
		vs.Add(2, 20)
		vs.Add(3, 30)

		// Query for ID 1 - should return time 10 (when it was added, oldest time it exists)
		found, stateTime := vs.ContainsWithin(1, 100)
		if !found {
			t.Error("expected ID 1 to be found")
		}
		// ID 1 was added at time 10, so that's the oldest time where it exists
		if stateTime != 10 {
			t.Errorf("expected oldest time 10, got %d", stateTime)
		}
	})

	t.Run("found at 10 not at 9 uses 10", func(t *testing.T) {
		vs := newVersionedSet(0)

		// Create state at time 9 (ID 1 not present)
		vs.Add(2, 9) // unrelated, creates a snapshot at time 9
		// ID 1 added at time 10
		vs.Add(1, 10)

		// Query for ID 1 at max=20 - should return 10 (oldest time where found)
		// At time 9: ID 1 not present
		// At time 10: ID 1 present
		found, stateTime := vs.ContainsWithin(1, 20)
		if !found {
			t.Error("expected ID 1 to be found")
		}
		if stateTime != 10 {
			t.Errorf("expected time 10 (when added), got %d", stateTime)
		}

		// Query for ID 1 at max=9 - not found, oldest "not found" time is 9
		found, stateTime = vs.ContainsWithin(1, 9)
		if found {
			t.Error("expected ID 1 to NOT be found at max=9")
		}
		if stateTime != 9 {
			t.Errorf("expected time 9 (oldest not-found time), got %d", stateTime)
		}
	})

	t.Run("not found at 10 or 9 but found at 8 uses 9", func(t *testing.T) {
		vs := newVersionedSet(0)

		// ID 1: add at 8, remove at 9
		vs.Add(1, 8)
		vs.Remove(1, 9)
		vs.Add(2, 10) // unrelated

		// Query for ID 1 at max=10 - not found, oldest time with "not found" is 9
		found, stateTime := vs.ContainsWithin(1, 10)
		if found {
			t.Error("expected ID 1 to not be found at max=10")
		}
		// ID 1 was removed at time 9, so 9 is the oldest time where it's absent
		if stateTime != 9 {
			t.Errorf("expected time 9 (when removed), got %d", stateTime)
		}

		// Query at max=8 - found (before removal)
		found, stateTime = vs.ContainsWithin(1, 8)
		if !found {
			t.Error("expected ID 1 to be found at max=8")
		}
		if stateTime != 8 {
			t.Errorf("expected time 8, got %d", stateTime)
		}
	})

	t.Run("add remove add sequence", func(t *testing.T) {
		vs := newVersionedSet(0)

		// ID 1: add at 10, remove at 20, add at 30
		vs.Add(1, 10)
		vs.Remove(1, 20)
		vs.Add(1, 30)

		// At max=100: found, oldest time with "found" is 30 (since removed at 20)
		found, stateTime := vs.ContainsWithin(1, 100)
		if !found {
			t.Error("expected ID 1 to be found at max=100")
		}
		if stateTime != 30 {
			t.Errorf("expected time 30 (re-added), got %d", stateTime)
		}

		// At max=25: not found, oldest time with "not found" is 20
		found, stateTime = vs.ContainsWithin(1, 25)
		if found {
			t.Error("expected ID 1 to not be found at max=25")
		}
		if stateTime != 20 {
			t.Errorf("expected time 20 (removed), got %d", stateTime)
		}

		// At max=15: found, oldest time with "found" is 10
		found, stateTime = vs.ContainsWithin(1, 15)
		if !found {
			t.Error("expected ID 1 to be found at max=15")
		}
		if stateTime != 10 {
			t.Errorf("expected time 10 (added), got %d", stateTime)
		}
	})

	t.Run("multiple unrelated changes preserve oldest time", func(t *testing.T) {
		vs := newVersionedSet(0)

		// ID 1 added at time 10
		vs.Add(1, 10)
		// Many unrelated changes
		vs.Add(2, 20)
		vs.Add(3, 30)
		vs.Add(4, 40)
		vs.Remove(2, 50)
		vs.Add(5, 60)

		// Query for ID 1 - should still return 10 (unchanged since then)
		found, stateTime := vs.ContainsWithin(1, 100)
		if !found {
			t.Error("expected ID 1 to be found")
		}
		if stateTime != 10 {
			t.Errorf("expected oldest time 10, got %d", stateTime)
		}

		// Query for ID 2 - removed at 50, should return 50
		found, stateTime = vs.ContainsWithin(2, 100)
		if found {
			t.Error("expected ID 2 to not be found (removed)")
		}
		if stateTime != 50 {
			t.Errorf("expected time 50 (removed), got %d", stateTime)
		}
	})
}

// TestVersionedSet_ContainsWithin_TimeZero tests that data added at time 0 is correctly found.
// This is important for hydration scenarios where data is loaded at the initial time.
// Regression test for bug where resultTime==0 was incorrectly treated as "no state available".
func TestVersionedSet_ContainsWithin_TimeZero(t *testing.T) {
	t.Run("data_added_at_time_zero_is_found", func(t *testing.T) {
		vs := newVersionedSet(0)
		vs.Add(1, 0)
		vs.Add(2, 0)
		vs.Add(3, 0)

		// Query at time 0 - should find the data
		found, stateTime := vs.ContainsWithin(1, 0)
		if !found {
			t.Error("expected ID 1 to be found at time 0")
		}
		if stateTime != 0 {
			t.Errorf("expected stateTime 0, got %d", stateTime)
		}

		// Query at time 100 - should still find the data (added at 0, still present)
		found, stateTime = vs.ContainsWithin(2, 100)
		if !found {
			t.Error("expected ID 2 to be found at time 100")
		}
		if stateTime != 0 {
			t.Errorf("expected stateTime 0 (oldest time answer is valid), got %d", stateTime)
		}

		// Query for non-existent ID - should not be found
		found, stateTime = vs.ContainsWithin(999, 100)
		if found {
			t.Error("expected ID 999 to not be found")
		}
	})

	t.Run("multiple_ids_added_at_time_zero", func(t *testing.T) {
		vs := newVersionedSet(0)
		
		// Simulate hydration: many IDs added at time 0
		for i := schema.ID(1); i <= 100; i++ {
			vs.Add(i, 0)
		}

		// Verify all IDs are found
		for i := schema.ID(1); i <= 100; i++ {
			found, stateTime := vs.ContainsWithin(i, 1000)
			if !found {
				t.Errorf("expected ID %d to be found", i)
			}
			if stateTime != 0 {
				t.Errorf("expected stateTime 0 for ID %d, got %d", i, stateTime)
			}
		}

		// Verify IDs outside range are not found
		found, _ := vs.ContainsWithin(101, 1000)
		if found {
			t.Error("expected ID 101 to not be found")
		}
	})

	t.Run("time_zero_with_later_changes", func(t *testing.T) {
		vs := newVersionedSet(0)
		
		// Add at time 0
		vs.Add(1, 0)
		
		// Remove at time 10
		vs.Remove(1, 10)
		
		// Add back at time 20
		vs.Add(1, 20)

		// Query at time 0 - should be found (original add)
		found, stateTime := vs.ContainsWithin(1, 0)
		if !found {
			t.Error("expected ID 1 to be found at time 0")
		}
		if stateTime != 0 {
			t.Errorf("expected stateTime 0, got %d", stateTime)
		}

		// Query at time 5 - should be found (between add and remove)
		found, stateTime = vs.ContainsWithin(1, 5)
		if !found {
			t.Error("expected ID 1 to be found at time 5")
		}

		// Query at time 15 - should NOT be found (after remove, before re-add)
		found, stateTime = vs.ContainsWithin(1, 15)
		if found {
			t.Error("expected ID 1 to NOT be found at time 15")
		}

		// Query at time 25 - should be found (after re-add)
		found, stateTime = vs.ContainsWithin(1, 25)
		if !found {
			t.Error("expected ID 1 to be found at time 25")
		}
	})
}
