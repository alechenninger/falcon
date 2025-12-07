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

// TestVersionedSet_ContainsAt tests historical queries with ContainsAt.
func TestVersionedSet_ContainsAt(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history: add 1 at t=10, add 2 at t=20, remove 1 at t=30
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Remove(1, 30)

	tests := []struct {
		name     string
		id       schema.ID
		time     store.StoreTime
		expected bool
	}{
		// Query at t=5 (before any changes)
		{"id1 before any changes", 1, 5, false},
		{"id2 before any changes", 2, 5, false},

		// Query at t=10 (exactly when id1 was added)
		{"id1 at add time", 1, 10, true},
		{"id2 at t=10", 2, 10, false},

		// Query at t=15 (between id1 add and id2 add)
		{"id1 at t=15", 1, 15, true},
		{"id2 at t=15", 2, 15, false},

		// Query at t=20 (exactly when id2 was added)
		{"id1 at t=20", 1, 20, true},
		{"id2 at add time", 2, 20, true},

		// Query at t=25 (between id2 add and id1 remove)
		{"id1 at t=25", 1, 25, true},
		{"id2 at t=25", 2, 25, true},

		// Query at t=30 (exactly when id1 was removed)
		{"id1 at remove time", 1, 30, false},
		{"id2 at t=30", 2, 30, true},

		// Query at t=100 (after all changes, same as HEAD)
		{"id1 after all changes", 1, 100, false},
		{"id2 after all changes", 2, 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := vs.ContainsAt(tt.id, tt.time)
			if result != tt.expected {
				t.Errorf("ContainsAt(%d, %d) = %v, want %v", tt.id, tt.time, result, tt.expected)
			}
		})
	}
}

// TestVersionedSet_SnapshotAt tests SnapshotAt returns correct historical state.
func TestVersionedSet_SnapshotAt(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history: add 1 at t=10, add 2 at t=20, add 3 at t=30, remove 1 at t=40
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Add(3, 30)
	vs.Remove(1, 40)

	tests := []struct {
		name     string
		time     store.StoreTime
		expected []schema.ID
	}{
		{"before any changes", 5, nil},
		{"after id1 add", 10, []schema.ID{1}},
		{"after id2 add", 20, []schema.ID{1, 2}},
		{"after id3 add", 30, []schema.ID{1, 2, 3}},
		{"after id1 remove", 40, []schema.ID{2, 3}},
		{"at HEAD", 100, []schema.ID{2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := vs.SnapshotAt(tt.time)
			for _, id := range tt.expected {
				if !snapshot.Contains(uint32(id)) {
					t.Errorf("SnapshotAt(%d) missing expected ID %d", tt.time, id)
				}
			}
			if int(snapshot.GetCardinality()) != len(tt.expected) {
				t.Errorf("SnapshotAt(%d) has %d elements, want %d",
					tt.time, snapshot.GetCardinality(), len(tt.expected))
			}
		})
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

	// Verify ContainsAt works correctly for each time
	for i, time := range times {
		// ID should be present at its add time
		if !vs.ContainsAt(schema.ID(i+1), time) {
			t.Errorf("ID %d should be present at time %d", i+1, time)
		}
		// ID should not be present just before its add time
		if time > 0 && vs.ContainsAt(schema.ID(i+1), time-1) {
			t.Errorf("ID %d should NOT be present at time %d", i+1, time-1)
		}
	}
}

// TestVersionedSet_StateTimeWithin tests finding the latest usable state.
func TestVersionedSet_StateTimeWithin(t *testing.T) {
	vs := newVersionedSet(0)

	// Build history at times 10, 20, 30
	vs.Add(1, 10)
	vs.Add(2, 20)
	vs.Add(3, 30)

	tests := []struct {
		name     string
		maxTime  store.StoreTime
		expected store.StoreTime
	}{
		{"max >= head returns head", 100, 30},
		{"max == head returns head", 30, 30},
		{"max between entries returns earlier", 25, 20},
		{"max == earlier entry returns it", 20, 20},
		{"max < oldest returns 0", 5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := vs.StateTimeWithin(tt.maxTime)
			if result != tt.expected {
				t.Errorf("StateTimeWithin(%d) = %d, want %d", tt.maxTime, result, tt.expected)
			}
		})
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
	if !vs.ContainsAt(1, 10) {
		t.Error("before truncation: ID 1 should be present at t=10")
	}

	// Truncate entries older than t=25
	vs.Truncate(25)

	// HEAD should still work
	if !vs.Contains(1) || !vs.Contains(2) || !vs.Contains(3) || !vs.Contains(4) {
		t.Error("after truncation: HEAD should still have all IDs")
	}

	// Query at t=30 should still work
	if !vs.ContainsAt(3, 30) {
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

	// ContainsAt should return false for any time
	if vs.ContainsAt(1, 50) {
		t.Error("ContainsAt should return false for empty set")
	}
	if vs.ContainsAt(1, 150) {
		t.Error("ContainsAt should return false for empty set")
	}

	// SnapshotAt should return empty bitmap
	snapshot := vs.SnapshotAt(50)
	if !snapshot.IsEmpty() {
		t.Error("SnapshotAt should return empty bitmap for empty set")
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

	// Historical queries
	if !vs.ContainsAt(1, 10) {
		t.Error("ID 1 should be present at t=10")
	}
	if !vs.ContainsAt(1, 15) {
		t.Error("ID 1 should be present at t=15 (still added)")
	}
	if vs.ContainsAt(1, 20) {
		t.Error("ID 1 should NOT be present at t=20 (just removed)")
	}
	if vs.ContainsAt(1, 25) {
		t.Error("ID 1 should NOT be present at t=25 (still removed)")
	}
	if !vs.ContainsAt(1, 30) {
		t.Error("ID 1 should be present at t=30 (re-added)")
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
		time     store.StoreTime
		expected []schema.ID
		absent   []schema.ID
	}{
		{5, nil, []schema.ID{1, 2, 3}},
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
			if !vs.ContainsAt(id, tt.time) {
				t.Errorf("at t=%d: expected ID %d to be present", tt.time, id)
			}
		}
		for _, id := range tt.absent {
			if vs.ContainsAt(id, tt.time) {
				t.Errorf("at t=%d: expected ID %d to be absent", tt.time, id)
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

	if !vs.ContainsAt(1, 1_000_000) {
		t.Error("ID 1 should be present at t=1_000_000")
	}
	if !vs.ContainsAt(2, 2_000_000_000) {
		t.Error("ID 2 should be present at t=2_000_000_000")
	}
	if vs.ContainsAt(2, 1_500_000_000) {
		t.Error("ID 2 should NOT be present at t=1_500_000_000")
	}
}

// TestVersionedSet_HeadTimeQuery tests querying at exactly headTime.
func TestVersionedSet_HeadTimeQuery(t *testing.T) {
	vs := newVersionedSet(0)

	vs.Add(1, 10)
	vs.Add(2, 20)

	// Query at exactly headTime should match HEAD state
	headTime := vs.HeadTime()
	if !vs.ContainsAt(1, headTime) {
		t.Error("ContainsAt(1, headTime) should return true")
	}
	if !vs.ContainsAt(2, headTime) {
		t.Error("ContainsAt(2, headTime) should return true")
	}

	snapshot := vs.SnapshotAt(headTime)
	if !snapshot.Contains(1) || !snapshot.Contains(2) {
		t.Error("SnapshotAt(headTime) should match HEAD")
	}
}

// TestVersionedSet_QueryFutureTime tests querying at a time after headTime.
func TestVersionedSet_QueryFutureTime(t *testing.T) {
	vs := newVersionedSet(0)

	vs.Add(1, 10)
	vs.Add(2, 20)

	headTime := vs.HeadTime()
	futureTime := headTime + 1000

	// Query at future time should match HEAD state
	if !vs.ContainsAt(1, futureTime) {
		t.Error("ContainsAt(1, futureTime) should return true")
	}
	if !vs.ContainsAt(2, futureTime) {
		t.Error("ContainsAt(2, futureTime) should return true")
	}
}
