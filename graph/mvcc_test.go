package graph_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
)

func mvccTestSchema() *schema.Schema {
	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"document": {
				ID:   2,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

// mvccHierarchySchema includes folder hierarchy for testing arrow narrowing.
func mvccHierarchySchema() *schema.Schema {
	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"folder": {
				ID:   2,
				Name: "folder",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
							schema.Arrow("parent", "viewer"), // inherit from parent
						},
					},
				},
			},
			"document": {
				ID:   3,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
							schema.Arrow("parent", "viewer"), // inherit from parent folder
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

func TestMVCC_VersionedSet_ContainsAt(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Add alice as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Capture time after alice is added
	timeAfterAlice := tg.ReplicatedTime()

	// Add bob as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// At HEAD: both alice and bob should be viewers
	ok, _, err := tg.Check(ctx, "user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at HEAD")
	}

	ok, _, err = tg.Check(ctx, "user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}

	// At timeAfterAlice: only alice should be viewer (bob was added later)
	windowAfterAlice := graph.NewSnapshotWindow(0, timeAfterAlice)
	ok, _, err = tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &windowAfterAlice)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at timeAfterAlice")
	}

	ok, _, err = tg.CheckAt(ctx, "user", bob, "document", doc1, "viewer", &windowAfterAlice)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer at timeAfterAlice")
	}
}

func TestMVCC_VersionedSet_RemoveAt(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Add alice as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Capture time after alice is added
	timeAfterAdd := tg.ReplicatedTime()

	// Remove alice
	if err := tg.DeleteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Capture time after removal
	timeAfterRemove := tg.ReplicatedTime()

	// At HEAD: alice should NOT be viewer
	ok, _, err := tg.Check(ctx, "user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	// At timeAfterAdd: alice should be viewer
	windowAfterAdd := graph.NewSnapshotWindow(0, timeAfterAdd)
	ok, _, err = tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &windowAfterAdd)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at timeAfterAdd")
	}

	// At timeAfterRemove: alice should NOT be viewer
	windowAfterRemove := graph.NewSnapshotWindow(0, timeAfterRemove)
	ok, _, err = tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &windowAfterRemove)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at timeAfterRemove")
	}
}

func TestMVCC_ReplicatedTime(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	// After first write, replicated time should be non-zero
	if err := tg.WriteTuple(ctx, "document", 1, "viewer", "user", 1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	time1 := tg.ReplicatedTime()
	if time1 == 0 {
		t.Error("expected replicated time to be non-zero after first write")
	}

	// After second write, replicated time should have advanced
	if err := tg.WriteTuple(ctx, "document", 2, "viewer", "user", 1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	time2 := tg.ReplicatedTime()
	if time2 <= time1 {
		t.Errorf("expected replicated time to advance: got %d, was %d", time2, time1)
	}
}

func TestMVCC_MemoryStore_ChangeStream(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Write to store via TestGraph (which writes to MemoryStore and waits for replication)
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check that the tuple was applied
	ok, _, err := tg.Check(ctx, "user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer after change stream processing")
	}
}

func TestMVCC_SnapshotWindowNarrowing(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Add alice as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	timeAfterAlice := tg.ReplicatedTime()

	// Add bob as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	timeAfterBob := tg.ReplicatedTime()

	// Check returns the time used for the query
	// When checking at HEAD, the returned min time should be at least as high as
	// the state time we used
	_, usedTime, err := tg.Check(ctx, "user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	// The used time should be the head time of the tuple we examined
	if usedTime < timeAfterAlice {
		t.Errorf("expected usedTime >= %d (timeAfterAlice), got %d", timeAfterAlice, usedTime)
	}

	// When querying with a max constraint, the min should be within that range
	windowBeforeBob := graph.NewSnapshotWindow(0, timeAfterAlice)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &windowBeforeBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}
	// The min should have been raised to the state time we used
	if resultWindow.Min() == 0 {
		t.Error("expected window.Min to be raised from 0")
	}
	// The min should not exceed max
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("expected window.Min <= window.Max, got %d > %d", resultWindow.Min(), resultWindow.Max())
	}
	// Check that bob is NOT a viewer at this snapshot (added after max)
	ok, _, err = tg.CheckAt(ctx, "user", bob, "document", doc1, "viewer", &windowBeforeBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer with max=timeAfterAlice")
	}

	// With max=timeAfterBob, both should be visible
	windowAfterBob := graph.NewSnapshotWindow(0, timeAfterBob)
	ok, _, err = tg.CheckAt(ctx, "user", bob, "document", doc1, "viewer", &windowAfterBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer with max=timeAfterBob")
	}
}

func TestMVCC_TruncateHistory(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Add alice
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Add bob
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	timeBeforeDelete := tg.ReplicatedTime()

	// Remove alice
	if err := tg.DeleteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Before truncation: query at timeBeforeDelete should show alice as viewer
	windowBeforeDelete := graph.NewSnapshotWindow(0, timeBeforeDelete)
	ok, _, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &windowBeforeDelete)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at timeBeforeDelete before truncation")
	}

	// Truncate history
	tg.TruncateHistory(timeBeforeDelete)

	// After truncation: HEAD should still work correctly
	ok, _, err = tg.Check(ctx, "user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	ok, _, err = tg.Check(ctx, "user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}
}

func TestMVCC_MaxSnapshotWindow_NarrowsToReplicatedTime(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Add alice as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check with MaxSnapshotWindow - the returned window should be narrowed
	// to the actual replicated time, not left at MaxUint64
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}

	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_Window_MultipleWrites(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Write two tuples - they get times 1 and 2
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check alice - should return oldest time where alice is a viewer (time 1)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}
	// Window should be [1, 2]: min=1 (oldest time alice was a viewer), max=2 (replicated time)
	expectedWindow := graph.NewSnapshotWindow(1, 2)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_Window_NoTuplesFound(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice   schema.ID = 1
		charlie schema.ID = 3
		doc1    schema.ID = 100
	)

	// Write a tuple for alice
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check charlie (who has no access) - window should still narrow to replicated time
	ok, resultWindow, err := tg.CheckAt(ctx, "user", charlie, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected charlie to NOT be viewer")
	}
	// Even though charlie wasn't found, the window narrows based on what we checked
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_Window_HistoricalQuery(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Add alice
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	timeAfterAlice := tg.ReplicatedTime()

	// Add bob
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Query with a historical window (max = timeAfterAlice)
	historicalWindow := graph.NewSnapshotWindow(0, timeAfterAlice)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &historicalWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at historical time")
	}
	// Window should be narrowed to timeAfterAlice
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}

	// Bob should NOT be visible at historical time
	ok, resultWindow, err = tg.CheckAt(ctx, "user", bob, "document", doc1, "viewer", &historicalWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer at historical time")
	}
}

func TestMVCC_Window_WideSpan(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Write a tuple for alice on doc1 - this will be at time 1
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Write many unrelated tuples to advance the replicated time
	// These are for different documents, so they won't be read when checking doc1
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "document", i, "viewer", "user", schema.ID(i), ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Replicated time should now be 11 (1 + 10 unrelated writes)
	replicatedTime := tg.ReplicatedTime()
	if replicatedTime != 11 {
		t.Fatalf("expected replicated time 11, got %d", replicatedTime)
	}

	// Check alice's access to doc1
	// The window should span from time 1 (when we read the tuple) to time 11 (replicated time)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}

	// The window should have a wide span: min=1 (tuple read time), max=11 (replicated time)
	// This demonstrates that the window reflects what data was actually accessed
	expectedWindow := graph.NewSnapshotWindow(1, 11)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}

	// Verify the delta is non-zero (wide window)
	if resultWindow.Min() == resultWindow.Max() {
		t.Error("expected a wide window with min != max")
	}
}

func TestMVCC_Window_ArrowNarrowing(t *testing.T) {
	// Test that window narrows based on arrow traversal.
	// Setup:
	//   t=1: doc1 -> folder1 (parent)  [old, won't be min]
	//   t=2: user -> folder1 (viewer)  [this should be our min]
	//   t=3..12: unrelated writes      [advance time]
	// Check: user can view doc1 via folder1
	// Expected window: [2, 12] - min is when viewer was added, not parent

	s := mvccHierarchySchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice   schema.ID = 1
		doc1    schema.ID = 100
		folder1 schema.ID = 10
	)

	// t=1: Old tuple - doc1's parent is folder1
	if err := tg.WriteTuple(ctx, "document", doc1, "parent", "folder", folder1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=2: Later - alice becomes viewer of folder1
	if err := tg.WriteTuple(ctx, "folder", folder1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=3..12: Unrelated writes to advance time
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "folder", i, "viewer", "user", i, ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Check alice's access to doc1 (via folder1)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc1")
	}

	// Window should be [2, 12]: min=2 (viewer tuple), max=12 (replicated time)
	// The parent tuple at t=1 doesn't raise min because we read newer data (t=2) during traversal
	expectedWindow := graph.NewSnapshotWindow(2, 12)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_Window_NestedArrowNarrowing(t *testing.T) {
	// Test window narrowing through nested arrows.
	// Setup:
	//   t=1: doc1 -> leafFolder (parent)
	//   t=2: leafFolder -> midFolder (parent)
	//   t=3: midFolder -> rootFolder (parent)
	//   t=4: user -> rootFolder (viewer)  [this should be our min]
	//   t=5..14: unrelated writes
	// Check: user can view doc1 via leafFolder -> midFolder -> rootFolder
	// Expected window: [4, 14]

	s := mvccHierarchySchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice      schema.ID = 1
		doc1       schema.ID = 100
		leafFolder schema.ID = 10
		midFolder  schema.ID = 11
		rootFolder schema.ID = 12
	)

	// t=1: doc1's parent is leafFolder
	if err := tg.WriteTuple(ctx, "document", doc1, "parent", "folder", leafFolder, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=2: leafFolder's parent is midFolder
	if err := tg.WriteTuple(ctx, "folder", leafFolder, "parent", "folder", midFolder, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=3: midFolder's parent is rootFolder
	if err := tg.WriteTuple(ctx, "folder", midFolder, "parent", "folder", rootFolder, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=4: alice becomes viewer of rootFolder
	if err := tg.WriteTuple(ctx, "folder", rootFolder, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=5..14: Unrelated writes to advance time
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "folder", i, "viewer", "user", i, ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Check alice's access to doc1 (via leafFolder -> midFolder -> rootFolder)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc1")
	}

	// Window should be [4, 14]: min=4 (viewer tuple at root), max=14 (replicated time)
	expectedWindow := graph.NewSnapshotWindow(4, 14)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_Window_BumpInMiddle(t *testing.T) {
	// Test that the newest tuple read becomes min, even if it's "in the middle" of the traversal.
	// Setup:
	//   t=1: user -> ancestorFolder (viewer)  [old]
	//   t=2: doc1 -> leafFolder (parent)      [old]
	//   t=3: leafFolder -> ancestorFolder (parent)  [THIS is our expected min - newest read]
	//   t=4..13: unrelated writes
	// Check: user can view doc1 via leafFolder -> ancestorFolder
	// Expected window: [3, 13] - min is when leaf was moved to ancestor

	s := mvccHierarchySchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice          schema.ID = 1
		doc1           schema.ID = 100
		leafFolder     schema.ID = 10
		ancestorFolder schema.ID = 11
	)

	// t=1: alice becomes viewer of ancestorFolder (old)
	if err := tg.WriteTuple(ctx, "folder", ancestorFolder, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=2: doc1's parent is leafFolder (old)
	if err := tg.WriteTuple(ctx, "document", doc1, "parent", "folder", leafFolder, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=3: leafFolder's parent is ancestorFolder (newest - this will be min)
	if err := tg.WriteTuple(ctx, "folder", leafFolder, "parent", "folder", ancestorFolder, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=4..13: Unrelated writes to advance time
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "folder", i, "viewer", "user", i, ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Check alice's access to doc1 (via leafFolder -> ancestorFolder)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc1")
	}

	// Window should be [3, 13]: min=3 (newest tuple read - leaf->ancestor), max=13 (replicated time)
	// Even though the viewer tuple (t=1) and doc parent (t=2) are older,
	// the leaf->ancestor link (t=3) is the newest thing we read, so it's our min.
	expectedWindow := graph.NewSnapshotWindow(3, 13)
	if resultWindow != expectedWindow {
		t.Errorf("expected resultWindow == %v, got %v", expectedWindow, resultWindow)
	}
}

func TestMVCC_CheckUnion_IndependentWindows(t *testing.T) {
	// Test that CheckUnion uses independent windows per type.
	// Setup a schema where a user can view a document via multiple groups.
	// Each group's userset check gets its own window based on reading that group's data.

	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"group": {
				ID:   2,
				Name: "group",
				Relations: map[schema.RelationName]*schema.Relation{
					"member": {
						ID:   1,
						Name: "member",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
						},
					},
				},
			},
			"document": {
				ID:   3,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
						},
					},
				},
			},
		},
	}
	s.Compile()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice  schema.ID = 1
		bob    schema.ID = 2
		group1 schema.ID = 10
		group2 schema.ID = 20
		doc1   schema.ID = 100
	)

	// t=1: group1 can view doc1 (old)
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=2: group2 can view doc1 (old)
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "group", group2, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=3: alice is member of group1 (old)
	if err := tg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=4: bob is member of group2 (newer - will be our min if we check group2)
	if err := tg.WriteTuple(ctx, "group", group2, "member", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=5..14: Unrelated writes to advance time
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "document", i, "viewer", "user", i, ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Check alice's access to doc1
	// Alice is in group1, so the check succeeds via group1.
	// The window for group1's check is based on:
	//   - Reading doc1's viewer tuples (t=1 for group1, t=2 for group2)
	//   - Recursively checking group1#member where alice was added at t=3
	// The min should be 3 (alice -> group1)
	ok, resultWindow, err := tg.CheckAt(ctx, "user", alice, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc1")
	}

	// The window min should reflect the path taken (group1's userset at t=1, alice's membership at t=3)
	// Max is 14 (replicated time)
	if resultWindow.Max() != 14 {
		t.Errorf("expected window.Max() = 14, got %d", resultWindow.Max())
	}
	// Min should be 3 (alice's membership in group1)
	if resultWindow.Min() != 3 {
		t.Errorf("expected window.Min() = 3, got %d", resultWindow.Min())
	}
}

func TestMVCC_CheckUnion_TightestWindowOnNotFound(t *testing.T) {
	// Test that CheckUnion returns the tightest window when no match is found.
	// This ensures subsequent checks (like exclusions) are properly constrained.

	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"group": {
				ID:   2,
				Name: "group",
				Relations: map[schema.RelationName]*schema.Relation{
					"member": {
						ID:   1,
						Name: "member",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
						},
					},
				},
			},
			"document": {
				ID:   3,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
						},
					},
				},
			},
		},
	}
	s.Compile()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	const (
		alice   schema.ID = 1
		bob     schema.ID = 2
		charlie schema.ID = 3 // Not a member of any group
		group1  schema.ID = 10
		group2  schema.ID = 20
		doc1    schema.ID = 100
	)

	// t=1: group1 can view doc1 (old)
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=2: group2 can view doc1 (old)
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "group", group2, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=3: alice is member of group1
	if err := tg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=4: bob is member of group2 (newest - this will be our min for tightest window)
	if err := tg.WriteTuple(ctx, "group", group2, "member", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// t=5..14: Unrelated writes to advance time
	for i := schema.ID(200); i < 210; i++ {
		if err := tg.WriteTuple(ctx, "document", i, "viewer", "user", i, ""); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	// Check charlie's access to doc1 - charlie is not in any group
	// The check should fail, but we check both groups' memberships.
	// The tightest window should have min = max of all mins from all checks.
	ok, resultWindow, err := tg.CheckAt(ctx, "user", charlie, "document", doc1, "viewer", &graph.MaxSnapshotWindow)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected charlie to NOT be viewer of doc1")
	}

	// The window should be tight: we checked both groups
	// - group1 check reads group1#member (no match), window narrowed by that read
	// - group2 check reads group2#member (no match), window narrowed by that read
	// Max should be 14 (replicated time)
	if resultWindow.Max() != 14 {
		t.Errorf("expected window.Max() = 14, got %d", resultWindow.Max())
	}
}
