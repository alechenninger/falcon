package graph_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
)

func mvccTestSchema() *schema.Schema {
	return &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"document": {
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
						},
					},
				},
			},
		},
	}
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
	ok, _, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at HEAD")
	}

	ok, _, err = tg.Check("user", bob, "document", doc1, "viewer")
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
	ok, _, err := tg.Check("user", alice, "document", doc1, "viewer")
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
	ok, _, err := tg.Check("user", alice, "document", doc1, "viewer")
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
	_, usedTime, err := tg.Check("user", alice, "document", doc1, "viewer")
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
	ok, _, err = tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	ok, _, err = tg.Check("user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}
}
