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

	// Capture LSN after alice is added
	lsnAfterAlice := tg.ReplicatedLSN()

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

	// At lsnAfterAlice: only alice should be viewer (bob was added later)
	windowAfterAlice := graph.NewSnapshotWindow(0, lsnAfterAlice)
	ok, _, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &windowAfterAlice)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnAfterAlice")
	}

	ok, _, err = tg.CheckAt("user", bob, "document", doc1, "viewer", &windowAfterAlice)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer at lsnAfterAlice")
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

	// Capture LSN after alice is added
	lsnAfterAdd := tg.ReplicatedLSN()

	// Remove alice
	if err := tg.DeleteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Capture LSN after removal
	lsnAfterRemove := tg.ReplicatedLSN()

	// At HEAD: alice should NOT be viewer
	ok, _, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	// At lsnAfterAdd: alice should be viewer
	windowAfterAdd := graph.NewSnapshotWindow(0, lsnAfterAdd)
	ok, _, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &windowAfterAdd)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnAfterAdd")
	}

	// At lsnAfterRemove: alice should NOT be viewer
	windowAfterRemove := graph.NewSnapshotWindow(0, lsnAfterRemove)
	ok, _, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &windowAfterRemove)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at lsnAfterRemove")
	}
}

func TestMVCC_ReplicatedLSN(t *testing.T) {
	s := mvccTestSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()
	ctx := context.Background()

	// After first write, replicated LSN should be non-zero
	if err := tg.WriteTuple(ctx, "document", 1, "viewer", "user", 1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	lsn1 := tg.ReplicatedLSN()
	if lsn1 == 0 {
		t.Error("expected replicated LSN to be non-zero after first write")
	}

	// After second write, replicated LSN should have advanced
	if err := tg.WriteTuple(ctx, "document", 2, "viewer", "user", 1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	lsn2 := tg.ReplicatedLSN()
	if lsn2 <= lsn1 {
		t.Errorf("expected replicated LSN to advance: got %d, was %d", lsn2, lsn1)
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
	lsnAfterAlice := tg.ReplicatedLSN()

	// Add bob as viewer
	if err := tg.WriteTuple(ctx, "document", doc1, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	lsnAfterBob := tg.ReplicatedLSN()

	// Check returns the LSN used for the query
	// When checking at HEAD, the returned min LSN should be at least as high as
	// the state LSN we used
	_, usedLSN, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	// The used LSN should be the head LSN of the tuple we examined
	if usedLSN < lsnAfterAlice {
		t.Errorf("expected usedLSN >= %d (lsnAfterAlice), got %d", lsnAfterAlice, usedLSN)
	}

	// When querying with a max constraint, the min should be within that range
	windowBeforeBob := graph.NewSnapshotWindow(0, lsnAfterAlice)
	ok, resultWindow, err := tg.CheckAt("user", alice, "document", doc1, "viewer", &windowBeforeBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}
	// The min should have been raised to the state LSN we used
	if resultWindow.Min() == 0 {
		t.Error("expected window.Min to be raised from 0")
	}
	// The min should not exceed max
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("expected window.Min <= window.Max, got %d > %d", resultWindow.Min(), resultWindow.Max())
	}
	// Check that bob is NOT a viewer at this snapshot (added after max)
	ok, _, err = tg.CheckAt("user", bob, "document", doc1, "viewer", &windowBeforeBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer with max=lsnAfterAlice")
	}

	// With max=lsnAfterBob, both should be visible
	windowAfterBob := graph.NewSnapshotWindow(0, lsnAfterBob)
	ok, _, err = tg.CheckAt("user", bob, "document", doc1, "viewer", &windowAfterBob)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer with max=lsnAfterBob")
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
	lsnBeforeDelete := tg.ReplicatedLSN()

	// Remove alice
	if err := tg.DeleteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Before truncation: query at lsnBeforeDelete should show alice as viewer
	windowBeforeDelete := graph.NewSnapshotWindow(0, lsnBeforeDelete)
	ok, _, err := tg.CheckAt("user", alice, "document", doc1, "viewer", &windowBeforeDelete)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnBeforeDelete before truncation")
	}

	// Truncate history
	tg.TruncateHistory(lsnBeforeDelete)

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
