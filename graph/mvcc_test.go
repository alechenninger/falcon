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
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"),
						},
						Usersets: []schema.Userset{
							schema.Direct(),
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
	ok, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at HEAD")
	}

	ok, err = tg.Check("user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}

	// At lsnAfterAlice: only alice should be viewer (bob was added later)
	ok, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &lsnAfterAlice)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnAfterAlice")
	}

	ok, err = tg.CheckAt("user", bob, "document", doc1, "viewer", &lsnAfterAlice)
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
	ok, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	// At lsnAfterAdd: alice should be viewer
	ok, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &lsnAfterAdd)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnAfterAdd")
	}

	// At lsnAfterRemove: alice should NOT be viewer
	ok, err = tg.CheckAt("user", alice, "document", doc1, "viewer", &lsnAfterRemove)
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
	ok, err := tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer after change stream processing")
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
	ok, err := tg.CheckAt("user", alice, "document", doc1, "viewer", &lsnBeforeDelete)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at lsnBeforeDelete before truncation")
	}

	// Truncate history
	tg.TruncateHistory(lsnBeforeDelete)

	// After truncation: HEAD should still work correctly
	ok, err = tg.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	ok, err = tg.Check("user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}
}
