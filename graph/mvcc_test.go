package graph_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
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
	g := graph.New(s)

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Apply changes with increasing LSNs via ApplyChange
	g.ApplyChange(store.Change{
		LSN: 10,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   alice,
		},
	})

	g.ApplyChange(store.Change{
		LSN: 20,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   bob,
		},
	})

	// At HEAD: both alice and bob should be viewers
	ok, err := g.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at HEAD")
	}

	ok, err = g.Check("user", bob, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at HEAD")
	}

	// At LSN 15: only alice should be viewer (bob was added at LSN 20)
	lsn15 := graph.LSN(15)
	ok, err = g.CheckAt("user", alice, "document", doc1, "viewer", &lsn15)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at LSN 15")
	}

	ok, err = g.CheckAt("user", bob, "document", doc1, "viewer", &lsn15)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer at LSN 15")
	}

	// At LSN 5: neither should be viewer
	lsn5 := graph.LSN(5)
	ok, err = g.CheckAt("user", alice, "document", doc1, "viewer", &lsn5)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at LSN 5")
	}
}

func TestMVCC_VersionedSet_RemoveAt(t *testing.T) {
	s := mvccTestSchema()
	g := graph.New(s)

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Add alice at LSN 10
	g.ApplyChange(store.Change{
		LSN: 10,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   alice,
		},
	})

	// Remove alice at LSN 20
	g.ApplyChange(store.Change{
		LSN: 20,
		Op:  store.OpDelete,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   alice,
		},
	})

	// At HEAD: alice should NOT be viewer
	ok, err := g.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at HEAD")
	}

	// At LSN 15: alice should be viewer
	lsn15 := graph.LSN(15)
	ok, err = g.CheckAt("user", alice, "document", doc1, "viewer", &lsn15)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at LSN 15")
	}

	// At LSN 25: alice should NOT be viewer
	lsn25 := graph.LSN(25)
	ok, err = g.CheckAt("user", alice, "document", doc1, "viewer", &lsn25)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at LSN 25")
	}
}

func TestMVCC_ReplicatedLSN(t *testing.T) {
	s := mvccTestSchema()
	g := graph.New(s)

	// Initially replicated LSN should be 0
	if lsn := g.ReplicatedLSN(); lsn != 0 {
		t.Errorf("expected initial replicated LSN to be 0, got %d", lsn)
	}

	// Apply a change
	g.ApplyChange(store.Change{
		LSN: 100,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   1,
		},
	})

	// Replicated LSN should be updated
	if lsn := g.ReplicatedLSN(); lsn != 100 {
		t.Errorf("expected replicated LSN to be 100, got %d", lsn)
	}

	// Apply another change
	g.ApplyChange(store.Change{
		LSN: 200,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    2,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   1,
		},
	})

	if lsn := g.ReplicatedLSN(); lsn != 200 {
		t.Errorf("expected replicated LSN to be 200, got %d", lsn)
	}
}

func TestMVCC_MemoryStore_ChangeStream(t *testing.T) {
	s := mvccTestSchema()
	g := graph.New(s)
	ms := store.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		alice schema.ID = 1
		doc1  schema.ID = 100
	)

	// Subscribe to changes
	changes, errCh := ms.Subscribe(ctx, 0)

	// Channel to signal that the change was processed
	processed := make(chan struct{})

	// Start a goroutine to apply changes to the graph
	go func() {
		for {
			select {
			case change, ok := <-changes:
				if !ok {
					return
				}
				g.ApplyChange(change)
				// Signal that we processed a change
				select {
				case processed <- struct{}{}:
				default:
				}
			case err := <-errCh:
				if err != nil {
					t.Errorf("change stream error: %v", err)
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Write to store
	err := ms.WriteTuple(ctx, store.Tuple{
		ObjectType:  "document",
		ObjectID:    doc1,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   alice,
	})
	if err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Wait for the change to be processed
	<-processed

	// Check that the tuple was applied
	ok, err := g.Check("user", alice, "document", doc1, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer after change stream processing")
	}
}

func TestMVCC_TruncateHistory(t *testing.T) {
	s := mvccTestSchema()
	g := graph.New(s)

	const (
		alice schema.ID = 1
		bob   schema.ID = 2
		doc1  schema.ID = 100
	)

	// Apply changes at LSN 10, 20, 30
	g.ApplyChange(store.Change{
		LSN: 10,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   alice,
		},
	})
	g.ApplyChange(store.Change{
		LSN: 20,
		Op:  store.OpInsert,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   bob,
		},
	})
	g.ApplyChange(store.Change{
		LSN: 30,
		Op:  store.OpDelete,
		Tuple: store.Tuple{
			ObjectType:  "document",
			ObjectID:    doc1,
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   alice,
		},
	})

	// Before truncation: LSN 15 query should work
	lsn15 := graph.LSN(15)
	ok, err := g.CheckAt("user", alice, "document", doc1, "viewer", &lsn15)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at LSN 15 before truncation")
	}

	// Truncate history older than LSN 25
	g.TruncateHistory(25)

	// After truncation: LSN 15 query may not work correctly
	// (implementation detail: it will return the state at LSN 25 or later)
	// This is expected behavior - old history is garbage collected

	// But HEAD and recent LSNs should still work
	lsn35 := graph.LSN(35)
	ok, err = g.CheckAt("user", alice, "document", doc1, "viewer", &lsn35)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at LSN 35")
	}

	ok, err = g.CheckAt("user", bob, "document", doc1, "viewer", &lsn35)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer at LSN 35")
	}
}
