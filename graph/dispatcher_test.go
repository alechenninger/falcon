package graph_test

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestDispatcher_SingleGroup verifies that a single group optimization works.
func TestDispatcher_SingleGroup(t *testing.T) {
	s := testSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()

	ctx := context.Background()

	// Add a dispatcher using the TestGraph's existing router
	dispatcher := graph.NewDispatcher(tg.Router())
	tg.Graph = tg.Graph.WithDispatcher(dispatcher)

	const (
		alice  schema.ID = 1
		bob    schema.ID = 2
		group1 schema.ID = 10
		group2 schema.ID = 11
		doc100 schema.ID = 100
	)

	// Alice is a member of group1, Bob is a member of group2
	if err := tg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tg.WriteTuple(ctx, "group", group2, "member", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Both groups are viewers of doc100
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "group", group2, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be able to view doc100 via group1
	ok, _, err := tg.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group1 membership (with dispatcher)")
	}

	// Bob should be able to view doc100 via group2
	ok, _, err = tg.Check("user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer via group2 membership (with dispatcher)")
	}
}

// TestDispatcher_ArrowTraversal verifies arrow traversal with dispatcher.
func TestDispatcher_ArrowTraversal(t *testing.T) {
	s := testSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()

	ctx := context.Background()

	// Add a dispatcher using the TestGraph's existing router
	dispatcher := graph.NewDispatcher(tg.Router())
	tg.Graph = tg.Graph.WithDispatcher(dispatcher)

	const (
		alice    schema.ID = 1
		folder10 schema.ID = 10
		folder20 schema.ID = 20
		doc100   schema.ID = 100
	)

	// Document 100 has two parent folders
	if err := tg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder20, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice is a viewer of folder20 (not folder10)
	if err := tg.WriteTuple(ctx, "folder", folder20, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be able to view doc100 via folder20
	ok, _, err := tg.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via parent folder20 (with dispatcher)")
	}
}

// TestDispatcher_BatchCheckRelation tests the BatchCheckRelation method directly.
func TestDispatcher_BatchCheckRelation(t *testing.T) {
	s := testSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()

	ctx := context.Background()

	const (
		alice  schema.ID = 1
		group1 schema.ID = 10
		group2 schema.ID = 11
		group3 schema.ID = 12
	)

	// Alice is only a member of group2
	if err := tg.WriteTuple(ctx, "group", group2, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Create a batch check with multiple groups
	bitmap := roaring.New()
	bitmap.Add(uint32(group1))
	bitmap.Add(uint32(group2))
	bitmap.Add(uint32(group3))

	check := graph.RelationCheck{
		SubjectType: "user",
		SubjectID:   alice,
		Objects: []graph.ObjectSet{
			{Type: "group", IDs: bitmap},
		},
		Relation: "member",
	}

	client := graph.NewLocalGraphClient(tg.Graph)
	window := graph.NewSnapshotWindow(0, tg.ReplicatedTime())

	allowed, _, err := client.BatchCheckRelation(ctx, check, window, nil)
	if err != nil {
		t.Fatalf("BatchCheckRelation failed: %v", err)
	}
	if !allowed {
		t.Error("expected alice to be found in batch check (member of group2)")
	}

	// Try with groups that alice is not a member of
	bitmap2 := roaring.New()
	bitmap2.Add(uint32(group1))
	bitmap2.Add(uint32(group3))

	check2 := graph.RelationCheck{
		SubjectType: "user",
		SubjectID:   alice,
		Objects: []graph.ObjectSet{
			{Type: "group", IDs: bitmap2},
		},
		Relation: "member",
	}

	allowed, _, err = client.BatchCheckRelation(ctx, check2, window, nil)
	if err != nil {
		t.Fatalf("BatchCheckRelation failed: %v", err)
	}
	if allowed {
		t.Error("expected alice NOT to be found (not member of group1 or group3)")
	}
}

// TestRouter_GroupByDestination tests the LocalRouter grouping.
func TestRouter_GroupByDestination(t *testing.T) {
	s := testSchema()
	ms := store.NewMemoryStore()
	g := graph.New(s)
	client := graph.NewLocalGraphClient(g)
	router := graph.NewLocalRouter(client, ms, ms)

	ctx := context.Background()

	// Create some ObjectSets
	bitmap1 := roaring.New()
	bitmap1.Add(1)
	bitmap1.Add(2)

	bitmap2 := roaring.New()
	bitmap2.Add(10)
	bitmap2.Add(20)

	objects := []graph.ObjectSet{
		{Type: "group", IDs: bitmap1},
		{Type: "folder", IDs: bitmap2},
	}

	groups, err := router.GroupByDestination(ctx, objects)
	if err != nil {
		t.Fatalf("GroupByDestination failed: %v", err)
	}

	// LocalRouter should return a single group with all objects
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
	}

	if len(groups[0].Objects) != 2 {
		t.Errorf("expected 2 ObjectSets in group, got %d", len(groups[0].Objects))
	}
}

// TestDispatcher_EmptyObjects tests dispatch with no objects.
func TestDispatcher_EmptyObjects(t *testing.T) {
	s := testSchema()
	ms := store.NewMemoryStore()
	g := graph.New(s)
	client := graph.NewLocalGraphClient(g)
	router := graph.NewLocalRouter(client, ms, ms)
	dispatcher := graph.NewDispatcher(router)

	ctx := context.Background()
	window := graph.NewSnapshotWindow(0, g.ReplicatedTime())

	check := graph.RelationCheck{
		SubjectType: "user",
		SubjectID:   1,
		Objects:     nil, // Empty
		Relation:    "member",
	}

	allowed, _, err := dispatcher.DispatchCheck(ctx, check, window, nil)
	if err != nil {
		t.Fatalf("DispatchCheck failed: %v", err)
	}
	if allowed {
		t.Error("expected false for empty objects")
	}
}

// TestBitmapSerialization tests that bitmaps round-trip through proto.
func TestBitmapSerialization(t *testing.T) {
	original := roaring.New()
	original.Add(1)
	original.Add(100)
	original.Add(1000)
	original.Add(10000)

	// Serialize
	bytes, err := original.ToBytes()
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	// Deserialize
	restored := roaring.New()
	_, err = restored.FromBuffer(bytes)
	if err != nil {
		t.Fatalf("FromBuffer failed: %v", err)
	}

	// Verify
	if !original.Equals(restored) {
		t.Error("bitmap not equal after round-trip")
	}
}
