package graph_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestLocalRouter_AlwaysReturnsLocalClient verifies that LocalRouter
// always returns the same client regardless of object type/ID.
func TestLocalRouter_AlwaysReturnsLocalClient(t *testing.T) {
	s := testSchema()
	ms := store.NewMemoryStore()
	g := graph.New(s)
	client := graph.NewLocalGraphClient(g)
	router := graph.NewLocalRouter(client, ms, ms)

	ctx := context.Background()

	// Route to different objects should all return the same client
	c1, err := router.Route(ctx, "document", 100)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}
	c2, err := router.Route(ctx, "folder", 200)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}
	c3, err := router.Route(ctx, "user", 1)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	if c1 != c2 || c2 != c3 {
		t.Error("expected LocalRouter to return the same client for all objects")
	}
}

// TestGraph_WithRouter_ArrowTraversal tests that arrow traversal uses routing.
// In single-node mode with LocalRouter, this should behave the same as before.
func TestGraph_WithRouter_ArrowTraversal(t *testing.T) {
	s := testSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()

	// TestGraph already has a LocalRouter set up
	ctx := context.Background()

	const (
		alice    schema.ID = 1
		folder10 schema.ID = 10
		doc100   schema.ID = 100
	)

	// Document 100's parent is folder 10
	if err := tg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice is a viewer of folder 10
	if err := tg.WriteTuple(ctx, "folder", folder10, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be able to view doc100 via parent folder
	// This involves arrow traversal: doc100 → parent → folder10 → viewer
	// The check on folder10's viewer should go through the router.
	ok, _, err := tg.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via parent folder (with routing)")
	}
}

// TestGraph_WithRouter_UsersetSubject tests that userset subject checks use routing.
func TestGraph_WithRouter_UsersetSubject(t *testing.T) {
	s := testSchema()
	tg := graph.NewTestGraph(s)
	defer tg.Close()

	// TestGraph already has a LocalRouter set up
	ctx := context.Background()

	const (
		alice  schema.ID = 1
		group1 schema.ID = 10
		doc100 schema.ID = 100
	)

	// Alice is a member of group 1
	if err := tg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Members of group 1 are viewers of doc100
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be able to view doc100 via group membership
	// The check on group1's member relation should go through the router.
	ok, _, err := tg.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via group membership (with routing)")
	}
}
