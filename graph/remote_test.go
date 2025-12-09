package graph_test

import (
	"context"
	"net"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/graph"
	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// setupTestServer creates a LocalGraph with test data and serves it via gRPC over bufconn.
// Returns the listener, cleanup function, and the underlying LocalGraph for verification.
func setupTestServer(t *testing.T, s *schema.Schema) (*bufconn.Listener, func(), *graph.TestGraph) {
	t.Helper()

	lis := bufconn.Listen(bufSize)

	tg := graph.NewTestGraph(s)

	server := grpc.NewServer()
	graphpb.RegisterGraphServiceServer(server, graph.NewGraphServer(tg.LocalGraph))

	go func() {
		if err := server.Serve(lis); err != nil {
			// Ignore errors from server shutdown
		}
	}()

	cleanup := func() {
		server.Stop()
		tg.Close()
	}

	return lis, cleanup, tg
}

// dialBufconn creates a gRPC client connection over bufconn.
func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	return conn
}

// TestRemoteGraph_Check tests the Check RPC via RemoteGraph.
func TestRemoteGraph_Check(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1  = 1
		user2  = 2
		doc100 = 100
	)

	// Add user 1 as a viewer of document 100 (first write -> time 1)
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check via remote - user1 should be a viewer
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100")
	}
	// Window should be narrowed to the single tuple's time [1, 1]
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}

	// Check via remote - user2 should NOT be a viewer
	ok, resultWindow, err = remote.Check(ctx, "user", user2, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
	// Window should be [1, 1]: we accessed the versioned set at time 1 (even though user2 wasn't in it)
	expectedNotFound := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedNotFound {
		t.Errorf("expected window %v, got %v", expectedNotFound, resultWindow)
	}
}

// TestRemoteGraph_CheckUnion tests the CheckUnion RPC via RemoteGraph.
func TestRemoteGraph_CheckUnion(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1    = 1
		user2    = 2
		folder10 = 10
		folder11 = 11
		folder12 = 12
	)

	// User 1 is a viewer of folder 11 (first write -> time 1)
	if err := tg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with multiple folders
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))
	bitmap.Add(uint32(folder12))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// CheckUnion via remote - should find user1 on folder11
	ok, resultWindow, err := remote.CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if !ok {
		t.Error("expected CheckUnion to find user1 as viewer")
	}
	// Window should be narrowed to the single tuple's time [1, 1]
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}

	// CheckUnion via remote - user2 should NOT be found
	ok, resultWindow, err = remote.CheckUnion(ctx, "user", user2, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if ok {
		t.Error("expected CheckUnion to NOT find user2 as viewer")
	}
	// Window should be [1, 1]: we accessed the versioned set at time 1 (even though user2 wasn't in it)
	expectedNotFound := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedNotFound {
		t.Errorf("expected window %v, got %v", expectedNotFound, resultWindow)
	}
}

// TestRemoteGraph_CheckWithArrow tests arrow traversal via RemoteGraph.
func TestRemoteGraph_CheckWithArrow(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1    = 1
		folder10 = 10
		doc100   = 100
	)

	// Document 100's parent is folder 10 (first write -> time 1)
	if err := tg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 is a viewer of folder 10 (second write -> time 2)
	if err := tg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check via remote - user1 should be viewer of doc100 via parent folder
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via parent folder")
	}
	// Window narrowed to [2, 2]: min raised to newest accessed tuple (viewer at time 2),
	// max narrowed to replicated time (2)
	expectedWindow := graph.NewSnapshotWindow(2, 2)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}
}

// TestRemoteGraph_CheckWithUserset tests userset subject via RemoteGraph.
func TestRemoteGraph_CheckWithUserset(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		alice  = 1
		bob    = 2
		group1 = 10
		doc100 = 100
	)

	// Alice is a member of group 1 (first write -> time 1)
	if err := tg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Members of group 1 are viewers of document 100 (second write -> time 2)
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check via remote - alice should be viewer via group membership
	ok, resultWindow, err := remote.Check(ctx, "user", alice, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via group membership")
	}
	// Window should be [2, 2]: min raised to newest accessed tuple (userset relation at time 2)
	expectedWindow := graph.NewSnapshotWindow(2, 2)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}

	// Check via remote - bob should NOT be viewer
	ok, resultWindow, err = remote.Check(ctx, "user", bob, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer of doc100")
	}
	// Window should be [2, 2]: we accessed the userset at time 2 (even though bob wasn't in group)
	expectedNotFound := graph.NewSnapshotWindow(2, 2)
	if resultWindow != expectedNotFound {
		t.Errorf("expected window %v, got %v", expectedNotFound, resultWindow)
	}
}

// TestRemoteGraph_CheckUnionEmpty tests CheckUnion with empty checks.
func TestRemoteGraph_CheckUnionEmpty(t *testing.T) {
	s := testSchema()
	lis, cleanup, _ := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	// CheckUnion with empty checks
	ok, _, err := remote.CheckUnion(ctx, "user", 1, nil, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if ok {
		t.Error("expected CheckUnion with empty checks to return false")
	}
}

// TestRemoteGraph_Schema tests that Schema returns the correct schema.
func TestRemoteGraph_Schema(t *testing.T) {
	s := testSchema()
	lis, cleanup, _ := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	// Schema should return the schema we passed in
	if remote.Schema() != s {
		t.Error("expected Schema() to return the schema passed to NewRemoteGraph")
	}
}

// TestRemoteGraph_WindowNarrowing tests that window narrowing works correctly through RPC.
func TestRemoteGraph_WindowNarrowing(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1  = 1
		doc100 = 100
	)

	// Add user 1 as a viewer (first write -> time 1)
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check with MaxSnapshotWindow - should be narrowed to [1, 1]
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", graph.MaxSnapshotWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer")
	}

	// Window should be exactly [1, 1]
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}
}

// TestRemoteGraph_VisitedPropagation tests that visited nodes are propagated through RPC.
func TestRemoteGraph_VisitedPropagation(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1  = 1
		doc100 = 100
	)

	// Add user 1 as a viewer (first write -> time 1)
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Create visited list that includes the node we're checking
	visited := []graph.VisitedKey{{
		ObjectType: "document",
		ObjectID:   doc100,
		Relation:   "viewer",
	}}

	// Check with the node already visited - should return false (cycle detection)
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", graph.MaxSnapshotWindow, visited)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected Check to return false when node is in visited list (cycle detection)")
	}
	// When cycle is detected, we bail out early without accessing any tuples,
	// so the input window is returned unchanged (MaxSnapshotWindow)
	if resultWindow != graph.MaxSnapshotWindow {
		t.Errorf("expected window %v, got %v", graph.MaxSnapshotWindow, resultWindow)
	}
}

// TestRemoteGraph_NonZeroLengthWindow tests that windows with min != max are
// correctly serialized/deserialized through the RPC layer, respecting both bounds.
func TestRemoteGraph_NonZeroLengthWindow(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1  = 1
		user2  = 2
		doc100 = 100
		doc101 = 101
	)

	// Time 1: user1 is viewer of doc100
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Time 2: user1 is viewer of doc101
	if err := tg.WriteTuple(ctx, "document", doc101, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Time 3: user2 is viewer of doc100
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Time 4: some unrelated write to advance replicated time
	if err := tg.WriteTuple(ctx, "document", doc101, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	// Replicated time is now 4

	// Test 1: Request window [1, 3] - should find user1
	// user1 was added at time 1 and never removed, so oldest valid time is 1
	// Max stays at our requested 3 (not replicated time 4)
	requestWindow := graph.NewSnapshotWindow(1, 3)
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", requestWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 with window [1, 3]")
	}
	// Window should be [1, 3]: min=1 (oldest time user1 is viewer), max=3 (requested max)
	expectedWindow := graph.NewSnapshotWindow(1, 3)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}

	// Test 2: Request narrow window [3, 3] - should find user2 (added at time 3)
	requestWindow2 := graph.NewSnapshotWindow(3, 3)
	ok, resultWindow, err = remote.Check(ctx, "user", user2, "document", doc100, "viewer", requestWindow2, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user2 to be viewer of doc100 with window [3, 3]")
	}
	// Window should stay [3, 3]
	expectedWindow2 := graph.NewSnapshotWindow(3, 3)
	if resultWindow != expectedWindow2 {
		t.Errorf("expected window %v, got %v", expectedWindow2, resultWindow)
	}

	// Test 3: Request window [1, 2] with max=2 which is less than current replicated time (4)
	// This tests that max is respected through serialization - we need historical snapshot
	requestWindow3 := graph.NewSnapshotWindow(1, 2)
	ok, resultWindow, err = remote.Check(ctx, "user", user1, "document", doc100, "viewer", requestWindow3, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 with window [1, 2]")
	}
	// Window should be [1, 2]: historical snapshot at time 1, max stays at requested 2
	expectedWindow3 := graph.NewSnapshotWindow(1, 2)
	if resultWindow != expectedWindow3 {
		t.Errorf("expected window %v, got %v", expectedWindow3, resultWindow)
	}
}

// TestRemoteGraph_WindowMinRespected tests that the window bounds are correctly
// serialized and respected, using MVCC time-travel to verify snapshots at different times.
func TestRemoteGraph_WindowMinRespected(t *testing.T) {
	s := testSchema()
	lis, cleanup, tg := setupTestServer(t, s)
	defer cleanup()

	conn := dialBufconn(t, lis)
	defer conn.Close()

	client := graphpb.NewGraphServiceClient(conn)
	remote := graph.NewRemoteGraph(client, s)

	const (
		user1  = 1
		doc100 = 100
	)

	// Time 1: user1 is viewer of doc100
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Time 2: remove user1 as viewer
	if err := tg.DeleteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Time 3: add user1 back as viewer
	if err := tg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	// Replicated time is now 3

	// Test: Request window [1, 1] - should find user1 (existed at time 1 before deletion)
	requestWindow := graph.NewSnapshotWindow(1, 1)
	ok, resultWindow, err := remote.Check(ctx, "user", user1, "document", doc100, "viewer", requestWindow, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer at time 1 (before deletion)")
	}
	expectedWindow := graph.NewSnapshotWindow(1, 1)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}

	// Test: Request window [2, 2] - should NOT find user1 (deleted at time 2)
	requestWindow2 := graph.NewSnapshotWindow(2, 2)
	ok, resultWindow, err = remote.Check(ctx, "user", user1, "document", doc100, "viewer", requestWindow2, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user1 to NOT be viewer at time 2 (after deletion)")
	}
	// Window should be [2, 2] - we accessed the set at that time
	expectedWindow2 := graph.NewSnapshotWindow(2, 2)
	if resultWindow != expectedWindow2 {
		t.Errorf("expected window %v, got %v", expectedWindow2, resultWindow)
	}

	// Test: Request window [3, 3] - should find user1 (re-added at time 3)
	requestWindow3 := graph.NewSnapshotWindow(3, 3)
	ok, resultWindow, err = remote.Check(ctx, "user", user1, "document", doc100, "viewer", requestWindow3, nil)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer at time 3 (re-added)")
	}
	expectedWindow3 := graph.NewSnapshotWindow(3, 3)
	if resultWindow != expectedWindow3 {
		t.Errorf("expected window %v, got %v", expectedWindow3, resultWindow)
	}
}
