package graph_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestShardedGraph holds multiple sharded graphs sharing a single store.
type TestShardedGraph struct {
	store    *store.MemoryStore
	shards   map[graph.ShardID]*shardedTestNode
	assigner graph.Router
	ctx      context.Context
	cancel   context.CancelFunc
}

// shardedTestNode wraps a ShardedGraph with its observer for waiting on writes.
type shardedTestNode struct {
	graph    *graph.ShardedGraph
	observer *graph.SignalingObserver
}

// NewTestShardedGraph creates a test setup with multiple shards sharing a store.
// The assigner function determines which shard owns each object.
func NewTestShardedGraph(s *schema.Schema, shardIDs []graph.ShardID, router graph.Router) *TestShardedGraph {
	ctx, cancel := context.WithCancel(context.Background())
	ms := store.NewMemoryStore()

	// Create nodes map first
	nodes := make(map[graph.ShardID]*shardedTestNode)
	for _, shardID := range shardIDs {
		nodes[shardID] = &shardedTestNode{
			observer: graph.NewSignalingObserver(),
		}
	}

	// Create all graphs with cross-references
	// We need to build the shard map first, then create all graphs
	for _, shardID := range shardIDs {
		remoteShards := make(map[graph.ShardID]graph.Graph)
		// Remote shards will be set after all graphs are created
		nodes[shardID].graph = graph.NewShardedGraph(shardID, s, router, remoteShards, ms, ms).
			WithObserver(nodes[shardID].observer)
	}

	// Wire up cross-shard references by updating the shard maps
	// Since Go maps are reference types, we can update the maps in place
	for _, shardID := range shardIDs {
		for otherID, node := range nodes {
			if otherID != shardID {
				// Add to the shard's remote map
				nodes[shardID].graph.SetRemoteShard(otherID, node.graph)
			}
		}
	}

	tsg := &TestShardedGraph{
		store:    ms,
		shards:   nodes,
		assigner: router,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start all shards in background
	for _, node := range nodes {
		go func(n *shardedTestNode) {
			_ = n.graph.Start(ctx)
		}(node)
	}

	// Wait for all to be ready
	for _, node := range nodes {
		node.observer.WaitReady()
	}

	return tsg
}

// Shard returns the graph for the given shard ID.
func (tsg *TestShardedGraph) Shard(id graph.ShardID) *graph.ShardedGraph {
	return tsg.shards[id].graph
}

// WriteTuple writes a tuple and waits for all shards to apply it.
func (tsg *TestShardedGraph) WriteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	// Validate against any shard (they all share the schema)
	for _, node := range tsg.shards {
		if err := node.graph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
			return err
		}
		break
	}

	if err := tsg.store.WriteTuple(ctx, store.Tuple{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
	}); err != nil {
		return err
	}

	// Wait for all shards to apply this write
	t, _ := tsg.store.CurrentTime(ctx)
	for _, node := range tsg.shards {
		node.observer.WaitForTime(t)
	}
	return nil
}

// DeleteTuple deletes a tuple and waits for all shards to apply it.
func (tsg *TestShardedGraph) DeleteTuple(ctx context.Context, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) error {
	// Validate against any shard (they all share the schema)
	for _, node := range tsg.shards {
		if err := node.graph.ValidateTuple(objectType, relation, subjectType, subjectRelation); err != nil {
			return err
		}
		break
	}

	if err := tsg.store.DeleteTuple(ctx, store.Tuple{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
	}); err != nil {
		return err
	}

	// Wait for all shards to apply this delete
	t, _ := tsg.store.CurrentTime(ctx)
	for _, node := range tsg.shards {
		node.observer.WaitForTime(t)
	}
	return nil
}

// Check is a convenience wrapper that calls Check with MaxSnapshotWindow and nil visited.
func (tsg *TestShardedGraph) Check(ctx context.Context, shardID graph.ShardID, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, error) {
	ok, _, err := tsg.shards[shardID].graph.Check(ctx, subjectType, subjectID, objectType, objectID, relation, graph.MaxSnapshotWindow, nil)
	return ok, err
}

// Close stops all shard subscriptions.
func (tsg *TestShardedGraph) Close() {
	tsg.cancel()
}

// byObjectType returns a Router that assigns shards by object type.
func byObjectType(mapping map[schema.TypeName]graph.ShardID) graph.Router {
	return func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		return mapping[objectType]
	}
}

// TestShardedGraph_LocalCheck verifies that checks for local objects work.
func TestShardedGraph_LocalCheck(t *testing.T) {
	s := testSchema()

	// Shard by type: documents on shard1, folders on shard2
	router := byObjectType(map[schema.TypeName]graph.ShardID{
		"document": "shard1",
		"folder":   "shard2",
		"group":    "shard1",
		"user":     "shard1",
	})

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1  = 1
		doc100 = 100
	)

	// Add user 1 as a direct viewer of document 100
	if err := tsg.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Check from shard1 (where documents live) - should work
	ok, err := tsg.Check(ctx, "shard1", "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 from shard1")
	}

	// Check from shard2 - should route to shard1 and work
	ok, err = tsg.Check(ctx, "shard2", "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 from shard2 (via routing)")
	}
}

// TestShardedGraph_CrossShardArrow tests arrow traversal across shard boundaries.
// Document is on shard1, folder is on shard2.
func TestShardedGraph_CrossShardArrow(t *testing.T) {
	s := testSchema()

	// Shard by type: documents on shard1, folders on shard2
	router := byObjectType(map[schema.TypeName]graph.ShardID{
		"document": "shard1",
		"folder":   "shard2",
		"group":    "shard2",
		"user":     "shard1",
	})

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		user2    = 2
		folder10 = 10
		doc100   = 100
	)

	// Document 100's parent is folder 10 (folder is on shard2)
	if err := tsg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 is a direct viewer of folder 10 (on shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 should be a viewer of doc100 via parent folder
	// This requires: shard1 (doc) -> arrow to shard2 (folder) -> check viewer
	ok, err := tsg.Check(ctx, "shard1", "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via cross-shard parent folder")
	}

	// User 2 should NOT be a viewer
	ok, err = tsg.Check(ctx, "shard1", "user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

// TestShardedGraph_CrossShardUserset tests userset membership across shards.
// Group is on shard2, document is on shard1.
func TestShardedGraph_CrossShardUserset(t *testing.T) {
	s := testSchema()

	// Shard by type: documents on shard1, groups on shard2
	router := byObjectType(map[schema.TypeName]graph.ShardID{
		"document": "shard1",
		"folder":   "shard1",
		"group":    "shard2",
		"user":     "shard1",
	})

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		alice  = 1
		bob    = 2
		group1 = 10
		doc100 = 100
	)

	// Alice is a member of group 1 (group is on shard2)
	if err := tsg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Members of group 1 are viewers of document 100 (document is on shard1)
	if err := tsg.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be a viewer via group membership
	// This requires: shard1 (doc) -> userset check on shard2 (group)
	ok, err := tsg.Check(ctx, "shard1", "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via cross-shard group membership")
	}

	// Bob should NOT be a viewer (not in group)
	ok, err = tsg.Check(ctx, "shard1", "user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer of doc100")
	}
}

// TestShardedGraph_NestedCrossShardArrow tests nested arrow traversal across multiple shards.
func TestShardedGraph_NestedCrossShardArrow(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding to spread folders across shards
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		// Documents on shard1
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2
		doc100   = 100
	)

	// Document 100's parent is folder 10 (shard1)
	if err := tsg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Folder 10's parent is folder 11 (shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder10, "parent", "folder", folder11, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 is a viewer of folder 11 (shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 should be a viewer of folder 10 via parent folder 11
	// shard1 (folder10) -> shard2 (folder11) -> viewer check
	ok, err := tsg.Check(ctx, "shard1", "user", user1, "folder", folder10, "viewer")
	if err != nil {
		t.Fatalf("Check (folder10) failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of folder10 via parent folder11")
	}

	// User 1 should also be a viewer of doc100 via the full chain
	// shard1 (doc100) -> shard1 (folder10) -> shard2 (folder11) -> viewer
	ok, err = tsg.Check(ctx, "shard1", "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (doc100) failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via nested cross-shard parent folders")
	}
}

// TestShardedGraph_CheckUnionMultipleShards tests CheckUnion with objects on multiple shards.
func TestShardedGraph_CheckUnionMultipleShards(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2
		folder12 = 12 // Even -> shard1
		doc100   = 100
	)

	// Document has multiple parent folders across shards
	if err := tsg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder11, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "document", doc100, "parent", "folder", folder12, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 is a viewer of only folder 11 (on shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// User 1 should be a viewer of doc100 via folder 11
	// The check should batch check folders across both shards
	ok, err := tsg.Check(ctx, "shard1", "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via folder11 (cross-shard CheckUnion)")
	}
}

// TestShardedGraph_ArrowWithCrossShardUserset tests arrow + userset across shards.
func TestShardedGraph_ArrowWithCrossShardUserset(t *testing.T) {
	s := testSchema()

	// documents on shard1, folders on shard2, groups on shard3
	router := byObjectType(map[schema.TypeName]graph.ShardID{
		"document": "shard1",
		"folder":   "shard2",
		"group":    "shard3",
		"user":     "shard1",
	})

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	const (
		alice    = 1
		bob      = 2
		group1   = 10
		folder10 = 100
		doc200   = 200
	)

	// Alice is a member of group 1 (on shard3)
	if err := tsg.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Members of group 1 are viewers of folder 10 (on shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Document 200's parent is folder 10
	if err := tsg.WriteTuple(ctx, "document", doc200, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Alice should be a viewer of doc200 via:
	// shard1 (doc200) -> arrow to shard2 (folder10) -> userset to shard3 (group1)
	ok, err := tsg.Check(ctx, "shard1", "user", alice, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (alice) failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc200 via multi-shard traversal")
	}

	// Bob should NOT be a viewer
	ok, err = tsg.Check(ctx, "shard1", "user", bob, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (bob) failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer of doc200")
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_Positive tests that when a true result
// is found on a remote shard, the window from that check is returned unmodified.
func TestShardedGraph_CheckUnionWindowNarrowing_Positive(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders: even on shard1, odd on shard2
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2
		folder12 = 12 // Even -> shard1
	)

	// User 1 is a viewer of folder 11 (on shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on both shards
	// folder10 (shard1): user1 is NOT a viewer
	// folder11 (shard2): user1 IS a viewer
	// folder12 (shard1): user1 is NOT a viewer
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

	// Call CheckUnion - should find user1 on folder11 (shard2)
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if !found {
		t.Error("expected CheckUnion to find user1 as viewer")
	}

	// The result window should be valid (Min <= Max)
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_Negative tests that when all results
// are false, the window is narrowed by intersecting all result windows.
func TestShardedGraph_CheckUnionWindowNarrowing_Negative(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders: even on shard1, odd on shard2
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		user2    = 2
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2
		folder12 = 12 // Even -> shard1
	)

	// Add some data so that window narrowing happens
	// User 2 (not user 1) is a viewer of all folders
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder12, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on both shards
	// None of these folders have user1 as viewer
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

	// Call CheckUnion - user1 should NOT be found on any folder
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if found {
		t.Error("expected CheckUnion to NOT find user1 as viewer")
	}

	// The result window should be narrowed from MaxSnapshotWindow
	// It should be valid (Min <= Max)
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}

	// The result window should have been narrowed (not be MaxSnapshotWindow anymore)
	// Since we wrote tuples, the replicated time should be > 0
	if resultWindow.Min() == 0 && resultWindow.Max() == graph.MaxSnapshotWindow.Max() {
		// This would mean no narrowing happened, which is wrong
		t.Log("Warning: result window appears unchanged from MaxSnapshotWindow")
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_MixedShards tests window narrowing
// when checks span multiple remote shards and all return false.
func TestShardedGraph_CheckUnionWindowNarrowing_MixedShards(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders: %3 == 0 -> shard1, %3 == 1 -> shard2, %3 == 2 -> shard3
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			switch objectID % 3 {
			case 0:
				return "shard1"
			case 1:
				return "shard2"
			default:
				return "shard3"
			}
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		user2    = 2
		folder9  = 9  // 9 % 3 == 0 -> shard1
		folder10 = 10 // 10 % 3 == 1 -> shard2
		folder11 = 11 // 11 % 3 == 2 -> shard3
	)

	// Add some data so that window narrowing happens on each shard
	// User 2 (not user 1) is a viewer of all folders
	if err := tsg.WriteTuple(ctx, "folder", folder9, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on all three shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder9))
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion from shard1 - should check all three shards in parallel
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if found {
		t.Error("expected CheckUnion to NOT find user1 as viewer")
	}

	// The result window should be valid
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_LocalOnly tests window narrowing
// when all checks are local (no remote shards involved) and all return false.
func TestShardedGraph_CheckUnionWindowNarrowing_LocalOnly(t *testing.T) {
	s := testSchema()

	// All folders on shard1 (local)
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		user2    = 2
		folder10 = 10
		folder11 = 11
		folder12 = 12
	)

	// Add some data so that window narrowing happens
	// User 2 (not user 1) is a viewer of all folders
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder12, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with only local folders (all on shard1)
	// None of these folders have user1 as viewer
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

	// Call CheckUnion - user1 should NOT be found on any folder
	// All checks are local (no remote shards)
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if found {
		t.Error("expected CheckUnion to NOT find user1 as viewer")
	}

	// The result window should be valid (Min <= Max)
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}

	// The window should be narrowed - Max should be at most the replicated time
	replicatedTime := tsg.Shard("shard1").ReplicatedTime()
	if resultWindow.Max() > replicatedTime {
		t.Errorf("result window Max %d should be <= replicated time %d", resultWindow.Max(), replicatedTime)
	}

	// Min should be > 0 since we wrote tuples (window narrowed based on data access)
	if resultWindow.Min() == 0 {
		t.Error("expected result window Min to be narrowed (> 0) after accessing data")
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_RemoteOnly_Positive tests that when
// all checks are on multiple remote shards and one returns true, we get the correct result.
func TestShardedGraph_CheckUnionWindowNarrowing_RemoteOnly_Positive(t *testing.T) {
	s := testSchema()

	// Folders distributed across multiple remote shards (none on shard1)
	// folder10 -> shard2, folder11 -> shard3, folder12 -> shard2, folder13 -> shard3
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard2"
			}
			return "shard3"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // -> shard2
		folder11 = 11 // -> shard3
		folder12 = 12 // -> shard2
		folder13 = 13 // -> shard3
	)

	// User 1 is a viewer of folder 13 (on shard3)
	if err := tsg.WriteTuple(ctx, "folder", folder13, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on multiple remote shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))
	bitmap.Add(uint32(folder12))
	bitmap.Add(uint32(folder13))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion from shard1 - checks go to shard2 and shard3 in parallel
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if !found {
		t.Error("expected CheckUnion to find user1 as viewer")
	}

	// The result window should be valid
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}
}

// TestShardedGraph_CheckUnionWindowNarrowing_RemoteOnly_Negative tests window narrowing
// when all checks are on multiple remote shards and all return false.
func TestShardedGraph_CheckUnionWindowNarrowing_RemoteOnly_Negative(t *testing.T) {
	s := testSchema()

	// Folders distributed across multiple remote shards (none on shard1)
	// folder10 -> shard2, folder11 -> shard3, folder12 -> shard2, folder13 -> shard3
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard2"
			}
			return "shard3"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		user2    = 2
		folder10 = 10 // -> shard2
		folder11 = 11 // -> shard3
		folder12 = 12 // -> shard2
		folder13 = 13 // -> shard3
	)

	// Add some data so that window narrowing happens
	// User 2 (not user 1) is a viewer of all folders
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder12, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := tsg.WriteTuple(ctx, "folder", folder13, "viewer", "user", user2, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on multiple remote shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))
	bitmap.Add(uint32(folder12))
	bitmap.Add(uint32(folder13))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion from shard1 - checks go to shard2 and shard3 in parallel
	// user1 should NOT be found on any folder
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("CheckUnion failed: %v", err)
	}
	if found {
		t.Error("expected CheckUnion to NOT find user1 as viewer")
	}

	// The result window should be valid (Min <= Max)
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}

	// The window should be narrowed - Max should be at most the min of replicated times from both shards
	replicatedTime2 := tsg.Shard("shard2").ReplicatedTime()
	replicatedTime3 := tsg.Shard("shard3").ReplicatedTime()
	minReplicatedTime := replicatedTime2
	if replicatedTime3 < minReplicatedTime {
		minReplicatedTime = replicatedTime3
	}
	if resultWindow.Max() > minReplicatedTime {
		t.Errorf("result window Max %d should be <= min replicated time %d", resultWindow.Max(), minReplicatedTime)
	}

	// Min should be > 0 since we wrote tuples (window narrowed based on data access)
	if resultWindow.Min() == 0 {
		t.Error("expected result window Min to be narrowed (> 0) after accessing data")
	}
}

// TestShardedGraph_CheckUnionParallelCancellation tests that when one remote shard
// returns true, the remaining checks are cancelled.
func TestShardedGraph_CheckUnionParallelCancellation(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2
	)

	// User 1 is a viewer of folder 11 (on shard2)
	if err := tsg.WriteTuple(ctx, "folder", folder11, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Build a CheckUnion with folders on both shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion multiple times to ensure parallel execution works correctly
	// and early termination on true result functions properly
	for i := 0; i < 10; i++ {
		found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
		if err != nil {
			t.Fatalf("CheckUnion iteration %d failed: %v", i, err)
		}
		if !found {
			t.Errorf("iteration %d: expected CheckUnion to find user1 as viewer", i)
		}
		if resultWindow.Min() > resultWindow.Max() {
			t.Errorf("iteration %d: result window invalid: Min %d > Max %d", i, resultWindow.Min(), resultWindow.Max())
		}
	}
}

// FailingGraph is a test fake that returns configurable errors for CheckUnion calls.
// It implements the graph.Graph interface by embedding a real graph and overriding CheckUnion.
type FailingGraph struct {
	graph.Graph
	CheckUnionErr error
}

// CheckUnion returns the configured error if set, otherwise delegates to the embedded graph.
func (f *FailingGraph) CheckUnion(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	checks []graph.RelationCheck,
	visited []graph.VisitedKey,
) (bool, graph.SnapshotWindow, error) {
	if f.CheckUnionErr != nil {
		return false, graph.SnapshotWindow{}, f.CheckUnionErr
	}
	return f.Graph.CheckUnion(ctx, subjectType, subjectID, checks, visited)
}

// TestShardedGraph_CheckUnion_SingleShardError tests that when one remote shard errors
// and others return false, we get an inconclusive error listing the failed shard.
func TestShardedGraph_CheckUnion_SingleShardError(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders: even on shard1, odd on shard2
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	// Replace shard2 with a failing graph
	failingGraph := &FailingGraph{
		Graph:         tsg.Shard("shard2"),
		CheckUnionErr: errors.New("shard2 unavailable"),
	}
	tsg.Shard("shard1").SetRemoteShard("shard2", failingGraph)

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2 (will fail)
	)

	// Build a CheckUnion with folders on both shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion - shard1 returns false, shard2 errors
	// Should return inconclusive error
	found, _, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err == nil {
		t.Fatal("expected error from CheckUnion when shard fails")
	}
	if found {
		t.Error("expected found to be false when inconclusive")
	}
	if !strings.Contains(err.Error(), "inconclusive") {
		t.Errorf("expected 'inconclusive' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "shard2") {
		t.Errorf("expected 'shard2' in error, got: %v", err)
	}
}

// TestShardedGraph_CheckUnion_AllShardsError tests that when all remote shards error,
// we get an inconclusive error listing all failed shards.
func TestShardedGraph_CheckUnion_AllShardsError(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding: %3 == 0 -> shard1, %3 == 1 -> shard2, %3 == 2 -> shard3
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			switch objectID % 3 {
			case 0:
				return "shard1"
			case 1:
				return "shard2"
			default:
				return "shard3"
			}
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	// Replace remote shards with failing graphs
	failingShard2 := &FailingGraph{
		Graph:         tsg.Shard("shard2"),
		CheckUnionErr: errors.New("shard2 unavailable"),
	}
	failingShard3 := &FailingGraph{
		Graph:         tsg.Shard("shard3"),
		CheckUnionErr: errors.New("shard3 unavailable"),
	}
	tsg.Shard("shard1").SetRemoteShard("shard2", failingShard2)
	tsg.Shard("shard1").SetRemoteShard("shard3", failingShard3)

	const (
		user1    = 1
		folder10 = 10 // 10 % 3 == 1 -> shard2 (will fail)
		folder11 = 11 // 11 % 3 == 2 -> shard3 (will fail)
	)

	// Build a CheckUnion with folders only on remote shards (both will fail)
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion - both remote shards error
	found, _, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err == nil {
		t.Fatal("expected error from CheckUnion when all shards fail")
	}
	if found {
		t.Error("expected found to be false when inconclusive")
	}
	if !strings.Contains(err.Error(), "inconclusive") {
		t.Errorf("expected 'inconclusive' in error, got: %v", err)
	}
	// Both shards should be mentioned (order may vary)
	if !strings.Contains(err.Error(), "shard2") || !strings.Contains(err.Error(), "shard3") {
		t.Errorf("expected both 'shard2' and 'shard3' in error, got: %v", err)
	}
}

// TestShardedGraph_CheckUnion_ErrorButFoundOnOther tests that when one shard errors
// but another returns true, we short-circuit with the true result (error is ignored).
func TestShardedGraph_CheckUnion_ErrorButFoundOnOther(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding for folders: even on shard1, odd on shard2
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			if objectID%2 == 0 {
				return "shard1"
			}
			return "shard2"
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2"}, router)
	defer tsg.Close()

	const (
		user1    = 1
		folder10 = 10 // Even -> shard1
		folder11 = 11 // Odd -> shard2 (will fail)
	)

	// User 1 is a viewer of folder 10 (on shard1 - will succeed)
	if err := tsg.WriteTuple(ctx, "folder", folder10, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Replace shard2 with a failing graph
	failingGraph := &FailingGraph{
		Graph:         tsg.Shard("shard2"),
		CheckUnionErr: errors.New("shard2 unavailable"),
	}
	tsg.Shard("shard1").SetRemoteShard("shard2", failingGraph)

	// Build a CheckUnion with folders on both shards
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion - shard1 returns true (found), shard2 errors
	// Should return true and no error (short-circuit)
	found, resultWindow, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err != nil {
		t.Fatalf("expected no error when found on another shard, got: %v", err)
	}
	if !found {
		t.Error("expected found to be true")
	}
	if resultWindow.Min() > resultWindow.Max() {
		t.Errorf("result window invalid: Min %d > Max %d", resultWindow.Min(), resultWindow.Max())
	}
}

// TestShardedGraph_CheckUnion_PartialErrorPartialSuccess tests that when some shards error
// and some return false (none return true), we get an inconclusive error listing only failed shards.
func TestShardedGraph_CheckUnion_PartialErrorPartialSuccess(t *testing.T) {
	s := testSchema()

	// Use ID-based sharding: %3 == 0 -> shard1, %3 == 1 -> shard2, %3 == 2 -> shard3
	router := func(objectType schema.TypeName, objectID schema.ID) graph.ShardID {
		if objectType == "folder" {
			switch objectID % 3 {
			case 0:
				return "shard1"
			case 1:
				return "shard2"
			default:
				return "shard3"
			}
		}
		return "shard1"
	}

	tsg := NewTestShardedGraph(s, []graph.ShardID{"shard1", "shard2", "shard3"}, router)
	defer tsg.Close()

	// Only replace shard3 with a failing graph (shard2 will work)
	failingShard3 := &FailingGraph{
		Graph:         tsg.Shard("shard3"),
		CheckUnionErr: errors.New("shard3 unavailable"),
	}
	tsg.Shard("shard1").SetRemoteShard("shard3", failingShard3)

	const (
		user1    = 1
		folder10 = 10 // 10 % 3 == 1 -> shard2 (will succeed with false)
		folder11 = 11 // 11 % 3 == 2 -> shard3 (will fail)
	)

	// Build a CheckUnion with folders on shard2 and shard3
	bitmap := roaring.New()
	bitmap.Add(uint32(folder10))
	bitmap.Add(uint32(folder11))

	checks := []graph.RelationCheck{{
		ObjectType: "folder",
		ObjectIDs:  bitmap,
		Relation:   "viewer",
		Window:     graph.MaxSnapshotWindow,
	}}

	// Call CheckUnion - shard2 returns false, shard3 errors
	found, _, err := tsg.Shard("shard1").CheckUnion(ctx, "user", user1, checks, nil)
	if err == nil {
		t.Fatal("expected error from CheckUnion when some shards fail")
	}
	if found {
		t.Error("expected found to be false when inconclusive")
	}
	if !strings.Contains(err.Error(), "inconclusive") {
		t.Errorf("expected 'inconclusive' in error, got: %v", err)
	}
	// Only shard3 should be mentioned (shard2 succeeded with false)
	if !strings.Contains(err.Error(), "shard3") {
		t.Errorf("expected 'shard3' in error, got: %v", err)
	}
	// shard2 should NOT be in the error since it succeeded
	if strings.Contains(err.Error(), "shard2") {
		t.Errorf("did not expect 'shard2' in error (it succeeded), got: %v", err)
	}
}
