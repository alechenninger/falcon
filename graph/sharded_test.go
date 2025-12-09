package graph_test

import (
	"context"
	"testing"

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
