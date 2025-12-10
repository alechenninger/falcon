package graph_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
)

var ctx = context.Background()

// testSchema creates a realistic document/folder/user/group schema for testing.
//
// The schema models:
//   - user: a leaf type with no relations (subjects are user IDs)
//   - group: has "member" relation pointing to users
//   - folder: has "viewer" relation pointing to users or group#member, with hierarchy via "parent"
//   - document: has "parent" pointing to folder, "viewer"/"editor" computed from parent
//
// Type IDs: user=1, group=2, folder=3, document=4
// Relation IDs: member=1, parent=1, viewer=2, editor=3
func testSchema() *schema.Schema {
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
							schema.Direct(schema.Ref("user")), // @user:1
						},
					},
				},
			},
			"folder": {
				ID:   3,
				Name: "folder",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")), // @folder:1
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),                        // @user:1
								schema.RefWithRelation("group", "member"), // @group:1#member
							),
							schema.Arrow("parent", "viewer"), // viewers of parent folder
						},
					},
				},
			},
			"document": {
				ID:   4,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")), // @folder:1
						},
					},
					"editor": {
						ID:   3,
						Name: "editor",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),                        // @user:1
								schema.RefWithRelation("group", "member"), // @group:1#member
							),
							schema.Arrow("parent", "editor"), // editors of parent folder
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),                        // @user:1
								schema.RefWithRelation("group", "member"), // @group:1#member
							),
							schema.Computed("editor"),        // editors can view
							schema.Arrow("parent", "viewer"), // viewers of parent folder
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

func TestCheck_DirectMembership(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	// Add user 1 as a direct viewer of document 100
	const (
		user1  = 1
		user2  = 2
		doc100 = 100
	)

	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer
	ok, _, err := g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100")
	}

	// User 2 should NOT be a viewer
	ok, _, err = g.Check(ctx, "user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_ComputedRelation(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	// Add user 1 as an editor of document 100
	const (
		user1  = 1
		user2  = 2
		doc100 = 100
	)

	if err := g.WriteTuple(ctx, "document", doc100, "editor", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be an editor
	ok, _, err := g.Check(ctx, "user", user1, "document", doc100, "editor")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be editor of doc100")
	}

	// User 1 should also be a viewer (editors can view)
	ok, _, err = g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 (editor) to also be viewer of doc100")
	}

	// User 2 should NOT be a viewer
	ok, _, err = g.Check(ctx, "user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_ArrowTraversal(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	// Setup: folder 10 is parent of document 100
	// User 1 is a viewer of folder 10
	const (
		user1    = 1
		user2    = 2
		folder10 = 10
		doc100   = 100
	)

	// Document 100's parent is folder 10
	if err := g.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 is a direct viewer of folder 10
	if err := g.WriteTuple(ctx, "folder", folder10, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer of doc100 (via parent folder)
	ok, _, err := g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via parent folder")
	}

	// User 2 should NOT be a viewer
	ok, _, err = g.Check(ctx, "user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_NestedArrowTraversal(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	// Setup: folder 20 is parent of folder 10, folder 10 is parent of document 100
	// User 1 is a viewer of folder 20
	const (
		user1    = 1
		folder10 = 10
		folder20 = 20
		doc100   = 100
	)

	// Document 100's parent is folder 10
	if err := g.WriteTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Folder 10's parent is folder 20
	if err := g.WriteTuple(ctx, "folder", folder10, "parent", "folder", folder20, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 is a direct viewer of folder 20 (the grandparent)
	if err := g.WriteTuple(ctx, "folder", folder20, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer of folder 10 (via parent folder 20)
	ok, _, err := g.Check(ctx, "user", user1, "folder", folder10, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of folder10 via parent folder20")
	}

	// User 1 should be a viewer of doc100 (via folder10 -> folder20)
	ok, _, err = g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via nested parent folders")
	}
}

func TestCheck_UnknownObjectType(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	_, _, err := g.Check(ctx, "user", 1, "unknown_type", 100, "viewer")
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestCheck_UnknownRelation(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	_, _, err := g.Check(ctx, "user", 1, "document", 100, "unknown_relation")
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestAddTuple_UnknownObjectType(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	err := g.WriteTuple(ctx, "unknown_type", 100, "viewer", "user", 1, "")
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestAddTuple_UnknownRelation(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	err := g.WriteTuple(ctx, "document", 100, "unknown_relation", "user", 1, "")
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestRemoveTuple(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		user1  = 1
		doc100 = 100
	)

	// Add then remove
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Verify user is a viewer
	ok, _, err := g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer after AddTuple")
	}

	// Remove the tuple
	if err := g.DeleteTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("RemoveTuple failed: %v", err)
	}

	// Verify user is no longer a viewer
	ok, _, err = g.Check(ctx, "user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user1 to NOT be viewer after RemoveTuple")
	}
}

func TestCheck_MultipleUsersAndDocuments(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3

		doc1 = 101
		doc2 = 102
		doc3 = 103
	)

	// Alice can view doc1
	if err := g.WriteTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob can edit doc2 (and therefore view it)
	if err := g.WriteTuple(ctx, "document", doc2, "editor", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Charlie can view doc3
	if err := g.WriteTuple(ctx, "document", doc3, "viewer", "user", charlie, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	tests := []struct {
		name        string
		subjectType schema.TypeName
		subjectID   schema.ID
		objectType  schema.TypeName
		objectID    schema.ID
		relation    schema.RelationName
		want        bool
	}{
		{"alice can view doc1", "user", alice, "document", doc1, "viewer", true},
		{"alice cannot view doc2", "user", alice, "document", doc2, "viewer", false},
		{"alice cannot view doc3", "user", alice, "document", doc3, "viewer", false},
		{"bob cannot view doc1", "user", bob, "document", doc1, "viewer", false},
		{"bob can view doc2 (is editor)", "user", bob, "document", doc2, "viewer", true},
		{"bob can edit doc2", "user", bob, "document", doc2, "editor", true},
		{"bob cannot view doc3", "user", bob, "document", doc3, "viewer", false},
		{"charlie cannot view doc1", "user", charlie, "document", doc1, "viewer", false},
		{"charlie cannot view doc2", "user", charlie, "document", doc2, "viewer", false},
		{"charlie can view doc3", "user", charlie, "document", doc3, "viewer", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := g.Check(ctx, tt.subjectType, tt.subjectID, tt.objectType, tt.objectID, tt.relation)
			if err != nil {
				t.Fatalf("Check failed: %v", err)
			}
			if got != tt.want {
				t.Errorf("Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheck_DirectRelationOnly(t *testing.T) {
	// Test a relation with no usersets (direct-only)
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		user1   = 1
		group10 = 10
	)

	// Add user1 as member of group10
	if err := g.WriteTuple(ctx, "group", group10, "member", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Check membership
	ok, _, err := g.Check(ctx, "user", user1, "group", group10, "member")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be member of group10")
	}
}

// TestCheck_SubjectTypeDistinction verifies that subjects with the same ID but
// different subject references (type + relation) are correctly distinguished.
func TestCheck_SubjectTypeDistinction(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		doc100 = 100
		group1 = 1
		alice  = 1 // Same ID as group1!
	)

	// Add group:1#member as viewers of doc100 (userset subject)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (group#member) failed: %v", err)
	}

	// user:1 (alice) should NOT be a viewer - the userset subject is group:1#member,
	// but alice is not a member of group 1
	ok, _, err := g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if ok {
		t.Error("expected user:1 to NOT be viewer (group:1#member added, but user:1 is not in group)")
	}

	// Now add alice as a member of group 1
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice in group) failed: %v", err)
	}

	// Now alice should be a viewer (via group membership)
	ok, _, err = g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user after group membership) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to be viewer via group membership")
	}
}

// TestCheck_BothSubjectRefsWithSameID verifies that we can have both user:1
// (direct) and group:1#member (userset) as viewers, and they are tracked separately.
func TestCheck_BothSubjectRefsWithSameID(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		doc100 = 100
		id1    = 1 // Same ID for user and group
	)

	// Alice (user:1) is a member of group:1
	if err := g.WriteTuple(ctx, "group", id1, "member", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user in group) failed: %v", err)
	}

	// Add both user:1 (direct) and group:1#member (userset) as viewers
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user direct) failed: %v", err)
	}
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", id1, "member"); err != nil {
		t.Fatalf("AddTuple (group#member) failed: %v", err)
	}

	// user:1 should be a viewer (via both direct AND group membership)
	ok, _, err := g.Check(ctx, "user", id1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to be viewer of doc100")
	}

	// Remove the direct user:1 tuple - user should still be viewer via group
	if err := g.DeleteTuple(ctx, "document", doc100, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("RemoveTuple (user direct) failed: %v", err)
	}

	// user:1 should still be a viewer (via group:1#member)
	ok, _, err = g.Check(ctx, "user", id1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user after direct removal) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to still be viewer via group membership")
	}

	// Now remove the group userset tuple
	if err := g.DeleteTuple(ctx, "document", doc100, "viewer", "group", id1, "member"); err != nil {
		t.Fatalf("RemoveTuple (group#member) failed: %v", err)
	}

	// user:1 should no longer be a viewer
	ok, _, err = g.Check(ctx, "user", id1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user after all removals) failed: %v", err)
	}
	if ok {
		t.Error("expected user:1 to NOT be viewer after removing all tuples")
	}
}

// TestCheck_UsersetSubject tests userset subjects like document:100#viewer@group:1#member
// which means "all members of group 1 are viewers of document 100".
func TestCheck_UsersetSubject(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		doc100  = 100
	)

	// Alice and Bob are members of group 1
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice member) failed: %v", err)
	}
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple (bob member) failed: %v", err)
	}

	// Members of group 1 are viewers of document 100
	// This is a userset subject: document:100#viewer@group:1#member
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (userset) failed: %v", err)
	}

	// Alice should be a viewer (via group membership)
	ok, _, err := g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (alice) failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via group membership")
	}

	// Bob should be a viewer (via group membership)
	ok, _, err = g.Check(ctx, "user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (bob) failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer of doc100 via group membership")
	}

	// Charlie should NOT be a viewer (not a member of group 1)
	ok, _, err = g.Check(ctx, "user", charlie, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (charlie) failed: %v", err)
	}
	if ok {
		t.Error("expected charlie to NOT be viewer of doc100")
	}
}

// TestCheck_UsersetSubject_MultipleGroups tests multiple userset subjects.
func TestCheck_UsersetSubject_MultipleGroups(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		group2  = 20
		doc100  = 100
	)

	// Alice is a member of group 1 only
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob is a member of group 2 only
	if err := g.WriteTuple(ctx, "group", group2, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of BOTH groups are viewers of document 100
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group2, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view (via group 1)
	ok, _, err := g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group 1")
	}

	// Bob can view (via group 2)
	ok, _, err = g.Check(ctx, "user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer via group 2")
	}

	// Charlie cannot view
	ok, _, err = g.Check(ctx, "user", charlie, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected charlie to NOT be viewer")
	}
}

// TestCheck_UsersetAndDirectCombined tests combining direct and userset subjects.
func TestCheck_UsersetAndDirectCombined(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		doc100  = 100
	)

	// Alice is a member of group 1
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of group 1 are viewers (userset subject)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob is a direct viewer
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view (via group membership)
	ok, _, err := g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group")
	}

	// Bob can view (direct)
	ok, _, err = g.Check(ctx, "user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be direct viewer")
	}

	// Charlie cannot view
	ok, _, err = g.Check(ctx, "user", charlie, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected charlie to NOT be viewer")
	}
}

// TestRemoveTuple_UsersetSubject tests removing userset subject tuples.
func TestRemoveTuple_UsersetSubject(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice  = 1
		group1 = 10
		doc100 = 100
	)

	// Alice is a member of group 1
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of group 1 are viewers
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view
	ok, _, err := g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}

	// Remove the userset tuple
	if err := g.DeleteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("RemoveTuple failed: %v", err)
	}

	// Alice can no longer view
	ok, _, err = g.Check(ctx, "user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer after removing userset tuple")
	}
}

// TestCheck_ArrowWithUsersetSubject tests the combination of arrow traversal
// and userset subjects: group members granted access on a parent folder should
// grant access to documents in that folder.
//
// Setup:
//   - folder:10#viewer@group:1#member (members of group 1 can view folder 10)
//   - document:100#parent@folder:10   (document 100 is in folder 10)
//   - group:1#member@user:alice       (alice is a member of group 1)
//
// Result: alice can view document:100 via parent→viewer arrow + userset subject
func TestCheck_ArrowWithUsersetSubject(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)

	const (
		alice    = 1
		bob      = 2
		group1   = 10
		folder10 = 100
		doc200   = 200
	)

	// Alice is a member of group 1
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice in group) failed: %v", err)
	}

	// Members of group 1 are viewers of folder 10 (userset subject on folder)
	if err := g.WriteTuple(ctx, "folder", folder10, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (group viewers of folder) failed: %v", err)
	}

	// Document 200's parent is folder 10
	if err := g.WriteTuple(ctx, "document", doc200, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple (doc parent) failed: %v", err)
	}

	// Alice should be a viewer of doc200 via:
	// 1. doc200 → parent → folder10
	// 2. folder10#viewer includes group1#member
	// 3. alice is in group1#member
	ok, _, err := g.Check(ctx, "user", alice, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (alice) failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc200 via parent folder's group membership")
	}

	// Bob should NOT be a viewer (not in group 1)
	ok, _, err = g.Check(ctx, "user", bob, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (bob) failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer of doc200")
	}

	// Now add bob to the group
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple (bob in group) failed: %v", err)
	}

	// Bob should now be a viewer
	ok, _, err = g.Check(ctx, "user", bob, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (bob after joining group) failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer of doc200 after joining group")
	}
}

// TestCheck_DirectSubjectTypeCollision tests that direct subjects of different types
// with the same ID are correctly distinguished. This uses a custom schema that allows
// multiple direct subject types (no subject relations) on the same relation.
func TestCheck_DirectSubjectTypeCollision(t *testing.T) {
	// Create a schema where "resource" has a "viewer" relation that allows
	// both "user" and "serviceAccount" as direct subjects
	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"serviceAccount": {
				ID:        2,
				Name:      "serviceAccount",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"resource": {
				ID:   3,
				Name: "resource",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),           // @user:1
								schema.Ref("serviceAccount"), // @serviceAccount:1
							),
						},
					},
				},
			},
		},
	}
	s.Compile()

	g := graph.NewTestGraph(s)

	const (
		resource1 = 100
		id1       = 1 // Same ID for both user and serviceAccount
	)

	// Add serviceAccount:1 as a viewer (direct subject)
	if err := g.WriteTuple(ctx, "resource", resource1, "viewer", "serviceAccount", id1, ""); err != nil {
		t.Fatalf("AddTuple (serviceAccount) failed: %v", err)
	}

	// serviceAccount:1 should be a viewer
	ok, _, err := g.Check(ctx, "serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount) failed: %v", err)
	}
	if !ok {
		t.Error("expected serviceAccount:1 to be viewer")
	}

	// user:1 should NOT be a viewer (different type, same ID)
	ok, _, err = g.Check(ctx, "user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if ok {
		t.Error("expected user:1 to NOT be viewer (only serviceAccount:1 is)")
	}

	// Now add user:1 as well
	if err := g.WriteTuple(ctx, "resource", resource1, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user) failed: %v", err)
	}

	// Both should now be viewers
	ok, _, err = g.Check(ctx, "user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user after add) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to be viewer after adding")
	}

	ok, _, err = g.Check(ctx, "serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount after user add) failed: %v", err)
	}
	if !ok {
		t.Error("expected serviceAccount:1 to still be viewer")
	}

	// Remove serviceAccount:1 - user:1 should still be a viewer
	if err := g.DeleteTuple(ctx, "resource", resource1, "viewer", "serviceAccount", id1, ""); err != nil {
		t.Fatalf("RemoveTuple (serviceAccount) failed: %v", err)
	}

	ok, _, err = g.Check(ctx, "user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user after serviceAccount removal) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to still be viewer after removing serviceAccount:1")
	}

	ok, _, err = g.Check(ctx, "serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount after removal) failed: %v", err)
	}
	if ok {
		t.Error("expected serviceAccount:1 to NOT be viewer after removal")
	}
}

// TestCheck_UnionWindowNarrowing_AllFalse tests that when multiple usersets in a union
// all return false, the result window is the intersection (tightest) of all windows.
func TestCheck_UnionWindowNarrowing_AllFalse(t *testing.T) {
	// Schema with a relation that has multiple userset types (direct + group member)
	s := testSchema()
	g := graph.NewTestGraph(s)
	defer g.Close()

	const (
		alice  = 1
		bob    = 2
		doc100 = 100
		group1 = 10
	)

	// Add bob as direct viewer (time 1)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time1 := g.ReplicatedTime()

	// Add bob to group1 (time 2)
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Add group1 as viewer of doc100 (time 3)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time3 := g.ReplicatedTime()

	// Check alice (who is NOT in any relation) - should be false
	// The window should be narrowed from all the "false" checks
	window := graph.MaxSnapshotWindow
	ok, resultWindow, err := g.CheckAt(ctx, "user", alice, "document", doc100, "viewer", &window)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer")
	}

	// The window max should be constrained to replicated time
	if resultWindow.Max() != time3 {
		t.Errorf("expected window.Max = %d (replicated time), got %d", time3, resultWindow.Max())
	}
	// The window min should be raised to the highest min across all checks
	// Both direct check (time 1) and userset check (time 3) contribute
	// The highest min should be time3 (from the userset relation access)
	if resultWindow.Min() < time1 {
		t.Errorf("expected window.Min >= %d, got %d", time1, resultWindow.Min())
	}
}

// TestCheck_UnionWindowNarrowing_FirstTrue tests that when the first userset in
// a union returns true, we use that window (not affected by subsequent checks).
func TestCheck_UnionWindowNarrowing_FirstTrue(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)
	defer g.Close()

	const (
		alice  = 1
		doc100 = 100
		group1 = 10
	)

	// Add alice as direct viewer (time 1)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time1 := g.ReplicatedTime()

	// Add more data that would affect window if checked (time 2, 3)
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time3 := g.ReplicatedTime()

	// Check alice - should find via direct (first userset)
	window := graph.MaxSnapshotWindow
	ok, resultWindow, err := g.CheckAt(ctx, "user", alice, "document", doc100, "viewer", &window)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}

	// The window should be from the direct check only [1, 3]
	// Min = 1 (when direct tuple was added), Max = replicated time
	expectedWindow := graph.NewSnapshotWindow(time1, time3)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}
}

// TestCheck_UnionWindowNarrowing_LaterTrue tests that when a later userset returns true,
// we use that window and don't include windows from prior "false" checks.
func TestCheck_UnionWindowNarrowing_LaterTrue(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)
	defer g.Close()

	const (
		alice  = 1
		bob    = 2
		doc100 = 100
		group1 = 10
	)

	// Add bob as direct viewer (time 1) - alice is NOT direct
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Add alice to group1 (time 2)
	if err := g.WriteTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Add group1 as viewer of doc100 (time 3)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time3 := g.ReplicatedTime()

	// Check alice - should NOT find via direct, but SHOULD find via group
	window := graph.MaxSnapshotWindow
	ok, resultWindow, err := g.CheckAt(ctx, "user", alice, "document", doc100, "viewer", &window)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group")
	}

	// The window should reflect when the group membership was valid
	// Min should be at least time3 (when userset relation was added - the highest min)
	if resultWindow.Min() < time3 {
		t.Errorf("expected window.Min >= %d (userset relation time), got %d", time3, resultWindow.Min())
	}
	if resultWindow.Max() != time3 {
		t.Errorf("expected window.Max = %d (replicated time), got %d", time3, resultWindow.Max())
	}
}

// TestCheck_WindowNarrowing_MVCCTimeTravel tests that window narrowing correctly
// handles MVCC time-travel scenarios where data is added then removed.
func TestCheck_WindowNarrowing_MVCCTimeTravel(t *testing.T) {
	s := testSchema()
	g := graph.NewTestGraph(s)
	defer g.Close()

	const (
		alice  = 1
		doc100 = 100
	)

	// Add alice as viewer (time 1)
	if err := g.WriteTuple(ctx, "document", doc100, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}
	time1 := g.ReplicatedTime()

	// Remove alice as viewer (time 2)
	if err := g.DeleteTuple(ctx, "document", doc100, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}
	time2 := g.ReplicatedTime()

	// At current time (time 2), alice is NOT a viewer
	window := graph.MaxSnapshotWindow
	ok, resultWindow, err := g.CheckAt(ctx, "user", alice, "document", doc100, "viewer", &window)
	if err != nil {
		t.Fatalf("CheckAt failed: %v", err)
	}
	if ok {
		t.Error("expected alice to NOT be viewer at current time")
	}

	// The window min for "not found" should be >= time2 (when removed)
	// because at time1, alice WAS a viewer
	if resultWindow.Min() < time2 {
		t.Errorf("expected window.Min >= %d (removal time), got %d", time2, resultWindow.Min())
	}

	// Time-travel to time1 - alice SHOULD be a viewer
	windowAtTime1 := graph.NewSnapshotWindow(0, time1)
	ok, resultWindow, err = g.CheckAt(ctx, "user", alice, "document", doc100, "viewer", &windowAtTime1)
	if err != nil {
		t.Fatalf("CheckAt at time1 failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer at time1")
	}
	// Window should be [1, 1]
	expectedWindow := graph.NewSnapshotWindow(time1, time1)
	if resultWindow != expectedWindow {
		t.Errorf("expected window %v, got %v", expectedWindow, resultWindow)
	}
}
