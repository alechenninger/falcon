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
func testSchema() *schema.Schema {
	return &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"group": {
				Name: "group",
				Relations: map[schema.RelationName]*schema.Relation{
					"member": {
						Name: "member",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"), // @user:1
						},
					},
				},
			},
			"folder": {
				Name: "folder",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						Name: "parent",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("folder"), // @folder:1
						},
					},
					"viewer": {
						Name: "viewer",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"),                        // @user:1
							schema.RefWithRelation("group", "member"), // @group:1#member
						},
						Usersets: []schema.Userset{
							schema.Direct(),                  // direct viewers
							schema.Arrow("parent", "viewer"), // viewers of parent folder
						},
					},
				},
			},
			"document": {
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						Name: "parent",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("folder"), // @folder:1
						},
					},
					"editor": {
						Name: "editor",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"),                        // @user:1
							schema.RefWithRelation("group", "member"), // @group:1#member
						},
						Usersets: []schema.Userset{
							schema.Direct(),                  // direct editors
							schema.Arrow("parent", "editor"), // editors of parent folder
						},
					},
					"viewer": {
						Name: "viewer",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"),                        // @user:1
							schema.RefWithRelation("group", "member"), // @group:1#member
						},
						Usersets: []schema.Userset{
							schema.Direct(),                  // direct viewers
							schema.Computed("editor"),        // editors can view
							schema.Arrow("parent", "viewer"), // viewers of parent folder
						},
					},
				},
			},
		},
	}
}

func TestCheck_DirectMembership(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	// Add user 1 as a direct viewer of document 100
	const (
		user1  = 1
		user2  = 2
		doc100 = 100
	)

	if err := g.AddTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer
	ok, err := g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100")
	}

	// User 2 should NOT be a viewer
	ok, err = g.Check("user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_ComputedRelation(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	// Add user 1 as an editor of document 100
	const (
		user1  = 1
		user2  = 2
		doc100 = 100
	)

	if err := g.AddTuple(ctx, "document", doc100, "editor", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be an editor
	ok, err := g.Check("user", user1, "document", doc100, "editor")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be editor of doc100")
	}

	// User 1 should also be a viewer (editors can view)
	ok, err = g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 (editor) to also be viewer of doc100")
	}

	// User 2 should NOT be a viewer
	ok, err = g.Check("user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_ArrowTraversal(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	// Setup: folder 10 is parent of document 100
	// User 1 is a viewer of folder 10
	const (
		user1    = 1
		user2    = 2
		folder10 = 10
		doc100   = 100
	)

	// Document 100's parent is folder 10
	if err := g.AddTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 is a direct viewer of folder 10
	if err := g.AddTuple(ctx, "folder", folder10, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer of doc100 (via parent folder)
	ok, err := g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via parent folder")
	}

	// User 2 should NOT be a viewer
	ok, err = g.Check("user", user2, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user2 to NOT be viewer of doc100")
	}
}

func TestCheck_NestedArrowTraversal(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	// Setup: folder 20 is parent of folder 10, folder 10 is parent of document 100
	// User 1 is a viewer of folder 20
	const (
		user1    = 1
		folder10 = 10
		folder20 = 20
		doc100   = 100
	)

	// Document 100's parent is folder 10
	if err := g.AddTuple(ctx, "document", doc100, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Folder 10's parent is folder 20
	if err := g.AddTuple(ctx, "folder", folder10, "parent", "folder", folder20, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 is a direct viewer of folder 20 (the grandparent)
	if err := g.AddTuple(ctx, "folder", folder20, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// User 1 should be a viewer of folder 10 (via parent folder 20)
	ok, err := g.Check("user", user1, "folder", folder10, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of folder10 via parent folder20")
	}

	// User 1 should be a viewer of doc100 (via folder10 -> folder20)
	ok, err = g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer of doc100 via nested parent folders")
	}
}

func TestCheck_UnknownObjectType(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	_, err := g.Check("user", 1, "unknown_type", 100, "viewer")
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestCheck_UnknownRelation(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	_, err := g.Check("user", 1, "document", 100, "unknown_relation")
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestAddTuple_UnknownObjectType(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	err := g.AddTuple(ctx, "unknown_type", 100, "viewer", "user", 1, "")
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestAddTuple_UnknownRelation(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	err := g.AddTuple(ctx, "document", 100, "unknown_relation", "user", 1, "")
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestRemoveTuple(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	const (
		user1  = 1
		doc100 = 100
	)

	// Add then remove
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Verify user is a viewer
	ok, err := g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected user1 to be viewer after AddTuple")
	}

	// Remove the tuple
	if err := g.RemoveTuple(ctx, "document", doc100, "viewer", "user", user1, ""); err != nil {
		t.Fatalf("RemoveTuple failed: %v", err)
	}

	// Verify user is no longer a viewer
	ok, err = g.Check("user", user1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if ok {
		t.Error("expected user1 to NOT be viewer after RemoveTuple")
	}
}

func TestCheck_MultipleUsersAndDocuments(t *testing.T) {
	s := testSchema()
	g := graph.New(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3

		doc1 = 101
		doc2 = 102
		doc3 = 103
	)

	// Alice can view doc1
	if err := g.AddTuple(ctx, "document", doc1, "viewer", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob can edit doc2 (and therefore view it)
	if err := g.AddTuple(ctx, "document", doc2, "editor", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Charlie can view doc3
	if err := g.AddTuple(ctx, "document", doc3, "viewer", "user", charlie, ""); err != nil {
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
			got, err := g.Check(tt.subjectType, tt.subjectID, tt.objectType, tt.objectID, tt.relation)
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
	g := graph.New(s)

	const (
		user1   = 1
		group10 = 10
	)

	// Add user1 as member of group10
	if err := g.AddTuple(ctx, "group", group10, "member", "user", user1, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Check membership
	ok, err := g.Check("user", user1, "group", group10, "member")
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
	g := graph.New(s)

	const (
		doc100 = 100
		group1 = 1
		alice  = 1 // Same ID as group1!
	)

	// Add group:1#member as viewers of doc100 (userset subject)
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (group#member) failed: %v", err)
	}

	// user:1 (alice) should NOT be a viewer - the userset subject is group:1#member,
	// but alice is not a member of group 1
	ok, err := g.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if ok {
		t.Error("expected user:1 to NOT be viewer (group:1#member added, but user:1 is not in group)")
	}

	// Now add alice as a member of group 1
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice in group) failed: %v", err)
	}

	// Now alice should be a viewer (via group membership)
	ok, err = g.Check("user", alice, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		doc100 = 100
		id1    = 1 // Same ID for user and group
	)

	// Alice (user:1) is a member of group:1
	if err := g.AddTuple(ctx, "group", id1, "member", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user in group) failed: %v", err)
	}

	// Add both user:1 (direct) and group:1#member (userset) as viewers
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user direct) failed: %v", err)
	}
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", id1, "member"); err != nil {
		t.Fatalf("AddTuple (group#member) failed: %v", err)
	}

	// user:1 should be a viewer (via both direct AND group membership)
	ok, err := g.Check("user", id1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to be viewer of doc100")
	}

	// Remove the direct user:1 tuple - user should still be viewer via group
	if err := g.RemoveTuple(ctx, "document", doc100, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("RemoveTuple (user direct) failed: %v", err)
	}

	// user:1 should still be a viewer (via group:1#member)
	ok, err = g.Check("user", id1, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (user after direct removal) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to still be viewer via group membership")
	}

	// Now remove the group userset tuple
	if err := g.RemoveTuple(ctx, "document", doc100, "viewer", "group", id1, "member"); err != nil {
		t.Fatalf("RemoveTuple (group#member) failed: %v", err)
	}

	// user:1 should no longer be a viewer
	ok, err = g.Check("user", id1, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		doc100  = 100
	)

	// Alice and Bob are members of group 1
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice member) failed: %v", err)
	}
	if err := g.AddTuple(ctx, "group", group1, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple (bob member) failed: %v", err)
	}

	// Members of group 1 are viewers of document 100
	// This is a userset subject: document:100#viewer@group:1#member
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (userset) failed: %v", err)
	}

	// Alice should be a viewer (via group membership)
	ok, err := g.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (alice) failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc100 via group membership")
	}

	// Bob should be a viewer (via group membership)
	ok, err = g.Check("user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check (bob) failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer of doc100 via group membership")
	}

	// Charlie should NOT be a viewer (not a member of group 1)
	ok, err = g.Check("user", charlie, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		group2  = 20
		doc100  = 100
	)

	// Alice is a member of group 1 only
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob is a member of group 2 only
	if err := g.AddTuple(ctx, "group", group2, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of BOTH groups are viewers of document 100
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group2, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view (via group 1)
	ok, err := g.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group 1")
	}

	// Bob can view (via group 2)
	ok, err = g.Check("user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be viewer via group 2")
	}

	// Charlie cannot view
	ok, err = g.Check("user", charlie, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		alice   = 1
		bob     = 2
		charlie = 3
		group1  = 10
		doc100  = 100
	)

	// Alice is a member of group 1
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of group 1 are viewers (userset subject)
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Bob is a direct viewer
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view (via group membership)
	ok, err := g.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer via group")
	}

	// Bob can view (direct)
	ok, err = g.Check("user", bob, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected bob to be direct viewer")
	}

	// Charlie cannot view
	ok, err = g.Check("user", charlie, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		alice  = 1
		group1 = 10
		doc100 = 100
	)

	// Alice is a member of group 1
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Members of group 1 are viewers
	if err := g.AddTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple failed: %v", err)
	}

	// Alice can view
	ok, err := g.Check("user", alice, "document", doc100, "viewer")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer")
	}

	// Remove the userset tuple
	if err := g.RemoveTuple(ctx, "document", doc100, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("RemoveTuple failed: %v", err)
	}

	// Alice can no longer view
	ok, err = g.Check("user", alice, "document", doc100, "viewer")
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
	g := graph.New(s)

	const (
		alice    = 1
		bob      = 2
		group1   = 10
		folder10 = 100
		doc200   = 200
	)

	// Alice is a member of group 1
	if err := g.AddTuple(ctx, "group", group1, "member", "user", alice, ""); err != nil {
		t.Fatalf("AddTuple (alice in group) failed: %v", err)
	}

	// Members of group 1 are viewers of folder 10 (userset subject on folder)
	if err := g.AddTuple(ctx, "folder", folder10, "viewer", "group", group1, "member"); err != nil {
		t.Fatalf("AddTuple (group viewers of folder) failed: %v", err)
	}

	// Document 200's parent is folder 10
	if err := g.AddTuple(ctx, "document", doc200, "parent", "folder", folder10, ""); err != nil {
		t.Fatalf("AddTuple (doc parent) failed: %v", err)
	}

	// Alice should be a viewer of doc200 via:
	// 1. doc200 → parent → folder10
	// 2. folder10#viewer includes group1#member
	// 3. alice is in group1#member
	ok, err := g.Check("user", alice, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (alice) failed: %v", err)
	}
	if !ok {
		t.Error("expected alice to be viewer of doc200 via parent folder's group membership")
	}

	// Bob should NOT be a viewer (not in group 1)
	ok, err = g.Check("user", bob, "document", doc200, "viewer")
	if err != nil {
		t.Fatalf("Check (bob) failed: %v", err)
	}
	if ok {
		t.Error("expected bob to NOT be viewer of doc200")
	}

	// Now add bob to the group
	if err := g.AddTuple(ctx, "group", group1, "member", "user", bob, ""); err != nil {
		t.Fatalf("AddTuple (bob in group) failed: %v", err)
	}

	// Bob should now be a viewer
	ok, err = g.Check("user", bob, "document", doc200, "viewer")
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
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"serviceAccount": {
				Name:      "serviceAccount",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"resource": {
				Name: "resource",
				Relations: map[schema.RelationName]*schema.Relation{
					"viewer": {
						Name: "viewer",
						TargetTypes: []schema.SubjectRef{
							schema.Ref("user"),           // @user:1
							schema.Ref("serviceAccount"), // @serviceAccount:1
						},
						Usersets: []schema.Userset{
							schema.Direct(),
						},
					},
				},
			},
		},
	}

	g := graph.New(s)

	const (
		resource1 = 100
		id1       = 1 // Same ID for both user and serviceAccount
	)

	// Add serviceAccount:1 as a viewer (direct subject)
	if err := g.AddTuple(ctx, "resource", resource1, "viewer", "serviceAccount", id1, ""); err != nil {
		t.Fatalf("AddTuple (serviceAccount) failed: %v", err)
	}

	// serviceAccount:1 should be a viewer
	ok, err := g.Check("serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount) failed: %v", err)
	}
	if !ok {
		t.Error("expected serviceAccount:1 to be viewer")
	}

	// user:1 should NOT be a viewer (different type, same ID)
	ok, err = g.Check("user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user) failed: %v", err)
	}
	if ok {
		t.Error("expected user:1 to NOT be viewer (only serviceAccount:1 is)")
	}

	// Now add user:1 as well
	if err := g.AddTuple(ctx, "resource", resource1, "viewer", "user", id1, ""); err != nil {
		t.Fatalf("AddTuple (user) failed: %v", err)
	}

	// Both should now be viewers
	ok, err = g.Check("user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user after add) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to be viewer after adding")
	}

	ok, err = g.Check("serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount after user add) failed: %v", err)
	}
	if !ok {
		t.Error("expected serviceAccount:1 to still be viewer")
	}

	// Remove serviceAccount:1 - user:1 should still be a viewer
	if err := g.RemoveTuple(ctx, "resource", resource1, "viewer", "serviceAccount", id1, ""); err != nil {
		t.Fatalf("RemoveTuple (serviceAccount) failed: %v", err)
	}

	ok, err = g.Check("user", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (user after serviceAccount removal) failed: %v", err)
	}
	if !ok {
		t.Error("expected user:1 to still be viewer after removing serviceAccount:1")
	}

	ok, err = g.Check("serviceAccount", id1, "resource", resource1, "viewer")
	if err != nil {
		t.Fatalf("Check (serviceAccount after removal) failed: %v", err)
	}
	if ok {
		t.Error("expected serviceAccount:1 to NOT be viewer after removal")
	}
}
