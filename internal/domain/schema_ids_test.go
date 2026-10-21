package domain_test

import (
	"testing"

	"github.com/alechenninger/falcon/internal/domain"
)

func TestCheckSchemaIDsAbove255(t *testing.T) {
	s := testSchema()
	for _, ot := range s.Types {
		ot.ID += 256
		for _, rel := range ot.Relations {
			rel.ID += 256
		}
	}
	s.Types["document"].ID = domain.MaxTypeID
	s.Types["document"].Relations["viewer"].ID = domain.MaxRelationID
	s.Compile()
	g := newTestGraph(s)
	t.Cleanup(g.Close)

	// Check direct membership, a userset subject, and a computed relation.
	for _, tuple := range []struct {
		objectType, subjectType domain.TypeName
		objectID, subjectID     domain.ID
		relation, subjectRel    domain.RelationName
	}{
		{"group", "user", 10, 1, "member", ""},
		{"document", "group", 100, 10, "editor", "member"},
		{"document", "user", 100, 2, "viewer", ""},
	} {
		if err := g.WriteTuple(ctx, tuple.objectType, tuple.objectID, tuple.relation, tuple.subjectType, tuple.subjectID, tuple.subjectRel); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		user domain.ID
		want bool
	}{{1, true}, {2, true}, {3, false}} {
		allowed, _, err := g.Check(ctx, "user", tt.user, "document", 100, "viewer")
		if err != nil || allowed != tt.want {
			t.Fatalf("Check(user:%d) = %v, %v; want %v", tt.user, allowed, err, tt.want)
		}
	}
	if err := g.DeleteTuple(ctx, "document", 100, "editor", "group", 10, "member"); err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := g.Check(ctx, "user", 1, "document", 100, "viewer"); err != nil || allowed {
		t.Fatalf("Check after delete = %v, %v; want false", allowed, err)
	}
}
