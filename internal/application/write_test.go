package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alechenninger/falcon/internal/application"
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/internal/infrastructure/memory"
)

// Test schema with document, folder, user, and group types.
func testSchema() *domain.Schema {
	s := &domain.Schema{
		Types: map[domain.TypeName]*domain.ObjectType{
			"document": {
				ID:   1,
				Name: "document",
				Relations: map[domain.RelationName]*domain.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("user"), domain.RefWithRelation("group", "member")),
						},
					},
					"editor": {
						ID:   2,
						Name: "editor",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("user")),
						},
					},
					"parent": {
						ID:   3,
						Name: "parent",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("folder")),
						},
					},
				},
			},
			"folder": {
				ID:   2,
				Name: "folder",
				Relations: map[domain.RelationName]*domain.Relation{
					"viewer": {
						ID:   1,
						Name: "viewer",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("user"), domain.RefWithRelation("group", "member")),
						},
					},
				},
			},
			"user": {
				ID:   3,
				Name: "user",
				Relations: map[domain.RelationName]*domain.Relation{
					// Users typically don't have relations, but we need at least one
					// for schema validation in some cases.
				},
			},
			"group": {
				ID:   4,
				Name: "group",
				Relations: map[domain.RelationName]*domain.Relation{
					"member": {
						ID:   1,
						Name: "member",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("user")),
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

func newWriteService(t *testing.T) (*application.WriteService, *memory.Store) {
	t.Helper()
	store := memory.NewStore()
	schema := testSchema()
	svc := application.NewWriteService(application.WriteServiceConfig{
		Store:  store,
		Schema: schema,
	})
	return svc, store
}

// tuple is a helper to create an ExternalTupleRef.
func tuple(objectType, objectID, relation, subjectType, subjectID string) application.ExternalTupleRef {
	return application.ExternalTupleRef{
		ObjectType:  domain.TypeName(objectType),
		ObjectID:    domain.ExternalID(objectID),
		Relation:    domain.RelationName(relation),
		SubjectType: domain.TypeName(subjectType),
		SubjectID:   domain.ExternalID(subjectID),
	}
}

// tupleWithSubjectRelation is a helper to create an ExternalTupleRef with a subject relation.
func tupleWithSubjectRelation(objectType, objectID, relation, subjectType, subjectID, subjectRelation string) application.ExternalTupleRef {
	return application.ExternalTupleRef{
		ObjectType:      domain.TypeName(objectType),
		ObjectID:        domain.ExternalID(objectID),
		Relation:        domain.RelationName(relation),
		SubjectType:     domain.TypeName(subjectType),
		SubjectID:       domain.ExternalID(subjectID),
		SubjectRelation: domain.RelationName(subjectRelation),
	}
}

func insert(t application.ExternalTupleRef) application.Mutation {
	return application.Mutation{Op: application.Insert, Tuple: t}
}

func delete(t application.ExternalTupleRef) application.Mutation {
	return application.Mutation{Op: application.Delete, Tuple: t}
}

func TestWrite_EmptyMutations(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{},
	})
	if err != nil {
		t.Errorf("expected no error for empty mutations, got: %v", err)
	}
}

func TestWrite_SingleInsert(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWrite_MultipleInserts(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
			insert(tuple("document", "doc-1", "editor", "user", "bob")),
			insert(tuple("document", "doc-2", "viewer", "user", "charlie")),
		},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWrite_InsertWithUsersetSubject(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tupleWithSubjectRelation("document", "doc-1", "viewer", "group", "engineering", "member")),
		},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWrite_Delete(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// First insert
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Then delete
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			delete(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestWrite_DeleteNonExistent(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Delete something that was never inserted should not error
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			delete(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Errorf("Delete of non-existent tuple should not error, got: %v", err)
	}
}

func TestWrite_Idempotent(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	tup := tuple("document", "doc-1", "viewer", "user", "alice")

	// Insert twice
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{insert(tup)},
	})
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{insert(tup)},
	})
	if err != nil {
		t.Fatalf("Second insert (idempotent) failed: %v", err)
	}
}

func TestWrite_UnknownObjectType(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("unknown_type", "obj-1", "viewer", "user", "alice")),
		},
	})
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestWrite_UnknownRelation(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "unknown_relation", "user", "alice")),
		},
	})
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestWrite_UnknownSubjectType(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "unknown_type", "alice")),
		},
	})
	if err == nil {
		t.Error("expected error for unknown subject type")
	}
}

func TestWrite_SubjectTypeNotAllowed(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// document#viewer only allows user and group#member, not folder
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "folder", "folder-1")),
		},
	})
	if err == nil {
		t.Error("expected error for disallowed subject type")
	}
}

// --- Precondition Tests ---

func TestWrite_Precondition_Satisfied(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// First write a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Now write with a precondition that should be satisfied
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "alice")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-1"},
		),
	})
	if err != nil {
		t.Errorf("Write with satisfied precondition should succeed, got: %v", err)
	}
}

func TestWrite_Precondition_NotSatisfied(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write with a precondition that cannot be satisfied (no tuples exist)
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-1"},
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed, got: %v", err)
	}
}

func TestWrite_Precondition_NotSatisfied_DifferentObject(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// First write a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Now write with a precondition for a different object
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-2", "viewer", "user", "bob")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-999"},
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed for non-existent object, got: %v", err)
	}
}

func TestWrite_Precondition_Not_MatchesDifferentTuples(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// First write a tuple for doc-1
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// PNot semantics: "there exists a tuple NOT matching the predicate"
	// Since doc-1 exists and we're checking NOT(objectID = doc-999),
	// this should succeed because doc-1 does NOT match doc-999.
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-2", "viewer", "user", "bob")),
		},
		Precondition: application.PNot(
			application.PEq(
				application.RefObjectID,
				application.ObjectIDRef{Type: "document", ID: "doc-999"},
			),
		),
	})
	if err != nil {
		t.Errorf("Write with NOT(non-matching) precondition should succeed, got: %v", err)
	}
}

func TestWrite_Precondition_Not_NoMatchingTuplesOnEmptyStore(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// PNot semantics: "there exists a tuple NOT matching the predicate"
	// On an empty store, there are no tuples to check, so Contains returns false.
	// This means PNot cannot be used for "require no tuple exists" semantics.
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
		Precondition: application.PNot(
			application.PEq(
				application.RefObjectID,
				application.ObjectIDRef{Type: "document", ID: "doc-1"},
			),
		),
	})
	// This fails because there are no tuples, so "exists tuple NOT matching X" is false
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed on empty store with NOT, got: %v", err)
	}
}

func TestWrite_Precondition_RequireNotExists_NotSatisfied(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// First write a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Now try to write with precondition that it should NOT exist
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "bob")),
		},
		Precondition: application.PNot(
			application.PEq(
				application.RefObjectID,
				application.ObjectIDRef{Type: "document", ID: "doc-1"},
			),
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed when object exists but precondition requires it not to, got: %v", err)
	}
}

func TestWrite_Precondition_And(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Precondition: object is doc-1 AND subject is alice
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "alice")),
		},
		Precondition: application.PAnd(
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-1"}),
			application.PEq(application.RefSubjectID, application.ObjectIDRef{Type: "user", ID: "alice"}),
		),
	})
	if err != nil {
		t.Errorf("Write with satisfied AND precondition should succeed, got: %v", err)
	}

	// Precondition: object is doc-1 AND subject is bob (bob not present)
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "bob")),
		},
		Precondition: application.PAnd(
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-1"}),
			application.PEq(application.RefSubjectID, application.ObjectIDRef{Type: "user", ID: "bob"}),
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed for unsatisfied AND, got: %v", err)
	}
}

func TestWrite_Precondition_Or(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Precondition: object is doc-1 OR object is doc-999 (doc-1 exists)
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-2", "viewer", "user", "bob")),
		},
		Precondition: application.POr(
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-1"}),
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-999"}),
		),
	})
	if err != nil {
		t.Errorf("Write with satisfied OR precondition should succeed, got: %v", err)
	}

	// Precondition: object is doc-888 OR object is doc-999 (neither exists)
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-3", "viewer", "user", "charlie")),
		},
		Precondition: application.POr(
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-888"}),
			application.PEq(application.RefObjectID, application.ObjectIDRef{Type: "document", ID: "doc-999"}),
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed for unsatisfied OR, got: %v", err)
	}
}

func TestWrite_Precondition_ByType(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write a document tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Precondition: any tuple with object type 'document' exists
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("folder", "folder-1", "viewer", "user", "bob")),
		},
		Precondition: application.PEq(application.RefObjectType, domain.TypeName("document")),
	})
	if err != nil {
		t.Errorf("Write with type-based precondition should succeed, got: %v", err)
	}
}

func TestWrite_Precondition_ByRelation(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write an editor tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Precondition: a tuple with document#editor relation exists
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "bob")),
		},
		Precondition: application.PEq(
			application.RefRelation,
			application.RelationRef{Type: "document", Relation: "editor"},
		),
	})
	if err != nil {
		t.Errorf("Write with relation-based precondition should succeed, got: %v", err)
	}
}

func TestWrite_Precondition_AfterDelete(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Write then delete a tuple
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			delete(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Precondition should now fail since tuple was deleted
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "editor", "user", "bob")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-1"},
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed after delete, got: %v", err)
	}
}

func TestWrite_MutationsNotAppliedOnPreconditionFailure(t *testing.T) {
	svc, _ := newWriteService(t)
	ctx := context.Background()

	// Try to write with a failing precondition
	_, err := svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-does-not-exist"},
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
	}

	// Verify the tuple was not written by trying to use it as a precondition
	_, err = svc.Write(ctx, application.WriteCommand{
		Mutations: []application.Mutation{
			insert(tuple("document", "doc-2", "viewer", "user", "bob")),
		},
		Precondition: application.PEq(
			application.RefObjectID,
			application.ObjectIDRef{Type: "document", ID: "doc-1"},
		),
	})
	if !errors.Is(err, application.ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed (doc-1 should not exist), got: %v", err)
	}
}
