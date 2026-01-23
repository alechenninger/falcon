package domain

import (
	"context"
	"testing"
)

// Test type IDs used throughout tests
const (
	testDocType    TypeID = 1
	testUserType   TypeID = 2
	testGroupType  TypeID = 3
	testFolderType TypeID = 4
)

// objRef creates an ObjectRef with the given type and external ID.
func objRef(typeID TypeID, extID string) ObjectRef {
	return ObjectRef{Type: typeID, ID: ExternalID(extID)}
}

// StoreFactory returns a store ready for testing.
// The factory is called before each test. For expensive stores (like postgres),
// the caller should set up the store once before calling RunStoreTests,
// and the factory should just reset state and return the shared instance.
type StoreFactory func(t *testing.T) Store

// RunStoreTests runs the full store test suite against any Store implementation.
func RunStoreTests(t *testing.T, factory StoreFactory) {
	// ID Provisioning
	t.Run("ProvisionID", func(t *testing.T) { testProvisionID(t, factory) })
	t.Run("ProvisionID_Idempotent", func(t *testing.T) { testProvisionIDIdempotent(t, factory) })
	t.Run("ProvisionID_PersistsAfterCommit", func(t *testing.T) { testProvisionIDPersistsAfterCommit(t, factory) })
	t.Run("ProvisionID_RollbackDiscards", func(t *testing.T) { testProvisionIDRollbackDiscards(t, factory) })
	t.Run("GetID", func(t *testing.T) { testGetID(t, factory) })
	t.Run("GetID_NotFound", func(t *testing.T) { testGetIDNotFound(t, factory) })

	// Write Operations
	t.Run("Write_CommitPersists", func(t *testing.T) { testWriteCommitPersists(t, factory) })
	t.Run("Write_RollbackDiscards", func(t *testing.T) { testWriteRollbackDiscards(t, factory) })
	t.Run("Write_Idempotent", func(t *testing.T) { testWriteIdempotent(t, factory) })
	t.Run("Write_Delete", func(t *testing.T) { testWriteDelete(t, factory) })
	t.Run("Write_DeleteNonExistent", func(t *testing.T) { testWriteDeleteNonExistent(t, factory) })
	t.Run("Write_EmptyCommit", func(t *testing.T) { testWriteEmptyCommit(t, factory) })
	t.Run("Write_BatchAtomic", func(t *testing.T) { testWriteBatchAtomic(t, factory) })

	// LoadAll
	t.Run("LoadAll_Empty", func(t *testing.T) { testLoadAllEmpty(t, factory) })
	t.Run("LoadAll_Multiple", func(t *testing.T) { testLoadAllMultiple(t, factory) })

	// Contains - Basic
	t.Run("Contains_EmptyStore", func(t *testing.T) { testContainsEmptyStore(t, factory) })
	t.Run("Contains_Eq", func(t *testing.T) { testContainsEq(t, factory) })
	t.Run("Contains_Neq", func(t *testing.T) { testContainsNeq(t, factory) })

	// Contains - Logical Operators
	t.Run("Contains_And", func(t *testing.T) { testContainsAnd(t, factory) })
	t.Run("Contains_Or", func(t *testing.T) { testContainsOr(t, factory) })
	t.Run("Contains_Not", func(t *testing.T) { testContainsNot(t, factory) })
	t.Run("Contains_EmptyAnd", func(t *testing.T) { testContainsEmptyAnd(t, factory) })
	t.Run("Contains_EmptyOr", func(t *testing.T) { testContainsEmptyOr(t, factory) })

	// Contains - Comparison Operators
	t.Run("Contains_Comparison", func(t *testing.T) { testContainsComparison(t, factory) })
	t.Run("Contains_StartsWith", func(t *testing.T) { testContainsStartsWith(t, factory) })

	// Contains - Complex
	t.Run("Contains_Complex", func(t *testing.T) { testContainsComplex(t, factory) })

	// Tuple Key Semantics
	t.Run("Write_LargeIDs", func(t *testing.T) { testWriteLargeIDs(t, factory) })
	t.Run("Write_DifferentSubjectTypes", func(t *testing.T) { testWriteDifferentSubjectTypes(t, factory) })
	t.Run("Write_UsersetSubject", func(t *testing.T) { testWriteUsersetSubject(t, factory) })
}

// --- ID Provisioning Tests ---

func testProvisionID(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	id1, err := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-abc"), NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}
	if id1 == 0 {
		t.Error("expected non-zero ID")
	}

	id2, err := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-def"), NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}
	if id2 == 0 {
		t.Error("expected non-zero ID")
	}
	if id1 == id2 {
		t.Error("expected different IDs for different external IDs")
	}
}

func testProvisionIDIdempotent(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	ref := objRef(testDocType, "doc-abc")
	id1, err := tx.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}

	id2, err := tx.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID (again) failed: %v", err)
	}
	if id1 != id2 {
		t.Errorf("expected same ID for same external ID, got %d and %d", id1, id2)
	}
}

func testProvisionIDPersistsAfterCommit(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	ref := objRef(testDocType, "doc-abc")
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	id1, err := tx.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx2, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx2.Rollback(ctx)

	id2, err := tx2.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID (after commit) failed: %v", err)
	}
	if id1 != id2 {
		t.Errorf("expected same ID after commit, got %d and %d", id1, id2)
	}
}

func testProvisionIDRollbackDiscards(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	ref := objRef(testDocType, "rollback-id")
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	_, err = tx.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.GetID(ctx, ref)
	if err != ErrIDNotFound {
		t.Errorf("expected ErrIDNotFound after rollback, got: %v", err)
	}
}

func testGetID(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	ref := objRef(testUserType, "user-123")
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	provisionedID, err := tx.GetOrProvisionID(ctx, ref, NoObject)
	if err != nil {
		t.Fatalf("ProvisionID failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	gotID, err := tx.GetID(ctx, ref)
	if err != nil {
		t.Fatalf("GetID failed: %v", err)
	}
	if gotID != provisionedID {
		t.Errorf("GetID returned %d, want %d", gotID, provisionedID)
	}
}

func testGetIDNotFound(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.GetID(ctx, objRef(testUserType, "nonexistent"))
	if err != ErrIDNotFound {
		t.Errorf("expected ErrIDNotFound, got: %v", err)
	}
}

// --- Write Operation Tests ---

func testWriteCommitPersists(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 1 {
		t.Errorf("expected 1 tuple, got %d", len(tuples))
	}
}

func testWriteRollbackDiscards(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 0 {
		t.Errorf("expected 0 tuples after rollback, got %d", len(tuples))
	}
}

func testWriteIdempotent(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	writeTuple(t, s, ctx, tuple)
	writeTuple(t, s, ctx, tuple)

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 1 {
		t.Errorf("expected 1 tuple (idempotent), got %d", len(tuples))
	}
}

func testWriteDelete(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	writeTuple(t, s, ctx, tuple)
	deleteTuple(t, s, ctx, tuple)

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 0 {
		t.Errorf("expected 0 tuples after delete, got %d", len(tuples))
	}
}

func testWriteDeleteNonExistent(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	deleteTuple(t, s, ctx, tuple) // Should not error
}

func testWriteEmptyCommit(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit (empty) failed: %v", err)
	}
}

func testWriteBatchAtomic(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	obj1, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	obj2, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-2"), NoObject)
	obj3, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-3"), NoObject)
	subj, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)

	mutations := []Mutation{
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj1, Relation: 1, SubjectType: testUserType, SubjectID: subj}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj2, Relation: 1, SubjectType: testUserType, SubjectID: subj}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj3, Relation: 1, SubjectType: testUserType, SubjectID: subj}},
	}
	if err := tx.Write(ctx, mutations); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 3 {
		t.Errorf("expected 3 tuples, got %d", len(tuples))
	}
}

// --- LoadAll Tests ---

func testLoadAllEmpty(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 0 {
		t.Errorf("expected 0 tuples, got %d", len(tuples))
	}
}

func testLoadAllMultiple(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	obj1, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	obj2, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-2"), NoObject)
	subj1, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	subj2, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-2"), NoObject)

	mutations := []Mutation{
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj1, Relation: 1, SubjectType: testUserType, SubjectID: subj1}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj1, Relation: 1, SubjectType: testUserType, SubjectID: subj2}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: obj2, Relation: 2, SubjectType: testUserType, SubjectID: subj1}},
	}
	if err := tx.Write(ctx, mutations); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 3 {
		t.Errorf("expected 3 tuples, got %d", len(tuples))
	}
}

// --- Contains Tests ---

func testContainsEmptyStore(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	contains, err := tx.Contains(ctx, Eq(FieldObjectID, objRef(testDocType, "anything")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected Contains to return false on empty store")
	}
}

func testContainsEq(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	contains, err := tx.Contains(ctx, Eq(FieldObjectID, objRef(testDocType, "doc-100")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected Contains to return true")
	}

	contains, err = tx.Contains(ctx, Eq(FieldObjectType, testDocType))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected Contains to return true for type match")
	}

	contains, err = tx.Contains(ctx, Eq(FieldObjectID, objRef(testDocType, "doc-999")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected Contains to return false for non-existent")
	}
}

func testContainsNeq(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	contains, err := tx.Contains(ctx, Neq(FieldObjectID, objRef(testDocType, "doc-999")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected Neq to match")
	}

	contains, err = tx.Contains(ctx, Neq(FieldObjectID, objRef(testDocType, "doc-100")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected Neq not to match actual value")
	}
}

func testContainsAnd(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	pred := And(Eq(FieldObjectID, objRef(testDocType, "doc-100")), Eq(FieldSubjectID, objRef(testUserType, "user-1")))
	contains, err := tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected And to match when both conditions match")
	}

	pred = And(Eq(FieldObjectID, objRef(testDocType, "doc-100")), Eq(FieldSubjectID, objRef(testUserType, "user-999")))
	contains, err = tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected And not to match when one condition fails")
	}
}

func testContainsOr(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	pred := Or(Eq(FieldObjectID, objRef(testDocType, "doc-999")), Eq(FieldObjectID, objRef(testDocType, "doc-100")))
	contains, err := tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected Or to match when one condition matches")
	}

	pred = Or(Eq(FieldObjectID, objRef(testDocType, "doc-888")), Eq(FieldObjectID, objRef(testDocType, "doc-999")))
	contains, err = tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected Or not to match when no conditions match")
	}
}

func testContainsNot(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	pred := Not(Eq(FieldObjectID, objRef(testDocType, "doc-999")))
	contains, err := tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected Not(non-match) to match")
	}

	pred = Not(Eq(FieldObjectID, objRef(testDocType, "doc-100")))
	contains, err = tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected Not(match) not to match")
	}
}

func testContainsEmptyAnd(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	contains, err := tx.Contains(ctx, And())
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected empty And to match (TRUE)")
	}
}

func testContainsEmptyOr(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-1"), NoObject)
	subjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	tuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: subjID}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	contains, err := tx.Contains(ctx, Or())
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected empty Or not to match (FALSE)")
	}
}

func testContainsComparison(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	doc10, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-010"), NoObject)
	doc20, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-020"), NoObject)
	doc30, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-030"), NoObject)
	user, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)

	mutations := []Mutation{
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc10, Relation: 1, SubjectType: testUserType, SubjectID: user}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc20, Relation: 1, SubjectType: testUserType, SubjectID: user}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc30, Relation: 1, SubjectType: testUserType, SubjectID: user}},
	}
	if err := tx.Write(ctx, mutations); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	tests := []struct {
		name   string
		pred   TuplePredicate
		expect bool
	}{
		{"Lt doc-015", Lt(FieldObjectID, objRef(testDocType, "doc-015")), true},
		{"Lt doc-010", Lt(FieldObjectID, objRef(testDocType, "doc-010")), false},
		{"Lte doc-010", Lte(FieldObjectID, objRef(testDocType, "doc-010")), true},
		{"Gt doc-025", Gt(FieldObjectID, objRef(testDocType, "doc-025")), true},
		{"Gt doc-030", Gt(FieldObjectID, objRef(testDocType, "doc-030")), false},
		{"Gte doc-030", Gte(FieldObjectID, objRef(testDocType, "doc-030")), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contains, err := tx.Contains(ctx, tt.pred)
			if err != nil {
				t.Fatalf("Contains failed: %v", err)
			}
			if contains != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, contains)
			}
		})
	}
}

func testContainsStartsWith(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	doc1, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-abc-123"), NoObject)
	doc2, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-abc-456"), NoObject)
	doc3, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-xyz-789"), NoObject)
	user, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)

	mutations := []Mutation{
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc1, Relation: 1, SubjectType: testUserType, SubjectID: user}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc2, Relation: 1, SubjectType: testUserType, SubjectID: user}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc3, Relation: 1, SubjectType: testUserType, SubjectID: user}},
	}
	if err := tx.Write(ctx, mutations); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	// StartsWith with ObjectRef - type must match, ID is prefix
	contains, err := tx.Contains(ctx, StartsWith(FieldObjectID, objRef(testDocType, "doc-abc")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected StartsWith to match")
	}

	contains, err = tx.Contains(ctx, StartsWith(FieldObjectID, objRef(testDocType, "doc-zzz")))
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected StartsWith not to match")
	}
}

func testContainsComplex(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	doc100, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	doc200, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-200"), NoObject)
	folder300, _ := tx.GetOrProvisionID(ctx, objRef(testFolderType, "folder-300"), NoObject)
	user1, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	user2, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-2"), NoObject)
	group3, _ := tx.GetOrProvisionID(ctx, objRef(testGroupType, "group-3"), NoObject)

	mutations := []Mutation{
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc100, Relation: 1, SubjectType: testUserType, SubjectID: user1}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testDocType, ObjectID: doc200, Relation: 2, SubjectType: testUserType, SubjectID: user2}},
		{Op: OpInsert, Tuple: Tuple{ObjectType: testFolderType, ObjectID: folder300, Relation: 1, SubjectType: testGroupType, SubjectID: group3}},
	}
	if err := tx.Write(ctx, mutations); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx, err = s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	// Complex: (type=doc AND relation=1) OR (type=folder AND subjectType=group)
	pred := Or(
		And(Eq(FieldObjectType, testDocType), Eq(FieldRelation, RelationID(1))),
		And(Eq(FieldObjectType, testFolderType), Eq(FieldSubjectType, testGroupType)),
	)
	contains, err := tx.Contains(ctx, pred)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected complex predicate to match")
	}

	// Complex with Not: type=doc AND NOT relation=1
	pred2 := And(Eq(FieldObjectType, testDocType), Not(Eq(FieldRelation, RelationID(1))))
	contains, err = tx.Contains(ctx, pred2)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !contains {
		t.Error("expected 'type=doc AND NOT relation=1' to match (doc:200#editor)")
	}

	// Should not match: type=doc AND relation=2 AND subjectID=user-1
	pred3 := And(Eq(FieldObjectType, testDocType), Eq(FieldRelation, RelationID(2)), Eq(FieldSubjectID, objRef(testUserType, "user-1")))
	contains, err = tx.Contains(ctx, pred3)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if contains {
		t.Error("expected complex predicate not to match (editor is user:2, not user:1)")
	}
}

// --- Test Helpers ---

func collectTuples(t *testing.T, s Store, ctx context.Context) []Tuple {
	t.Helper()
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	defer iter.Close()

	var tuples []Tuple
	for iter.Next() {
		tuples = append(tuples, iter.Tuple())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return tuples
}

func writeTuple(t *testing.T, s Store, ctx context.Context, tuple Tuple) {
	t.Helper()
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func deleteTuple(t *testing.T, s Store, ctx context.Context, tuple Tuple) {
	t.Helper()
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Write(ctx, []Mutation{{Op: OpDelete, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

// --- Tuple Key Semantics Tests ---

func testWriteLargeIDs(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	// Test that large 64-bit ID values work
	tuple := Tuple{
		ObjectType:  1,
		ObjectID:    1<<62 + 12345,
		Relation:    1,
		SubjectType: 2,
		SubjectID:   1<<62 + 67890,
	}

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := tx.Write(ctx, []Mutation{{Op: OpInsert, Tuple: tuple}}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(tuples))
	}
	if tuples[0] != tuple {
		t.Errorf("got tuple %+v, want %+v", tuples[0], tuple)
	}
}

func testWriteDifferentSubjectTypes(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	// Same object, relation, and subject external_id but different subject_types
	// should be stored as distinct tuples (different internal IDs)
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	objID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	userSubjID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "entity-1"), NoObject)
	groupSubjID, _ := tx.GetOrProvisionID(ctx, objRef(testGroupType, "entity-1"), NoObject)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	userTuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testUserType, SubjectID: userSubjID}
	groupTuple := Tuple{ObjectType: testDocType, ObjectID: objID, Relation: 1, SubjectType: testGroupType, SubjectID: groupSubjID}

	writeTuple(t, s, ctx, userTuple)
	writeTuple(t, s, ctx, groupTuple)

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples (different subject types), got %d", len(tuples))
	}

	// Deleting one should not affect the other
	deleteTuple(t, s, ctx, groupTuple)

	tuples = collectTuples(t, s, ctx)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple after delete, got %d", len(tuples))
	}
	if tuples[0] != userTuple {
		t.Errorf("remaining tuple should be user tuple, got %+v", tuples[0])
	}
}

func testWriteUsersetSubject(t *testing.T, factory StoreFactory) {
	s := factory(t)
	defer s.Close()
	ctx := context.Background()

	// Userset subjects (with SubjectRelation set) should be stored distinctly
	// from direct subjects (SubjectRelation = NoRelation)
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	docID, _ := tx.GetOrProvisionID(ctx, objRef(testDocType, "doc-100"), NoObject)
	userID, _ := tx.GetOrProvisionID(ctx, objRef(testUserType, "user-1"), NoObject)
	groupID, _ := tx.GetOrProvisionID(ctx, objRef(testGroupType, "group-1"), NoObject)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Direct subject: document:100#viewer@user:1
	directTuple := Tuple{
		ObjectType:      testDocType,
		ObjectID:        docID,
		Relation:        1,
		SubjectType:     testUserType,
		SubjectID:       userID,
		SubjectRelation: NoRelation,
	}

	// Userset subject: document:100#viewer@group:1#member
	usersetTuple := Tuple{
		ObjectType:      testDocType,
		ObjectID:        docID,
		Relation:        1,
		SubjectType:     testGroupType,
		SubjectID:       groupID,
		SubjectRelation: 5, // "member" relation
	}

	writeTuple(t, s, ctx, directTuple)
	writeTuple(t, s, ctx, usersetTuple)

	tuples := collectTuples(t, s, ctx)
	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples, got %d", len(tuples))
	}

	loadedMap := make(map[Tuple]bool)
	for _, tuple := range tuples {
		loadedMap[tuple] = true
	}

	if !loadedMap[directTuple] {
		t.Error("direct tuple not found")
	}
	if !loadedMap[usersetTuple] {
		t.Error("userset tuple not found")
	}

	// Deleting userset should leave direct
	deleteTuple(t, s, ctx, usersetTuple)

	tuples = collectTuples(t, s, ctx)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple after delete, got %d", len(tuples))
	}
	if tuples[0] != directTuple {
		t.Errorf("remaining tuple should be direct tuple, got %+v", tuples[0])
	}
}
