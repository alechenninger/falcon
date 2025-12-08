package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alechenninger/falcon/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// collectTuples is a test helper that collects all tuples from an iterator.
func collectTuples(t *testing.T, iter store.TupleIterator) []store.Tuple {
	t.Helper()
	defer iter.Close()

	var tuples []store.Tuple
	for iter.Next() {
		tuples = append(tuples, iter.Tuple())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return tuples
}

func init() {
	// Disable Ryuk for podman compatibility
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

// setupPostgres creates a Postgres container and returns a connected PostgresStore.
// The container is automatically cleaned up when the test ends.
func setupPostgres(t *testing.T) *store.PostgresStore {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("falcon_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	s, err := store.NewPostgresStore(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create postgres store: %v", err)
	}

	t.Cleanup(func() {
		s.Close()
	})

	// Create the schema
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	return s
}

func TestPostgresStore_WriteTuple(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	tuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   1,
	}

	// Write tuple
	if err := s.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Verify it was written by loading all
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(tuples))
	}
	if tuples[0] != tuple {
		t.Errorf("got tuple %+v, want %+v", tuples[0], tuple)
	}
}

func TestPostgresStore_WriteTuple_Idempotent(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	tuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   1,
	}

	// Write same tuple twice - should not error
	if err := s.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("first WriteTuple failed: %v", err)
	}
	if err := s.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("second WriteTuple failed: %v", err)
	}

	// Should still only have one tuple
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple (idempotent), got %d", len(tuples))
	}
}

func TestPostgresStore_DeleteTuple(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	tuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   1,
	}

	// Write tuple
	if err := s.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("WriteTuple failed: %v", err)
	}

	// Delete tuple
	if err := s.DeleteTuple(ctx, tuple); err != nil {
		t.Fatalf("DeleteTuple failed: %v", err)
	}

	// Verify it was deleted
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 0 {
		t.Fatalf("expected 0 tuples after delete, got %d", len(tuples))
	}
}

func TestPostgresStore_DeleteTuple_NotExists(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	tuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   1,
	}

	// Delete non-existent tuple - should not error
	if err := s.DeleteTuple(ctx, tuple); err != nil {
		t.Fatalf("DeleteTuple on non-existent should not error: %v", err)
	}
}

func TestPostgresStore_LoadAll_Empty(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 0 {
		t.Fatalf("expected 0 tuples, got %d", len(tuples))
	}
}

func TestPostgresStore_LoadAll_Multiple(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	tuples := []store.Tuple{
		{ObjectType: "document", ObjectID: 100, Relation: "viewer", SubjectType: "user", SubjectID: 1},
		{ObjectType: "document", ObjectID: 100, Relation: "viewer", SubjectType: "user", SubjectID: 2},
		{ObjectType: "document", ObjectID: 100, Relation: "editor", SubjectType: "user", SubjectID: 3},
		{ObjectType: "folder", ObjectID: 10, Relation: "viewer", SubjectType: "user", SubjectID: 1},
	}

	for _, tuple := range tuples {
		if err := s.WriteTuple(ctx, tuple); err != nil {
			t.Fatalf("WriteTuple failed: %v", err)
		}
	}

	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	loaded := collectTuples(t, iter)

	if len(loaded) != len(tuples) {
		t.Fatalf("expected %d tuples, got %d", len(tuples), len(loaded))
	}

	// Convert to map for easier comparison (order not guaranteed)
	loadedMap := make(map[store.Tuple]bool)
	for _, tuple := range loaded {
		loadedMap[tuple] = true
	}

	for _, tuple := range tuples {
		if !loadedMap[tuple] {
			t.Errorf("tuple %+v not found in loaded tuples", tuple)
		}
	}
}

func TestPostgresStore_MaxUint32(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	// Test that we can store max uint32 values (OID should handle this)
	tuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    4294967295, // max uint32
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   4294967295, // max uint32
	}

	if err := s.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("WriteTuple with max uint32 failed: %v", err)
	}

	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(tuples))
	}
	if tuples[0] != tuple {
		t.Errorf("got tuple %+v, want %+v", tuples[0], tuple)
	}
}

// TestPostgresStore_DifferentSubjectTypes verifies that tuples with the same
// subject_id but different subject_types are stored separately.
func TestPostgresStore_DifferentSubjectTypes(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	// Same object, same relation, same subject_id, but different subject_types
	userTuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   1,
	}
	groupTuple := store.Tuple{
		ObjectType:  "document",
		ObjectID:    100,
		Relation:    "viewer",
		SubjectType: "group",
		SubjectID:   1, // Same ID as user!
	}

	// Write both tuples
	if err := s.WriteTuple(ctx, userTuple); err != nil {
		t.Fatalf("WriteTuple (user) failed: %v", err)
	}
	if err := s.WriteTuple(ctx, groupTuple); err != nil {
		t.Fatalf("WriteTuple (group) failed: %v", err)
	}

	// Should have 2 distinct tuples
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples (different subject types), got %d", len(tuples))
	}

	// Delete only the group tuple
	if err := s.DeleteTuple(ctx, groupTuple); err != nil {
		t.Fatalf("DeleteTuple (group) failed: %v", err)
	}

	// User tuple should still exist
	iter, err = s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples = collectTuples(t, iter)

	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple after deleting group, got %d", len(tuples))
	}
	if tuples[0] != userTuple {
		t.Errorf("remaining tuple should be user tuple, got %+v", tuples[0])
	}
}

// TestPostgresStore_UsersetSubject verifies that userset subject tuples
// (with subject_relation set) are stored correctly.
func TestPostgresStore_UsersetSubject(t *testing.T) {
	s := setupPostgres(t)
	ctx := context.Background()

	// Direct subject: document:100#viewer@user:1
	directTuple := store.Tuple{
		ObjectType:      "document",
		ObjectID:        100,
		Relation:        "viewer",
		SubjectType:     "user",
		SubjectID:       1,
		SubjectRelation: "", // empty = direct subject
	}

	// Userset subject: document:100#viewer@group:1#member
	usersetTuple := store.Tuple{
		ObjectType:      "document",
		ObjectID:        100,
		Relation:        "viewer",
		SubjectType:     "group",
		SubjectID:       1,
		SubjectRelation: "member", // non-empty = userset subject
	}

	// Write both tuples
	if err := s.WriteTuple(ctx, directTuple); err != nil {
		t.Fatalf("WriteTuple (direct) failed: %v", err)
	}
	if err := s.WriteTuple(ctx, usersetTuple); err != nil {
		t.Fatalf("WriteTuple (userset) failed: %v", err)
	}

	// Should have 2 distinct tuples
	iter, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples := collectTuples(t, iter)

	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples, got %d", len(tuples))
	}

	// Verify both tuples exist
	loadedMap := make(map[store.Tuple]bool)
	for _, tuple := range tuples {
		loadedMap[tuple] = true
	}

	if !loadedMap[directTuple] {
		t.Error("direct tuple not found")
	}
	if !loadedMap[usersetTuple] {
		t.Error("userset tuple not found")
	}

	// Delete only the userset tuple
	if err := s.DeleteTuple(ctx, usersetTuple); err != nil {
		t.Fatalf("DeleteTuple (userset) failed: %v", err)
	}

	// Direct tuple should still exist
	iter, err = s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	tuples = collectTuples(t, iter)

	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple after deleting userset, got %d", len(tuples))
	}
	if tuples[0] != directTuple {
		t.Errorf("remaining tuple should be direct tuple, got %+v", tuples[0])
	}
}
