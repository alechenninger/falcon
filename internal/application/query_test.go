package application_test

import (
	"context"
	"testing"

	"github.com/alechenninger/falcon/internal/application"
	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/internal/infrastructure/memory"
)

// testServices holds the services needed for query integration tests.
type testServices struct {
	writeSvc *application.WriteService
	querySvc *application.QueryService
	observer *domain.SignalingObserver
	store    *memory.Store
	graph    *domain.LocalGraph
	cancel   context.CancelFunc
}

// newTestServices creates all the services needed for query tests.
// Call cleanup() when done to stop the graph subscription.
func newTestServices(t *testing.T) *testServices {
	t.Helper()

	store := memory.NewStore()
	schema := testSchema()

	// WriteService for writing tuples
	writeSvc := application.NewWriteService(application.WriteServiceConfig{
		Store:  store,
		Schema: schema,
	})

	// LocalGraph with SignalingObserver for waiting on replication
	observer := domain.NewSignalingObserver()
	graph := domain.NewLocalGraph(schema, store, store)
	graph.SetUsersetsObserver(observer)

	// Start the graph in background
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = graph.Start(ctx)
	}()

	// Wait for the subscription to be ready
	observer.WaitReady()

	// IdResolver backed by store
	resolver := memory.NewStoreIdResolver(store)

	// QueryService
	querySvc := application.NewQueryService(application.QueryServiceConfig{
		Schema:     schema,
		IdResolver: resolver,
		Graph:      graph,
	})

	t.Cleanup(func() {
		cancel()
	})

	return &testServices{
		writeSvc: writeSvc,
		querySvc: querySvc,
		observer: observer,
		store:    store,
		graph:    graph,
		cancel:   cancel,
	}
}

// write is a helper to write tuples and wait for replication.
func (s *testServices) write(t *testing.T, ctx context.Context, mutations ...application.Mutation) {
	t.Helper()

	_, err := s.writeSvc.Write(ctx, application.WriteCommand{Mutations: mutations})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Wait for replication
	time, err := s.store.CurrentTime(ctx)
	if err != nil {
		t.Fatalf("CurrentTime failed: %v", err)
	}
	s.observer.WaitForTime(time)
}

// check is a helper to run a check query.
func (s *testServices) check(ctx context.Context, objectType, objectID, relation, subjectType, subjectID string) (application.CheckResult, error) {
	return s.querySvc.Check(ctx, application.CheckQuery{
		ObjectType:  domain.TypeName(objectType),
		ObjectID:    domain.ExternalID(objectID),
		Relation:    domain.RelationName(relation),
		SubjectType: domain.TypeName(subjectType),
		SubjectID:   domain.ExternalID(subjectID),
	})
}

func TestCheck_AfterWrite_Allowed(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: alice is a viewer of doc-1
	svc.write(t, ctx, insert(tuple("document", "doc-1", "viewer", "user", "alice")))

	// Query: check if alice is a viewer of doc-1
	result, err := svc.check(ctx, "document", "doc-1", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !result.Allowed {
		t.Error("expected Allowed=true")
	}
}

func TestCheck_AfterWrite_Denied_DifferentRelation(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: alice is a viewer of doc-1
	svc.write(t, ctx, insert(tuple("document", "doc-1", "viewer", "user", "alice")))

	// Query: check if alice is an editor of doc-1 (should be denied)
	result, err := svc.check(ctx, "document", "doc-1", "editor", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false for different relation")
	}
}

func TestCheck_AfterWrite_Denied_DifferentSubject(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: alice is a viewer of doc-1, and also provision bob by writing a different tuple
	svc.write(t, ctx,
		insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		insert(tuple("document", "doc-2", "viewer", "user", "bob")), // provisions bob's ID
	)

	// Query: check if bob is a viewer of doc-1 (should be denied)
	result, err := svc.check(ctx, "document", "doc-1", "viewer", "user", "bob")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false for different subject")
	}
}

func TestCheck_AfterWrite_Denied_DifferentObject(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: alice is a viewer of doc-1, and also provision doc-2 by writing a different tuple
	svc.write(t, ctx,
		insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		insert(tuple("document", "doc-2", "viewer", "user", "bob")), // provisions doc-2's ID
	)

	// Query: check if alice is a viewer of doc-2 (should be denied)
	result, err := svc.check(ctx, "document", "doc-2", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false for different object")
	}
}

func TestCheck_NoTuples_Denied(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write tuples to provision the IDs we'll query, but NOT the combination we're checking
	svc.write(t, ctx,
		insert(tuple("document", "doc-1", "viewer", "user", "bob")),   // provisions doc-1 and bob
		insert(tuple("document", "doc-2", "viewer", "user", "alice")), // provisions alice
	)

	// Query for a combination that doesn't exist (alice is not a viewer of doc-1)
	result, err := svc.check(ctx, "document", "doc-1", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false when tuple doesn't exist")
	}
}

func TestCheck_AfterDelete_Denied(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: alice is a viewer of doc-1
	svc.write(t, ctx, insert(tuple("document", "doc-1", "viewer", "user", "alice")))

	// Verify it's allowed first
	result, err := svc.check(ctx, "document", "doc-1", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !result.Allowed {
		t.Fatal("expected Allowed=true before delete")
	}

	// Delete the tuple
	svc.write(t, ctx, delete(tuple("document", "doc-1", "viewer", "user", "alice")))

	// Query again - should now be denied
	result, err = svc.check(ctx, "document", "doc-1", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false after delete")
	}
}

func TestCheck_MultipleWrites(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write multiple tuples
	svc.write(t, ctx,
		insert(tuple("document", "doc-1", "viewer", "user", "alice")),
		insert(tuple("document", "doc-1", "editor", "user", "bob")),
		insert(tuple("document", "doc-2", "viewer", "user", "charlie")),
	)

	// Check all of them
	tests := []struct {
		name     string
		object   string
		relation string
		subject  string
		allowed  bool
	}{
		{"alice viewer doc-1", "doc-1", "viewer", "alice", true},
		{"bob editor doc-1", "doc-1", "editor", "bob", true},
		{"charlie viewer doc-2", "doc-2", "viewer", "charlie", true},
		{"alice editor doc-1", "doc-1", "editor", "alice", false},
		{"bob viewer doc-1", "doc-1", "viewer", "bob", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.check(ctx, "document", tt.object, tt.relation, "user", tt.subject)
			if err != nil {
				t.Fatalf("Check failed: %v", err)
			}
			if result.Allowed != tt.allowed {
				t.Errorf("expected Allowed=%v, got %v", tt.allowed, result.Allowed)
			}
		})
	}
}

func TestCheck_UnknownObjectType(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	_, err := svc.querySvc.Check(ctx, application.CheckQuery{
		ObjectType:  "unknown_type",
		ObjectID:    "obj-1",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "alice",
	})
	if err == nil {
		t.Error("expected error for unknown object type")
	}
}

func TestCheck_UnknownSubjectType(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	_, err := svc.querySvc.Check(ctx, application.CheckQuery{
		ObjectType:  "document",
		ObjectID:    "doc-1",
		Relation:    "viewer",
		SubjectType: "unknown_type",
		SubjectID:   "alice",
	})
	if err == nil {
		t.Error("expected error for unknown subject type")
	}
}

func TestCheck_UnknownRelation(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	_, err := svc.querySvc.Check(ctx, application.CheckQuery{
		ObjectType:  "document",
		ObjectID:    "doc-1",
		Relation:    "unknown_relation",
		SubjectType: "user",
		SubjectID:   "alice",
	})
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestCheck_IDNotFound_ReturnsDenied(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Query for an object that was never written (no ID provisioned)
	// Should return Allowed=false, not an error - unknown ID means no relation
	result, err := svc.querySvc.Check(ctx, application.CheckQuery{
		ObjectType:  "document",
		ObjectID:    "never-written",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "also-never-written",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false for unknown IDs")
	}
}

func TestCheck_WithUsersetSubject(t *testing.T) {
	svc := newTestServices(t)
	ctx := context.Background()

	// Write: group engineering members can view doc-1
	// Also provision bob by adding him to a different group
	svc.write(t, ctx,
		insert(tupleWithSubjectRelation("document", "doc-1", "viewer", "group", "engineering", "member")),
		insert(tuple("group", "engineering", "member", "user", "alice")),
		insert(tuple("group", "sales", "member", "user", "bob")), // provisions bob's ID
	)

	// Query: check if alice is a viewer of doc-1 (should be allowed via group membership)
	result, err := svc.check(ctx, "document", "doc-1", "viewer", "user", "alice")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if !result.Allowed {
		t.Error("expected Allowed=true via userset membership")
	}

	// Query: check if bob is a viewer of doc-1 (should be denied, not a member of engineering)
	result, err = svc.check(ctx, "document", "doc-1", "viewer", "user", "bob")
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Allowed {
		t.Error("expected Allowed=false for non-member")
	}
}
