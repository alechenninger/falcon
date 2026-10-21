package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alechenninger/falcon/internal/domain"
	infrapostgres "github.com/alechenninger/falcon/internal/infrastructure/postgres"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	// Disable Ryuk for podman compatibility
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func TestPostgresStore(t *testing.T) {
	ctx := context.Background()

	// Expensive part: start container once
	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
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

	// Create schema once with a temporary store
	setupStore, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create setup store: %v", err)
	}
	if err := setupStore.EnsureSchema(ctx); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}
	setupStore.Close()

	// Cheap factory: new store instance per test, truncate for clean state
	factory := func(t *testing.T) domain.Store {
		s, err := infrapostgres.NewStore(ctx, connStr)
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		if err := s.Truncate(ctx); err != nil {
			t.Fatalf("failed to truncate: %v", err)
		}
		return s
	}
	domain.RunStoreTests(t, factory)
	t.Run("RejectOutOfRangeSchemaIDs", func(t *testing.T) {
		s := factory(t)
		defer s.Close()
		for _, id := range []uint16{32768, 65535} {
			for _, tt := range []struct {
				name string
				run  func(domain.Tx) error
			}{
				{"GetID", func(tx domain.Tx) error {
					_, err := tx.GetID(ctx, domain.ObjectRef{Type: domain.TypeID(id), ID: "doc"})
					return err
				}},
				{"GetOrProvisionID", func(tx domain.Tx) error {
					_, err := tx.GetOrProvisionID(ctx, domain.ObjectRef{Type: domain.TypeID(id), ID: "doc"}, domain.NoObject)
					return err
				}},
				{"GetRef", func(tx domain.Tx) error {
					_, err := tx.GetRef(ctx, domain.TypeID(id), 1)
					return err
				}},
				{"Contains", func(tx domain.Tx) error {
					_, err := tx.Contains(ctx, domain.Eq(domain.FieldObjectType, domain.TypeID(id)))
					return err
				}},
			} {
				t.Run(fmt.Sprintf("%s/%d", tt.name, id), func(t *testing.T) {
					tx, err := s.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback(ctx)
					if err := tt.run(tx); err == nil {
						t.Fatal("accepted an out of range ID")
					}
				})
			}
			for _, field := range []domain.TupleField{domain.FieldObjectType, domain.FieldRelation, domain.FieldSubjectType, domain.FieldSubjectRelation} {
				for _, op := range []domain.ChangeOp{domain.OpInsert, domain.OpDelete} {
					t.Run(fmt.Sprintf("Write/%d/%d/%d", field, op, id), func(t *testing.T) {
						tx, err := s.Begin(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback(ctx)
						tuple := domain.Tuple{ObjectType: 1, ObjectID: 1, Relation: 1, SubjectType: 2, SubjectID: 2}
						switch field {
						case domain.FieldObjectType:
							tuple.ObjectType = domain.TypeID(id)
						case domain.FieldRelation:
							tuple.Relation = domain.RelationID(id)
						case domain.FieldSubjectType:
							tuple.SubjectType = domain.TypeID(id)
						case domain.FieldSubjectRelation:
							tuple.SubjectRelation = domain.RelationID(id)
						}
						if err := tx.Write(ctx, []domain.Mutation{{Op: op, Tuple: tuple}}); err == nil {
							t.Fatal("accepted an out of range tuple ID")
						}
					})
				}
			}
		}
	})
}
