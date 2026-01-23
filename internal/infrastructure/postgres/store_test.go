package postgres_test

import (
	"context"
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
	domain.RunStoreTests(t, func(t *testing.T) domain.Store {
		s, err := infrapostgres.NewStore(ctx, connStr)
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		if err := s.Truncate(ctx); err != nil {
			t.Fatalf("failed to truncate: %v", err)
		}
		return s
	})
}
