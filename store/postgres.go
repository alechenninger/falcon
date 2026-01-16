package store

import (
	"context"

	"github.com/alechenninger/falcon/internal/infrastructure/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the SQL DDL for the tuples table.
// Deprecated: Use postgres.Schema instead.
const Schema = postgres.Schema

// PostgresStore implements Store using PostgreSQL.
// Deprecated: Use postgres.Store instead.
type PostgresStore = postgres.Store

// NewPostgresStore creates a new PostgresStore connected to the given database.
// Deprecated: Use postgres.NewStore instead.
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	return postgres.NewStore(ctx, connString)
}

// NewPostgresStoreFromPool creates a PostgresStore from an existing connection pool.
// Deprecated: Use postgres.NewStoreFromPool instead.
func NewPostgresStoreFromPool(pool *pgxpool.Pool) *PostgresStore {
	return postgres.NewStoreFromPool(pool)
}
