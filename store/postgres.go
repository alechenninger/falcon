package store

import (
	"context"
	"fmt"

	"github.com/alechenninger/falcon/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the SQL DDL for the tuples table.
const Schema = `
CREATE TABLE IF NOT EXISTS tuples (
    object_type      TEXT NOT NULL,
    object_id        OID NOT NULL,
    relation         TEXT NOT NULL,
    subject_type     TEXT NOT NULL,
    subject_id       OID NOT NULL,
    subject_relation TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (object_type, object_id, relation, subject_type, subject_id, subject_relation)
);
`

// PostgresStore implements Store using PostgreSQL.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a new PostgresStore connected to the given database.
// The connString should be a PostgreSQL connection string (e.g.,
// "postgres://user:pass@localhost:5432/dbname").
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// NewPostgresStoreFromPool creates a PostgresStore from an existing connection pool.
// This is useful for testing or when you want to manage the pool externally.
func NewPostgresStoreFromPool(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// EnsureSchema creates the tuples table if it doesn't exist.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, Schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// WriteTuple persists a tuple to the database.
func (s *PostgresStore) WriteTuple(ctx context.Context, t Tuple) error {
	// Use INSERT ... ON CONFLICT DO NOTHING for idempotency
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tuples (object_type, object_id, relation, subject_type, subject_id, subject_relation)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
	`, string(t.ObjectType), uint32(t.ObjectID), string(t.Relation), string(t.SubjectType), uint32(t.SubjectID), string(t.SubjectRelation))
	if err != nil {
		return fmt.Errorf("failed to write tuple: %w", err)
	}
	return nil
}

// DeleteTuple removes a tuple from the database.
func (s *PostgresStore) DeleteTuple(ctx context.Context, t Tuple) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM tuples
		WHERE object_type = $1 AND object_id = $2 AND relation = $3 
		  AND subject_type = $4 AND subject_id = $5 AND subject_relation = $6
	`, string(t.ObjectType), uint32(t.ObjectID), string(t.Relation), string(t.SubjectType), uint32(t.SubjectID), string(t.SubjectRelation))
	if err != nil {
		return fmt.Errorf("failed to delete tuple: %w", err)
	}
	return nil
}

// LoadAll retrieves all tuples from the database.
func (s *PostgresStore) LoadAll(ctx context.Context) ([]Tuple, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_type, object_id, relation, subject_type, subject_id, subject_relation
		FROM tuples
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tuples: %w", err)
	}
	defer rows.Close()

	tuples, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Tuple, error) {
		var (
			objectType      string
			objectID        uint32
			relation        string
			subjectType     string
			subjectID       uint32
			subjectRelation string
		)
		err := row.Scan(&objectType, &objectID, &relation, &subjectType, &subjectID, &subjectRelation)
		if err != nil {
			return Tuple{}, err
		}
		return Tuple{
			ObjectType:      schema.TypeName(objectType),
			ObjectID:        schema.ID(objectID),
			Relation:        schema.RelationName(relation),
			SubjectType:     schema.TypeName(subjectType),
			SubjectID:       schema.ID(subjectID),
			SubjectRelation: schema.RelationName(subjectRelation),
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan tuples: %w", err)
	}

	return tuples, nil
}

// Close releases the connection pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}
