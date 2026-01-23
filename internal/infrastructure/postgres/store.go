package postgres

import (
	"context"
	"fmt"

	"github.com/alechenninger/falcon/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the SQL DDL for the tuples and id_mappings tables.
// Uses compact integer types for type/relation IDs (SMALLINT = 2 bytes, supports 0-255).
// BIGINT is used for object/subject IDs (8 bytes, supports 64-bit IDs).
const Schema = `
CREATE TABLE IF NOT EXISTS tuples (
    object_type      SMALLINT NOT NULL,
    object_id        BIGINT NOT NULL,
    relation         SMALLINT NOT NULL,
    subject_type     SMALLINT NOT NULL,
    subject_id       BIGINT NOT NULL,
    subject_relation SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (object_type, object_id, relation, subject_type, subject_id, subject_relation)
);

CREATE SEQUENCE IF NOT EXISTS id_seq START 1;

CREATE TABLE IF NOT EXISTS id_mappings (
    internal_id BIGINT PRIMARY KEY DEFAULT nextval('id_seq'),
    type_id     SMALLINT NOT NULL,
    external_id TEXT NOT NULL,
    UNIQUE(type_id, external_id)
);
`

// Store implements domain.Store using PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store connected to the given database.
// The connString should be a PostgreSQL connection string (e.g.,
// "postgres://user:pass@localhost:5432/dbname").
func NewStore(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Store{pool: pool}, nil
}

// NewStoreFromPool creates a Store from an existing connection pool.
// This is useful for testing or when you want to manage the pool externally.
func NewStoreFromPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// EnsureSchema creates the tuples table if it doesn't exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, Schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// Truncate removes all data from the store and resets sequences.
// This is useful for testing to reset state between tests.
func (s *Store) Truncate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		TRUNCATE tuples, id_mappings;
		ALTER SEQUENCE id_seq RESTART WITH 1;
	`)
	if err != nil {
		return fmt.Errorf("failed to truncate: %w", err)
	}
	return nil
}

// Begin starts a new database transaction.
func (s *Store) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &pgxTx{tx: tx}, nil
}

// LoadAll returns an iterator over all tuples in the database.
func (s *Store) LoadAll(ctx context.Context) (domain.TupleIterator, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_type, object_id, relation, subject_type, subject_id, subject_relation
		FROM tuples
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tuples: %w", err)
	}

	return &pgxRowsIterator{rows: rows}, nil
}

// pgxTx implements domain.Tx using pgx.
type pgxTx struct {
	tx pgx.Tx
}

// GetID returns the internal ID for an object reference.
// Returns domain.ErrIDNotFound if the external ID is not mapped.
func (t *pgxTx) GetID(ctx context.Context, ref domain.ObjectRef) (domain.ID, error) {
	var internalID int64
	err := t.tx.QueryRow(ctx, `
		SELECT internal_id FROM id_mappings WHERE type_id = $1 AND external_id = $2
	`, int16(ref.Type), string(ref.ID)).Scan(&internalID)
	if err == pgx.ErrNoRows {
		return 0, domain.ErrIDNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get ID: %w", err)
	}
	return domain.ID(internalID), nil
}

// GetOrProvisionID returns the internal ID for an object reference, creating a new
// mapping if one does not exist.
//
// The root parameter specifies the shard root for this object. Currently this is
// ignored and IDs are provisioned from a global sequence. Future implementations
// will use the root to encode shard information in the high bits of the ID.
func (t *pgxTx) GetOrProvisionID(ctx context.Context, ref domain.ObjectRef, root domain.ObjectRef) (domain.ID, error) {
	// TODO: Use root to determine shard-aware ID provisioning.
	// For now, we ignore root and use a global sequence.
	_ = root

	var internalID int64
	// INSERT with ON CONFLICT to handle concurrent insertions, then return the ID
	err := t.tx.QueryRow(ctx, `
		INSERT INTO id_mappings (type_id, external_id)
		VALUES ($1, $2)
		ON CONFLICT (type_id, external_id) DO UPDATE SET external_id = EXCLUDED.external_id
		RETURNING internal_id
	`, int16(ref.Type), string(ref.ID)).Scan(&internalID)
	if err != nil {
		return 0, fmt.Errorf("failed to provision ID: %w", err)
	}
	return domain.ID(internalID), nil
}

// Write applies mutations within this transaction.
func (t *pgxTx) Write(ctx context.Context, mutations []domain.Mutation) error {
	for _, m := range mutations {
		var err error
		switch m.Op {
		case domain.OpInsert:
			_, err = t.tx.Exec(ctx, `
				INSERT INTO tuples (object_type, object_id, relation, subject_type, subject_id, subject_relation)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT DO NOTHING
			`, int16(m.Tuple.ObjectType), int64(m.Tuple.ObjectID), int16(m.Tuple.Relation),
				int16(m.Tuple.SubjectType), int64(m.Tuple.SubjectID), int16(m.Tuple.SubjectRelation))
		case domain.OpDelete:
			_, err = t.tx.Exec(ctx, `
				DELETE FROM tuples
				WHERE object_type = $1 AND object_id = $2 AND relation = $3 
				  AND subject_type = $4 AND subject_id = $5 AND subject_relation = $6
			`, int16(m.Tuple.ObjectType), int64(m.Tuple.ObjectID), int16(m.Tuple.Relation),
				int16(m.Tuple.SubjectType), int64(m.Tuple.SubjectID), int16(m.Tuple.SubjectRelation))
		default:
			return fmt.Errorf("unknown mutation op: %v", m.Op)
		}
		if err != nil {
			return fmt.Errorf("failed to execute mutation: %w", err)
		}
	}
	return nil
}

// Contains checks if any tuple matches the predicate within this transaction's view.
func (t *pgxTx) Contains(ctx context.Context, predicate domain.TuplePredicate) (bool, error) {
	// Determine which ID field joins we need
	needsObjectIDJoin := predicateUsesField(predicate, domain.FieldObjectID)
	needsSubjectIDJoin := predicateUsesField(predicate, domain.FieldSubjectID)

	// Build FROM clause with necessary joins
	fromClause := "tuples t"
	if needsObjectIDJoin {
		fromClause += " JOIN id_mappings om ON t.object_id = om.internal_id"
	}
	if needsSubjectIDJoin {
		fromClause += " JOIN id_mappings sm ON t.subject_id = sm.internal_id"
	}

	whereClause, args := predicateToSQL(predicate, 1)
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s)", fromClause, whereClause)

	var exists bool
	if err := t.tx.QueryRow(ctx, query, args...).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check predicate: %w", err)
	}
	return exists, nil
}

// predicateUsesField checks if a predicate references the given field.
func predicateUsesField(p domain.TuplePredicate, field domain.TupleField) bool {
	switch pred := p.(type) {
	case domain.FieldPredicate:
		return pred.Field == field
	case domain.AndPredicate:
		for _, child := range pred.Predicates {
			if predicateUsesField(child, field) {
				return true
			}
		}
	case domain.OrPredicate:
		for _, child := range pred.Predicates {
			if predicateUsesField(child, field) {
				return true
			}
		}
	case domain.NotPredicate:
		return predicateUsesField(pred.Predicate, field)
	}
	return false
}

// predicateToSQL converts a domain predicate to a SQL WHERE clause.
// Returns the clause string and the argument values.
// paramStart is the starting parameter number ($1, $2, etc.).
// For ObjectID and SubjectID fields, it uses the joined id_mappings tables
// (aliases: om for object, sm for subject) to compare external IDs.
// ObjectRef values become compound conditions checking both type_id and external_id.
func predicateToSQL(p domain.TuplePredicate, paramStart int) (string, []any) {
	switch pred := p.(type) {
	case domain.FieldPredicate:
		// Handle ObjectID and SubjectID with ObjectRef values
		if pred.Field == domain.FieldObjectID || pred.Field == domain.FieldSubjectID {
			return fieldPredicateToSQL(pred, paramStart)
		}
		col := fieldToColumn(pred.Field)
		if pred.Op == domain.OpStartsWith {
			// LIKE with prefix match
			return fmt.Sprintf("%s LIKE $%d || '%%'", col, paramStart), []any{pred.Value}
		}
		op := compareOpToSQL(pred.Op)
		return fmt.Sprintf("%s %s $%d", col, op, paramStart), []any{fieldValue(pred.Field, pred.Value)}

	case domain.AndPredicate:
		if len(pred.Predicates) == 0 {
			return "TRUE", nil
		}
		var clauses []string
		var args []any
		param := paramStart
		for _, child := range pred.Predicates {
			clause, childArgs := predicateToSQL(child, param)
			clauses = append(clauses, clause)
			args = append(args, childArgs...)
			param += len(childArgs)
		}
		return "(" + joinClauses(clauses, " AND ") + ")", args

	case domain.OrPredicate:
		if len(pred.Predicates) == 0 {
			return "FALSE", nil
		}
		var clauses []string
		var args []any
		param := paramStart
		for _, child := range pred.Predicates {
			clause, childArgs := predicateToSQL(child, param)
			clauses = append(clauses, clause)
			args = append(args, childArgs...)
			param += len(childArgs)
		}
		return "(" + joinClauses(clauses, " OR ") + ")", args

	case domain.NotPredicate:
		clause, args := predicateToSQL(pred.Predicate, paramStart)
		return "NOT (" + clause + ")", args

	default:
		// Should not happen if all predicate types are handled
		return "FALSE", nil
	}
}

func fieldToColumn(f domain.TupleField) string {
	switch f {
	case domain.FieldObjectType:
		return "t.object_type"
	case domain.FieldObjectID:
		// Uses joined id_mappings table (alias: om) for external ID comparison
		return "om.external_id"
	case domain.FieldRelation:
		return "t.relation"
	case domain.FieldSubjectType:
		return "t.subject_type"
	case domain.FieldSubjectID:
		// Uses joined id_mappings table (alias: sm) for external ID comparison
		return "sm.external_id"
	case domain.FieldSubjectRelation:
		return "t.subject_relation"
	default:
		return "unknown"
	}
}

func compareOpToSQL(op domain.CompareOp) string {
	switch op {
	case domain.OpEq:
		return "="
	case domain.OpNeq:
		return "<>"
	case domain.OpLt:
		return "<"
	case domain.OpLte:
		return "<="
	case domain.OpGt:
		return ">"
	case domain.OpGte:
		return ">="
	default:
		return "="
	}
}

// fieldPredicateToSQL handles ObjectID/SubjectID field predicates with ObjectRef values.
// Since an ObjectRef contains both type and external ID, we generate compound conditions.
func fieldPredicateToSQL(pred domain.FieldPredicate, paramStart int) (string, []any) {
	ref, ok := pred.Value.(domain.ObjectRef)
	if !ok {
		// Fallback for simple external ID comparison (e.g., StartsWith)
		if pred.Field == domain.FieldObjectID {
			if pred.Op == domain.OpStartsWith {
				return fmt.Sprintf("om.external_id LIKE $%d || '%%'", paramStart), []any{pred.Value}
			}
			return fmt.Sprintf("om.external_id %s $%d", compareOpToSQL(pred.Op), paramStart), []any{pred.Value}
		}
		if pred.Op == domain.OpStartsWith {
			return fmt.Sprintf("sm.external_id LIKE $%d || '%%'", paramStart), []any{pred.Value}
		}
		return fmt.Sprintf("sm.external_id %s $%d", compareOpToSQL(pred.Op), paramStart), []any{pred.Value}
	}

	// ObjectRef: compare both type_id and external_id
	var typeCol, extIDCol string
	if pred.Field == domain.FieldObjectID {
		typeCol = "om.type_id"
		extIDCol = "om.external_id"
	} else {
		typeCol = "sm.type_id"
		extIDCol = "sm.external_id"
	}

	switch pred.Op {
	case domain.OpEq:
		// Both type and external_id must match
		return fmt.Sprintf("(%s = $%d AND %s = $%d)", typeCol, paramStart, extIDCol, paramStart+1),
			[]any{int16(ref.Type), string(ref.ID)}
	case domain.OpNeq:
		// Either type or external_id can differ
		return fmt.Sprintf("(%s <> $%d OR %s <> $%d)", typeCol, paramStart, extIDCol, paramStart+1),
			[]any{int16(ref.Type), string(ref.ID)}
	case domain.OpStartsWith:
		// Match type exactly, external_id starts with prefix
		return fmt.Sprintf("(%s = $%d AND %s LIKE $%d || '%%')", typeCol, paramStart, extIDCol, paramStart+1),
			[]any{int16(ref.Type), string(ref.ID)}
	default:
		// For Lt, Lte, Gt, Gte: compare type first, then external_id within type
		// This gives lexicographic ordering: (type, external_id)
		op := compareOpToSQL(pred.Op)
		return fmt.Sprintf("(%s = $%d AND %s %s $%d)", typeCol, paramStart, extIDCol, op, paramStart+1),
			[]any{int16(ref.Type), string(ref.ID)}
	}
}

// fieldValue converts the predicate value to the appropriate SQL type.
func fieldValue(f domain.TupleField, v any) any {
	switch f {
	case domain.FieldObjectType, domain.FieldSubjectType:
		// TypeID -> SMALLINT
		if tid, ok := v.(domain.TypeID); ok {
			return int16(tid)
		}
	case domain.FieldRelation, domain.FieldSubjectRelation:
		// RelationID -> SMALLINT
		if rid, ok := v.(domain.RelationID); ok {
			return int16(rid)
		}
	case domain.FieldObjectID, domain.FieldSubjectID:
		// ObjectRef or ExternalID - handled by fieldPredicateToSQL
		return v
	}
	return v
}

func joinClauses(clauses []string, sep string) string {
	if len(clauses) == 0 {
		return ""
	}
	result := clauses[0]
	for i := 1; i < len(clauses); i++ {
		result += sep + clauses[i]
	}
	return result
}

// Commit commits the transaction.
func (t *pgxTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// Rollback aborts the transaction.
func (t *pgxTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// pgxRowsIterator wraps pgx.Rows as a domain.TupleIterator.
type pgxRowsIterator struct {
	rows    pgx.Rows
	current domain.Tuple
	err     error
}

// Next advances to the next row.
func (it *pgxRowsIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.rows.Next() {
		return false
	}

	var (
		objectType      int16
		objectID        int64
		relation        int16
		subjectType     int16
		subjectID       int64
		subjectRelation int16
	)
	it.err = it.rows.Scan(&objectType, &objectID, &relation, &subjectType, &subjectID, &subjectRelation)
	if it.err != nil {
		return false
	}

	it.current = domain.Tuple{
		ObjectType:      domain.TypeID(objectType),
		ObjectID:        domain.ID(objectID),
		Relation:        domain.RelationID(relation),
		SubjectType:     domain.TypeID(subjectType),
		SubjectID:       domain.ID(subjectID),
		SubjectRelation: domain.RelationID(subjectRelation),
	}
	return true
}

// Tuple returns the current tuple.
func (it *pgxRowsIterator) Tuple() domain.Tuple {
	return it.current
}

// Err returns any error encountered during iteration.
func (it *pgxRowsIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.rows.Err()
}

// Close releases the underlying rows.
func (it *pgxRowsIterator) Close() error {
	it.rows.Close()
	return nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Compile-time interface check
var _ domain.Store = (*Store)(nil)
