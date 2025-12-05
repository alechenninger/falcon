// Package store defines the persistence interface for the authorization graph.
package store

import (
	"context"

	"github.com/alechenninger/falcon/schema"
)

// LSN represents a Log Sequence Number from the WAL.
type LSN = uint64

// Tuple represents a single authorization tuple.
//
// For direct subjects: object_type:object_id#relation@subject_type:subject_id
// Example: document:100#viewer@user:1
//
// For userset subjects: object_type:object_id#relation@subject_type:subject_id#subject_relation
// Example: document:100#viewer@group:1#member (all members of group 1 are viewers)
type Tuple struct {
	ObjectType      schema.TypeName
	ObjectID        schema.ID
	Relation        schema.RelationName
	SubjectType     schema.TypeName
	SubjectID       schema.ID
	SubjectRelation schema.RelationName // Optional: empty for direct subjects, set for userset subjects
}

// ChangeOp represents the type of change (insert or delete).
type ChangeOp int

const (
	// OpInsert indicates a tuple was inserted.
	OpInsert ChangeOp = iota
	// OpDelete indicates a tuple was deleted.
	OpDelete
)

// Change represents a tuple change with its LSN.
type Change struct {
	LSN   LSN
	Op    ChangeOp
	Tuple Tuple
}

// Store defines the persistence interface for authorization tuples.
// Implementations handle durable storage of tuples (e.g., Postgres).
//
// Note: Writes do not return LSNs. The LSN is only available via the
// ChangeStream, which delivers changes in WAL order with their LSNs.
type Store interface {
	// WriteTuple persists a tuple to the store. If the tuple already exists,
	// this is a no-op.
	WriteTuple(ctx context.Context, t Tuple) error

	// DeleteTuple removes a tuple from the store. If the tuple doesn't exist,
	// this is a no-op.
	DeleteTuple(ctx context.Context, t Tuple) error

	// LoadAll retrieves all tuples from the store. This is used to hydrate
	// the in-memory graph on startup.
	LoadAll(ctx context.Context) ([]Tuple, error)

	// Close releases any resources held by the store.
	Close() error
}

// ChangeStream emits ordered tuple changes from the store.
// Implementations tail the WAL (Postgres) or emit changes directly (in-memory).
type ChangeStream interface {
	// Subscribe returns a channel of changes starting after the given LSN.
	// Pass 0 to get all changes from the beginning.
	// The channel is closed when the context is canceled or an error occurs.
	Subscribe(ctx context.Context, afterLSN LSN) (<-chan Change, <-chan error)

	// CurrentLSN returns the current LSN of the store (latest committed change).
	CurrentLSN(ctx context.Context) (LSN, error)
}
