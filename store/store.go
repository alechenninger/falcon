// Package store defines the persistence interface for the authorization graph.
package store

import "context"

// Tuple represents a single authorization tuple.
//
// For direct subjects: object_type:object_id#relation@subject_type:subject_id
// Example: document:100#viewer@user:1
//
// For userset subjects: object_type:object_id#relation@subject_type:subject_id#subject_relation
// Example: document:100#viewer@group:1#member (all members of group 1 are viewers)
type Tuple struct {
	ObjectType      string
	ObjectID        uint32
	Relation        string
	SubjectType     string
	SubjectID       uint32
	SubjectRelation string // Optional: empty for direct subjects, set for userset subjects
}

// Store defines the persistence interface for authorization tuples.
// Implementations handle durable storage of tuples (e.g., Postgres).
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
