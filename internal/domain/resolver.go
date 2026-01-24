package domain

import "context"

// IdResolver resolves between external and internal object identifiers.
// Implementations may cache mappings or delegate to a Store.
type IdResolver interface {
	// ResolveIDs returns internal IDs for the given external object references.
	// Returns a slice of IDs in the same order as the input refs.
	// Returns ErrIDNotFound if any object has no mapping.
	ResolveIDs(ctx context.Context, refs []ObjectRef) ([]ID, error)

	// ResolveRefs returns external references for the given internal IDs.
	// Each InternalRef includes both TypeID and ID since IDs are only unique within a type.
	// Returns a slice of ObjectRefs in the same order as the input.
	// Returns ErrIDNotFound if any ID has no mapping.
	// Used for reverse queries (list_objects, list_subjects).
	ResolveRefs(ctx context.Context, refs []InternalRef) ([]ObjectRef, error)
}

// InternalRef identifies an object by its type and internal ID.
// Used for reverse resolution from internal to external IDs.
type InternalRef struct {
	Type TypeID
	ID   ID
}
