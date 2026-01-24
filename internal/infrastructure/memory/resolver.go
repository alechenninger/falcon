package memory

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
)

// StoreIdResolver implements domain.IdResolver using a Store.
// It opens a read-only transaction for each resolution.
type StoreIdResolver struct {
	store domain.Store
}

// NewStoreIdResolver creates a new StoreIdResolver backed by the given store.
func NewStoreIdResolver(store domain.Store) *StoreIdResolver {
	return &StoreIdResolver{store: store}
}

// ResolveIDs returns internal IDs for the given external object references.
// Returns a slice of IDs in the same order as the input refs.
// Returns domain.ErrIDNotFound if any object has no mapping.
func (r *StoreIdResolver) ResolveIDs(ctx context.Context, refs []domain.ObjectRef) ([]domain.ID, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	tx, err := r.store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // cleanup on error paths

	ids := make([]domain.ID, len(refs))
	for i, ref := range refs {
		id, err := tx.GetID(ctx, ref)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

// ResolveRefs returns external references for the given internal IDs.
// Each InternalRef includes both TypeID and ID since IDs are only unique within a type.
// Returns a slice of ObjectRefs in the same order as the input.
// Returns domain.ErrIDNotFound if any ID has no mapping.
func (r *StoreIdResolver) ResolveRefs(ctx context.Context, refs []domain.InternalRef) ([]domain.ObjectRef, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	tx, err := r.store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // cleanup on error paths

	objRefs := make([]domain.ObjectRef, len(refs))
	for i, ref := range refs {
		objRef, err := tx.GetRef(ctx, ref.Type, ref.ID)
		if err != nil {
			return nil, err
		}
		objRefs[i] = objRef
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return objRefs, nil
}

// Compile-time interface check
var _ domain.IdResolver = (*StoreIdResolver)(nil)
