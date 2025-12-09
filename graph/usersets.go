package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// usersetKey uniquely identifies a set of subjects for a given object, relation,
// subject type, and subject relation.
type usersetKey struct {
	ObjectType      schema.TypeName
	ObjectID        schema.ID
	Relation        schema.RelationName
	SubjectType     schema.TypeName
	SubjectRelation schema.RelationName // Empty for direct subjects
}

// MultiversionUsersets stores authorization tuples as compressed subject sets
// (roaring bitmaps) with MVCC support for snapshot isolation.
//
// Each tuple (object_type, object_id, relation, subject_type, subject_relation)
// maps to a versioned set of subject IDs. The MVCC undo chain allows reading
// historical snapshots for consistent distributed queries.
//
// This is the core data structure - it does not contain query logic.
// The Graph interface wraps this with the check algorithm.
type MultiversionUsersets struct {
	schema *schema.Schema

	mu     sync.RWMutex
	tuples map[usersetKey]*versionedSet

	// replicatedTime is the latest time we've seen from the change stream.
	// This represents the point in the log that we know our in-memory state
	// is up to date with.
	replicatedTime store.AtomicStoreTime
}

// NewMultiversionUsersets creates a new MultiversionUsersets with the given schema.
func NewMultiversionUsersets(s *schema.Schema) *MultiversionUsersets {
	return &MultiversionUsersets{
		schema: s,
		tuples: make(map[usersetKey]*versionedSet),
	}
}

// Schema returns the schema for this userset store.
func (u *MultiversionUsersets) Schema() *schema.Schema {
	return u.schema
}

// ReplicatedTime returns the latest time that has been applied to the in-memory state.
func (u *MultiversionUsersets) ReplicatedTime() store.StoreTime {
	return u.replicatedTime.Load()
}

// constrainWindow narrows the window's Max to the replicated time.
// This ensures callers passing MaxSnapshotWindow get a realistic window back.
func (u *MultiversionUsersets) constrainWindow(window SnapshotWindow) SnapshotWindow {
	return window.NarrowMax(u.replicatedTime.Load())
}

// Hydrate loads tuples from the given iterator into memory.
// This should be called on startup before subscribing to changes.
func (u *MultiversionUsersets) Hydrate(iter store.TupleIterator) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	for iter.Next() {
		t := iter.Tuple()
		key := usersetKey{
			ObjectType:      t.ObjectType,
			ObjectID:        t.ObjectID,
			Relation:        t.Relation,
			SubjectType:     t.SubjectType,
			SubjectRelation: t.SubjectRelation,
		}

		vs, ok := u.tuples[key]
		if !ok {
			vs = newVersionedSet(0)
			u.tuples[key] = vs
		}
		vs.Add(t.SubjectID, 0)
	}

	return iter.Err()
}

// Subscribe consumes changes from the given ChangeStream and applies them.
// The observer is notified at key points for instrumentation/testing.
// This blocks until the context is canceled or an error occurs.
func (u *MultiversionUsersets) Subscribe(ctx context.Context, stream store.ChangeStream, observer UsersetsObserver) error {
	afterTime := u.replicatedTime.Load()
	changes, errCh := stream.Subscribe(ctx, afterTime)

	// Signal that we're ready to receive changes
	observer.SubscribeReady(ctx)

	for {
		select {
		case change, ok := <-changes:
			if !ok {
				return nil // Channel closed
			}
			u.applyChange(ctx, change, observer)
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("change stream error: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyChange applies a single change to the in-memory state.
// Empty tuples (with zero-value ObjectType) are skipped but still advance replicatedTime.
// This allows sharded graphs to filter tuple data while keeping time synchronized.
func (u *MultiversionUsersets) applyChange(ctx context.Context, change store.Change, observer UsersetsObserver) {
	_, probe := observer.ApplyChangeStarted(ctx, change)
	defer probe.End()

	// NOTE: This updates set versions before setting replicated time.
	// This means momentarily, sets may have versions ahead of the max replicated time.
	// These versions should be ignored by the current max window.
	// This is to avoid locking the entire graph for every change.
	// Those values are effectively "locked" by virtue of being out of the window,
	// without literally blocking reads.

	t := change.Tuple
	// Skip empty tuples (filtered changes) but still advance time
	if t.ObjectType != "" {
		switch change.Op {
		case store.OpInsert:
			u.applyAdd(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.Time)
		case store.OpDelete:
			u.applyRemove(t.ObjectType, t.ObjectID, t.Relation, t.SubjectType, t.SubjectID, t.SubjectRelation, change.Time)
		}
	}
	u.replicatedTime.Store(change.Time)
	probe.Applied(change.Time)
}

// applyAdd adds a subject to the versioned set for the given tuple key.
func (u *MultiversionUsersets) applyAdd(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, t store.StoreTime) {
	u.mu.Lock()
	defer u.mu.Unlock()

	key := usersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	vs, ok := u.tuples[key]
	if !ok {
		vs = newVersionedSet(t)
		u.tuples[key] = vs
	}
	vs.Add(subjectID, t)
}

// applyRemove removes a subject from the versioned set for the given tuple key.
func (u *MultiversionUsersets) applyRemove(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName, t store.StoreTime) {
	u.mu.Lock()
	defer u.mu.Unlock()

	key := usersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	if vs, ok := u.tuples[key]; ok {
		vs.Remove(subjectID, t)
	}
}

// ContainsDirectWithin checks if the subject is directly in the relation within the window.
// Returns (found, stateTime, narrowedWindow).
func (u *MultiversionUsersets) ContainsDirectWithin(
	objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	subjectType schema.TypeName, subjectID schema.ID,
	window SnapshotWindow,
) (bool, store.StoreTime, SnapshotWindow) {
	// Constrain window to replicated time
	window = u.constrainWindow(window)

	u.mu.RLock()
	defer u.mu.RUnlock()

	key := usersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: "",
	}

	vs, ok := u.tuples[key]
	if !ok {
		return false, 0, window
	}

	found, actualTime := vs.ContainsWithin(subjectID, window.Max())
	if actualTime == 0 {
		return false, 0, window
	}

	// Narrow window
	newWindow := window.NarrowMin(actualTime)
	return found, actualTime, newWindow
}

// GetSubjectBitmapWithin gets the subject bitmap for the given tuple key within the snapshot window.
// Returns the bitmap (possibly cloned), the state time, and the narrowed window.
// Returns nil bitmap if no tuples exist.
func (u *MultiversionUsersets) GetSubjectBitmapWithin(
	objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	subjectType schema.TypeName, subjectRelation schema.RelationName,
	window SnapshotWindow,
) (*roaring.Bitmap, store.StoreTime, SnapshotWindow) {
	// Constrain window to replicated time
	window = u.constrainWindow(window)

	u.mu.RLock()
	defer u.mu.RUnlock()

	key := usersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}

	vs, ok := u.tuples[key]
	if !ok {
		return nil, 0, window
	}

	// If head is within bounds, use it directly
	if vs.HeadTime() <= window.Max() {
		newWindow := window.NarrowMin(vs.HeadTime())
		return vs.Head(), vs.HeadTime(), newWindow
	}

	// Need historical snapshot
	bitmap, actualTime := vs.SnapshotWithin(window.Max())
	if bitmap == nil {
		return nil, 0, window
	}

	newWindow := window.NarrowMin(actualTime)
	return bitmap, actualTime, newWindow
}

// ForEachUsersetSubjectWithin iterates over userset subjects within the given snapshot window.
// For each (subjectType, subjectRelation, subjectID) tuple, fn is called with the current window.
// fn returns (stop, newWindow). If stop is true, iteration ends early.
// Returns (anyTrue, finalWindow).
//
// targetTypes specifies the allowed subject types from the Direct userset.
func (u *MultiversionUsersets) ForEachUsersetSubjectWithin(
	objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	targetTypes []schema.SubjectRef, window SnapshotWindow,
	fn func(schema.TypeName, schema.RelationName, schema.ID, SnapshotWindow) (bool, SnapshotWindow),
) (bool, SnapshotWindow) {
	// Constrain window to replicated time
	window = u.constrainWindow(window)

	u.mu.RLock()
	defer u.mu.RUnlock()

	for _, ref := range targetTypes {
		if ref.Relation == "" {
			continue // Skip direct subjects
		}

		key := usersetKey{
			ObjectType:      objectType,
			ObjectID:        objectID,
			Relation:        relation,
			SubjectType:     ref.Type,
			SubjectRelation: ref.Relation,
		}

		vs, ok := u.tuples[key]
		if !ok {
			continue
		}

		var bitmap *roaring.Bitmap
		if vs.HeadTime() <= window.Max() {
			bitmap = vs.Head()
			window = window.NarrowMin(vs.HeadTime())
		} else {
			var t store.StoreTime
			bitmap, t = vs.SnapshotWithin(window.Max())
			if bitmap == nil {
				continue
			}
			window = window.NarrowMin(t)
		}

		// Iterate over subject IDs
		iter := bitmap.Iterator()
		for iter.HasNext() {
			subjectID := schema.ID(iter.Next())
			stop, newWindow := fn(ref.Type, ref.Relation, subjectID, window)
			window = newWindow
			if stop {
				return true, window
			}
		}
	}

	return false, window
}

// ValidateTuple checks that the object type, relation, and subject reference
// are valid according to the schema.
func (u *MultiversionUsersets) ValidateTuple(objectType schema.TypeName, relation schema.RelationName, subjectType schema.TypeName, subjectRelation schema.RelationName) error {
	ot, ok := u.schema.Types[objectType]
	if !ok {
		return fmt.Errorf("unknown object type: %s", objectType)
	}
	rel, ok := ot.Relations[relation]
	if !ok {
		return fmt.Errorf("unknown relation %s on type %s", relation, objectType)
	}

	targetTypes := rel.DirectTargetTypes()
	if targetTypes == nil {
		return fmt.Errorf("relation %s#%s does not allow direct tuples", objectType, relation)
	}

	for _, ref := range targetTypes {
		if ref.Type == subjectType && ref.Relation == subjectRelation {
			return nil
		}
	}

	if subjectRelation == "" {
		return fmt.Errorf("subject type %s is not allowed for %s#%s", subjectType, objectType, relation)
	}
	return fmt.Errorf("subject %s#%s is not allowed for %s#%s", subjectType, subjectRelation, objectType, relation)
}

// TruncateHistory removes undo entries older than the given time from all
// versioned sets. This is used for garbage collection.
func (u *MultiversionUsersets) TruncateHistory(minTime store.StoreTime) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	for _, vs := range u.tuples {
		vs.Truncate(minTime)
	}
}
