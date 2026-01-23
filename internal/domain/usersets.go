package domain

import (
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// usersetKey uniquely identifies a set of subjects for a given object, relation,
// subject type, and subject relation.
//
// Uses compact TypeID/RelationID instead of strings for memory efficiency.
// Total size: 12 bytes (vs ~72 bytes with strings).
type usersetKey struct {
	ObjectType      TypeID     // 1 byte
	Relation        RelationID // 1 byte
	SubjectType     TypeID     // 1 byte
	SubjectRelation RelationID // 1 byte (0 = no relation)
	ObjectID        ID         // 8 bytes
}

// tupleToKey converts a Tuple directly to a usersetKey.
// Since both now use TypeID/RelationID, this is a direct mapping.
func tupleToKey(t Tuple) usersetKey {
	return usersetKey{
		ObjectType:      t.ObjectType,
		ObjectID:        t.ObjectID,
		Relation:        t.Relation,
		SubjectType:     t.SubjectType,
		SubjectRelation: t.SubjectRelation,
	}
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
	schema *Schema

	mu     sync.RWMutex
	tuples map[usersetKey]*VersionedSet

	// replicatedTime is the latest time we've seen from the change stream.
	// This represents the point in the log that we know our in-memory state
	// is up to date with.
	replicatedTime AtomicStoreTime

	// observer is called for read/write operations. Never nil.
	observer UsersetsObserver
}

// NewMultiversionUsersets creates a new MultiversionUsersets with the given
func NewMultiversionUsersets(s *Schema) *MultiversionUsersets {
	return &MultiversionUsersets{
		schema:   s,
		tuples:   make(map[usersetKey]*VersionedSet),
		observer: NoOpUsersetsObserver{},
	}
}

// SetObserver sets the observer for read operation instrumentation.
// This is separate from the Subscribe observer which is only for write operations.
func (u *MultiversionUsersets) SetObserver(obs UsersetsObserver) {
	if obs == nil {
		obs = NoOpUsersetsObserver{}
	}
	u.observer = obs
}

// Schema returns the schema for this userset store.
func (u *MultiversionUsersets) Schema() *Schema {
	return u.schema
}

// ReplicatedTime returns the latest time that has been applied to the in-memory state.
func (u *MultiversionUsersets) ReplicatedTime() StoreTime {
	return u.replicatedTime.Load()
}

// SetReplicatedTime sets the replicated time.
// This is used after hydration to indicate the snapshot time of the loaded data.
// Normally replicatedTime is advanced by the change stream, but for static/test
// scenarios we need to set it manually after loading a snapshot.
// TODO: this is temporary; remove this after we have a proper hydration protocol.
func (u *MultiversionUsersets) SetReplicatedTime(t StoreTime) {
	u.replicatedTime.Store(t)
}

// constrainWindow narrows the window's Max to the replicated time.
// This ensures callers passing MaxSnapshotWindow get a realistic window back.
func (u *MultiversionUsersets) constrainWindow(window SnapshotWindow) SnapshotWindow {
	return window.NarrowMax(u.replicatedTime.Load())
}

// Hydrate loads tuples from the given iterator into memory.
// This should be called on startup before subscribing to changes.
// Hydration uses bulk operations that skip undo history since there's
// no need to time-travel before the initial snapshot.
func (u *MultiversionUsersets) Hydrate(iter TupleIterator) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	for iter.Next() {
		t := iter.Tuple()
		key := tupleToKey(t)

		vs, ok := u.tuples[key]
		if !ok {
			vs = NewVersionedSet(0)
			u.tuples[key] = vs
		}
		// Use AddBulk to skip undo history - no need to time-travel before snapshot
		vs.AddBulk(t.SubjectID)
	}

	return iter.Err()
}

// Subscribe consumes changes from the given ChangeStream and applies them.
// The observer is notified at key points for instrumentation/testing.
// This blocks until the context is canceled or an error occurs.
func (u *MultiversionUsersets) Subscribe(ctx context.Context, stream ChangeStream, observer UsersetsObserver) error {
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
func (u *MultiversionUsersets) applyChange(ctx context.Context, change Change, observer UsersetsObserver) {
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
	if t.ObjectType != 0 {
		switch change.Op {
		case OpInsert:
			u.applyAdd(t, change.Time)
		case OpDelete:
			u.applyRemove(t, change.Time)
		}
	}
	u.replicatedTime.Store(change.Time)
	probe.Applied(change.Time)
}

// applyAdd adds a subject to the versioned set for the given tuple.
func (u *MultiversionUsersets) applyAdd(t Tuple, time StoreTime) {
	u.mu.Lock()
	defer u.mu.Unlock()

	key := tupleToKey(t)

	vs, ok := u.tuples[key]
	if !ok {
		vs = NewVersionedSet(time)
		u.tuples[key] = vs
	}
	vs.Add(t.SubjectID, time)
}

// applyRemove removes a subject from the versioned set for the given tuple.
func (u *MultiversionUsersets) applyRemove(t Tuple, time StoreTime) {
	u.mu.Lock()
	defer u.mu.Unlock()

	key := tupleToKey(t)

	if vs, ok := u.tuples[key]; ok {
		vs.Remove(t.SubjectID, time)
	}
}

// ContainsDirectWithin checks if the subject is directly in the relation within the window.
// Only constrains result by window.Max(); result min is the actual store time of accessed data.
// Returns (found, narrowedWindow) where window.Min() == oldest time the answer is valid.
func (u *MultiversionUsersets) ContainsDirectWithin(
	objectType TypeID, objectID ID, relation RelationID,
	subjectType TypeID, subjectID ID,
	window SnapshotWindow,
) (bool, SnapshotWindow) {
	// Constrain max to replicated time
	window = u.constrainWindow(window)

	obsKey := UsersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: NoRelation,
	}
	probe := u.observer.ContainsDirectStarted(obsKey, subjectID, window)
	defer probe.End()

	u.mu.RLock()
	defer u.mu.RUnlock()

	key := usersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: NoRelation,
	}

	vs, ok := u.tuples[key]
	if !ok {
		// Not found - window max is still correct, min is 0 (always not found)
		result := NewSnapshotWindow(0, window.Max())
		probe.Result(false, result)
		return false, result
	}

	found, actualTime := vs.ContainsWithin(subjectID, window.Max())

	// Return window with min = oldest time the result is valid
	result := NewSnapshotWindow(actualTime, window.Max())
	probe.Result(found, result)
	return found, result
}

// ContainsUsersetSubjectWithin checks if a specific userset subject is in the relation.
// Unlike ContainsDirectWithin, this is for userset subjects (with a non-zero subjectRelation).
// Returns (found, narrowedWindow) where window.Min() == oldest time the answer is valid.
func (u *MultiversionUsersets) ContainsUsersetSubjectWithin(
	objectType TypeID, objectID ID, relation RelationID,
	subjectType TypeID, subjectID ID, subjectRelation RelationID,
	window SnapshotWindow,
) (bool, SnapshotWindow) {
	// Constrain max to replicated time
	window = u.constrainWindow(window)

	obsKey := UsersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	probe := u.observer.ContainsUsersetSubjectStarted(obsKey, subjectID, window)
	defer probe.End()

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
		result := NewSnapshotWindow(0, window.Max())
		probe.Result(false, result)
		return false, result
	}

	found, actualTime := vs.ContainsWithin(subjectID, window.Max())

	result := NewSnapshotWindow(actualTime, window.Max())
	probe.Result(found, result)
	return found, result
}

// GetSubjectBitmapWithin gets the subject bitmap for the given tuple key within the snapshot window.
// Only constrains result by window.Max(); result min is the actual store time of accessed data.
// Returns the bitmap (possibly cloned) and narrowed window where window.Min() == store time.
// Returns nil bitmap if no tuples exist.
func (u *MultiversionUsersets) GetSubjectBitmapWithin(
	objectType TypeID, objectID ID, relation RelationID,
	subjectType TypeID, subjectRelation RelationID,
	window SnapshotWindow,
) (*roaring64.Bitmap, SnapshotWindow) {
	// Constrain max to replicated time
	window = u.constrainWindow(window)

	obsKey := UsersetKey{
		ObjectType:      objectType,
		ObjectID:        objectID,
		Relation:        relation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	probe := u.observer.GetSubjectBitmapStarted(obsKey, window)
	defer probe.End()

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
		// Not found - return window with min=0 (always empty)
		probe.NotFound()
		return nil, NewSnapshotWindow(0, window.Max())
	}

	// If head is within bounds, use it directly
	if vs.HeadTime() <= window.Max() {
		bitmap := vs.Head()
		result := NewSnapshotWindow(vs.HeadTime(), window.Max())
		probe.Result(int(bitmap.GetCardinality()), result)
		return bitmap, result
	}

	// Need historical snapshot
	bitmap, actualTime := vs.SnapshotWithin(window.Max())
	if bitmap == nil {
		probe.NotFound()
		return nil, NewSnapshotWindow(0, window.Max())
	}

	result := NewSnapshotWindow(actualTime, window.Max())
	probe.Result(int(bitmap.GetCardinality()), result)
	return bitmap, result
}

// ValidateTuple checks that the object type, relation, and subject reference
// are valid according to the
func (u *MultiversionUsersets) ValidateTuple(objectType TypeName, relation RelationName, subjectType TypeName, subjectRelation RelationName) error {
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
func (u *MultiversionUsersets) TruncateHistory(minTime StoreTime) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	for _, vs := range u.tuples {
		vs.Truncate(minTime)
	}
}
