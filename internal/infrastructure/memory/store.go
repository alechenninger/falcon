package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/alechenninger/falcon/internal/domain"
)

// Store implements domain.Store and domain.ChangeStream for testing.
// Writes immediately emit changes to subscribers with sequential times.
type Store struct {
	mu            sync.RWMutex
	tuples        map[tupleKey]struct{}
	idToObjectRef map[domain.ID]domain.ObjectRef // internal ID -> (type, external ID)
	objectRefToID map[domain.ObjectRef]domain.ID // (type, external ID) -> internal ID
	nextID        domain.ID
	nextTime      domain.StoreTime
	subscribers   []chan domain.Change
}

// tupleKey is the map key for deduplication.
type tupleKey struct {
	ObjectType      domain.TypeID
	ObjectID        domain.ID
	Relation        domain.RelationID
	SubjectType     domain.TypeID
	SubjectID       domain.ID
	SubjectRelation domain.RelationID
}

func toKey(t domain.Tuple) tupleKey {
	return tupleKey{
		ObjectType:      t.ObjectType,
		ObjectID:        t.ObjectID,
		Relation:        t.Relation,
		SubjectType:     t.SubjectType,
		SubjectID:       t.SubjectID,
		SubjectRelation: t.SubjectRelation,
	}
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		tuples:        make(map[tupleKey]struct{}),
		idToObjectRef: make(map[domain.ID]domain.ObjectRef),
		objectRefToID: make(map[domain.ObjectRef]domain.ID),
		nextID:        1, // Start at 1 so 0 can be "no ID"
		nextTime:      1, // Start at 1 so 0 means "from beginning"
	}
}

// Begin starts a new transaction.
// The store is locked for the duration of the transaction (simple but correct for testing).
func (s *Store) Begin(ctx context.Context) (domain.Tx, error) {
	s.mu.Lock()
	return &memTx{
		store:             s,
		provisionedNextID: s.nextID, // snapshot for rollback
	}, nil
}

// memTx implements domain.Tx for the in-memory store.
// Holds the store lock for the entire transaction duration.
type memTx struct {
	store     *Store
	mutations []domain.Mutation
	// provisioned tracks IDs created in this tx (not yet durable until commit)
	provisioned       map[domain.ObjectRef]domain.ID // (type, external) -> internal
	provisionedByID   map[domain.ID]domain.ObjectRef // internal -> (type, external) (for Contains lookups)
	provisionedNextID domain.ID                      // snapshot of nextID at tx start for rollback
	done              bool                           // true after commit or rollback
}

// GetID returns the internal ID for an object reference.
// Returns domain.ErrIDNotFound if the external ID is not mapped.
func (t *memTx) GetID(ctx context.Context, ref domain.ObjectRef) (domain.ID, error) {
	if t.done {
		return 0, fmt.Errorf("transaction already finished")
	}
	// Check transaction-local IDs first
	if id, ok := t.provisioned[ref]; ok {
		return id, nil
	}
	// Check committed store IDs
	if id, ok := t.store.objectRefToID[ref]; ok {
		return id, nil
	}
	return 0, domain.ErrIDNotFound
}

// GetOrProvisionID returns the internal ID for an object reference, creating a new
// mapping if one does not exist.
//
// The root parameter specifies the shard root for this object. Currently this is
// ignored and IDs are provisioned from a global sequence. Future implementations
// will use the root to encode shard information in the high bits of the ID.
func (t *memTx) GetOrProvisionID(ctx context.Context, ref domain.ObjectRef, root domain.ObjectRef) (domain.ID, error) {
	// TODO: Use root to determine shard-aware ID provisioning.
	// For now, we ignore root and use a global sequence.
	_ = root

	if t.done {
		return 0, fmt.Errorf("transaction already finished")
	}
	// Check transaction-local IDs first
	if id, ok := t.provisioned[ref]; ok {
		return id, nil
	}
	// Check committed store IDs
	if id, ok := t.store.objectRefToID[ref]; ok {
		return id, nil
	}
	// Provision new ID (in transaction only, not durable yet)
	id := t.store.nextID
	t.store.nextID++
	if t.provisioned == nil {
		t.provisioned = make(map[domain.ObjectRef]domain.ID)
		t.provisionedByID = make(map[domain.ID]domain.ObjectRef)
	}
	t.provisioned[ref] = id
	t.provisionedByID[id] = ref
	return id, nil
}

// Write accumulates mutations to be applied on commit.
func (t *memTx) Write(ctx context.Context, mutations []domain.Mutation) error {
	if t.done {
		return fmt.Errorf("transaction already finished")
	}
	t.mutations = append(t.mutations, mutations...)
	return nil
}

// Contains checks if any tuple matches the predicate.
func (t *memTx) Contains(ctx context.Context, predicate domain.TuplePredicate) (bool, error) {
	if t.done {
		return false, fmt.Errorf("transaction already finished")
	}
	for key := range t.store.tuples {
		tuple := keyToTuple(key)
		if matchesPredicate(tuple, predicate, t.store.idToObjectRef, t.provisionedByID) {
			return true, nil
		}
	}
	return false, nil
}

// matchesPredicate evaluates whether a tuple matches the given predicate.
// idToRef is the committed store mappings, txIDToRef is for uncommitted tx-local mappings.
func matchesPredicate(t domain.Tuple, p domain.TuplePredicate, idToRef, txIDToRef map[domain.ID]domain.ObjectRef) bool {
	switch pred := p.(type) {
	case domain.FieldPredicate:
		return matchesFieldPredicate(t, pred, idToRef, txIDToRef)
	case domain.AndPredicate:
		for _, child := range pred.Predicates {
			if !matchesPredicate(t, child, idToRef, txIDToRef) {
				return false
			}
		}
		return true
	case domain.OrPredicate:
		for _, child := range pred.Predicates {
			if matchesPredicate(t, child, idToRef, txIDToRef) {
				return true
			}
		}
		return false
	case domain.NotPredicate:
		return !matchesPredicate(t, pred.Predicate, idToRef, txIDToRef)
	default:
		return false
	}
}

// lookupObjectRef checks both committed and tx-local mappings.
func lookupObjectRef(id domain.ID, idToRef, txIDToRef map[domain.ID]domain.ObjectRef) (domain.ObjectRef, bool) {
	if ref, ok := txIDToRef[id]; ok {
		return ref, true
	}
	if ref, ok := idToRef[id]; ok {
		return ref, true
	}
	return domain.ObjectRef{}, false
}

func matchesFieldPredicate(t domain.Tuple, pred domain.FieldPredicate, idToRef, txIDToRef map[domain.ID]domain.ObjectRef) bool {
	switch pred.Field {
	case domain.FieldObjectType:
		return compareIntValues(uint64(t.ObjectType), pred.Op, pred.Value)
	case domain.FieldObjectID:
		// Compare ObjectRef (type + external ID)
		ref, ok := lookupObjectRef(t.ObjectID, idToRef, txIDToRef)
		if !ok {
			return false
		}
		return compareObjectRef(ref, pred.Op, pred.Value)
	case domain.FieldRelation:
		return compareIntValues(uint64(t.Relation), pred.Op, pred.Value)
	case domain.FieldSubjectType:
		return compareIntValues(uint64(t.SubjectType), pred.Op, pred.Value)
	case domain.FieldSubjectID:
		// Compare ObjectRef (type + external ID)
		ref, ok := lookupObjectRef(t.SubjectID, idToRef, txIDToRef)
		if !ok {
			return false
		}
		return compareObjectRef(ref, pred.Op, pred.Value)
	case domain.FieldSubjectRelation:
		return compareIntValues(uint64(t.SubjectRelation), pred.Op, pred.Value)
	default:
		return false
	}
}

// compareObjectRef compares a tuple's ObjectRef against a predicate value.
// The predicate value should be an ObjectRef for Eq/Neq/StartsWith comparisons.
func compareObjectRef(tupleRef domain.ObjectRef, op domain.CompareOp, predVal any) bool {
	predRef, ok := predVal.(domain.ObjectRef)
	if !ok {
		// Fallback: might be just an ExternalID string for backward compatibility
		if extID, ok := predVal.(string); ok {
			return compareStringValues(string(tupleRef.ID), op, extID)
		}
		if extID, ok := predVal.(domain.ExternalID); ok {
			return compareStringValues(string(tupleRef.ID), op, string(extID))
		}
		return false
	}

	switch op {
	case domain.OpEq:
		return tupleRef.Type == predRef.Type && tupleRef.ID == predRef.ID
	case domain.OpNeq:
		return tupleRef.Type != predRef.Type || tupleRef.ID != predRef.ID
	case domain.OpStartsWith:
		return tupleRef.Type == predRef.Type && strings.HasPrefix(string(tupleRef.ID), string(predRef.ID))
	case domain.OpLt:
		// Lexicographic: compare type first, then external ID
		if tupleRef.Type != predRef.Type {
			return tupleRef.Type < predRef.Type
		}
		return tupleRef.ID < predRef.ID
	case domain.OpLte:
		if tupleRef.Type != predRef.Type {
			return tupleRef.Type < predRef.Type
		}
		return tupleRef.ID <= predRef.ID
	case domain.OpGt:
		if tupleRef.Type != predRef.Type {
			return tupleRef.Type > predRef.Type
		}
		return tupleRef.ID > predRef.ID
	case domain.OpGte:
		if tupleRef.Type != predRef.Type {
			return tupleRef.Type > predRef.Type
		}
		return tupleRef.ID >= predRef.ID
	default:
		return false
	}
}

func compareIntValues(fieldVal uint64, op domain.CompareOp, predVal any) bool {
	pv := toUint64(predVal)

	switch op {
	case domain.OpEq:
		return fieldVal == pv
	case domain.OpNeq:
		return fieldVal != pv
	case domain.OpLt:
		return fieldVal < pv
	case domain.OpLte:
		return fieldVal <= pv
	case domain.OpGt:
		return fieldVal > pv
	case domain.OpGte:
		return fieldVal >= pv
	default:
		return false
	}
}

func compareStringValues(fieldVal string, op domain.CompareOp, predVal any) bool {
	pv, ok := predVal.(string)
	if !ok {
		return false
	}

	switch op {
	case domain.OpEq:
		return fieldVal == pv
	case domain.OpNeq:
		return fieldVal != pv
	case domain.OpLt:
		return fieldVal < pv
	case domain.OpLte:
		return fieldVal <= pv
	case domain.OpGt:
		return fieldVal > pv
	case domain.OpGte:
		return fieldVal >= pv
	case domain.OpStartsWith:
		return strings.HasPrefix(fieldVal, pv)
	default:
		return false
	}
}

func toUint64(v any) uint64 {
	switch val := v.(type) {
	case domain.TypeID:
		return uint64(val)
	case domain.RelationID:
		return uint64(val)
	case domain.ID:
		return uint64(val)
	case uint64:
		return val
	case int64:
		return uint64(val)
	case uint32:
		return uint64(val)
	case int32:
		return uint64(val)
	case int:
		return uint64(val)
	default:
		return 0
	}
}

// Commit applies all accumulated mutations and releases the store lock.
func (t *memTx) Commit(ctx context.Context) error {
	if t.done {
		return nil // Already finished, no-op
	}
	t.done = true
	defer t.store.mu.Unlock()

	// Persist provisioned IDs to the store
	for ref, id := range t.provisioned {
		t.store.objectRefToID[ref] = id
		t.store.idToObjectRef[id] = ref
	}

	if len(t.mutations) == 0 {
		return nil
	}

	// Apply all mutations at the same time
	time := t.store.nextTime
	t.store.nextTime++

	for _, m := range t.mutations {
		key := toKey(m.Tuple)
		switch m.Op {
		case domain.OpInsert:
			if _, exists := t.store.tuples[key]; !exists {
				t.store.tuples[key] = struct{}{}
				change := domain.Change{Time: time, Op: domain.OpInsert, Tuple: m.Tuple}
				t.store.emitChange(change)
			}
		case domain.OpDelete:
			if _, exists := t.store.tuples[key]; exists {
				delete(t.store.tuples, key)
				change := domain.Change{Time: time, Op: domain.OpDelete, Tuple: m.Tuple}
				t.store.emitChange(change)
			}
		}
	}

	return nil
}

// Rollback discards all accumulated mutations and releases the store lock.
func (t *memTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil // Already finished, no-op
	}
	t.done = true
	// Reset nextID to discard any provisioned IDs
	t.store.nextID = t.provisionedNextID
	t.mutations = nil
	t.provisioned = nil
	t.provisionedByID = nil
	t.store.mu.Unlock()
	return nil
}

// emitChange sends a change to all subscribers without blocking.
// Must be called with s.mu held.
func (s *Store) emitChange(change domain.Change) {
	for _, ch := range s.subscribers {
		select {
		case ch <- change:
		default:
			// Subscriber not keeping up, drop the change
		}
	}
}

// LoadAll returns an iterator over all tuples currently in the store.
func (s *Store) LoadAll(ctx context.Context) (domain.TupleIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]domain.Tuple, 0, len(s.tuples))
	for key := range s.tuples {
		result = append(result, keyToTuple(key))
	}
	return domain.NewSliceIterator(result), nil
}

func keyToTuple(k tupleKey) domain.Tuple {
	return domain.Tuple{
		ObjectType:      k.ObjectType,
		ObjectID:        k.ObjectID,
		Relation:        k.Relation,
		SubjectType:     k.SubjectType,
		SubjectID:       k.SubjectID,
		SubjectRelation: k.SubjectRelation,
	}
}

// Close is a no-op for the in-memory store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscriber channels
	for _, ch := range s.subscribers {
		close(ch)
	}
	s.subscribers = nil
	return nil
}

// Subscribe returns a channel that receives changes after the given time.
// The channel is closed when Close() is called or the context is canceled.
func (s *Store) Subscribe(ctx context.Context, after domain.StoreTime) (<-chan domain.Change, <-chan error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Buffer some changes to avoid blocking writers
	ch := make(chan domain.Change, 100)
	errCh := make(chan error, 1)
	s.subscribers = append(s.subscribers, ch)

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		// Remove this subscriber
		for i, sub := range s.subscribers {
			if sub == ch {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}()

	return ch, errCh
}

// CurrentTime returns the latest time (the next time minus 1).
func (s *Store) CurrentTime(ctx context.Context) (domain.StoreTime, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nextTime == 1 {
		return 0, nil
	}
	return s.nextTime - 1, nil
}

// Compile-time interface checks
var (
	_ domain.Store        = (*Store)(nil)
	_ domain.ChangeStream = (*Store)(nil)
)
