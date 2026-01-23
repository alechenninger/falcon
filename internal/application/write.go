package application

import (
	"context"
	"fmt"

	"github.com/alechenninger/falcon/internal/domain"
)

// ExternalTupleRef represents an authorization tuple using external string identifiers.
// This is the application-layer representation used in API requests.
//
// For direct subjects: ObjectType:ObjectID#Relation@SubjectType:SubjectID
// Example: document:doc-abc#viewer@user:alice
//
// For userset subjects: ObjectType:ObjectID#Relation@SubjectType:SubjectID#SubjectRelation
// Example: document:doc-abc#viewer@group:engineering#member
type ExternalTupleRef struct {
	ObjectType      domain.TypeName
	ObjectID        domain.ExternalID
	Relation        domain.RelationName
	SubjectType     domain.TypeName
	SubjectID       domain.ExternalID
	SubjectRelation domain.RelationName // Empty for direct subjects
}

// MutationOp represents the type of mutation (insert or delete).
type MutationOp int

const (
	// Insert adds a tuple.
	Insert MutationOp = iota
	// Delete removes a tuple.
	Delete
)

// Mutation represents a single tuple change in a write request.
type Mutation struct {
	Op    MutationOp
	Tuple ExternalTupleRef
}

// WriteCommand represents a batch write operation using external identifiers.
// This is the application-layer representation used in API requests.
type WriteCommand struct {
	// Mutations are the tuple changes to apply.
	Mutations []Mutation

	// Precondition is an optional condition that must be satisfied for the write
	// to proceed. The predicate is evaluated atomically within the transaction.
	// If the predicate does not match any tuples, the write is rejected with
	// [ErrPreconditionFailed].
	//
	// To require a tuple exists: use the predicate directly.
	// To require no tuple exists: wrap with [PNot].
	// To combine conditions: use [PAnd] or [POr].
	Precondition Predicate
}

// ErrPreconditionFailed is returned when the write precondition is not satisfied.
var ErrPreconditionFailed = fmt.Errorf("precondition failed: no matching tuples")

// TupleFieldRef identifies a field of a tuple for use in predicates.
type TupleFieldRef int

const (
	// RefObjectType refers to the object's type name.
	RefObjectType TupleFieldRef = iota
	// RefObjectID refers to the object's external ID.
	RefObjectID
	// RefRelation refers to the relation name.
	RefRelation
	// RefSubjectType refers to the subject's type name.
	RefSubjectType
	// RefSubjectID refers to the subject's external ID.
	RefSubjectID
	// RefSubjectRelation refers to the subject's relation name (for usersets).
	RefSubjectRelation
)

// PredicateOp specifies how to compare a field value.
type PredicateOp int

const (
	// PredEq matches if the field equals the value.
	PredEq PredicateOp = iota
	// PredNeq matches if the field does not equal the value.
	PredNeq
	// PredLt matches if the field is less than the value (lexicographic for strings).
	PredLt
	// PredLte matches if the field is less than or equal to the value.
	PredLte
	// PredGt matches if the field is greater than the value.
	PredGt
	// PredGte matches if the field is greater than or equal to the value.
	PredGte
	// PredStartsWith matches if the field value starts with the given prefix.
	// Only valid for ID fields (ObjectID, SubjectID).
	PredStartsWith
)

// Predicate defines a condition over tuples using external identifiers.
// This is the application-layer representation that gets converted to
// domain.TuplePredicate for storage operations.
type Predicate interface {
	isPredicate()
}

// FieldPredicate compares a tuple field to a value.
// For type fields (ObjectType, SubjectType), Value should be a TypeName.
// For relation fields (Relation, SubjectRelation), Value should be a RelationRef.
// For ID fields (ObjectID, SubjectID), Value should be an ObjectIDRef.
type FieldPredicate struct {
	Field TupleFieldRef
	Op    PredicateOp
	Value any // TypeName, RelationRef, or ObjectIDRef depending on Field
}

func (FieldPredicate) isPredicate() {}

// AndPredicate matches if all child predicates match.
type AndPredicate struct {
	Predicates []Predicate
}

func (AndPredicate) isPredicate() {}

// OrPredicate matches if any child predicate matches.
type OrPredicate struct {
	Predicates []Predicate
}

func (OrPredicate) isPredicate() {}

// NotPredicate inverts the result of its child predicate.
type NotPredicate struct {
	Predicate Predicate
}

func (NotPredicate) isPredicate() {}

// Predicate constructors for convenient usage.

// PEq creates a predicate that matches if the field equals the value.
func PEq(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredEq, Value: value}
}

// PNeq creates a predicate that matches if the field does not equal the value.
func PNeq(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredNeq, Value: value}
}

// PLt creates a predicate that matches if the field is less than the value.
func PLt(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredLt, Value: value}
}

// PLte creates a predicate that matches if the field is less than or equal to the value.
func PLte(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredLte, Value: value}
}

// PGt creates a predicate that matches if the field is greater than the value.
func PGt(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredGt, Value: value}
}

// PGte creates a predicate that matches if the field is greater than or equal to the value.
func PGte(field TupleFieldRef, value any) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredGte, Value: value}
}

// PStartsWith creates a predicate that matches if the field value starts with the prefix.
// Only valid for ObjectID and SubjectID fields.
// The ObjectIDRef.ID is used as the prefix to match against.
func PStartsWith(field TupleFieldRef, ref ObjectIDRef) FieldPredicate {
	return FieldPredicate{Field: field, Op: PredStartsWith, Value: ref}
}

// PAnd creates a predicate that matches if all predicates match.
func PAnd(predicates ...Predicate) AndPredicate {
	return AndPredicate{Predicates: predicates}
}

// POr creates a predicate that matches if any predicate matches.
func POr(predicates ...Predicate) OrPredicate {
	return OrPredicate{Predicates: predicates}
}

// PNot creates a predicate that inverts the result.
func PNot(predicate Predicate) NotPredicate {
	return NotPredicate{Predicate: predicate}
}

// WriteResult contains the outcome of a write operation.
// Note: CommitTime is not available because PostgreSQL cannot reliably expose
// the exact commit LSN from within a transaction. For write acknowledgements,
// use a WriteToken tracked through the ChangeStream instead.
type WriteResult struct {
	// Reserved for future use (e.g., affected count, write token).
}

// WriteServiceConfig holds configuration for WriteService.
type WriteServiceConfig struct {
	Store  domain.Store
	Schema *domain.Schema
}

// WriteService orchestrates write operations, providing validation,
// ID mapping, and coordinating with the underlying store.
type WriteService struct {
	store  domain.Store
	schema *domain.Schema
}

// NewWriteService creates a new WriteService.
func NewWriteService(cfg WriteServiceConfig) *WriteService {
	return &WriteService{
		store:  cfg.Store,
		schema: cfg.Schema,
	}
}

// Write validates and executes a write request.
// Converts external identifiers to internal IDs (provisioning new IDs as needed),
// validates against the schema, evaluates the precondition (if any), and executes
// the write within a transaction.
//
// If the precondition is set and does not match any tuples, returns
// [ErrPreconditionFailed] and no mutations are applied.
func (s *WriteService) Write(ctx context.Context, cmd WriteCommand) (WriteResult, error) {
	// Nothing to do if no mutations
	if len(cmd.Mutations) == 0 {
		return WriteResult{}, nil
	}

	// Begin transaction - all ID resolution happens within the transaction
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return WriteResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// First we map from external IDs to internal IDs across mutations and preconditions
	mutations, err := s.toInternalMutations(ctx, tx, cmd.Mutations)
	if err != nil {
		return WriteResult{}, err
	}

	// Convert and evaluate precondition if present
	if cmd.Precondition != nil {
		precondition, err := s.toInternalPredicate(cmd.Precondition)
		if err != nil {
			return WriteResult{}, fmt.Errorf("precondition: %w", err)
		}
		matches, err := tx.Contains(ctx, precondition)
		if err != nil {
			return WriteResult{}, fmt.Errorf("precondition: %w", err)
		}
		if !matches {
			return WriteResult{}, ErrPreconditionFailed
		}
	}

	// Apply mutations
	if err := tx.Write(ctx, mutations); err != nil {
		return WriteResult{}, fmt.Errorf("write: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return WriteResult{}, fmt.Errorf("commit: %w", err)
	}

	return WriteResult{}, nil
}

// externalObjectKey identifies an object by its type name and external ID.
// Used for grouping tuples by object during mutation processing.
type externalObjectKey struct {
	Type domain.TypeName
	ID   domain.ExternalID
}

// toInternalMutations converts application-level mutations to domain mutations.
// Tuples are grouped by object to ensure consistent root detection: if any tuple
// in the batch establishes a root for an object, that root is used when
// provisioning the object's ID, regardless of tuple order.
func (s *WriteService) toInternalMutations(ctx context.Context, tx domain.Tx, mutations []Mutation) ([]domain.Mutation, error) {
	if len(mutations) == 0 {
		return nil, nil
	}

	// Group mutation indices by object (type + external ID)
	objectMutations := make(map[externalObjectKey][]int)
	for i, m := range mutations {
		key := externalObjectKey{Type: m.Tuple.ObjectType, ID: m.Tuple.ObjectID}
		objectMutations[key] = append(objectMutations[key], i)
	}

	// Provision object IDs: for each unique object, find root from its tuples and provision once
	objectIDs := make(map[externalObjectKey]domain.ID)
	for key, indices := range objectMutations {
		// Collect the tuples for this object
		tuples := make([]ExternalTupleRef, len(indices))
		for i, idx := range indices {
			tuples[i] = mutations[idx].Tuple
		}

		// Find root among this object's tuples
		root := s.findRootForObject(tuples)

		// Look up the object type to get the TypeID
		objectType := s.schema.TypeByName(key.Type)
		if objectType == nil {
			return nil, fmt.Errorf("mutation %d: unknown object type: %s", indices[0], key.Type)
		}

		// Provision the ID once with the correct root
		id, err := tx.GetOrProvisionID(ctx, domain.ObjectRef{
			Type: objectType.ID,
			ID:   key.ID,
		}, root)
		if err != nil {
			return nil, fmt.Errorf("mutation %d: provisioning object ID: %w", indices[0], err)
		}
		objectIDs[key] = id
	}

	// Provision subject IDs (subjects are provisioned without root info from these tuples)
	subjectIDs := make(map[externalObjectKey]domain.ID)
	for _, m := range mutations {
		key := externalObjectKey{Type: m.Tuple.SubjectType, ID: m.Tuple.SubjectID}
		if _, exists := subjectIDs[key]; exists {
			continue
		}
		// Also skip if this subject is already provisioned as an object
		if _, exists := objectIDs[key]; exists {
			subjectIDs[key] = objectIDs[key]
			continue
		}

		subjectType := s.schema.TypeByName(key.Type)
		if subjectType == nil {
			// Error will be caught during tuple conversion
			continue
		}

		id, err := tx.GetOrProvisionID(ctx, domain.ObjectRef{
			Type: subjectType.ID,
			ID:   key.ID,
		}, domain.NoObject)
		if err != nil {
			// Error will be caught during tuple conversion with proper index
			continue
		}
		subjectIDs[key] = id
	}

	// Convert each mutation using the pre-provisioned IDs
	result := make([]domain.Mutation, len(mutations))
	for i, m := range mutations {
		objectKey := externalObjectKey{Type: m.Tuple.ObjectType, ID: m.Tuple.ObjectID}
		subjectKey := externalObjectKey{Type: m.Tuple.SubjectType, ID: m.Tuple.SubjectID}

		objectID := objectIDs[objectKey]
		subjectID := subjectIDs[subjectKey]

		tuple, err := s.toInternalTuple(ctx, tx, m.Tuple, objectID, subjectID)
		if err != nil {
			return nil, fmt.Errorf("mutation %d: %w", i, err)
		}

		var op domain.ChangeOp
		switch m.Op {
		case Insert:
			op = domain.OpInsert
		case Delete:
			op = domain.OpDelete
		default:
			return nil, fmt.Errorf("mutation %d: unknown operation %d", i, m.Op)
		}

		result[i] = domain.Mutation{Op: op, Tuple: tuple}
	}
	return result, nil
}

// findRootForObject scans an object's tuples to find one that establishes a root.
// Returns the root ObjectRef if found, or NoObject if no root relation exists.
func (s *WriteService) findRootForObject(tuples []ExternalTupleRef) domain.ObjectRef {
	if len(tuples) == 0 {
		return domain.NoObject
	}

	objectType := s.schema.TypeByName(tuples[0].ObjectType)
	if objectType == nil || objectType.RootRelation == "" {
		return domain.NoObject
	}

	for _, t := range tuples {
		if t.Relation == objectType.RootRelation {
			return s.schema.RootFor(t.ObjectType, t.Relation, t.SubjectType, t.SubjectID)
		}
	}
	return domain.NoObject
}

// toInternalTuple converts a TupleRef to a domain.Tuple using pre-provisioned IDs.
// The objectID and subjectID should already be provisioned by the caller.
// Validates the tuple against the schema before returning.
func (s *WriteService) toInternalTuple(ctx context.Context, tx domain.Tx, ref ExternalTupleRef, objectID, subjectID domain.ID) (domain.Tuple, error) {
	// Look up type IDs
	objectType := s.schema.TypeByName(ref.ObjectType)
	if objectType == nil {
		return domain.Tuple{}, fmt.Errorf("unknown object type: %s", ref.ObjectType)
	}

	relation := objectType.RelationByName(ref.Relation)
	if relation == nil {
		return domain.Tuple{}, fmt.Errorf("unknown relation %s on type %s", ref.Relation, ref.ObjectType)
	}

	subjectType := s.schema.TypeByName(ref.SubjectType)
	if subjectType == nil {
		return domain.Tuple{}, fmt.Errorf("unknown subject type: %s", ref.SubjectType)
	}

	var subjectRelationID domain.RelationID
	if ref.SubjectRelation != "" {
		subjectRelation := subjectType.RelationByName(ref.SubjectRelation)
		if subjectRelation == nil {
			return domain.Tuple{}, fmt.Errorf("unknown subject relation %s on type %s", ref.SubjectRelation, ref.SubjectType)
		}
		subjectRelationID = subjectRelation.ID
	}

	tuple := domain.Tuple{
		ObjectType:      objectType.ID,
		ObjectID:        objectID,
		Relation:        relation.ID,
		SubjectType:     subjectType.ID,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelationID,
	}

	// Validate the tuple against the schema (checks allowed subject types, etc.)
	if err := s.schema.ValidateTuple(tuple); err != nil {
		return domain.Tuple{}, err
	}

	return tuple, nil
}

// toInternalPredicate converts an application-layer predicate to a domain predicate.
// Type names and relation names are resolved via the schema.
// ID fields (ObjectID, SubjectID) pass through external IDs as strings;
// the Store handles the join with the ID mapping table.
func (s *WriteService) toInternalPredicate(pred Predicate) (domain.TuplePredicate, error) {
	switch p := pred.(type) {
	case FieldPredicate:
		return s.toInternalFieldPredicate(p)
	case AndPredicate:
		children := make([]domain.TuplePredicate, len(p.Predicates))
		for i, child := range p.Predicates {
			converted, err := s.toInternalPredicate(child)
			if err != nil {
				return nil, err
			}
			children[i] = converted
		}
		return domain.And(children...), nil
	case OrPredicate:
		children := make([]domain.TuplePredicate, len(p.Predicates))
		for i, child := range p.Predicates {
			converted, err := s.toInternalPredicate(child)
			if err != nil {
				return nil, err
			}
			children[i] = converted
		}
		return domain.Or(children...), nil
	case NotPredicate:
		converted, err := s.toInternalPredicate(p.Predicate)
		if err != nil {
			return nil, err
		}
		return domain.Not(converted), nil
	default:
		return nil, fmt.Errorf("unknown predicate type: %T", pred)
	}
}

func (s *WriteService) toInternalFieldPredicate(p FieldPredicate) (domain.FieldPredicate, error) {
	domainField, err := s.toDomainField(p.Field)
	if err != nil {
		return domain.FieldPredicate{}, err
	}

	domainOp := toDomainOp(p.Op)

	domainValue, err := s.toDomainValue(p.Field, p.Value)
	if err != nil {
		return domain.FieldPredicate{}, err
	}

	return domain.FieldPredicate{
		Field: domainField,
		Op:    domainOp,
		Value: domainValue,
	}, nil
}

func (s *WriteService) toDomainField(f TupleFieldRef) (domain.TupleField, error) {
	switch f {
	case RefObjectType:
		return domain.FieldObjectType, nil
	case RefObjectID:
		return domain.FieldObjectID, nil
	case RefRelation:
		return domain.FieldRelation, nil
	case RefSubjectType:
		return domain.FieldSubjectType, nil
	case RefSubjectID:
		return domain.FieldSubjectID, nil
	case RefSubjectRelation:
		return domain.FieldSubjectRelation, nil
	default:
		return 0, fmt.Errorf("unknown field: %d", f)
	}
}

func toDomainOp(op PredicateOp) domain.CompareOp {
	switch op {
	case PredEq:
		return domain.OpEq
	case PredNeq:
		return domain.OpNeq
	case PredLt:
		return domain.OpLt
	case PredLte:
		return domain.OpLte
	case PredGt:
		return domain.OpGt
	case PredGte:
		return domain.OpGte
	case PredStartsWith:
		return domain.OpStartsWith
	default:
		return domain.OpEq
	}
}

func (s *WriteService) toDomainValue(field TupleFieldRef, value any) (any, error) {
	switch field {
	case RefObjectType, RefSubjectType:
		// Expect TypeName
		typeName, ok := value.(domain.TypeName)
		if !ok {
			return nil, fmt.Errorf("expected TypeName for type field, got %T", value)
		}
		t := s.schema.TypeByName(typeName)
		if t == nil {
			return nil, fmt.Errorf("unknown type: %s", typeName)
		}
		return t.ID, nil

	case RefRelation:
		// Expect RelationRef (type + relation) to resolve the RelationID
		switch v := value.(type) {
		case domain.RelationName:
			// Without knowing the type, we can't resolve the ID.
			return nil, fmt.Errorf("relation field requires RelationRef{Type, Relation}, not just RelationName")
		case RelationRef:
			t := s.schema.TypeByName(v.Type)
			if t == nil {
				return nil, fmt.Errorf("unknown type: %s", v.Type)
			}
			r := t.RelationByName(v.Relation)
			if r == nil {
				return nil, fmt.Errorf("unknown relation %s on type %s", v.Relation, v.Type)
			}
			return r.ID, nil
		default:
			return nil, fmt.Errorf("expected RelationRef for relation field, got %T", value)
		}

	case RefSubjectRelation:
		// Similar to RefRelation
		switch v := value.(type) {
		case domain.RelationName:
			return nil, fmt.Errorf("subject relation field requires RelationRef{Type, Relation}, not just RelationName")
		case RelationRef:
			t := s.schema.TypeByName(v.Type)
			if t == nil {
				return nil, fmt.Errorf("unknown type: %s", v.Type)
			}
			r := t.RelationByName(v.Relation)
			if r == nil {
				return nil, fmt.Errorf("unknown relation %s on type %s", v.Relation, v.Type)
			}
			return r.ID, nil
		default:
			return nil, fmt.Errorf("expected RelationRef for subject relation field, got %T", value)
		}

	case RefObjectID:
		// Expect ObjectIDRef which contains both type and external ID
		ref, ok := value.(ObjectIDRef)
		if !ok {
			return nil, fmt.Errorf("expected ObjectIDRef for ObjectID field, got %T", value)
		}
		t := s.schema.TypeByName(ref.Type)
		if t == nil {
			return nil, fmt.Errorf("unknown type: %s", ref.Type)
		}
		return domain.ObjectRef{Type: t.ID, ID: ref.ID}, nil

	case RefSubjectID:
		// Expect ObjectIDRef which contains both type and external ID
		ref, ok := value.(ObjectIDRef)
		if !ok {
			return nil, fmt.Errorf("expected ObjectIDRef for SubjectID field, got %T", value)
		}
		t := s.schema.TypeByName(ref.Type)
		if t == nil {
			return nil, fmt.Errorf("unknown type: %s", ref.Type)
		}
		return domain.ObjectRef{Type: t.ID, ID: ref.ID}, nil

	default:
		return nil, fmt.Errorf("unknown field: %d", field)
	}
}

// RelationRef identifies a relation on a specific type.
// Used in predicates where relation names need type context for ID resolution.
type RelationRef struct {
	Type     domain.TypeName
	Relation domain.RelationName
}

// ObjectIDRef identifies an object by its type and external ID.
// Used in predicates where external IDs need type context for ID resolution.
// This is the application-layer equivalent of [domain.ObjectRef].
type ObjectIDRef struct {
	Type domain.TypeName
	ID   domain.ExternalID
}
