package domain

import "fmt"

// TypeName identifies an object type (e.g., "document", "folder", "user").
// This is used for both object types and subject types.
type TypeName string

// RelationName identifies a relation on an object type (e.g., "viewer", "editor").
type RelationName string

// ID is a unique identifier for an object or subject within its type.
// 64 bits to support encoding shard/routing information in high bits.
type ID uint64

// TypeID is a stable numeric identifier for an object type.
// Like protobuf field numbers, these must be stable across schema versions.
// Valid values are 1-255; 0 is reserved.
type TypeID uint8

// RelationID is a stable numeric identifier for a relation.
// Unique within each object type.
// Valid values are 1-255; 0 means "no relation" (direct subject).
type RelationID uint8

// NoRelation is the RelationID for direct subjects (no relation).
const NoRelation RelationID = 0

// Schema defines the complete authorization model, mapping type names to their
// definitions.
type Schema struct {
	Types map[TypeName]*ObjectType

	// typesByID is a reverse lookup from TypeID to ObjectType.
	// Built by Compile().
	typesByID map[TypeID]*ObjectType
}

// ObjectType defines a type of object in the authorization graph (e.g.,
// "document", "folder", "user").
type ObjectType struct {
	// ID is a stable numeric identifier for this type.
	// Must be unique across all types and stable across schema versions.
	ID TypeID
	// Name is the type identifier (e.g., "document").
	Name TypeName
	// Relations maps relation names to their definitions.
	Relations map[RelationName]*Relation

	// RootRelation is the relation that establishes this object's shard root.
	// If set, the subject of a tuple with this relation becomes the object's root.
	// If empty, objects of this type are their own roots.
	RootRelation RelationName

	// relationsByID is a reverse lookup from RelationID to Relation.
	// Built by Schema.Compile().
	relationsByID map[RelationID]*Relation
}

// Relation defines a named relation on an object type. A relation can be
// satisfied by direct membership, by membership in another relation (union),
// or by traversing to another object and checking a relation there (arrow).
//
// Usersets defines how this relation is computed. The relation is the union
// of all usersets. Each userset can reference direct membership (with allowed
// subject types), another relation on this object, or an arrow traversal to
// another object.
type Relation struct {
	// ID is a stable numeric identifier for this relation within its type.
	// Must be unique within the type and stable across schema versions.
	// 0 is reserved for "no relation" (direct subjects).
	ID RelationID
	// Name is the relation identifier (e.g., "viewer", "editor", "parent").
	Name RelationName
	// Usersets defines how this relation is computed. The relation is the union
	// of all usersets.
	Usersets []Userset
}

// SubjectRef specifies an allowed subject type and optional relation for a
// direct userset's TargetTypes.
type SubjectRef struct {
	// Type is the subject's object type (e.g., "user", "group", "folder").
	Type TypeName
	// Relation is the subject's relation (e.g., "member"). Empty string means
	// direct subject (no relation).
	Relation RelationName
}

// Ref creates a SubjectRef for direct subjects (no relation).
// Example: Ref("user") allows @user:1
func Ref(subjectType TypeName) SubjectRef {
	return SubjectRef{Type: subjectType}
}

// RefWithRelation creates a SubjectRef for userset subjects.
// Example: RefWithRelation("group", "member") allows @group:1#member
func RefWithRelation(subjectType TypeName, relation RelationName) SubjectRef {
	return SubjectRef{Type: subjectType, Relation: relation}
}

// Userset defines one way a subject can satisfy a relation. It represents
// either direct membership, a reference to another relation on the same object,
// or an arrow traversal.
type Userset struct {
	// This specifies direct tuple membership with allowed subject types.
	// If non-empty, tuples can be written with these subject types.
	//
	// Examples:
	//   - Ref("user") allows direct user subjects: @user:1
	//   - RefWithRelation("group", "member") allows userset subjects: @group:1#member
	//   - Ref("folder") allows direct folder subjects for parent relations
	This []SubjectRef

	// ComputedRelation references another relation on the same object.
	// For example, "editor" userset on a "viewer" relation means editors are
	// also viewers.
	ComputedRelation RelationName

	// TupleToUserset defines an arrow traversal: follow TuplesetRelation to
	// find target objects, then check ComputedUsersetRelation on those targets.
	// For example, TuplesetRelation="parent", ComputedUsersetRelation="viewer"
	// means "viewers of my parent are also my viewers".
	TupleToUserset *TupleToUserset
}

// TupleToUserset represents an arrow operation: follow a relation to find
// target objects, then check another relation on those targets.
type TupleToUserset struct {
	// TuplesetRelation is the relation to follow to find target objects.
	// For example, "parent" to find the parent folder of a document.
	TuplesetRelation RelationName
	// ComputedUsersetRelation is the relation to check on the target objects.
	// For example, "viewer" to check if the subject is a viewer of the parent.
	ComputedUsersetRelation RelationName
}

// Direct creates a Userset that checks direct tuple membership with the
// specified allowed subject types.
//
// Example:
//
//	Direct(Ref("user"), RefWithRelation("group", "member"))
func Direct(types ...SubjectRef) Userset {
	return Userset{This: types}
}

// Computed creates a Userset that references another relation on the same object.
func Computed(relation RelationName) Userset {
	return Userset{ComputedRelation: relation}
}

// Arrow creates a Userset that follows a relation and checks a permission on
// the target. This is the "tuple to userset" operation in Zanzibar terminology.
func Arrow(throughRelation, checkRelation RelationName) Userset {
	return Userset{
		TupleToUserset: &TupleToUserset{
			TuplesetRelation:        throughRelation,
			ComputedUsersetRelation: checkRelation,
		},
	}
}

// DirectTargetTypes returns the target types from the Direct userset if one
// exists, or nil if the relation has no direct membership.
func (r *Relation) DirectTargetTypes() []SubjectRef {
	for _, us := range r.Usersets {
		if len(us.This) > 0 {
			return us.This
		}
	}
	return nil
}

// Compile builds the reverse lookup maps for efficient ID-based access.
// This must be called after constructing the schema and before using
// the lookup methods.
func (s *Schema) Compile() {
	s.typesByID = make(map[TypeID]*ObjectType, len(s.Types))
	for _, ot := range s.Types {
		s.typesByID[ot.ID] = ot
		ot.relationsByID = make(map[RelationID]*Relation, len(ot.Relations))
		for _, rel := range ot.Relations {
			ot.relationsByID[rel.ID] = rel
		}
	}
}

// TypeByID returns the ObjectType for the given TypeID, or nil if not found.
func (s *Schema) TypeByID(id TypeID) *ObjectType {
	return s.typesByID[id]
}

// TypeByName returns the ObjectType for the given TypeName, or nil if not found.
func (s *Schema) TypeByName(name TypeName) *ObjectType {
	return s.Types[name]
}

// RelationByID returns the Relation for the given RelationID, or nil if not found.
func (ot *ObjectType) RelationByID(id RelationID) *Relation {
	return ot.relationsByID[id]
}

// RelationByName returns the Relation for the given RelationName, or nil if not found.
func (ot *ObjectType) RelationByName(name RelationName) *Relation {
	return ot.Relations[name]
}

// GetTypeID returns the TypeID for the given TypeName.
// Panics if the type is not found (use TypeByName to check first).
func (s *Schema) GetTypeID(name TypeName) TypeID {
	ot := s.Types[name]
	if ot == nil {
		panic("schema: unknown type: " + string(name))
	}
	return ot.ID
}

// GetRelationID returns the RelationID for the given relation on the given type.
// Returns NoRelation (0) if relationName is empty.
// Panics if the type or relation is not found.
func (s *Schema) GetRelationID(typeName TypeName, relationName RelationName) RelationID {
	if relationName == "" {
		return NoRelation
	}
	ot := s.Types[typeName]
	if ot == nil {
		panic("schema: unknown type: " + string(typeName))
	}
	rel := ot.Relations[relationName]
	if rel == nil {
		panic("schema: unknown relation " + string(relationName) + " on type " + string(typeName))
	}
	return rel.ID
}

// RootFor returns the root ObjectRef for an object based on a tuple's components,
// if the tuple establishes a root relationship. Returns NoObject if the tuple does
// not establish a root or if the object type has no root relation.
//
// The root is determined by checking if the object type has a RootRelation defined
// and if the given relation matches it. If so, the subject becomes the root.
func (s *Schema) RootFor(objectTypeName TypeName, relation RelationName, subjectTypeName TypeName, subjectID ExternalID) ObjectRef {
	objectType := s.TypeByName(objectTypeName)
	if objectType == nil || objectType.RootRelation == "" || objectType.RootRelation != relation {
		return NoObject
	}
	subjectType := s.TypeByName(subjectTypeName)
	if subjectType == nil {
		return NoObject
	}
	return ObjectRef{Type: subjectType.ID, ID: subjectID}
}

// ValidateTuple checks that the tuple is valid according to the schema.
// It verifies that:
//   - The object type and relation exist
//   - The subject type exists
//   - The subject relation exists (if specified)
//   - The subject reference is allowed by the relation's direct target types
func (s *Schema) ValidateTuple(t Tuple) error {
	// Check object type exists
	ot := s.TypeByID(t.ObjectType)
	if ot == nil {
		return &ValidationError{Field: "object_type", Message: "unknown object type ID", ID: int(t.ObjectType)}
	}

	// Check relation exists on object type
	rel := ot.RelationByID(t.Relation)
	if rel == nil {
		return &ValidationError{Field: "relation", Message: "unknown relation ID on type " + string(ot.Name), ID: int(t.Relation)}
	}

	// Check subject type exists
	st := s.TypeByID(t.SubjectType)
	if st == nil {
		return &ValidationError{Field: "subject_type", Message: "unknown subject type ID", ID: int(t.SubjectType)}
	}

	// Check subject relation exists if specified
	if t.SubjectRelation != NoRelation {
		subRel := st.RelationByID(t.SubjectRelation)
		if subRel == nil {
			return &ValidationError{Field: "subject_relation", Message: "unknown subject relation ID on type " + string(st.Name), ID: int(t.SubjectRelation)}
		}
	}

	// Check that the subject reference is allowed by the relation's direct types
	targetTypes := rel.DirectTargetTypes()
	if targetTypes == nil {
		return &ValidationError{
			Field:   "relation",
			Message: "relation " + string(ot.Name) + "#" + string(rel.Name) + " does not allow direct tuples",
		}
	}

	// Find matching subject ref
	for _, ref := range targetTypes {
		refType := s.TypeByName(ref.Type)
		if refType == nil {
			continue
		}
		if refType.ID != t.SubjectType {
			continue
		}

		// Check subject relation matches
		if ref.Relation == "" && t.SubjectRelation == NoRelation {
			return nil // Direct subject match
		}
		if ref.Relation != "" {
			expectedRelID := s.GetRelationID(ref.Type, ref.Relation)
			if expectedRelID == t.SubjectRelation {
				return nil // Userset subject match
			}
		}
	}

	// No matching subject ref found
	if t.SubjectRelation == NoRelation {
		return &ValidationError{
			Field:   "subject_type",
			Message: "subject type " + string(st.Name) + " is not allowed for " + string(ot.Name) + "#" + string(rel.Name),
		}
	}

	subRel := st.RelationByID(t.SubjectRelation)
	subRelName := ""
	if subRel != nil {
		subRelName = string(subRel.Name)
	}
	return &ValidationError{
		Field:   "subject_relation",
		Message: "subject " + string(st.Name) + "#" + subRelName + " is not allowed for " + string(ot.Name) + "#" + string(rel.Name),
	}
}

// ValidationError represents a schema validation error.
type ValidationError struct {
	Field   string // Which field failed validation
	Message string // Human-readable error description
	ID      int    // The invalid ID value (if applicable)
}

func (e *ValidationError) Error() string {
	if e.ID != 0 {
		return fmt.Sprintf("%s: %d", e.Message, e.ID)
	}
	return e.Message
}
