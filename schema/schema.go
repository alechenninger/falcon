// Package schema defines types for representing a Zanzibar-style authorization
// schema in code. A schema describes object types and their relations, including
// how relations are computed from other relations or by traversing the graph.
package schema

// TypeName identifies an object type (e.g., "document", "folder", "user").
// This is used for both object types and subject types.
type TypeName string

// RelationName identifies a relation on an object type (e.g., "viewer", "editor").
type RelationName string

// ID is a unique identifier for an object or subject within its type.
type ID uint32

// Schema defines the complete authorization model, mapping type names to their
// definitions.
type Schema struct {
	Types map[TypeName]*ObjectType
}

// ObjectType defines a type of object in the authorization graph (e.g.,
// "document", "folder", "user").
type ObjectType struct {
	// Name is the type identifier (e.g., "document").
	Name TypeName
	// Relations maps relation names to their definitions.
	Relations map[RelationName]*Relation
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
