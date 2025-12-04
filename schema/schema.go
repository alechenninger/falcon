// Package schema defines types for representing a Zanzibar-style authorization
// schema in code. A schema describes object types and their relations, including
// how relations are computed from other relations or by traversing the graph.
package schema

// Schema defines the complete authorization model, mapping type names to their
// definitions.
type Schema struct {
	Types map[string]*ObjectType
}

// ObjectType defines a type of object in the authorization graph (e.g.,
// "document", "folder", "user").
type ObjectType struct {
	// Name is the type identifier (e.g., "document").
	Name string
	// Relations maps relation names to their definitions.
	Relations map[string]*Relation
}

// Relation defines a named relation on an object type. A relation can be
// satisfied by direct membership, by membership in another relation (union),
// or by traversing to another object and checking a relation there (arrow).
//
// If Usersets is empty, this is a "direct-only" relation where subjects are
// stored directly in the tuple bitmap.
//
// If Usersets is non-empty, the relation is computed as the union of all
// usersets. Each userset can reference direct membership, another relation
// on this object, or an arrow traversal to another object.
type Relation struct {
	// Name is the relation identifier (e.g., "viewer", "editor", "parent").
	Name string
	// TargetTypes specifies what subjects can be assigned to this relation.
	// Each entry defines a subject type and optionally a subject relation.
	//
	// Examples:
	//   - Ref("user") allows direct user subjects: @user:1
	//   - Ref("group", "member") allows userset subjects: @group:1#member
	//   - Ref("folder") allows direct folder subjects for parent relations
	TargetTypes []SubjectRef
	// Usersets defines how this relation is computed. If empty, only direct
	// tuple membership is checked. If non-empty, the relation is the union
	// of all usersets.
	Usersets []Userset
}

// SubjectRef specifies an allowed subject type and optional relation for a
// relation's TargetTypes.
type SubjectRef struct {
	// Type is the subject's object type (e.g., "user", "group", "folder").
	Type string
	// Relation is the subject's relation (e.g., "member"). Empty string means
	// direct subject (no relation).
	Relation string
}

// Ref creates a SubjectRef for direct subjects (no relation).
// Example: Ref("user") allows @user:1
func Ref(subjectType string) SubjectRef {
	return SubjectRef{Type: subjectType}
}

// RefWithRelation creates a SubjectRef for userset subjects.
// Example: RefWithRelation("group", "member") allows @group:1#member
func RefWithRelation(subjectType, relation string) SubjectRef {
	return SubjectRef{Type: subjectType, Relation: relation}
}

// Userset defines one way a subject can satisfy a relation. It represents
// either direct membership, a reference to another relation on the same object,
// or an arrow traversal.
type Userset struct {
	// This indicates we should check direct tuple membership for this relation.
	// When true, other fields should be empty.
	This bool

	// ComputedRelation references another relation on the same object.
	// For example, "editor" userset on a "viewer" relation means editors are
	// also viewers.
	ComputedRelation string

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
	TuplesetRelation string
	// ComputedUsersetRelation is the relation to check on the target objects.
	// For example, "viewer" to check if the subject is a viewer of the parent.
	ComputedUsersetRelation string
}

// Direct creates a Userset that checks direct tuple membership.
func Direct() Userset {
	return Userset{This: true}
}

// Computed creates a Userset that references another relation on the same object.
func Computed(relation string) Userset {
	return Userset{ComputedRelation: relation}
}

// Arrow creates a Userset that follows a relation and checks a permission on
// the target. This is the "tuple to userset" operation in Zanzibar terminology.
func Arrow(throughRelation, checkRelation string) Userset {
	return Userset{
		TupleToUserset: &TupleToUserset{
			TuplesetRelation:        throughRelation,
			ComputedUsersetRelation: checkRelation,
		},
	}
}
