package domain

import (
	"context"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// ShardID identifies a shard in a distributed graph.
type ShardID string

// Router determines which shard owns a given object.
// This is called during Check and CheckUnion to route requests.
type Router func(objectType TypeID, objectID ID) ShardID

// RelationCheck represents a check against a single object type's relation.
// Used by CheckUnion to batch multiple checks with independent windows.
type RelationCheck struct {
	ObjectType TypeID
	ObjectIDs  *roaring64.Bitmap
	Relation   RelationID
	Window     SnapshotWindow // Window narrowed based on reading this type's data
}

// DependentSet identifies objects that were relevant to a check result.
type DependentSet struct {
	ObjectType TypeID
	Relation   RelationID

	// ObjectIDs identifies the specific objects that mattered.
	// If nil, means "all objects from the corresponding input check" (optimization).
	// If non-nil, the specific subset (e.g., single matching ID for union positive).
	ObjectIDs *roaring64.Bitmap
}

// CheckResult represents the outcome of a check with provenance information.
type CheckResult struct {
	Found         bool
	DependentSets []DependentSet
	Window        SnapshotWindow // combined window for the result
}

// VisitedKey tracks nodes visited during graph traversal for cycle detection.
type VisitedKey struct {
	ObjectType TypeID
	ObjectID   ID
	Relation   RelationID
}

// Graph provides core authorization check operations.
// This interface can be implemented by both local and remote graphs.
type Graph interface {
	// Check determines if the subject has the relation on the object.
	// The window constrains what snapshot times can be used; pass MaxSnapshotWindow
	// for an unconstrained query. The visited slice tracks nodes for cycle
	// detection; pass nil for a fresh query.
	// Returns (allowed, narrowedWindow, error).
	Check(ctx context.Context,
		subjectType TypeID, subjectID ID,
		objectType TypeID, objectID ID,
		relation RelationID,
		window SnapshotWindow, visited []VisitedKey,
	) (bool, SnapshotWindow, error)

	// CheckUnion checks if subject is in the union of all the given usersets.
	// Returns true if subject has the relation on ANY of the objects across all checks.
	// Each RelationCheck has its own window, allowing independent narrowing per type.
	//
	// The returned CheckResult includes:
	//   - Found: whether subject was found in any userset
	//   - DependentSets: which object sets were relevant to the decision
	//   - Window: combined snapshot window for the result
	CheckUnion(ctx context.Context,
		subjectType TypeID, subjectID ID,
		checks []RelationCheck,
		visited []VisitedKey,
	) (CheckResult, error)

	// Schema returns the authorization
	Schema() *Schema
}

// GraphService extends Graph with lifecycle management.
// Used for in-process graphs that need hydration and change subscription.
type GraphService interface {
	Graph

	// Start hydrates from the store and subscribes to the change stream.
	// This blocks until the context is canceled or an error occurs.
	Start(ctx context.Context) error
}
