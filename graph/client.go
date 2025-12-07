package graph

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/schema"
)

// VisitedKey represents a node that has been visited during query traversal.
// Used for cycle detection across distributed nodes.
type VisitedKey struct {
	ObjectType schema.TypeName
	ObjectID   schema.ID
	Relation   schema.RelationName
}

// ObjectSet represents a set of objects of the same type.
// The IDs are stored as a roaring bitmap for compression.
type ObjectSet struct {
	Type schema.TypeName
	IDs  *roaring.Bitmap
}

// RelationCheck is a batch check request: is subject in any of these objects' relation?
type RelationCheck struct {
	SubjectType schema.TypeName
	SubjectID   schema.ID
	Objects     []ObjectSet // Objects to check, grouped by type
	Relation    schema.RelationName
}

// GraphClient abstracts local vs remote graph access for cross-object checks.
//
// When traversing the graph (e.g., following arrows or checking userset subjects),
// the check algorithm uses a Router to get a GraphClient for each target object.
// The Router returns either a LocalGraphClient (for local objects) or a
// RemoteGraphClient (for objects on other shards).
type GraphClient interface {
	// CheckRelation checks if subject has relation on object within the snapshot window.
	// This is called when the check algorithm needs to evaluate a relation on a
	// potentially remote object (e.g., after traversing an arrow or for userset subjects).
	//
	// The window constrains the snapshot for consistent reads across shards.
	// The returned window may be narrowed based on the state examined.
	//
	// visited contains nodes already visited in this query traversal for cycle detection.
	CheckRelation(
		ctx context.Context,
		subjectType schema.TypeName,
		subjectID schema.ID,
		objectType schema.TypeName,
		objectID schema.ID,
		relation schema.RelationName,
		window SnapshotWindow,
		visited []VisitedKey,
	) (allowed bool, resultWindow SnapshotWindow, err error)

	// BatchCheckRelation checks if subject has relation on ANY of the given objects.
	// This is used for scatter-gather batch checking. The caller groups objects by
	// destination node using Router.GroupByDestination, then calls this method once
	// per node with all relevant objects.
	//
	// Returns true if the subject has the relation on any object (short-circuits).
	// The window is narrowed based on all state examined.
	BatchCheckRelation(
		ctx context.Context,
		check RelationCheck,
		window SnapshotWindow,
		visited []VisitedKey,
	) (allowed bool, resultWindow SnapshotWindow, err error)
}
