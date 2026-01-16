package graph

import (
	"context"

	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/schema"
)

// Check is the core walk algorithm. It's a standalone function that takes
// Graph for recursion and MultiversionUsersets for data access.
// Deprecated: Use domain.Check instead.
func Check(
	ctx context.Context,
	graph Graph,
	usersets *MultiversionUsersets,
	observer CheckObserver,
	subjectType schema.TypeID, subjectID schema.ID,
	objectType schema.TypeID, objectID schema.ID,
	relation schema.RelationID,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	return domain.Check(ctx, graph, usersets, observer, subjectType, subjectID, objectType, objectID, relation, window, visited)
}
