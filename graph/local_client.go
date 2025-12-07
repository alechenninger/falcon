package graph

import (
	"context"

	"github.com/alechenninger/falcon/schema"
)

// LocalGraphClient implements GraphClient by delegating to a local Graph.
// This is used when the target object is on the same shard as the caller.
type LocalGraphClient struct {
	graph *Graph
}

// NewLocalGraphClient creates a LocalGraphClient wrapping the given Graph.
func NewLocalGraphClient(g *Graph) *LocalGraphClient {
	return &LocalGraphClient{graph: g}
}

// CheckRelation delegates to the local Graph's CheckAtWithVisited method.
func (c *LocalGraphClient) CheckRelation(
	ctx context.Context,
	subjectType schema.TypeName,
	subjectID schema.ID,
	objectType schema.TypeName,
	objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	return c.graph.CheckAtWithVisited(ctx, subjectType, subjectID, objectType, objectID, relation, &window, visited)
}

// BatchCheckRelation checks if subject has relation on ANY of the given objects.
// It iterates through all objects in the ObjectSets, short-circuiting on first true.
func (c *LocalGraphClient) BatchCheckRelation(
	ctx context.Context,
	check RelationCheck,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	currentWindow := window

	for _, objSet := range check.Objects {
		// Iterate over each object ID in the bitmap
		iter := objSet.IDs.Iterator()
		for iter.HasNext() {
			objectID := schema.ID(iter.Next())

			allowed, newWindow, err := c.graph.CheckAtWithVisited(
				ctx,
				check.SubjectType,
				check.SubjectID,
				objSet.Type,
				objectID,
				check.Relation,
				&currentWindow,
				visited,
			)
			if err != nil {
				return false, currentWindow, err
			}

			// Always narrow the window based on state examined
			currentWindow = newWindow

			// Short-circuit on first true
			if allowed {
				return true, currentWindow, nil
			}
		}
	}

	return false, currentWindow, nil
}

// Graph returns the underlying Graph.
func (c *LocalGraphClient) Graph() *Graph {
	return c.graph
}
