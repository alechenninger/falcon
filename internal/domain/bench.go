package domain

import (
	"context"

	"github.com/alechenninger/falcon/schema"
)

// BenchGraph wraps a LocalGraph with direct population methods for benchmarking.
// This bypasses the store/observer machinery to enable fast bulk loading.
// Only use this for benchmarking - not for production code.
type BenchGraph struct {
	usersets *MultiversionUsersets
	Graph    *LocalGraph
	time     StoreTime // Simple incrementing counter for populating
}

// NewBenchGraph creates a graph for benchmarking with direct population.
func NewBenchGraph(s *schema.Schema) *BenchGraph {
	usersets := NewMultiversionUsersets(s)
	// Create a minimal LocalGraph for benchmarking
	graph := &LocalGraph{
		usersets:      usersets,
		observer:      NoOpUsersetsObserver{},
		checkObserver: NoOpCheckObserver{},
		localObserver: NoOpLocalGraphObserver{},
	}
	return &BenchGraph{
		usersets: usersets,
		Graph:    graph,
		time:     1,
	}
}

// Check delegates to the underlying LocalGraph.
// Takes string names for convenience and converts to IDs internally.
func (bg *BenchGraph) Check(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, StoreTime, error) {
	s := bg.Graph.Schema()
	ok, window, err := bg.Graph.Check(ctx,
		s.GetTypeID(subjectType), subjectID,
		s.GetTypeID(objectType), objectID,
		s.GetRelationID(objectType, relation),
		MaxSnapshotWindow, nil)
	return ok, window.Max(), err
}

// AddDirect adds a tuple directly to the graph's in-memory state.
// Takes string names for convenience and converts to IDs internally.
func (bg *BenchGraph) AddDirect(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) {
	s := bg.usersets.Schema()
	bg.usersets.applyAdd(Tuple{
		ObjectType:      s.GetTypeID(objectType),
		ObjectID:        objectID,
		Relation:        s.GetRelationID(objectType, relation),
		SubjectType:     s.GetTypeID(subjectType),
		SubjectID:       subjectID,
		SubjectRelation: s.GetRelationID(subjectType, subjectRelation),
	}, bg.time)
	bg.time++
}

// SetReplicatedTime sets the replicated time after bulk loading.
func (bg *BenchGraph) SetReplicatedTime(t StoreTime) {
	bg.usersets.SetReplicatedTime(t)
}

// ReplicatedTime returns the current replicated time.
func (bg *BenchGraph) ReplicatedTime() StoreTime {
	return bg.usersets.ReplicatedTime()
}

// UsersetCount returns the number of distinct usersets in the graph.
func (bg *BenchGraph) UsersetCount() int {
	bg.usersets.mu.RLock()
	defer bg.usersets.mu.RUnlock()
	return len(bg.usersets.tuples)
}

// TruncateHistory removes undo entries older than the given time.
func (bg *BenchGraph) TruncateHistory(minTime StoreTime) {
	bg.usersets.TruncateHistory(minTime)
}

// TupleCount returns the number of tuples that have been added.
// This equals the internal time counter minus 1 (since time starts at 1).
func (bg *BenchGraph) TupleCount() int64 {
	return int64(bg.time - 1)
}
