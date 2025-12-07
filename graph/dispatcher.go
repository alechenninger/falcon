package graph

import (
	"context"
	"sync"

	"github.com/alechenninger/falcon/schema"
)

// Dispatcher handles scatter-gather for batch relation checks.
// It groups objects by destination using the Router, then executes
// checks in parallel across nodes, short-circuiting on first true.
type Dispatcher struct {
	router Router
}

// NewDispatcher creates a Dispatcher with the given Router.
func NewDispatcher(router Router) *Dispatcher {
	return &Dispatcher{router: router}
}

// DispatchCheck checks if subject has relation on ANY of the given objects.
// It groups objects by destination using the Router, then calls BatchCheckRelation
// on each destination in parallel.
//
// Returns true as soon as any node returns true (short-circuit).
// The returned window is the intersection of all examined windows.
func (d *Dispatcher) DispatchCheck(
	ctx context.Context,
	check RelationCheck,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// Group objects by destination
	groups, err := d.router.GroupByDestination(ctx, check.Objects)
	if err != nil {
		return false, window, err
	}

	if len(groups) == 0 {
		return false, window, nil
	}

	// Single group optimization: skip goroutine overhead
	if len(groups) == 1 {
		groupCheck := RelationCheck{
			SubjectType: check.SubjectType,
			SubjectID:   check.SubjectID,
			Objects:     groups[0].Objects,
			Relation:    check.Relation,
		}
		return groups[0].Client.BatchCheckRelation(ctx, groupCheck, window, visited)
	}

	// Multiple groups: execute in parallel with short-circuit
	type result struct {
		allowed bool
		window  SnapshotWindow
		err     error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan result, len(groups))
	var wg sync.WaitGroup

	for _, group := range groups {
		wg.Add(1)
		go func(g RoutedGroup) {
			defer wg.Done()

			groupCheck := RelationCheck{
				SubjectType: check.SubjectType,
				SubjectID:   check.SubjectID,
				Objects:     g.Objects,
				Relation:    check.Relation,
			}

			allowed, newWindow, err := g.Client.BatchCheckRelation(ctx, groupCheck, window, visited)

			select {
			case resultCh <- result{allowed, newWindow, err}:
			case <-ctx.Done():
			}
		}(group)
	}

	// Close result channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results, short-circuiting on first true or error
	// Each result window is already narrowed (it received our window as input).
	for r := range resultCh {
		if r.err != nil {
			cancel() // Cancel other goroutines
			return false, r.window, r.err
		}

		if r.allowed {
			cancel() // Cancel other goroutines
			return true, r.window, nil
		}
	}

	return false, window, nil
}

// DispatchSingleCheck is a convenience method for checking a single object.
// It creates an ObjectSet with a single ID and dispatches it.
func (d *Dispatcher) DispatchSingleCheck(
	ctx context.Context,
	subjectType schema.TypeName,
	subjectID schema.ID,
	objectType schema.TypeName,
	objectID schema.ID,
	relation schema.RelationName,
	window SnapshotWindow,
	visited []VisitedKey,
) (bool, SnapshotWindow, error) {
	// For a single object, just route and call directly
	client, err := d.router.Route(ctx, objectType, objectID)
	if err != nil {
		return false, window, err
	}
	return client.CheckRelation(ctx, subjectType, subjectID, objectType, objectID, relation, window, visited)
}
