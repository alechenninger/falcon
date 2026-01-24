package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/alechenninger/falcon/internal/domain"
)

// CheckQuery represents an authorization check using external identifiers.
type CheckQuery struct {
	ObjectType  domain.TypeName
	ObjectID    domain.ExternalID
	Relation    domain.RelationName
	SubjectType domain.TypeName
	SubjectID   domain.ExternalID
}

// CheckResult represents the outcome of a Check query.
type CheckResult struct {
	Allowed bool
	// Future: add consistency token, debug info, etc.
}

// QueryServiceConfig holds configuration for QueryService.
type QueryServiceConfig struct {
	Schema     *domain.Schema
	IdResolver domain.IdResolver
	Graph      domain.Graph
	Observer   QueryObserver
}

// QueryService provides the external API for authorization queries.
// It speaks in terms of external identifiers and delegates to a Graph
// which handles all internal routing during recursive traversal.
type QueryService struct {
	schema     *domain.Schema
	idResolver domain.IdResolver
	graph      domain.Graph
	observer   QueryObserver
}

// NewQueryService creates a new QueryService.
func NewQueryService(cfg QueryServiceConfig) *QueryService {
	obs := cfg.Observer
	if obs == nil {
		obs = NoOpQueryObserver{}
	}
	return &QueryService{
		schema:     cfg.Schema,
		idResolver: cfg.IdResolver,
		graph:      cfg.Graph,
		observer:   obs,
	}
}

// Check determines if the subject has the specified relation on the object.
// This is the main authorization query method.
func (s *QueryService) Check(ctx context.Context, q CheckQuery) (CheckResult, error) {
	ctx, probe := s.observer.CheckStarted(ctx, q)
	defer probe.End()

	// 1. Resolve type names to TypeIDs via schema
	objectType := s.schema.TypeByName(q.ObjectType)
	if objectType == nil {
		err := fmt.Errorf("unknown object type: %s", q.ObjectType)
		probe.Error(err)
		return CheckResult{}, err
	}

	subjectType := s.schema.TypeByName(q.SubjectType)
	if subjectType == nil {
		err := fmt.Errorf("unknown subject type: %s", q.SubjectType)
		probe.Error(err)
		return CheckResult{}, err
	}

	relation := objectType.RelationByName(q.Relation)
	if relation == nil {
		err := fmt.Errorf("unknown relation %s on type %s", q.Relation, q.ObjectType)
		probe.Error(err)
		return CheckResult{}, err
	}

	// 2. Resolve external IDs to internal IDs via IdResolver
	// If an ID doesn't exist, there's no relation (same as "not allowed")
	refs := []domain.ObjectRef{
		{Type: objectType.ID, ID: q.ObjectID},
		{Type: subjectType.ID, ID: q.SubjectID},
	}

	ids, err := s.idResolver.ResolveIDs(ctx, refs)
	if errors.Is(err, domain.ErrIDNotFound) {
		// Unknown ID means no relation exists
		probe.Error(err)
		probe.Result(false)
		return CheckResult{Allowed: false}, nil
	}
	if err != nil {
		probe.Error(err)
		return CheckResult{}, fmt.Errorf("resolving IDs: %w", err)
	}

	objectID := ids[0]
	subjectID := ids[1]
	probe.IdsResolved(objectID, subjectID)

	// 3. Call graph.Check with internal IDs (graph handles routing)
	allowed, _, err := s.graph.Check(ctx,
		subjectType.ID, subjectID,
		objectType.ID, objectID,
		relation.ID,
		domain.MaxSnapshotWindow, nil,
	)
	if err != nil {
		probe.Error(err)
		return CheckResult{}, fmt.Errorf("check: %w", err)
	}

	probe.Result(allowed)
	return CheckResult{Allowed: allowed}, nil
}
