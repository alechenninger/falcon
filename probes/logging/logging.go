// Package logging provides slog-based implementations of the graph observer interfaces.
package logging

import (
	"context"
	"log/slog"
	"time"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// RequestIDKey is the context key for request IDs.
type RequestIDKey struct{}

// RequestIDFromContext extracts the request ID from the context, or returns empty string.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// -----------------------------------------------------------------------------
// UsersetsObserver
// -----------------------------------------------------------------------------

// UsersetsObserver logs MultiversionUsersets operations.
type UsersetsObserver struct {
	graph.NoOpUsersetsObserver
	logger *slog.Logger
}

// NewUsersetsObserver creates a new logging UsersetsObserver.
func NewUsersetsObserver(logger *slog.Logger) *UsersetsObserver {
	return &UsersetsObserver{logger: logger.With("component", "usersets")}
}

// SubscribeReady logs that the subscription is ready.
func (o *UsersetsObserver) SubscribeReady(ctx context.Context) {
	if !o.logger.Enabled(ctx, slog.LevelInfo) {
		return
	}
	o.logger.LogAttrs(ctx, slog.LevelInfo, "subscription ready",
		slog.String("request_id", RequestIDFromContext(ctx)))
}

// ApplyChangeStarted logs the start of a change application.
func (o *UsersetsObserver) ApplyChangeStarted(ctx context.Context, change store.Change) (context.Context, graph.ApplyChangeProbe) {
	return ctx, &applyChangeProbe{
		logger:    o.logger,
		ctx:       ctx,
		change:    change,
		startTime: time.Now(),
	}
}

// GetSubjectBitmapStarted logs the start of a bitmap read.
func (o *UsersetsObserver) GetSubjectBitmapStarted(key graph.UsersetKey, window graph.SnapshotWindow) graph.BitmapReadProbe {
	return &bitmapReadProbe{
		logger:    o.logger,
		key:       key,
		window:    window,
		startTime: time.Now(),
	}
}

// ContainsDirectStarted logs the start of a direct contains check.
func (o *UsersetsObserver) ContainsDirectStarted(key graph.UsersetKey, subjectID schema.ID, window graph.SnapshotWindow) graph.ContainsReadProbe {
	return &containsReadProbe{
		logger:    o.logger,
		key:       key,
		subjectID: subjectID,
		window:    window,
		startTime: time.Now(),
		direct:    true,
	}
}

// ContainsUsersetSubjectStarted logs the start of a userset subject contains check.
func (o *UsersetsObserver) ContainsUsersetSubjectStarted(key graph.UsersetKey, subjectID schema.ID, window graph.SnapshotWindow) graph.ContainsReadProbe {
	return &containsReadProbe{
		logger:    o.logger,
		key:       key,
		subjectID: subjectID,
		window:    window,
		startTime: time.Now(),
		direct:    false,
	}
}

type applyChangeProbe struct {
	graph.NoOpApplyChangeProbe
	logger    *slog.Logger
	ctx       context.Context
	change    store.Change
	startTime time.Time
	applied   store.StoreTime
}

func (p *applyChangeProbe) Applied(t store.StoreTime) {
	p.applied = t
}

func (p *applyChangeProbe) End() {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "change applied",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(p.change.Tuple.ObjectType)),
		slog.Any("object_id", p.change.Tuple.ObjectID),
		slog.String("relation", string(p.change.Tuple.Relation)),
		slog.String("subject_type", string(p.change.Tuple.SubjectType)),
		slog.Any("subject_id", p.change.Tuple.SubjectID),
		slog.Any("op", p.change.Op),
		slog.Any("applied_time", p.applied),
		slog.Duration("duration", time.Since(p.startTime)))
}

type bitmapReadProbe struct {
	graph.NoOpBitmapReadProbe
	logger    *slog.Logger
	key       graph.UsersetKey
	window    graph.SnapshotWindow
	startTime time.Time
	found     bool
	size      int
	result    graph.SnapshotWindow
}

func (p *bitmapReadProbe) Result(size int, window graph.SnapshotWindow) {
	p.found = true
	p.size = size
	p.result = window
}

func (p *bitmapReadProbe) NotFound() {
	p.found = false
}

func (p *bitmapReadProbe) End() {
	if !p.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(context.Background(), slog.LevelDebug, "bitmap read",
		slog.String("object_type", string(p.key.ObjectType)),
		slog.Any("object_id", p.key.ObjectID),
		slog.String("relation", string(p.key.Relation)),
		slog.String("subject_type", string(p.key.SubjectType)),
		slog.String("subject_relation", string(p.key.SubjectRelation)),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Bool("found", p.found),
		slog.Int("size", p.size),
		slog.Uint64("result_window_min", uint64(p.result.Min())),
		slog.Uint64("result_window_max", uint64(p.result.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

type containsReadProbe struct {
	graph.NoOpContainsReadProbe
	logger    *slog.Logger
	key       graph.UsersetKey
	subjectID schema.ID
	window    graph.SnapshotWindow
	startTime time.Time
	direct    bool
	found     bool
	result    graph.SnapshotWindow
}

func (p *containsReadProbe) Result(found bool, window graph.SnapshotWindow) {
	p.found = found
	p.result = window
}

func (p *containsReadProbe) End() {
	if !p.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	checkType := "userset"
	if p.direct {
		checkType = "direct"
	}
	p.logger.LogAttrs(context.Background(), slog.LevelDebug, "contains check",
		slog.String("check_type", checkType),
		slog.String("object_type", string(p.key.ObjectType)),
		slog.Any("object_id", p.key.ObjectID),
		slog.String("relation", string(p.key.Relation)),
		slog.String("subject_type", string(p.key.SubjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Bool("found", p.found),
		slog.Uint64("result_window_min", uint64(p.result.Min())),
		slog.Uint64("result_window_max", uint64(p.result.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

// -----------------------------------------------------------------------------
// ShardedGraphObserver
// -----------------------------------------------------------------------------

// ShardedGraphObserver logs ShardedGraph operations.
type ShardedGraphObserver struct {
	graph.NoOpShardedGraphObserver
	logger *slog.Logger
}

// NewShardedGraphObserver creates a new logging ShardedGraphObserver.
func NewShardedGraphObserver(logger *slog.Logger) *ShardedGraphObserver {
	return &ShardedGraphObserver{logger: logger.With("component", "sharded_graph")}
}

// CheckStarted logs the start of a Check operation.
func (o *ShardedGraphObserver) CheckStarted(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
) (context.Context, graph.ShardedCheckProbe) {
	return ctx, &shardedCheckProbe{
		logger:      o.logger,
		ctx:         ctx,
		subjectType: subjectType,
		subjectID:   subjectID,
		objectType:  objectType,
		objectID:    objectID,
		relation:    relation,
		startTime:   time.Now(),
	}
}

// CheckUnionStarted logs the start of a CheckUnion operation.
func (o *ShardedGraphObserver) CheckUnionStarted(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID) (context.Context, graph.CheckUnionProbe) {
	return ctx, &checkUnionProbe{
		logger:      o.logger,
		ctx:         ctx,
		subjectType: subjectType,
		subjectID:   subjectID,
		startTime:   time.Now(),
	}
}

type shardedCheckProbe struct {
	graph.NoOpShardedCheckProbe
	logger      *slog.Logger
	ctx         context.Context
	subjectType schema.TypeName
	subjectID   schema.ID
	objectType  schema.TypeName
	objectID    schema.ID
	relation    schema.RelationName
	startTime   time.Time
	local       bool
	remoteShard graph.ShardID
	found       bool
	window      graph.SnapshotWindow
	err         error
}

func (p *shardedCheckProbe) LocalCheck() {
	p.local = true
}

func (p *shardedCheckProbe) RemoteCheck(shardID graph.ShardID) {
	p.remoteShard = shardID
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "routing to remote shard",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID),
		slog.String("object_type", string(p.objectType)),
		slog.Any("object_id", p.objectID))
}

func (p *shardedCheckProbe) UnknownShard(shardID graph.ShardID) {
	p.logger.LogAttrs(p.ctx, slog.LevelWarn, "unknown shard, falling back to local",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID),
		slog.String("object_type", string(p.objectType)),
		slog.Any("object_id", p.objectID))
}

func (p *shardedCheckProbe) Result(found bool, window graph.SnapshotWindow) {
	p.found = found
	p.window = window
}

func (p *shardedCheckProbe) Error(err error) {
	p.err = err
}

func (p *shardedCheckProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "sharded check error",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.String("object_type", string(p.objectType)),
			slog.Any("object_id", p.objectID),
			slog.String("relation", string(p.relation)),
			slog.Bool("local", p.local),
			slog.Any("remote_shard", p.remoteShard),
			slog.Any("error", p.err),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "sharded check completed",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(p.subjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.String("object_type", string(p.objectType)),
		slog.Any("object_id", p.objectID),
		slog.String("relation", string(p.relation)),
		slog.Bool("local", p.local),
		slog.Any("remote_shard", p.remoteShard),
		slog.Bool("found", p.found),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

type checkUnionProbe struct {
	graph.NoOpCheckUnionProbe
	logger      *slog.Logger
	ctx         context.Context
	subjectType schema.TypeName
	subjectID   schema.ID
	startTime   time.Time
	found       bool
	window      graph.SnapshotWindow
	empty       bool
	failed      []graph.ShardID
}

func (p *checkUnionProbe) Empty() {
	p.empty = true
}

func (p *checkUnionProbe) UnknownShard(shardID graph.ShardID) {
	p.logger.LogAttrs(p.ctx, slog.LevelWarn, "unknown shard",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID))
}

func (p *checkUnionProbe) RemoteShardDispatched(shardID graph.ShardID) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "dispatching to remote shard",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID))
}

func (p *checkUnionProbe) RemoteShardResult(shardID graph.ShardID, found bool, window graph.SnapshotWindow) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "remote shard result",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID),
		slog.Bool("found", found),
		slog.Uint64("window_min", uint64(window.Min())),
		slog.Uint64("window_max", uint64(window.Max())))
}

func (p *checkUnionProbe) RemoteShardError(shardID graph.ShardID, err error) {
	p.logger.LogAttrs(p.ctx, slog.LevelError, "remote shard error",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Any("shard_id", shardID),
		slog.Any("error", err))
}

func (p *checkUnionProbe) Result(found bool, window graph.SnapshotWindow) {
	p.found = found
	p.window = window
}

func (p *checkUnionProbe) Inconclusive(failedShards []graph.ShardID) {
	p.failed = failedShards
}

func (p *checkUnionProbe) End() {
	if p.empty {
		if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
			return
		}
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "check union empty",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if len(p.failed) > 0 {
		p.logger.LogAttrs(p.ctx, slog.LevelWarn, "check union inconclusive",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.Any("failed_shards", p.failed),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "check union completed",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(p.subjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.Bool("found", p.found),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

// -----------------------------------------------------------------------------
// CheckObserver
// -----------------------------------------------------------------------------

// CheckObserver logs check algorithm operations.
type CheckObserver struct {
	graph.NoOpCheckObserver
	logger *slog.Logger
}

// NewCheckObserver creates a new logging CheckObserver.
func NewCheckObserver(logger *slog.Logger) *CheckObserver {
	return &CheckObserver{logger: logger.With("component", "check")}
}

// CheckStarted logs the start of a check operation.
func (o *CheckObserver) CheckStarted(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
) (context.Context, graph.CheckProbe) {
	if o.logger.Enabled(ctx, slog.LevelDebug) {
		o.logger.LogAttrs(ctx, slog.LevelDebug, "check started",
			slog.String("request_id", RequestIDFromContext(ctx)),
			slog.String("subject_type", string(subjectType)),
			slog.Any("subject_id", subjectID),
			slog.String("object_type", string(objectType)),
			slog.Any("object_id", objectID),
			slog.String("relation", string(relation)))
	}
	return ctx, &checkProbe{
		logger:      o.logger,
		ctx:         ctx,
		subjectType: subjectType,
		subjectID:   subjectID,
		objectType:  objectType,
		objectID:    objectID,
		relation:    relation,
		startTime:   time.Now(),
	}
}

type checkProbe struct {
	graph.NoOpCheckProbe
	logger      *slog.Logger
	ctx         context.Context
	subjectType schema.TypeName
	subjectID   schema.ID
	objectType  schema.TypeName
	objectID    schema.ID
	relation    schema.RelationName
	startTime   time.Time
	found       bool
	window      graph.SnapshotWindow
	err         error
}

func (p *checkProbe) RelationEntered(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "relation entered",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(objectType)),
		slog.Any("object_id", objectID),
		slog.String("relation", string(relation)))
}

func (p *checkProbe) CycleDetected(key graph.VisitedKey) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "cycle detected",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(key.ObjectType)),
		slog.Any("object_id", key.ObjectID),
		slog.String("relation", string(key.Relation)))
}

func (p *checkProbe) UsersetChecking(userset *schema.Userset) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "checking userset",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("userset_type", usersetType(userset)))
}

// usersetType returns a string describing the userset variant.
func usersetType(us *schema.Userset) string {
	if us.TupleToUserset != nil {
		return "arrow"
	}
	if us.ComputedRelation != "" {
		return "computed"
	}
	if len(us.This) > 0 {
		return "direct"
	}
	return "unknown"
}

func (p *checkProbe) DirectLookup(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "direct lookup",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(objectType)),
		slog.Any("object_id", objectID),
		slog.String("relation", string(relation)),
		slog.String("subject_type", string(subjectType)))
}

func (p *checkProbe) DirectLookupResult(found bool, window graph.SnapshotWindow) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "direct lookup result",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Bool("found", found),
		slog.Uint64("window_min", uint64(window.Min())),
		slog.Uint64("window_max", uint64(window.Max())))
}

func (p *checkProbe) ArrowTraversal(tuplesetRelation, computedRelation schema.RelationName) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "arrow traversal",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("tupleset_relation", string(tuplesetRelation)),
		slog.String("computed_relation", string(computedRelation)))
}

func (p *checkProbe) RecursiveCheck(subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName, depth int) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "recursive check",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(subjectType)),
		slog.Any("subject_id", subjectID),
		slog.String("object_type", string(objectType)),
		slog.Any("object_id", objectID),
		slog.String("relation", string(relation)),
		slog.Int("depth", depth))
}

func (p *checkProbe) UnionBranchFound(branchIndex int) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "union branch found",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Int("branch_index", branchIndex))
}

func (p *checkProbe) Result(found bool, window graph.SnapshotWindow) {
	p.found = found
	p.window = window
}

func (p *checkProbe) Error(err error) {
	p.err = err
}

func (p *checkProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "check error",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.String("object_type", string(p.objectType)),
			slog.Any("object_id", p.objectID),
			slog.String("relation", string(p.relation)),
			slog.Any("error", p.err),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "check completed",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(p.subjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.String("object_type", string(p.objectType)),
		slog.Any("object_id", p.objectID),
		slog.String("relation", string(p.relation)),
		slog.Bool("found", p.found),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

// -----------------------------------------------------------------------------
// LocalGraphObserver
// -----------------------------------------------------------------------------

// LocalGraphObserver logs LocalGraph operations.
type LocalGraphObserver struct {
	graph.NoOpLocalGraphObserver
	logger *slog.Logger
}

// NewLocalGraphObserver creates a new logging LocalGraphObserver.
func NewLocalGraphObserver(logger *slog.Logger) *LocalGraphObserver {
	return &LocalGraphObserver{logger: logger.With("component", "local_graph")}
}

// CheckStarted logs the start of a LocalGraph.Check operation.
func (o *LocalGraphObserver) CheckStarted(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName,
) (context.Context, graph.LocalCheckProbe) {
	return ctx, &localCheckProbe{
		logger:      o.logger,
		ctx:         ctx,
		subjectType: subjectType,
		subjectID:   subjectID,
		objectType:  objectType,
		objectID:    objectID,
		relation:    relation,
		startTime:   time.Now(),
	}
}

// CheckUnionStarted logs the start of a LocalGraph.CheckUnion operation.
func (o *LocalGraphObserver) CheckUnionStarted(ctx context.Context,
	subjectType schema.TypeName, subjectID schema.ID,
	numChecks int,
) (context.Context, graph.LocalCheckUnionProbe) {
	return ctx, &localCheckUnionProbe{
		logger:      o.logger,
		ctx:         ctx,
		subjectType: subjectType,
		subjectID:   subjectID,
		numChecks:   numChecks,
		startTime:   time.Now(),
	}
}

type localCheckProbe struct {
	graph.NoOpLocalCheckProbe
	logger      *slog.Logger
	ctx         context.Context
	subjectType schema.TypeName
	subjectID   schema.ID
	objectType  schema.TypeName
	objectID    schema.ID
	relation    schema.RelationName
	startTime   time.Time
	found       bool
	window      graph.SnapshotWindow
	err         error
}

func (p *localCheckProbe) Result(found bool, window graph.SnapshotWindow) {
	p.found = found
	p.window = window
}

func (p *localCheckProbe) Error(err error) {
	p.err = err
}

func (p *localCheckProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "local check error",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.String("object_type", string(p.objectType)),
			slog.Any("object_id", p.objectID),
			slog.String("relation", string(p.relation)),
			slog.Any("error", p.err),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "local check completed",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(p.subjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.String("object_type", string(p.objectType)),
		slog.Any("object_id", p.objectID),
		slog.String("relation", string(p.relation)),
		slog.Bool("found", p.found),
		slog.Uint64("window_min", uint64(p.window.Min())),
		slog.Uint64("window_max", uint64(p.window.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

type localCheckUnionProbe struct {
	graph.NoOpLocalCheckUnionProbe
	logger      *slog.Logger
	ctx         context.Context
	subjectType schema.TypeName
	subjectID   schema.ID
	numChecks   int
	startTime   time.Time
	result      graph.CheckResult
	err         error
}

func (p *localCheckUnionProbe) BitmapLookup(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	subjectType schema.TypeName, subjectRelation schema.RelationName) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "bitmap lookup",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(objectType)),
		slog.Any("object_id", objectID),
		slog.String("relation", string(relation)),
		slog.String("subject_type", string(subjectType)),
		slog.String("subject_relation", string(subjectRelation)))
}

func (p *localCheckUnionProbe) BitmapLookupResult(size int, window graph.SnapshotWindow) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "bitmap lookup result",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Int("size", size),
		slog.Uint64("window_min", uint64(window.Min())),
		slog.Uint64("window_max", uint64(window.Max())))
}

func (p *localCheckUnionProbe) ContainsCheck(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	subjectType schema.TypeName, subjectID schema.ID) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "contains check",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("object_type", string(objectType)),
		slog.Any("object_id", objectID),
		slog.String("relation", string(relation)),
		slog.String("subject_type", string(subjectType)),
		slog.Any("subject_id", subjectID))
}

func (p *localCheckUnionProbe) ContainsCheckResult(found bool, window graph.SnapshotWindow) {
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "contains check result",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.Bool("found", found),
		slog.Uint64("window_min", uint64(window.Min())),
		slog.Uint64("window_max", uint64(window.Max())))
}

func (p *localCheckUnionProbe) Result(result graph.CheckResult) {
	p.result = result
}

func (p *localCheckUnionProbe) Error(err error) {
	p.err = err
}

func (p *localCheckUnionProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "local check union error",
			slog.String("request_id", RequestIDFromContext(p.ctx)),
			slog.String("subject_type", string(p.subjectType)),
			slog.Any("subject_id", p.subjectID),
			slog.Int("num_checks", p.numChecks),
			slog.Any("error", p.err),
			slog.Duration("duration", time.Since(p.startTime)))
		return
	}
	if !p.logger.Enabled(p.ctx, slog.LevelDebug) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelDebug, "local check union completed",
		slog.String("request_id", RequestIDFromContext(p.ctx)),
		slog.String("subject_type", string(p.subjectType)),
		slog.Any("subject_id", p.subjectID),
		slog.Int("num_checks", p.numChecks),
		slog.Bool("found", p.result.Found),
		slog.Uint64("window_min", uint64(p.result.Window.Min())),
		slog.Uint64("window_max", uint64(p.result.Window.Max())),
		slog.Duration("duration", time.Since(p.startTime)))
}

// -----------------------------------------------------------------------------
// MVCCObserver
// -----------------------------------------------------------------------------

// MVCCObserver logs versionedSet MVCC operations.
type MVCCObserver struct {
	graph.NoOpMVCCObserver
	logger *slog.Logger
}

// NewMVCCObserver creates a new logging MVCCObserver.
func NewMVCCObserver(logger *slog.Logger) *MVCCObserver {
	return &MVCCObserver{logger: logger.With("component", "mvcc")}
}

// ContainsWithinStarted logs the start of a ContainsWithin operation.
func (o *MVCCObserver) ContainsWithinStarted(id schema.ID, maxTime store.StoreTime) graph.MVCCProbe {
	return &mvccProbe{
		logger:    o.logger,
		operation: "contains_within",
		id:        id,
		maxTime:   maxTime,
	}
}

// SnapshotWithinStarted logs the start of a SnapshotWithin operation.
func (o *MVCCObserver) SnapshotWithinStarted(maxTime store.StoreTime) graph.MVCCProbe {
	return &mvccProbe{
		logger:    o.logger,
		operation: "snapshot_within",
		maxTime:   maxTime,
	}
}

type mvccProbe struct {
	graph.NoOpMVCCProbe
	logger       *slog.Logger
	operation    string
	id           schema.ID
	maxTime      store.StoreTime
	historyDepth int
	undoCount    int
	headUsed     bool
	found        bool
	stateTime    store.StoreTime
}

func (p *mvccProbe) HistoryDepth(depth int) {
	p.historyDepth = depth
}

func (p *mvccProbe) UndoApplied(timeDelta uint32) {
	p.undoCount++
}

func (p *mvccProbe) HeadUsed() {
	p.headUsed = true
}

func (p *mvccProbe) Result(found bool, stateTime store.StoreTime) {
	p.found = found
	p.stateTime = stateTime
}

func (p *mvccProbe) End() {
	if !p.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	attrs := []slog.Attr{
		slog.String("operation", p.operation),
		slog.Any("max_time", p.maxTime),
		slog.Int("history_depth", p.historyDepth),
		slog.Int("undo_count", p.undoCount),
		slog.Bool("head_used", p.headUsed),
		slog.Bool("found", p.found),
		slog.Any("state_time", p.stateTime),
	}
	if p.id != 0 {
		attrs = append([]slog.Attr{slog.Any("id", p.id)}, attrs...)
	}
	p.logger.LogAttrs(context.Background(), slog.LevelDebug, "mvcc operation", attrs...)
}
