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
	o.logger.InfoContext(ctx, "subscription ready",
		"request_id", RequestIDFromContext(ctx))
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
	p.logger.DebugContext(p.ctx, "change applied",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", p.change.Tuple.ObjectType,
		"object_id", p.change.Tuple.ObjectID,
		"relation", p.change.Tuple.Relation,
		"subject_type", p.change.Tuple.SubjectType,
		"subject_id", p.change.Tuple.SubjectID,
		"op", p.change.Op,
		"applied_time", p.applied,
		"duration", time.Since(p.startTime))
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
	p.logger.Debug("bitmap read",
		"object_type", p.key.ObjectType,
		"object_id", p.key.ObjectID,
		"relation", p.key.Relation,
		"subject_type", p.key.SubjectType,
		"subject_relation", p.key.SubjectRelation,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"found", p.found,
		"size", p.size,
		"result_window_min", p.result.Min,
		"result_window_max", p.result.Max,
		"duration", time.Since(p.startTime))
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
	checkType := "userset"
	if p.direct {
		checkType = "direct"
	}
	p.logger.Debug("contains check",
		"check_type", checkType,
		"object_type", p.key.ObjectType,
		"object_id", p.key.ObjectID,
		"relation", p.key.Relation,
		"subject_type", p.key.SubjectType,
		"subject_id", p.subjectID,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"found", p.found,
		"result_window_min", p.result.Min,
		"result_window_max", p.result.Max,
		"duration", time.Since(p.startTime))
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
	p.logger.DebugContext(p.ctx, "routing to remote shard",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID,
		"object_type", p.objectType,
		"object_id", p.objectID)
}

func (p *shardedCheckProbe) UnknownShard(shardID graph.ShardID) {
	p.logger.WarnContext(p.ctx, "unknown shard, falling back to local",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID,
		"object_type", p.objectType,
		"object_id", p.objectID)
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
		p.logger.ErrorContext(p.ctx, "sharded check error",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"object_type", p.objectType,
			"object_id", p.objectID,
			"relation", p.relation,
			"local", p.local,
			"remote_shard", p.remoteShard,
			"error", p.err,
			"duration", time.Since(p.startTime))
		return
	}
	p.logger.DebugContext(p.ctx, "sharded check completed",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", p.subjectType,
		"subject_id", p.subjectID,
		"object_type", p.objectType,
		"object_id", p.objectID,
		"relation", p.relation,
		"local", p.local,
		"remote_shard", p.remoteShard,
		"found", p.found,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"duration", time.Since(p.startTime))
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
	p.logger.WarnContext(p.ctx, "unknown shard",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID)
}

func (p *checkUnionProbe) RemoteShardDispatched(shardID graph.ShardID) {
	p.logger.DebugContext(p.ctx, "dispatching to remote shard",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID)
}

func (p *checkUnionProbe) RemoteShardResult(shardID graph.ShardID, found bool, window graph.SnapshotWindow) {
	p.logger.DebugContext(p.ctx, "remote shard result",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID,
		"found", found,
		"window_min", window.Min,
		"window_max", window.Max)
}

func (p *checkUnionProbe) RemoteShardError(shardID graph.ShardID, err error) {
	p.logger.ErrorContext(p.ctx, "remote shard error",
		"request_id", RequestIDFromContext(p.ctx),
		"shard_id", shardID,
		"error", err)
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
		p.logger.DebugContext(p.ctx, "check union empty",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"duration", time.Since(p.startTime))
		return
	}
	if len(p.failed) > 0 {
		p.logger.WarnContext(p.ctx, "check union inconclusive",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"failed_shards", p.failed,
			"duration", time.Since(p.startTime))
		return
	}
	p.logger.InfoContext(p.ctx, "check union completed",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", p.subjectType,
		"subject_id", p.subjectID,
		"found", p.found,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"duration", time.Since(p.startTime))
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
	o.logger.DebugContext(ctx, "check started",
		"request_id", RequestIDFromContext(ctx),
		"subject_type", subjectType,
		"subject_id", subjectID,
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation)
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
	p.logger.DebugContext(p.ctx, "relation entered",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation)
}

func (p *checkProbe) CycleDetected(key graph.VisitedKey) {
	p.logger.DebugContext(p.ctx, "cycle detected",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", key.ObjectType,
		"object_id", key.ObjectID,
		"relation", key.Relation)
}

func (p *checkProbe) UsersetChecking(userset *schema.Userset) {
	p.logger.DebugContext(p.ctx, "checking userset",
		"request_id", RequestIDFromContext(p.ctx),
		"userset_type", usersetType(userset))
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
	p.logger.DebugContext(p.ctx, "direct lookup",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation,
		"subject_type", subjectType)
}

func (p *checkProbe) DirectLookupResult(found bool, window graph.SnapshotWindow) {
	p.logger.DebugContext(p.ctx, "direct lookup result",
		"request_id", RequestIDFromContext(p.ctx),
		"found", found,
		"window_min", window.Min,
		"window_max", window.Max)
}

func (p *checkProbe) ArrowTraversal(tuplesetRelation, computedRelation schema.RelationName) {
	p.logger.DebugContext(p.ctx, "arrow traversal",
		"request_id", RequestIDFromContext(p.ctx),
		"tupleset_relation", tuplesetRelation,
		"computed_relation", computedRelation)
}

func (p *checkProbe) RecursiveCheck(subjectType schema.TypeName, subjectID schema.ID,
	objectType schema.TypeName, objectID schema.ID,
	relation schema.RelationName, depth int) {
	p.logger.DebugContext(p.ctx, "recursive check",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", subjectType,
		"subject_id", subjectID,
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation,
		"depth", depth)
}

func (p *checkProbe) UnionBranchFound(branchIndex int) {
	p.logger.DebugContext(p.ctx, "union branch found",
		"request_id", RequestIDFromContext(p.ctx),
		"branch_index", branchIndex)
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
		p.logger.ErrorContext(p.ctx, "check error",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"object_type", p.objectType,
			"object_id", p.objectID,
			"relation", p.relation,
			"error", p.err,
			"duration", time.Since(p.startTime))
		return
	}
	p.logger.InfoContext(p.ctx, "check completed",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", p.subjectType,
		"subject_id", p.subjectID,
		"object_type", p.objectType,
		"object_id", p.objectID,
		"relation", p.relation,
		"found", p.found,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"duration", time.Since(p.startTime))
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
		p.logger.ErrorContext(p.ctx, "local check error",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"object_type", p.objectType,
			"object_id", p.objectID,
			"relation", p.relation,
			"error", p.err,
			"duration", time.Since(p.startTime))
		return
	}
	p.logger.DebugContext(p.ctx, "local check completed",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", p.subjectType,
		"subject_id", p.subjectID,
		"object_type", p.objectType,
		"object_id", p.objectID,
		"relation", p.relation,
		"found", p.found,
		"window_min", p.window.Min,
		"window_max", p.window.Max,
		"duration", time.Since(p.startTime))
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
	p.logger.DebugContext(p.ctx, "bitmap lookup",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation,
		"subject_type", subjectType,
		"subject_relation", subjectRelation)
}

func (p *localCheckUnionProbe) BitmapLookupResult(size int, window graph.SnapshotWindow) {
	p.logger.DebugContext(p.ctx, "bitmap lookup result",
		"request_id", RequestIDFromContext(p.ctx),
		"size", size,
		"window_min", window.Min,
		"window_max", window.Max)
}

func (p *localCheckUnionProbe) ContainsCheck(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName,
	subjectType schema.TypeName, subjectID schema.ID) {
	p.logger.DebugContext(p.ctx, "contains check",
		"request_id", RequestIDFromContext(p.ctx),
		"object_type", objectType,
		"object_id", objectID,
		"relation", relation,
		"subject_type", subjectType,
		"subject_id", subjectID)
}

func (p *localCheckUnionProbe) ContainsCheckResult(found bool, window graph.SnapshotWindow) {
	p.logger.DebugContext(p.ctx, "contains check result",
		"request_id", RequestIDFromContext(p.ctx),
		"found", found,
		"window_min", window.Min,
		"window_max", window.Max)
}

func (p *localCheckUnionProbe) Result(result graph.CheckResult) {
	p.result = result
}

func (p *localCheckUnionProbe) Error(err error) {
	p.err = err
}

func (p *localCheckUnionProbe) End() {
	if p.err != nil {
		p.logger.ErrorContext(p.ctx, "local check union error",
			"request_id", RequestIDFromContext(p.ctx),
			"subject_type", p.subjectType,
			"subject_id", p.subjectID,
			"num_checks", p.numChecks,
			"error", p.err,
			"duration", time.Since(p.startTime))
		return
	}
	p.logger.DebugContext(p.ctx, "local check union completed",
		"request_id", RequestIDFromContext(p.ctx),
		"subject_type", p.subjectType,
		"subject_id", p.subjectID,
		"num_checks", p.numChecks,
		"found", p.result.Found,
		"window_min", p.result.Window.Min,
		"window_max", p.result.Window.Max,
		"duration", time.Since(p.startTime))
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
		startTime: time.Now(),
	}
}

// SnapshotWithinStarted logs the start of a SnapshotWithin operation.
func (o *MVCCObserver) SnapshotWithinStarted(maxTime store.StoreTime) graph.MVCCProbe {
	return &mvccProbe{
		logger:    o.logger,
		operation: "snapshot_within",
		maxTime:   maxTime,
		startTime: time.Now(),
	}
}

type mvccProbe struct {
	graph.NoOpMVCCProbe
	logger       *slog.Logger
	operation    string
	id           schema.ID
	maxTime      store.StoreTime
	startTime    time.Time
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
	attrs := []any{
		"operation", p.operation,
		"max_time", p.maxTime,
		"history_depth", p.historyDepth,
		"undo_count", p.undoCount,
		"head_used", p.headUsed,
		"found", p.found,
		"state_time", p.stateTime,
		"duration", time.Since(p.startTime),
	}
	if p.id != 0 {
		attrs = append([]any{"id", p.id}, attrs...)
	}
	p.logger.Debug("mvcc operation", attrs...)
}
