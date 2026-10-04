package postgres_test

// All three treatments use the same sink. Only the SQL delivery strategy and
// whether rows are held until EOF vary. No Falcon graph/MVCC code is changed.
import (
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/semaphore"
)

func (c domainCaptureConfig) pipeline() bool {
	return strings.HasPrefix(c.SnapshotFetch, "pipeline-")
}

type snapshotPipelineResult struct {
	FetchedRows, FetchedBytes, RootEpochs, StagedBeforeEOF int64
	FetchSeconds, PayloadMiBPerFetchSecond                 float64
	EpochFetch, StatementFirstRow, RootCompletionWait      domainCaptureSummary
	EpochFirstRow                                          domainCaptureSummary
	CommitToFetch, CommitToStage, FetchToStage             domainCaptureSummary
	BackpressureCount                                      int64
	BackpressureSeconds                                    float64
	BoundsQueries, BoundedCuts, FallbackCuts               int64
	CandidatePositions, PositionSpan                       int64
}

type snapshotPipelineMetrics struct {
	mu                                          sync.Mutex
	m                                           *domainCaptureMetrics
	fetchedRows, fetchedBytes, roots, beforeEOF int64
	fetchSeconds, pressureSeconds               float64
	pressureCount                               int64
	epochFetch, firstRow, rootWait              []time.Duration
	epochFirstRow                               []time.Duration
	boundsQueries, boundedCuts, fallbackCuts    int64
	candidatePositions, positionSpan            int64
}

func (x *snapshotPipelineMetrics) result() *snapshotPipelineResult {
	x.mu.Lock()
	defer x.mu.Unlock()
	r := &snapshotPipelineResult{
		FetchedRows: x.fetchedRows, FetchedBytes: x.fetchedBytes, RootEpochs: x.roots,
		StagedBeforeEOF: x.beforeEOF, FetchSeconds: x.fetchSeconds,
		EpochFetch: summarizeDomainDurations(x.epochFetch), StatementFirstRow: summarizeDomainDurations(x.firstRow),
		RootCompletionWait: summarizeDomainDurations(x.rootWait), BackpressureCount: x.pressureCount, BackpressureSeconds: x.pressureSeconds,
		EpochFirstRow: summarizeDomainDurations(x.epochFirstRow), BoundsQueries: x.boundsQueries, BoundedCuts: x.boundedCuts, FallbackCuts: x.fallbackCuts,
		CandidatePositions: x.candidatePositions, PositionSpan: x.positionSpan,
	}
	if x.fetchSeconds > 0 {
		r.PayloadMiBPerFetchSecond = float64(x.fetchedBytes) / (1 << 20) / x.fetchSeconds
	}
	fetch, stage, queue := []time.Duration{}, []time.Duration{}, []time.Duration{}
	for _, e := range x.m.events[1:] {
		if !e.fetched.IsZero() {
			if !e.committed.IsZero() {
				fetch = append(fetch, max(time.Duration(0), e.fetched.Sub(e.committed)))
				stage = append(stage, max(time.Duration(0), e.staged.Sub(e.committed)))
			}
			queue = append(queue, e.staged.Sub(e.fetched))
		}
	}
	r.CommitToFetch, r.CommitToStage, r.FetchToStage = summarizeDomainDurations(fetch), summarizeDomainDurations(stage), summarizeDomainDurations(queue)
	return r
}

type snapshotPipelineRoot struct {
	epoch, lastEpoch, position int64
	staged                     []domainCaptureFrame
}

type snapshotPipelineSink struct {
	c          domainCaptureConfig
	m          *domainCaptureMetrics
	x          *snapshotPipelineMetrics
	s          *domainCaptureScheduler
	budget     *semaphore.Weighted
	mu         sync.Mutex // protects root lookup; the scheduler serializes each root
	roots      map[int]*snapshotPipelineRoot
	batch      []domainCaptureFrame
	epoch      int64
	bytes      int64
	beforeTask func(domainCaptureTask) // deterministic protocol-test hooks
	afterStage func(domainCaptureTask)
	afterReady func(int, int64)
	afterFetch func(domainCaptureFrame) error
}

func newSnapshotPipelineSink(c domainCaptureConfig, m *domainCaptureMetrics) *snapshotPipelineSink {
	if c.ApplyBatchRows == 0 {
		c.ApplyBatchRows = 1 // route immediately; batching is an explicit treatment
	}
	if c.QueueBytes == 0 {
		c.QueueBytes = 32 << 20
	}
	x := &snapshotPipelineMetrics{m: m}
	m.experiment.pipeline = x
	s := newDomainCaptureScheduler()
	s.slots = make(chan struct{}, 65536)
	return &snapshotPipelineSink{c: c, m: m, x: x, s: s, budget: semaphore.NewWeighted(c.QueueBytes), roots: map[int]*snapshotPipelineRoot{}}
}

// The one SQL reader calls push/flush. Batches use identical global row/byte
// boundaries in paged and streamed treatments; a page boundary never flushes.
func (p *snapshotPipelineSink) push(ctx context.Context, epoch int64, f domainCaptureFrame) error {
	if p.epoch != epoch && len(p.batch) != 0 {
		return fmt.Errorf("unflushed pipeline epoch")
	}
	p.epoch = epoch
	p.batch = append(p.batch, f)
	p.bytes += int64(len(f.payload))
	if len(p.batch) >= p.c.ApplyBatchRows || p.bytes >= 256<<10 {
		return p.flush(ctx)
	}
	return nil
}

func (p *snapshotPipelineSink) flush(ctx context.Context) error {
	if len(p.batch) == 0 {
		return nil
	}
	if p.bytes > p.c.QueueBytes {
		return fmt.Errorf("application batch exceeds payload queue budget")
	}
	if !p.budget.TryAcquire(p.bytes) {
		start := time.Now()
		if err := p.budget.Acquire(ctx, p.bytes); err != nil {
			return err
		}
		p.x.mu.Lock()
		p.x.pressureCount++
		p.x.pressureSeconds += time.Since(start).Seconds()
		p.x.mu.Unlock()
	}
	groups := map[int][]domainCaptureFrame{}
	for _, f := range p.batch {
		groups[f.root] = append(groups[f.root], f)
	}
	// Sort at most ApplyBatchRows roots for reproducible, identical routing.
	for _, root := range sortedPipelineRoots(groups) {
		if err := p.s.enqueue(ctx, domainCaptureTask{root: root, frames: groups[root], pipelineEpoch: p.epoch}, p.m); err != nil {
			return err
		}
	}
	p.batch, p.bytes = nil, 0
	return nil
}

func sortedPipelineRoots[V any](groups map[int]V) []int {
	ids := make([]int, 0, len(groups))
	for r := range groups {
		ids = append(ids, r)
	}
	sort.Ints(ids)
	return ids
}

func (p *snapshotPipelineSink) seal(ctx context.Context, epoch int64, roots map[int]bool, eof time.Time) error {
	if err := p.flush(ctx); err != nil {
		return err
	}
	for _, root := range sortedPipelineRoots(roots) {
		if err := p.s.enqueue(ctx, domainCaptureTask{root: root, pipelineEpoch: epoch, pipelineEnd: eof}, p.m); err != nil {
			return err
		}
	}
	return nil
}

func (p *snapshotPipelineSink) process(t domainCaptureTask) error {
	p.mu.Lock()
	r := p.roots[t.root]
	if r == nil {
		r = &snapshotPipelineRoot{}
		p.roots[t.root] = r
	}
	p.mu.Unlock()
	if r.epoch == 0 {
		if t.pipelineEpoch <= r.lastEpoch {
			return fmt.Errorf("root epoch regression: %d <= %d", t.pipelineEpoch, r.lastEpoch)
		}
		r.epoch = t.pipelineEpoch
	}
	if t.pipelineEpoch != r.epoch {
		return fmt.Errorf("new root epoch before completion marker")
	}
	if !t.pipelineEnd.IsZero() {
		before := int64(0)
		for _, f := range r.staged {
			if p.m.events[f.id].staged.Before(t.pipelineEnd) {
				before++
			}
		}
		if err := p.m.completeVerified(r.staged); err != nil {
			return err
		}
		if p.c.PartitionGC {
			p.c.partitions.rootReady(t.pipelineEpoch)
		}
		p.m.experiment.ids(-int64(len(r.staged)))
		p.m.experiment.queue(-1)
		p.x.mu.Lock()
		p.x.roots++
		p.x.beforeEOF += before
		p.x.rootWait = append(p.x.rootWait, time.Since(t.pipelineEnd))
		p.x.mu.Unlock()
		r.lastEpoch, r.epoch, r.position, r.staged = r.epoch, 0, 0, nil
		if p.afterReady != nil {
			p.afterReady(t.root, t.pipelineEpoch)
		}
		return nil
	}
	domainCaptureDelay(p.c, t.frames)
	for _, f := range t.frames {
		if f.id <= 0 || f.id >= int64(len(p.m.events)) || f.root != t.root {
			return fmt.Errorf("invalid pipeline frame %d", f.id)
		}
		e := &p.m.events[f.id]
		if e.root != f.root || len(f.payload) != p.c.Rows*p.c.Payload || crc32.ChecksumIEEE(f.payload) != e.crc {
			return fmt.Errorf("invalid pipeline payload %d", f.id)
		}
		if !e.staged.IsZero() || f.position <= r.position {
			return fmt.Errorf("duplicate or reordered pipeline frame %d", f.id)
		}
		e.staged = time.Now()
		r.position = f.position
		r.staged = append(r.staged, domainCaptureFrame{position: f.position, root: f.root, id: f.id})
		p.m.experiment.ids(1)
	}
	p.m.experiment.buffer(t.frames, -1)
	n := int64(0)
	for _, f := range t.frames {
		n += int64(len(f.payload))
	}
	p.budget.Release(n)
	if p.afterStage != nil {
		p.afterStage(t)
	}
	return nil
}

func (p *snapshotPipelineSink) start(ctx context.Context, fail func(error), wg *sync.WaitGroup) {
	for i := 0; i < p.c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				t, ok := p.s.take(ctx)
				if !ok {
					return
				}
				if p.beforeTask != nil {
					p.beforeTask(t)
				}
				err := p.process(t)
				p.s.complete(t.root)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
			}
		}()
	}
}

func snapshotPipelineQuery(o replicationBenchObjects, paged bool) string {
	q := fmt.Sprintf("SELECT position,root_id,batch_id,payload FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) ORDER BY position", domainSnapshotCandidates(o, false))
	if paged {
		q += " LIMIT $6"
	}
	return q
}

// A pgx Rows iterator streams DataRow messages. There is no SQL DECLARE/FETCH
// cursor and no per-row query. The immutable journal + durable cuts make the
// epoch's membership fixed even when page statements use different snapshots.
func fetchSnapshotPipelineRows(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, cut snapshotFetchCut, c domainCaptureConfig, m *domainCaptureMetrics, row func(domainCaptureFrame) error) error {
	if c.FetchPlan != "" {
		return fetchSnapshotOptimizedRows(ctx, db, o, cut, c, m, row)
	}
	paged := c.SnapshotFetch != "pipeline-stream"
	limit := c.FetchPageRows
	if limit == 0 {
		limit = 128
	}
	query := snapshotPipelineQuery(o, paged)
	position := int64(0) // reset each epoch: late commits may have old positions
	for {
		args := []any{cut.a.xmax, cut.z.xmax, position, cut.a.xip, cut.z.text}
		if paged {
			args = append(args, limit)
		}
		start := time.Now()
		m.queries.Add(1)
		m.experiment.fetchQueries.Add(1)
		rows, err := db.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var f domainCaptureFrame
			if err = rows.Scan(&f.position, &f.root, &f.id, &f.payload); err != nil {
				rows.Close()
				return err
			}
			if count == 0 {
				x := m.experiment.pipeline
				x.mu.Lock()
				x.firstRow = append(x.firstRow, time.Since(start))
				x.mu.Unlock()
			}
			if f.position <= position {
				rows.Close()
				return fmt.Errorf("pipeline fetch position regression")
			}
			position = f.position
			count++
			if err = snapshotFetchPause(ctx, c, []domainCaptureFrame{f}, m.experiment); err == nil {
				err = row(f)
			}
			if err != nil {
				rows.Close()
				return err
			}
		}
		rows.Close()
		m.experiment.sql("fetch", time.Since(start))
		if err = rows.Err(); err != nil {
			return err
		}
		if !paged || count < limit {
			return nil
		}
		// Drain a full page immediately. Notification only captures new epochs.
	}
}

func runSnapshotPipelineCut(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, cut snapshotFetchCut, p *snapshotPipelineSink) error {
	start := time.Now()
	roots := map[int]bool{}
	var buffered []domainCaptureFrame
	rows, bytes := int64(0), int64(0)
	err := fetchSnapshotPipelineRows(ctx, db, o, cut, p.c, p.m, func(f domainCaptureFrame) error {
		if f.id <= 0 || f.id >= int64(len(p.m.events)) {
			return fmt.Errorf("unexpected pipeline operation %d", f.id)
		}
		p.m.events[f.id].fetched = time.Now()
		if rows == 0 {
			p.x.mu.Lock()
			p.x.epochFirstRow = append(p.x.epochFirstRow, time.Since(start))
			p.x.mu.Unlock()
		}
		p.m.experiment.buffer([]domainCaptureFrame{f}, 1)
		rows++
		bytes += int64(len(f.payload))
		if !roots[f.root] {
			roots[f.root] = true
			p.m.experiment.queue(1)
		}
		if p.c.SnapshotFetch == "pipeline-buffered" {
			buffered = append(buffered, f)
		} else if err := p.push(ctx, cut.epoch, f); err != nil {
			return err
		}
		if p.afterFetch != nil {
			return p.afterFetch(f)
		}
		return nil
	})
	if err != nil {
		return err // no completion marker: partial work cannot become readable
	}
	eof := time.Now()
	p.x.mu.Lock()
	p.x.fetchedRows += rows
	p.x.fetchedBytes += bytes
	p.x.fetchSeconds += eof.Sub(start).Seconds()
	p.x.epochFetch = append(p.x.epochFetch, eof.Sub(start))
	p.x.mu.Unlock()
	if rows == 0 {
		p.m.experiment.empty.Add(1)
	}
	if p.c.PartitionGC {
		p.c.partitions.seal(cut, len(roots))
	}
	for i := range buffered {
		f := buffered[i]
		buffered[i] = domainCaptureFrame{}
		if err := p.push(ctx, cut.epoch, f); err != nil {
			return err
		}
	}
	return p.seal(ctx, cut.epoch, roots, eof)
}

func startSnapshotPipeline(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, cuts <-chan snapshotFetchCut, fail func(error), wg *sync.WaitGroup) error {
	if c.SnapshotFetch != "pipeline-paged" && c.SnapshotFetch != "pipeline-stream" && c.SnapshotFetch != "pipeline-buffered" {
		return fmt.Errorf("invalid pipeline strategy %q", c.SnapshotFetch)
	}
	p := newSnapshotPipelineSink(c, m)
	p.start(ctx, fail, wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case cut := <-cuts:
				if err := runSnapshotPipelineCut(ctx, db, o, cut, p); err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
			}
		}
	}()
	return nil
}

func BenchmarkSnapshotPipeline(b *testing.B) {
	c := domainCaptureConfig{Name: "pipeline", Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed"}.defaults()
	if err := loadSnapshotFetchConfig(&c); err != nil {
		b.Fatal(err)
	}
	runDomainCapture(b, c.defaults())
}
