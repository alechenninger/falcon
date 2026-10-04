package postgres_test

// This experiment deliberately stops at complete, ordered change delivery.
// It does not implement Falcon's MVCC windows or query protocol.
import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type domainCaptureConfig struct {
	Name            string  `json:"name"`
	Mode            string  `json:"mode"` // snapshot-coupled, snapshot-roots, snapshot-routed, counter-{global,tenant,root}, wal
	Tenants         int     `json:"tenants"`
	RootsPerTenant  int     `json:"roots_per_tenant"`
	Rows            int     `json:"rows"`
	Payload         int     `json:"payload"`
	Keys            int     `json:"keys"`
	Rate            int     `json:"rate"`
	Skew            string  `json:"skew"`  // uniform, pareto, hot-root
	Fault           string  `json:"fault"` // before, after, rollback, delivery
	History         int     `json:"history"`
	Workers         int     `json:"workers"`
	CounterFetch    string  `json:"counter_fetch,omitempty"`
	SnapshotFetch   string  `json:"snapshot_fetch,omitempty"`   // global or fair; follow-up experiment only
	SnapshotWake    string  `json:"snapshot_wake,omitempty"`    // poll or notify
	SnapshotProbe   string  `json:"snapshot_probe,omitempty"`   // empty: original probe; indexed: bounded per-xid probe
	SnapshotCapture string  `json:"snapshot_capture,omitempty"` // empty/multi: sequential calls; single: one atomic SQL statement
	NotifyBatchMS   int     `json:"notify_batch_ms,omitempty"`
	FallbackMS      int     `json:"fallback_ms,omitempty"`
	FetchBatchRoots int     `json:"fetch_batch_roots,omitempty"`
	FetchPageRows   int     `json:"fetch_page_rows,omitempty"`
	HotShare        float64 `json:"hot_share,omitempty"`
	DropWakeEvery   int     `json:"drop_wake_every,omitempty"`
	ApplyBatchRows  int     `json:"apply_batch_rows,omitempty"`
	QueueBytes      int64   `json:"queue_bytes,omitempty"`
	Backlog         bool    `json:"backlog,omitempty"`
	BurstSize       int     `json:"burst_size,omitempty"`
	FetchPlan       string  `json:"fetch_plan,omitempty"`
	PositionCover   bool    `json:"position_cover,omitempty"`
	PlanCache       string  `json:"plan_cache,omitempty"`
	CompoundKey     bool    `json:"compound_key,omitempty"`
	PartitionSpan   int64   `json:"partition_span,omitempty"`
	PartitionAhead  int     `json:"partition_ahead,omitempty"`
	PartitionGC     bool    `json:"partition_gc,omitempty"`
	HistoryTx       int     `json:"history_tx,omitempty"`
	partitions      *snapshotPartitionManager
}

func (c domainCaptureConfig) roots() int     { return c.Tenants * c.RootsPerTenant }
func (c domainCaptureConfig) snapshot() bool { return strings.HasPrefix(c.Mode, "snapshot-") }
func (c domainCaptureConfig) domain(root int) int {
	switch c.Mode {
	case "counter-tenant":
		return root / c.RootsPerTenant
	case "counter-root":
		return root
	default:
		return 0
	}
}
func (c domainCaptureConfig) defaults() domainCaptureConfig {
	if c.Tenants == 0 {
		c.Tenants = 1
	}
	if c.RootsPerTenant == 0 {
		c.RootsPerTenant = 64
	}
	if c.Rows == 0 {
		c.Rows = 1
	}
	if c.Payload == 0 {
		c.Payload = 256
	}
	if c.Keys == 0 {
		c.Keys = 4096
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if strings.HasPrefix(c.Mode, "counter-") && c.CounterFetch == "" {
		c.CounterFetch = "bounded"
	}
	if c.Skew == "" {
		c.Skew = "uniform"
	}
	if c.PartitionSpan > 0 && c.partitions == nil {
		c.partitions = newSnapshotPartitionManager(c)
	}
	return c
}
func BenchmarkDomainCapture(b *testing.B) {
	c := domainCaptureConfig{Name: "default", Mode: "snapshot-routed"}.defaults()
	if value := os.Getenv("FALCON_DOMAIN_CAPTURE_CONFIG"); value != "" {
		if err := json.Unmarshal([]byte(value), &c); err != nil {
			b.Fatal(err)
		}
		c = c.defaults()
	}
	runDomainCapture(b, c)
}

type domainCaptureEvent struct {
	root                                                  int
	key                                                   int64
	scheduled, started, appendStarted, committed, applied time.Time
	crc                                                   uint32
	fetched, staged                                       time.Time
}
type domainCaptureFrame struct {
	position int64
	root     int
	id       int64
	payload  []byte
}
type domainCaptureTask struct {
	root           int
	frames         []domainCaptureFrame
	previous, next snapshotBenchCut
	count          int
	fetch          bool
	pipelineEpoch  int64
	pipelineEnd    time.Time
}
type domainCaptureMetrics struct {
	mu         sync.Mutex
	events     []domainCaptureEvent
	latest     map[[2]int64]int64
	applied    int
	done       chan struct{}
	finished   time.Time
	queries    atomic.Int64
	cuts       atomic.Int64
	rootTasks  atomic.Int64
	maxQueue   atomic.Int64
	onCommit   func(int64)
	experiment *snapshotFetchMetrics
}

func (m *domainCaptureMetrics) apply(c domainCaptureConfig, frames []domainCaptureFrame) error {
	if len(frames) == 0 {
		return nil
	}
	for _, f := range frames {
		if f.id <= 0 || f.id >= int64(len(m.events)) {
			return fmt.Errorf("unexpected operation %d", f.id)
		}
		if len(f.payload) != c.Rows*c.Payload || crc32.ChecksumIEEE(f.payload) != m.events[f.id].crc {
			return fmt.Errorf("invalid payload %d", f.id)
		}
	}
	return m.completeVerified(frames)
}

// Payloads have already been checked. Fair fetching stages verified IDs without
// retaining payloads, but records readiness only after the entire root epoch.
func (m *domainCaptureMetrics) completeVerified(frames []domainCaptureFrame) error {
	if len(frames) == 0 {
		return nil
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range frames {
		e := &m.events[f.id]
		if !e.applied.IsZero() {
			return fmt.Errorf("duplicate operation %d", f.id)
		}
		if f.root != e.root {
			return fmt.Errorf("root mismatch %d", f.id)
		}
		e.applied = now
		m.latest[[2]int64{int64(e.root), e.key}] = f.id
		m.applied++
	}
	if m.applied == len(m.events)-1 {
		m.finished = now
		close(m.done)
	}
	return nil
}

// A root has at most one active task. Other roots remain eligible while it is
// blocked. Queues are bounded globally, so overload is measured, not hidden.
type domainCaptureScheduler struct {
	mu     sync.Mutex
	queues map[int][]domainCaptureTask
	ready  []int
	active map[int]bool
	wake   chan struct{}
	slots  chan struct{}
}

func newDomainCaptureScheduler() *domainCaptureScheduler {
	return &domainCaptureScheduler{queues: map[int][]domainCaptureTask{}, active: map[int]bool{}, wake: make(chan struct{}, 1), slots: make(chan struct{}, 4096)}
}
func (s *domainCaptureScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *domainCaptureScheduler) enqueue(ctx context.Context, t domainCaptureTask, m *domainCaptureMetrics) error {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.queues[t.root] = append(s.queues[t.root], t)
	if !s.active[t.root] {
		s.active[t.root] = true
		s.ready = append(s.ready, t.root)
	}
	s.mu.Unlock()
	m.rootTasks.Add(1)
	n := int64(len(s.slots))
	for {
		old := m.maxQueue.Load()
		if n <= old || m.maxQueue.CompareAndSwap(old, n) {
			break
		}
	}
	s.signal()
	return nil
}
func (s *domainCaptureScheduler) take(ctx context.Context) (domainCaptureTask, bool) {
	for {
		s.mu.Lock()
		if len(s.ready) > 0 {
			r := s.ready[0]
			s.ready = s.ready[1:]
			t := s.queues[r][0]
			s.queues[r][0] = domainCaptureTask{} // release payload references in the backing array
			s.queues[r] = s.queues[r][1:]
			s.mu.Unlock()
			s.signal()
			return t, true
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return domainCaptureTask{}, false
		case <-s.wake:
		}
	}
}
func (s *domainCaptureScheduler) complete(root int) {
	s.mu.Lock()
	if len(s.queues[root]) > 0 {
		s.ready = append(s.ready, root)
	} else {
		delete(s.queues, root)
		delete(s.active, root)
	}
	s.mu.Unlock()
	<-s.slots
	s.signal()
}

func prepareDomainCapture(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig) error {
	_, err := p.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s(root_id INT NOT NULL, object_key BIGINT NOT NULL, batch_id BIGINT NOT NULL, ordinal INT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(root_id,object_key,ordinal)); ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;`, o.table, o.table))
	if err != nil {
		return err
	}
	if c.Mode == "wal" {
		_, err = p.Exec(ctx, "CREATE PUBLICATION "+o.publication+" FOR TABLE "+o.table)
		return err
	}
	if c.snapshot() && (c.CompoundKey || c.PartitionSpan > 0 || c.HistoryTx > 0) {
		return prepareSnapshotPartitionFixture(ctx, p, o, c)
	}
	if c.snapshot() {
		_, err = p.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE %s CACHE 1; CREATE TABLE %s(position BIGINT PRIMARY KEY DEFAULT nextval('%s'),root_id INT NOT NULL,writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id(),batch_id BIGINT NOT NULL,payload BYTEA NOT NULL); ALTER SEQUENCE %s OWNED BY %s.position; ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL; CREATE INDEX ON %s(writer_xid,position); CREATE INDEX ON %s(root_id,writer_xid,position); CREATE TABLE %s(epoch BIGINT PRIMARY KEY,previous_snapshot PG_SNAPSHOT NOT NULL,snapshot PG_SNAPSHOT NOT NULL);`, o.sequence, o.journal, o.sequence, o.sequence, o.journal, o.journal, o.journal, o.journal, o.clock))
	} else {
		domains := 1
		if c.Mode == "counter-tenant" {
			domains = c.Tenants
		}
		if c.Mode == "counter-root" {
			domains = c.roots()
		}
		_, err = p.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s(domain INT PRIMARY KEY,position BIGINT NOT NULL); INSERT INTO %s SELECT d,0 FROM generate_series(0,%d) d; CREATE TABLE %s(domain INT NOT NULL,position BIGINT NOT NULL,root_id INT NOT NULL,batch_id BIGINT NOT NULL,payload BYTEA NOT NULL,PRIMARY KEY(domain,position)); ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;`, o.clock, o.clock, domains-1, o.journal, o.journal))
	}
	if err != nil {
		return err
	}
	if c.PositionCover {
		if !c.snapshot() {
			return fmt.Errorf("position covering index requires a snapshot journal")
		}
		_, err = p.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s_pkey; ALTER TABLE %s ADD PRIMARY KEY(position) INCLUDE(writer_xid)`, o.journal, o.journal, o.journal))
		if err != nil {
			return err
		}
	}
	if c.History > 0 {
		if c.snapshot() {
			_, err = p.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(root_id,batch_id,payload) SELECT n %% $2,0,decode(repeat('ab',$3),'hex') FROM generate_series(1,$1) n`, o.journal), c.History, c.roots(), c.Rows*c.Payload)
		} else {
			expr := "0"
			if c.Mode == "counter-tenant" {
				expr = "root_id / " + strconv.Itoa(c.RootsPerTenant)
			}
			if c.Mode == "counter-root" {
				expr = "root_id"
			}
			_, err = p.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(domain,position,root_id,batch_id,payload) SELECT domain,row_number() OVER(PARTITION BY domain ORDER BY n),root_id,0,decode(repeat('ab',$3),'hex') FROM (SELECT n,n %% $2 AS root_id,%s AS domain FROM (SELECT n,n %% $2 AS root_id FROM generate_series(1,$1) n) roots) ds`, o.journal, expr), c.History, c.roots(), c.Rows*c.Payload)
			if err == nil {
				_, err = p.Exec(ctx, fmt.Sprintf(`UPDATE %s h SET position=s.p FROM(SELECT domain,max(position) p FROM %s GROUP BY domain)s WHERE h.domain=s.domain`, o.clock, o.journal))
			}
		}
		if err != nil {
			return err
		}
		_, err = p.Exec(ctx, "ANALYZE "+o.journal)
		if err != nil {
			return err
		}
	}
	if c.snapshot() {
		_, err = p.Exec(ctx, "INSERT INTO "+o.clock+" VALUES(0,pg_current_snapshot(),pg_current_snapshot())")
	}
	return err
}

func domainCapturePayload(id int64, c domainCaptureConfig) []byte {
	// A deterministic, incompressible ASCII payload. The changing prefix ensures
	// pgoutput supplies UPDATE payloads rather than an unchanged TOAST marker.
	row := make([]byte, c.Payload)
	rng := rand.New(rand.NewSource(42))
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range row {
		row[i] = alphabet[rng.Intn(len(alphabet))]
	}
	copy(row, fmt.Sprintf("%016x", id))
	return []byte(strings.Repeat(string(row), c.Rows))
}
func domainCaptureTrace(n int, c domainCaptureConfig) []domainCaptureEvent {
	rng := rand.New(rand.NewSource(913))
	events := make([]domainCaptureEvent, n+1)
	weights := func(count int) []float64 {
		out := make([]float64, count)
		sum := 0.0
		for i := range out {
			w := 1.0
			if c.Skew == "pareto" {
				w = math.Pow(float64(i+1), -1.05)
			}
			sum += w
			out[i] = sum
		}
		for i := range out {
			out[i] /= sum
		}
		return out
	}
	tenants, roots := weights(c.Tenants), weights(c.RootsPerTenant)
	ranked := make([]float64, c.roots())
	if c.Skew == "pareto-roots" {
		sum := 0.0
		for i := range ranked {
			sum += math.Pow(float64(i+1), -1.05)
			ranked[i] = sum
		}
		for i := range ranked {
			ranked[i] /= sum
		}
	}
	choose := func(w []float64) int { return sort.SearchFloat64s(w, rng.Float64()) }
	for i := 1; i <= n; i++ {
		root := choose(tenants)*c.RootsPerTenant + choose(roots)
		if c.Skew == "pareto-roots" {
			root = choose(ranked)
		}
		hotShare := c.HotShare
		if hotShare == 0 {
			hotShare = .8
		}
		if c.Skew == "hot-root" && rng.Float64() < hotShare {
			root = 0
		}
		if c.Fault != "" && i%1000 == 0 {
			root = 0
		}
		events[i] = domainCaptureEvent{root: root, key: int64(rng.Intn(c.Keys)), crc: crc32.ChecksumIEEE(domainCapturePayload(int64(i), c))}
	}
	return events
}
func writeDomainCapture(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, id int64, m *domainCaptureMetrics) error {
	e := &m.events[id]
	e.started = time.Now()
	payload := domainCapturePayload(id, c)
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for attempt := 0; attempt < 2; attempt++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(root_id,object_key,batch_id,ordinal,payload) SELECT $1,$2,$3,n,$5 FROM generate_series(1,$4)n ON CONFLICT(root_id,object_key,ordinal) DO UPDATE SET batch_id=EXCLUDED.batch_id,payload=EXCLUDED.payload`, o.table), e.root, e.key, id, c.Rows, string(payload[:c.Payload]))
		if err != nil {
			tx.Rollback(context.Background())
			return err
		}
		fault := c.Fault != "" && id%1000 == 0 && attempt == 0
		if fault && c.Fault == "before" {
			time.Sleep(200 * time.Millisecond)
		}
		e.appendStarted = time.Now()
		query := ""
		args := []any{}
		if c.snapshot() {
			query = "INSERT INTO " + o.journal + "(root_id,batch_id,payload) VALUES($1,$2,$3)"
			args = []any{e.root, id, payload}
		} else if c.Mode != "wal" {
			query = fmt.Sprintf(`WITH allocated AS(UPDATE %s SET position=position+1 WHERE domain=$1 RETURNING position) INSERT INTO %s(domain,position,root_id,batch_id,payload) SELECT $1,position,$2,$3,$4 FROM allocated`, o.clock, o.journal)
			args = []any{c.domain(e.root), e.root, id, payload}
		}
		if fault && (c.Fault == "after" || c.Fault == "rollback") {
			if query != "" {
				_, err = tx.Exec(ctx, query, args...)
				if err != nil {
					tx.Rollback(context.Background())
					return err
				}
			}
			if c.Fault == "rollback" {
				if err = tx.Rollback(ctx); err != nil {
					return err
				}
				continue
			}
			time.Sleep(200 * time.Millisecond)
			err = tx.Commit(ctx)
		} else {
			// Retain the acquired connection until append AND COMMIT finish; sending a
			// raw COMMIT through a pool-owned Tx risks returning the connection early.
			batch := &pgx.Batch{}
			if query != "" {
				batch.Queue(query, args...)
			}
			batch.Queue("COMMIT")
			result := conn.SendBatch(ctx, batch)
			err = result.Close()
		}
		if err != nil {
			if conn.Conn().PgConn().TxStatus() != 'I' {
				tx.Rollback(context.Background())
			}
			return err
		}
		e.committed = time.Now()
		if m.onCommit != nil {
			m.onCommit(id)
		}
		return nil
	}
	return fmt.Errorf("retry exhausted")
}

func domainSnapshotCandidates(o replicationBenchObjects, root bool) string {
	filter := ""
	if root {
		filter = " AND root_id=$6"
	}
	return fmt.Sprintf(`SELECT position,root_id,writer_xid,batch_id,payload FROM %s WHERE writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8 AND position>$3%s UNION ALL SELECT position,root_id,writer_xid,batch_id,payload FROM %s WHERE writer_xid=ANY($4::text[]::xid8[]) AND position>$3%s`, o.journal, filter, o.journal, filter)
}
func domainSnapshotFrames(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, a, z snapshotBenchCut, root *int, m *domainCaptureMetrics, afterPage func([]domainCaptureFrame) error) ([]domainCaptureFrame, error) {
	cursor := int64(0)
	all := []domainCaptureFrame{}
	filter := root != nil
	limitArg := 6
	args := []any{a.xmax, z.xmax, cursor, a.xip, z.text}
	if filter {
		args = append(args, *root)
		limitArg = 7
	}
	args = append(args, 128)
	query := fmt.Sprintf(`SELECT position,root_id,batch_id,payload FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) ORDER BY position LIMIT $%d`, domainSnapshotCandidates(o, filter), limitArg)
	for {
		args[2] = cursor
		m.queries.Add(1)
		rows, err := p.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		page := []domainCaptureFrame{}
		for rows.Next() {
			var f domainCaptureFrame
			if err = rows.Scan(&f.position, &f.root, &f.id, &f.payload); err != nil {
				rows.Close()
				return nil, err
			}
			if f.position <= cursor {
				rows.Close()
				return nil, fmt.Errorf("snapshot page regression")
			}
			cursor = f.position
			page = append(page, f)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return nil, err
		}
		if afterPage != nil {
			if err = afterPage(page); err != nil {
				return nil, err
			}
		}
		all = append(all, page...)
		if len(page) < 128 {
			return all, nil
		}
	}
}
func captureDomainSnapshot(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, epoch int64, a snapshotBenchCut, m *domainCaptureMetrics) (snapshotBenchCut, error) {
	tx, err := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return snapshotBenchCut{}, err
	}
	defer tx.Rollback(context.Background())
	var text string
	m.queries.Add(1)
	if err = tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
		return snapshotBenchCut{}, err
	}
	z, err := parseSnapshotBenchCut(text)
	if err != nil {
		return z, err
	}
	m.queries.Add(1)
	_, err = tx.Exec(ctx, "INSERT INTO "+o.clock+" VALUES($1,$2::text::pg_snapshot,$3::text::pg_snapshot)", epoch, a.text, z.text)
	if err != nil {
		return z, err
	}
	err = tx.Commit(ctx)
	if err == nil {
		m.cuts.Add(1)
	}
	return z, err
}
func domainCaptureDelay(c domainCaptureConfig, frames []domainCaptureFrame) {
	if c.Fault == "delivery" {
		for _, f := range frames {
			if f.id%1000 == 0 {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
func domainCaptureWorkers(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, s *domainCaptureScheduler, wg *sync.WaitGroup, fail func(error)) {
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, ok := s.take(ctx)
				if !ok {
					return
				}
				frames := task.frames
				var err error
				if task.fetch {
					frames, err = domainSnapshotFrames(ctx, p, o, task.previous, task.next, &task.root, m, nil)
					if err == nil && len(frames) != task.count {
						err = fmt.Errorf("incomplete root epoch: %d/%d", len(frames), task.count)
					}
				}
				if err == nil {
					domainCaptureDelay(c, frames)
					err = m.apply(c, frames)
				}
				if m.experiment != nil {
					m.experiment.buffer(frames, -1)
				}
				s.complete(task.root)
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
func domainCaptureRoute(ctx context.Context, c domainCaptureConfig, m *domainCaptureMetrics, s *domainCaptureScheduler, frames []domainCaptureFrame) error {
	roots := map[int][]domainCaptureFrame{}
	for _, f := range frames {
		roots[f.root] = append(roots[f.root], f)
	}
	// Sorting makes enqueue fairness reproducible rather than map-order dependent.
	ids := make([]int, 0, len(roots))
	for r := range roots {
		ids = append(ids, r)
	}
	sort.Ints(ids)
	for _, r := range ids {
		if err := s.enqueue(ctx, domainCaptureTask{root: r, frames: roots[r]}, m); err != nil {
			return err
		}
	}
	return nil
}
func consumeDomainSnapshots(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, s *domainCaptureScheduler, fail func(error), wg *sync.WaitGroup) error {
	var first string
	if err := p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&first); err != nil {
		return err
	}
	a, err := parseSnapshotBenchCut(first)
	if err != nil {
		return err
	}
	type cut struct {
		epoch int64
		a, z  snapshotBenchCut
	}
	cuts := make(chan cut, 64)
	// Coupled captures/fetches/publishes in one loop. Split commits the manifest
	// before payload discovery, and capture can progress while delivery waits.
	if c.Mode != "snapshot-coupled" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for epoch := int64(1); ; epoch++ {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				z, err := captureDomainSnapshot(ctx, p, o, epoch, a, m)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				select {
				case cuts <- cut{epoch, a, z}:
					a = z
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		epoch := int64(0)
		for {
			var next cut
			if c.Mode == "snapshot-coupled" {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				var text string
				m.queries.Add(1)
				if err := p.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				z, err := parseSnapshotBenchCut(text)
				if err != nil {
					fail(err)
					return
				}
				epoch++
				next = cut{epoch, a, z}
			} else {
				select {
				case <-ctx.Done():
					return
				case next = <-cuts:
				}
			}
			if c.Mode == "snapshot-roots" {
				m.queries.Add(1)
				query := fmt.Sprintf(`SELECT root_id,count(*) FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) GROUP BY root_id ORDER BY root_id`, domainSnapshotCandidates(o, false))
				rows, err := p.Query(ctx, query, next.a.xmax, next.z.xmax, int64(0), next.a.xip, next.z.text)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				tasks := []domainCaptureTask{}
				for rows.Next() {
					t := domainCaptureTask{previous: next.a, next: next.z, fetch: true}
					if err = rows.Scan(&t.root, &t.count); err != nil {
						break
					}
					tasks = append(tasks, t)
				}
				rows.Close()
				if err == nil {
					err = rows.Err()
				}
				if err != nil {
					fail(err)
					return
				}
				for _, t := range tasks {
					if err = s.enqueue(ctx, t, m); err != nil {
						return
					}
				}
			} else {
				frames, err := domainSnapshotFrames(ctx, p, o, next.a, next.z, nil, m, nil)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				if c.Mode == "snapshot-coupled" {
					// Model one common delivery boundary: the complete fetch and any
					// injected stall precede publication; synthetic delivery follows it.
					domainCaptureDelay(c, frames)
					m.queries.Add(1)
					_, err = p.Exec(ctx, "INSERT INTO "+o.clock+" VALUES($1,$2::text::pg_snapshot,$3::text::pg_snapshot)", next.epoch, next.a.text, next.z.text)
					if err == nil {
						m.cuts.Add(1)
						err = m.apply(c, frames)
					}
					a = next.z
				} else {
					err = domainCaptureRoute(ctx, c, m, s, frames)
				}
				if err != nil {
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

// A committed counter head certifies a dense committed prefix. Read all changed
// domains in a single indexed page query, instead of one round trip per root.
func consumeDomainCounters(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, s *domainCaptureScheduler, fail func(error), wg *sync.WaitGroup) error {
	cursors := map[int]int64{}
	rows, err := p.Query(ctx, "SELECT domain,position FROM "+o.clock)
	if err != nil {
		return err
	}
	for rows.Next() {
		var d int
		var pos int64
		if err = rows.Scan(&d, &pos); err != nil {
			rows.Close()
			return err
		}
		cursors[d] = pos
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			m.queries.Add(1)
			rows, err := p.Query(ctx, "SELECT domain,position FROM "+o.clock+" ORDER BY domain")
			if err != nil {
				if ctx.Err() == nil {
					fail(err)
				}
				return
			}
			ds := []int32{}
			lo, hi := []int64{}, []int64{}
			expected := int64(0)
			for rows.Next() {
				var d int
				var pos int64
				if err = rows.Scan(&d, &pos); err != nil {
					break
				}
				if pos > cursors[d] {
					ds = append(ds, int32(d))
					lo = append(lo, cursors[d])
					hi = append(hi, pos)
					expected += pos - cursors[d]
				}
			}
			rows.Close()
			if err == nil {
				err = rows.Err()
			}
			if err != nil {
				fail(err)
				return
			}
			if len(ds) == 0 {
				continue
			}
			cursorD, cursorP := -1, int64(0)
			seen := int64(0)
			for {
				m.queries.Add(1)
				rows, err = p.Query(ctx, domainCounterPageQuery(o, c.CounterFetch), ds, lo, hi, cursorD, cursorP)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				page := []domainCaptureFrame{}
				for rows.Next() {
					var f domainCaptureFrame
					var d int
					if err = rows.Scan(&d, &f.position, &f.root, &f.id, &f.payload); err != nil {
						break
					}
					if d == cursorD && f.position != cursorP+1 {
						err = fmt.Errorf("counter gap in domain %d at %d", d, f.position)
						break
					}
					cursorD, cursorP = d, f.position
					seen++
					page = append(page, f)
				}
				rows.Close()
				if err == nil {
					err = rows.Err()
				}
				if err != nil {
					fail(err)
					return
				}
				if err = domainCaptureRoute(ctx, c, m, s, page); err != nil {
					return
				}
				if len(page) < 128 {
					break
				}
			}
			if seen != expected {
				fail(fmt.Errorf("counter prefix incomplete %d/%d", seen, expected))
				return
			}
			for i, d := range ds {
				cursors[int(d)] = hi[i]
			}
		}
	}()
	return nil
}

func startDomainWAL(ctx context.Context, connString string, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, s *domainCaptureScheduler, fail func(error), wg *sync.WaitGroup) error {
	conn, err := pgconn.Connect(ctx, replicationBenchConnString(connString))
	if err != nil {
		return err
	}
	slot, err := pglogrepl.CreateReplicationSlot(ctx, conn, o.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		conn.Close(context.Background())
		return err
	}
	lsn, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return err
	}
	err = pglogrepl.StartReplication(ctx, conn, o.slot, lsn, pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names '" + o.publication + "'"}})
	if err != nil {
		conn.Close(context.Background())
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer conn.Close(context.Background())
		relations := map[uint32]*pglogrepl.RelationMessage{}
		var frames []domainCaptureFrame
		var current domainCaptureFrame
		var count int
		add := func(rel uint32, tuple *pglogrepl.TupleData) error {
			r := relations[rel]
			if r == nil || len(tuple.Columns) != len(r.Columns) {
				return fmt.Errorf("unknown WAL relation")
			}
			var f domainCaptureFrame
			for i, col := range r.Columns {
				v := tuple.Columns[i]
				switch col.Name {
				case "root_id":
					n, err := strconv.Atoi(string(v.Data))
					if err != nil {
						return err
					}
					f.root = n
				case "batch_id":
					n, err := strconv.ParseInt(string(v.Data), 10, 64)
					if err != nil {
						return err
					}
					f.id = n
				case "payload":
					if v.DataType != 't' {
						return fmt.Errorf("WAL payload unavailable: %c", v.DataType)
					}
					f.payload = append(f.payload, v.Data...)
				}
			}
			if count == 0 {
				current = f
			} else {
				if f.id != current.id || f.root != current.root {
					return fmt.Errorf("mixed WAL transaction")
				}
				current.payload = append(current.payload, f.payload...)
			}
			count++
			return nil
		}
		for {
			raw, err := conn.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					fail(err)
				}
				return
			}
			if e, ok := raw.(*pgproto3.ErrorResponse); ok {
				fail(fmt.Errorf("WAL: %s", e.Message))
				return
			}
			data, ok := raw.(*pgproto3.CopyData)
			if !ok || len(data.Data) == 0 {
				continue
			}
			if data.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
				k, e := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
				if e != nil {
					fail(e)
					return
				}
				if k.ReplyRequested { // Conservatively retain WAL until this sample finishes. No false applied ACK.
					if e = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn}); e != nil {
						if ctx.Err() == nil {
							fail(e)
						}
						return
					}
				}
				continue
			}
			if data.Data[0] != pglogrepl.XLogDataByteID {
				continue
			}
			x, e := pglogrepl.ParseXLogData(data.Data[1:])
			if e != nil {
				fail(e)
				return
			}
			message, e := pglogrepl.Parse(x.WALData)
			if e != nil {
				fail(e)
				return
			}
			switch v := message.(type) {
			case *pglogrepl.RelationMessage:
				relations[v.RelationID] = v
			case *pglogrepl.BeginMessage:
				frames = nil
				current = domainCaptureFrame{}
				count = 0
			case *pglogrepl.InsertMessage:
				e = add(v.RelationID, v.Tuple)
			case *pglogrepl.UpdateMessage:
				e = add(v.RelationID, v.NewTuple)
			case *pglogrepl.CommitMessage:
				if count > 0 {
					if count != c.Rows {
						e = fmt.Errorf("incomplete WAL batch %d", count)
					} else {
						frames = append(frames, current)
						e = domainCaptureRoute(ctx, c, m, s, frames)
					}
				}
			}
			if e != nil {
				if ctx.Err() == nil {
					fail(e)
				}
				return
			}
		}
	}()
	return nil
}

type domainCaptureSummary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

func summarizeDomainDurations(values []time.Duration) domainCaptureSummary {
	s := domainCaptureSummary{Count: len(values)}
	if len(values) == 0 {
		return s
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(p float64) float64 {
		i := int(math.Ceil(float64(len(values))*p)) - 1
		if i < 0 {
			i = 0
		}
		return float64(values[i]) / float64(time.Millisecond)
	}
	s.P50 = at(.5)
	s.P95 = at(.95)
	s.P99 = at(.99)
	s.P999 = at(.999)
	s.Max = at(1)
	return s
}

type domainCaptureResult struct {
	Config                                         domainCaptureConfig `json:"config"`
	Operations                                     int                 `json:"operations"`
	WriteTPS, ApplyTPS, WriteSeconds, DrainSeconds float64
	Write, AppendCommit, Freshness, Arrival        domainCaptureSummary
	Cohorts                                        map[string]domainCaptureSummary
	CohortWrites, CohortArrivals                   map[string]domainCaptureSummary
	Queries, Cuts, RootTasks, MaxQueued            int64
	ApplyBeforeAck                                 int
	RootTop20Share, RootTop1Share                  float64
	WALBytes, JournalBytes, SourceBytes            int64
	Snapshot                                       *snapshotFetchResult     `json:"snapshot,omitempty"`
	Partitions                                     *snapshotPartitionResult `json:"partitions,omitempty"`
}

func runDomainCapture(b *testing.B, c domainCaptureConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	connString, cleanup, _ := setupReplicationBenchPostgres(ctx, b)
	defer cleanup()
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		b.Fatal(err)
	}
	config.MaxConns = 16
	var experiment *snapshotFetchMetrics
	if c.SnapshotFetch != "" {
		experiment = newSnapshotFetchMetrics(c)
	}
	writers, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		b.Fatal(err)
	}
	defer writers.Close()
	consumerConfig := config.Copy()
	consumerConfig.MaxConns = 5
	if c.PlanCache != "" {
		consumerConfig.ConnConfig.RuntimeParams["plan_cache_mode"] = c.PlanCache
	}
	if experiment != nil {
		consumerConfig.ConnConfig.DialFunc = experiment.dial
	}
	consumers, err := pgxpool.NewWithConfig(ctx, consumerConfig)
	if err != nil {
		b.Fatal(err)
	}
	defer consumers.Close()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, writers, o, c); err != nil {
		b.Fatal(err)
	}
	defer cleanupReplicationBenchObjects(context.Background(), writers, connString, o, c.Mode == "wal")
	m := &domainCaptureMetrics{events: domainCaptureTrace(b.N, c), latest: map[[2]int64]int64{}, done: make(chan struct{})}
	m.experiment = experiment
	s := newDomainCaptureScheduler()
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { stop(); wg.Wait() }()
	errors := make(chan error, 1)
	fail := func(err error) {
		if runCtx.Err() != nil {
			return
		}
		select {
		case errors <- err:
		default:
		}
		stop()
	}
	if c.PartitionGC {
		if err = c.partitions.start(runCtx, connString, o, fail, &wg); err != nil {
			b.Fatal(err)
		}
	}
	if c.Mode != "snapshot-coupled" && c.SnapshotFetch != "fair" && !c.pipeline() {
		domainCaptureWorkers(runCtx, consumers, o, c, m, s, &wg, fail)
	}
	startConsumer := func() {
		if c.SnapshotFetch != "" {
			err = startSnapshotFetchExperiment(runCtx, consumers, connString, o, c, m, s, fail, &wg)
		} else if c.snapshot() {
			err = consumeDomainSnapshots(runCtx, consumers, o, c, m, s, fail, &wg)
		} else if c.Mode == "wal" {
			err = startDomainWAL(runCtx, connString, o, c, m, s, fail, &wg)
		} else {
			err = consumeDomainCounters(runCtx, consumers, o, c, m, s, fail, &wg)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	if !c.Backlog {
		startConsumer()
	}
	// Establish all writer connections before measuring, equally for every mode.
	conns := make([]*pgxpool.Conn, 0, 16)
	for i := 0; i < 16; i++ {
		conn, e := writers.Acquire(ctx)
		if e != nil {
			b.Fatal(e)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Release()
	}
	if experiment != nil {
		conns = conns[:0]
		for i := 0; i < 5; i++ {
			conn, e := consumers.Acquire(ctx)
			if e != nil {
				b.Fatal(e)
			}
			conns = append(conns, conn)
		}
		for _, conn := range conns {
			conn.Release()
		}
	}
	var walStart string
	if err = writers.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&walStart); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	started := time.Now()
	if c.partitions != nil {
		c.partitions.beginMeasurement()
	}
	if m.experiment != nil {
		m.experiment.start(started)
	}
	jobs := make(chan int64, 16)
	var sourceWG sync.WaitGroup
	for i := 0; i < 16; i++ {
		sourceWG.Add(1)
		go func() {
			defer sourceWG.Done()
			for id := range jobs {
				if runCtx.Err() != nil {
					return
				}
				if err := writeDomainCapture(runCtx, writers, o, c, id, m); err != nil {
					if runCtx.Err() == nil {
						fail(err)
					}
					return
				}
			}
		}()
	}
	schedule := func() {
		defer close(jobs)
		for id := int64(1); id <= int64(b.N); id++ {
			at := time.Time{}
			if c.Rate > 0 {
				n := id - 1
				if c.BurstSize > 0 {
					n = n / int64(c.BurstSize) * int64(c.BurstSize)
				}
				at = started.Add(time.Duration(float64(n) * float64(time.Second) / float64(c.Rate)))
				if wait := time.Until(at); wait > 0 {
					timer := time.NewTimer(wait)
					select {
					case <-timer.C:
					case <-runCtx.Done():
						timer.Stop()
						return
					}
				}
			}
			m.events[id].scheduled = at
			select {
			case jobs <- id:
			case <-runCtx.Done():
				return
			}
		}
	}
	schedule()
	sourceWG.Wait()
	writesFinished := time.Now()
	if c.Backlog {
		startConsumer()
	}
	select {
	case <-m.done:
	case e := <-errors:
		b.Fatal(e)
	case <-ctx.Done():
		b.Fatalf("drain %d/%d: %v", m.applied, b.N, ctx.Err())
	}
	b.StopTimer()
	stop()
	wg.Wait()
	select {
	case e := <-errors:
		b.Fatal(e)
	default:
	}
	// Compare the delivered final state to the actual latest-state table. This
	// catches same-key reordering even when counts and payload checksums pass.
	rows, err := writers.Query(ctx, "SELECT root_id,object_key,batch_id,count(*) FROM "+o.table+" GROUP BY root_id,object_key,batch_id")
	if err != nil {
		b.Fatal(err)
	}
	seen := 0
	for rows.Next() {
		var root, count int
		var key, id int64
		if err = rows.Scan(&root, &key, &id, &count); err != nil {
			rows.Close()
			b.Fatal(err)
		}
		if count != c.Rows || m.latest[[2]int64{int64(root), key}] != id {
			rows.Close()
			b.Fatalf("latest-state mismatch root=%d key=%d id=%d delivered=%d rows=%d", root, key, id, m.latest[[2]int64{int64(root), key}], count)
		}
		seen++
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		b.Fatal(err)
	}
	if seen != len(m.latest) {
		b.Fatal("latest-state cardinality mismatch")
	}
	r := domainCaptureResult{Config: c, Operations: b.N, WriteSeconds: writesFinished.Sub(started).Seconds(), DrainSeconds: math.Max(0, m.finished.Sub(writesFinished).Seconds()), Cohorts: map[string]domainCaptureSummary{}, Queries: m.queries.Load(), Cuts: m.cuts.Load(), RootTasks: m.rootTasks.Load(), MaxQueued: m.maxQueue.Load()}
	r.WriteTPS = float64(b.N) / r.WriteSeconds
	r.ApplyTPS = float64(b.N) / m.finished.Sub(started).Seconds()
	write, appendCommit, fresh, arrival := []time.Duration{}, []time.Duration{}, []time.Duration{}, []time.Duration{}
	cohorts := map[string][]time.Duration{}
	cohortWrites, cohortArrivals := map[string][]time.Duration{}, map[string][]time.Duration{}
	rootCounts := make([]int, c.roots())
	for id := 1; id <= b.N; id++ {
		e := m.events[id]
		if e.committed.IsZero() || e.applied.IsZero() {
			b.Fatalf("missing timing %d", id)
		}
		write = append(write, e.committed.Sub(e.started))
		appendCommit = append(appendCommit, e.committed.Sub(e.appendStarted))
		f := e.applied.Sub(e.committed)
		if f < 0 {
			r.ApplyBeforeAck++
			f = 0
		}
		fresh = append(fresh, f)
		if !e.scheduled.IsZero() {
			arrival = append(arrival, e.applied.Sub(e.scheduled))
		}
		rootCounts[e.root]++
		cohort := "other-tenants"
		if e.root == 0 {
			cohort = "affected-root"
		} else if e.root/c.RootsPerTenant == 0 {
			cohort = "sibling-roots"
		}
		if c.Fault != "" && id%1000 == 0 {
			cohort = "injected"
		}
		cohorts[cohort] = append(cohorts[cohort], f)
		cohortWrites[cohort] = append(cohortWrites[cohort], e.committed.Sub(e.started))
		if !e.scheduled.IsZero() {
			cohortArrivals[cohort] = append(cohortArrivals[cohort], e.applied.Sub(e.scheduled))
		}
		quiet := e.root != 0
		if c.Skew == "pareto-roots" {
			quiet = e.root >= (c.roots()+9)/10 // lower-ranked 90% of roots
		}
		if m.experiment != nil && quiet {
			cohorts["quiet-roots"] = append(cohorts["quiet-roots"], f)
			cohortWrites["quiet-roots"] = append(cohortWrites["quiet-roots"], e.committed.Sub(e.started))
			if !e.scheduled.IsZero() {
				cohortArrivals["quiet-roots"] = append(cohortArrivals["quiet-roots"], e.applied.Sub(e.scheduled))
			}
		}
	}
	r.Write = summarizeDomainDurations(write)
	r.AppendCommit = summarizeDomainDurations(appendCommit)
	r.Freshness = summarizeDomainDurations(fresh)
	r.Arrival = summarizeDomainDurations(arrival)
	for k, v := range cohorts {
		r.Cohorts[k] = summarizeDomainDurations(v)
	}
	r.CohortWrites, r.CohortArrivals = map[string]domainCaptureSummary{}, map[string]domainCaptureSummary{}
	for k, v := range cohortWrites {
		r.CohortWrites[k] = summarizeDomainDurations(v)
	}
	for k, v := range cohortArrivals {
		r.CohortArrivals[k] = summarizeDomainDurations(v)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(rootCounts)))
	for i, n := range rootCounts {
		if i < int(math.Ceil(float64(c.roots())*.2)) {
			r.RootTop20Share += float64(n) / float64(b.N)
		}
		if i < int(math.Ceil(float64(c.roots())*.01)) {
			r.RootTop1Share += float64(n) / float64(b.N)
		}
	}
	if err = writers.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint", walStart).Scan(&r.WALBytes); err != nil {
		b.Fatal(err)
	}
	if c.Mode != "wal" {
		sizeQuery := "SELECT pg_total_relation_size($1::regclass)"
		if c.PartitionSpan > 0 {
			sizeQuery = "SELECT COALESCE(sum(pg_total_relation_size(relid)),0)::bigint FROM pg_partition_tree($1::regclass) WHERE isleaf"
			r.Partitions = c.partitions.result()
		}
		if err = writers.QueryRow(ctx, sizeQuery, o.journal).Scan(&r.JournalBytes); err != nil {
			b.Fatal(err)
		}
	}
	if err = writers.QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)", o.table).Scan(&r.SourceBytes); err != nil {
		b.Fatal(err)
	}
	if m.experiment != nil {
		r.Snapshot = m.experiment.result(m, r.WriteSeconds+r.DrainSeconds)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("DOMAIN_SAMPLE %s", encoded)
	b.ReportMetric(r.WriteTPS, "writes/s")
	b.ReportMetric(r.Freshness.P99, "fresh-p99-ms")
	b.ReportMetric(r.Arrival.P99, "arrival-p99-ms")
}

// The inner bound prevents a generic prepared plan from scanning retained
// journal history to satisfy the outer ordering. The cursor excludes domains
// already completed and advances within the currently active domain.
func domainCounterPageQuery(o replicationBenchObjects, strategy string) string {
	if strategy == "join" {
		return fmt.Sprintf(`SELECT j.domain,j.position,j.root_id,j.batch_id,j.payload FROM unnest($1::int[],$2::bigint[],$3::bigint[]) AS h(domain,lo,hi) JOIN %s j ON j.domain=h.domain AND j.position>h.lo AND j.position<=h.hi WHERE (j.domain,j.position)>($4,$5) ORDER BY j.domain,j.position LIMIT 128`, o.journal)
	}
	return fmt.Sprintf(`SELECT j.domain,j.position,j.root_id,j.batch_id,j.payload FROM unnest($1::int[],$2::bigint[],$3::bigint[]) AS h(domain,lo,hi) CROSS JOIN LATERAL (SELECT domain,position,root_id,batch_id,payload FROM %s WHERE domain=h.domain AND position>greatest(h.lo,CASE WHEN h.domain=$4 THEN $5 ELSE 0 END) AND position<=h.hi ORDER BY position LIMIT 128) j WHERE h.domain >= $4 ORDER BY j.domain,j.position LIMIT 128`, o.journal)
}
