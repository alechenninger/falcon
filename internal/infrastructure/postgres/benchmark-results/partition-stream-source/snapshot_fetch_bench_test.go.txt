package postgres_test

// Follow-up to domain capture: the clock is still global. Only wakeups and
// payload-fetch scheduling change. Readiness means a complete root epoch;
// this is a synthetic sink, not Falcon's graph or query protocol.
import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshotFetchResult struct {
	FetchQueries, DiscoveryQueries, EmptyEpochs, SkippedCaptures, FallbackCaptures, WakeCaptures int64
	NotifySent, NotifyReceived, CommitHints, DroppedHints, CoalescedHints                        int64
	ReadWireBytes, WriteWireBytes, PeakBufferedPayload, PeakRootPayload                          int64
	PeakStagedIDs, PeakPendingEpochs, PeakHeapBytes, FaultPauses                                 int64
	CaptureSQL, DiscoverySQL, FetchSQL                                                           domainCaptureSummary
	ElapsedSeconds                                                                               float64
	Pipeline                                                                                     *snapshotPipelineResult `json:"pipeline,omitempty"`
}
type snapshotFetchMetrics struct {
	mu                                                                                      sync.Mutex
	c                                                                                       domainCaptureConfig
	wake                                                                                    *snapshotBenchWakeups
	buffered, peakBuffered                                                                  int64
	rootBuffered                                                                            map[int]int64
	peakRoot, staged, peakStaged, pending, peakPending                                      int64
	captureTimes, discoveryTimes, fetchTimes                                                []time.Duration
	fetchQueries, discoveryQueries, empty, skipped, fallback, wakeCaptures, dropped, faults atomic.Int64
	wireRead, wireWrite, heapPeak                                                           atomic.Int64
	readStart, writeStart                                                                   int64
	pipeline                                                                                *snapshotPipelineMetrics
}

func newSnapshotFetchMetrics(c domainCaptureConfig) *snapshotFetchMetrics {
	return &snapshotFetchMetrics{c: c, rootBuffered: map[int]int64{}}
}

type snapshotFetchConn struct {
	net.Conn
	m *snapshotFetchMetrics
}

func (c *snapshotFetchConn) Read(p []byte) (int, error) {
	n, e := c.Conn.Read(p)
	c.m.wireRead.Add(int64(n))
	return n, e
}
func (c *snapshotFetchConn) Write(p []byte) (int, error) {
	n, e := c.Conn.Write(p)
	c.m.wireWrite.Add(int64(n))
	return n, e
}
func (m *snapshotFetchMetrics) dial(ctx context.Context, network, address string) (net.Conn, error) {
	c, e := (&net.Dialer{}).DialContext(ctx, network, address)
	if e != nil {
		return nil, e
	}
	return &snapshotFetchConn{Conn: c, m: m}, nil
}
func (m *snapshotFetchMetrics) start(_ time.Time) {
	m.readStart, m.writeStart = m.wireRead.Load(), m.wireWrite.Load()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.heapPeak.Store(int64(ms.HeapAlloc))
}
func (m *snapshotFetchMetrics) buffer(frames []domainCaptureFrame, sign int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range frames {
		n := int64(len(f.payload)) * sign
		m.buffered += n
		m.rootBuffered[f.root] += n
		if m.rootBuffered[f.root] > m.peakRoot {
			m.peakRoot = m.rootBuffered[f.root]
		}
	}
	if m.buffered > m.peakBuffered {
		m.peakBuffered = m.buffered
	}
}
func (m *snapshotFetchMetrics) ids(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.staged += n
	if m.staged > m.peakStaged {
		m.peakStaged = m.staged
	}
}
func (m *snapshotFetchMetrics) queue(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending += n
	if m.pending > m.peakPending {
		m.peakPending = m.pending
	}
}
func (m *snapshotFetchMetrics) sql(kind string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch kind {
	case "capture":
		m.captureTimes = append(m.captureTimes, d)
	case "discovery":
		m.discoveryTimes = append(m.discoveryTimes, d)
	case "fetch":
		m.fetchTimes = append(m.fetchTimes, d)
	}
}
func (m *snapshotFetchMetrics) result(_ *domainCaptureMetrics, elapsed float64) *snapshotFetchResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := &snapshotFetchResult{
		FetchQueries: m.fetchQueries.Load(), DiscoveryQueries: m.discoveryQueries.Load(),
		EmptyEpochs: m.empty.Load(), SkippedCaptures: m.skipped.Load(), FallbackCaptures: m.fallback.Load(), WakeCaptures: m.wakeCaptures.Load(),
		DroppedHints: m.dropped.Load(), ReadWireBytes: m.wireRead.Load() - m.readStart, WriteWireBytes: m.wireWrite.Load() - m.writeStart,
		PeakBufferedPayload: m.peakBuffered, PeakRootPayload: m.peakRoot, PeakStagedIDs: m.peakStaged,
		PeakPendingEpochs: m.peakPending, PeakHeapBytes: m.heapPeak.Load(), FaultPauses: m.faults.Load(),
		CaptureSQL: summarizeDomainDurations(m.captureTimes), DiscoverySQL: summarizeDomainDurations(m.discoveryTimes),
		FetchSQL: summarizeDomainDurations(m.fetchTimes), ElapsedSeconds: elapsed,
	}
	if m.wake != nil {
		r.NotifySent = m.wake.sent.Load()
		r.NotifyReceived = m.wake.received.Load()
		r.CommitHints = m.wake.attempts.Load()
		r.CoalescedHints = m.wake.coalesced.Load() + m.wake.batchMerges.Load()
	}
	if m.pipeline != nil {
		r.Pipeline = m.pipeline.result()
	}
	return r
}
func snapshotFetchPause(ctx context.Context, c domainCaptureConfig, frames []domainCaptureFrame, m *snapshotFetchMetrics) error {
	if c.Fault != "fetch" {
		return nil
	}
	for _, f := range frames {
		if f.root == 0 && f.id%1000 == 0 {
			m.faults.Add(1)
			timer := time.NewTimer(200 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
	}
	return nil
}

type snapshotFetchCut struct {
	epoch int64
	a, z  snapshotBenchCut
}
type snapshotRootEpoch struct {
	cut             snapshotFetchCut
	root, remaining int
	position        int64
	staged          []domainCaptureFrame // verified identifiers only; payloads released per page
}
type snapshotRootWork struct {
	job   *snapshotRootEpoch
	limit int
}

// Metadata is queued by root. Only one page per root may be active. Each
// completed page returns that root to the end of the ready list. Large deltas
// (>=16 entries) and observed dominant roots (>50% after 16 entries) get their
// own SQL batch so their slow query cannot hold a batch of small roots hostage.
// Other roots share bounded LATERAL queries; epochs may differ within a batch.
type snapshotRootFetchScheduler struct {
	mu     sync.Mutex
	jobs   map[int][]*snapshotRootEpoch
	active map[int]bool
	ready  []int
	counts map[int]int64
	total  int64
	slots  chan struct{}
	wake   chan struct{}
	c      domainCaptureConfig
	m      *snapshotFetchMetrics
}

func newSnapshotRootFetchScheduler(c domainCaptureConfig, m *snapshotFetchMetrics) *snapshotRootFetchScheduler {
	if c.FetchBatchRoots == 0 {
		c.FetchBatchRoots = 64
	}
	if c.FetchPageRows == 0 {
		c.FetchPageRows = 128
	}
	// A batch's payload is bounded to 2 MiB, including large-payload workloads.
	c.FetchPageRows = min(c.FetchPageRows, max(1, (2<<20)/(c.Rows*c.Payload)))
	c.FetchBatchRoots = min(c.FetchBatchRoots, c.FetchPageRows)
	return &snapshotRootFetchScheduler{c: c, m: m, jobs: map[int][]*snapshotRootEpoch{}, active: map[int]bool{}, counts: map[int]int64{}, slots: make(chan struct{}, 4096), wake: make(chan struct{}, 1)}
}
func (s *snapshotRootFetchScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *snapshotRootFetchScheduler) enqueue(ctx context.Context, j *snapshotRootEpoch) error {
	return s.enqueueBatch(ctx, []*snapshotRootEpoch{j})
}
func (s *snapshotRootFetchScheduler) enqueueBatch(ctx context.Context, jobs []*snapshotRootEpoch) error {
	for i := range jobs {
		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			for range i {
				<-s.slots
			}
			return ctx.Err()
		}
	}
	s.mu.Lock()
	for _, j := range jobs {
		s.counts[j.root] += int64(j.remaining)
		s.total += int64(j.remaining)
		s.jobs[j.root] = append(s.jobs[j.root], j)
		if !s.active[j.root] {
			s.active[j.root] = true
			s.ready = append(s.ready, j.root)
		}
	}
	s.mu.Unlock()
	s.m.queue(int64(len(jobs)))
	s.signal()
	return nil
}
func (s *snapshotRootFetchScheduler) isolated(root int) bool {
	j := s.jobs[root][0]
	return j.remaining >= 16 || (s.counts[root] >= 16 && s.counts[root]*2 > s.total)
}
func (s *snapshotRootFetchScheduler) take(ctx context.Context) ([]snapshotRootWork, bool) {
	for {
		s.mu.Lock()
		if len(s.ready) > 0 {
			first := s.ready[0]
			alone := s.isolated(first)
			n := 1
			if !alone {
				for n < len(s.ready) && n < s.c.FetchBatchRoots && !s.isolated(s.ready[n]) {
					n++
				}
			}
			out := make([]snapshotRootWork, n)
			used := 0
			quota := max(1, s.c.FetchPageRows/n)
			for i, r := range s.ready[:n] {
				j := s.jobs[r][0]
				lim := min(j.remaining, quota, 64)
				out[i] = snapshotRootWork{job: j, limit: lim}
				used += lim
			}
			for i := range out {
				add := min(s.c.FetchPageRows-used, 64-out[i].limit, out[i].job.remaining-out[i].limit)
				out[i].limit += add
				used += add
			}
			s.ready = s.ready[n:]
			s.mu.Unlock()
			s.signal()
			return out, true
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-ctx.Done():
			return nil, false
		}
	}
}
func (s *snapshotRootFetchScheduler) complete(work snapshotRootWork, finished bool) {
	r := work.job.root
	s.mu.Lock()
	if finished {
		s.jobs[r] = s.jobs[r][1:]
	}
	if len(s.jobs[r]) > 0 {
		s.ready = append(s.ready, r)
	} else {
		delete(s.jobs, r)
		delete(s.active, r)
	}
	s.mu.Unlock()
	if finished {
		<-s.slots
		s.m.queue(-1)
	}
	s.signal()
}

func snapshotFairQuery(o replicationBenchObjects) string {
	// Each root has its own cursor AND snapshot pair. This allows a lagging
	// root to share a query with unrelated roots processing newer epochs.
	base := "position,root_id,batch_id,payload"
	newIDs := "SELECT " + base + ",writer_xid FROM " + o.journal + " WHERE root_id=h.root AND position>h.cursor AND writer_xid>=h.lo::xid8 AND writer_xid<h.hi::xid8"
	oldIDs := "SELECT " + base + ",writer_xid FROM " + o.journal + " WHERE root_id=h.root AND position>h.cursor AND writer_xid=ANY(string_to_array(h.active,',')::xid8[])"
	return "SELECT j.position,j.root_id,j.batch_id,j.payload FROM unnest($1::int[],$2::bigint[],$3::int[],$4::text[],$5::text[],$6::text[],$7::text[])h(root,cursor,n,lo,hi,active,snap) CROSS JOIN LATERAL(SELECT " + base + " FROM(" + newIDs + " UNION ALL " + oldIDs + ")c WHERE pg_visible_in_snapshot(writer_xid,h.snap::pg_snapshot) ORDER BY position LIMIT h.n)j ORDER BY j.root_id,j.position"
}
func fetchSnapshotFairPage(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, work []snapshotRootWork, m *domainCaptureMetrics) (map[int][]domainCaptureFrame, error) {
	roots := make([]int32, len(work))
	cursor := make([]int64, len(work))
	limit := make([]int32, len(work))
	lo, hi, active, snap := make([]string, len(work)), make([]string, len(work)), make([]string, len(work)), make([]string, len(work))
	for i, w := range work {
		roots[i] = int32(w.job.root)
		cursor[i] = w.job.position
		limit[i] = int32(w.limit)
		lo[i], hi[i] = w.job.cut.a.xmax, w.job.cut.z.xmax
		active[i], snap[i] = strings.Join(w.job.cut.a.xip, ","), w.job.cut.z.text
	}
	start := time.Now()
	m.queries.Add(1)
	m.experiment.fetchQueries.Add(1)
	rows, err := p.Query(ctx, snapshotFairQuery(o), roots, cursor, limit, lo, hi, active, snap)
	if err != nil {
		return nil, err
	}
	out := map[int][]domainCaptureFrame{}
	for rows.Next() {
		var f domainCaptureFrame
		if err = rows.Scan(&f.position, &f.root, &f.id, &f.payload); err != nil {
			break
		}
		out[f.root] = append(out[f.root], f)
		m.experiment.buffer([]domainCaptureFrame{f}, 1)
	}
	rows.Close()
	if err == nil {
		err = rows.Err()
	}
	m.experiment.sql("fetch", time.Since(start))
	return out, err
}
func stageSnapshotRootPage(c domainCaptureConfig, m *domainCaptureMetrics, w snapshotRootWork, frames []domainCaptureFrame) error {
	j := w.job
	if len(frames) != w.limit {
		return fmt.Errorf("incomplete root page: root=%d epoch=%d got=%d want=%d", j.root, j.cut.epoch, len(frames), w.limit)
	}
	for _, f := range frames {
		if f.root != j.root || f.position <= j.position || f.id <= 0 || f.id >= int64(len(m.events)) {
			return fmt.Errorf("invalid root cursor or ID: %+v", f)
		}
		if len(f.payload) != c.Rows*c.Payload || crc32.ChecksumIEEE(f.payload) != m.events[f.id].crc {
			return fmt.Errorf("invalid fair payload %d", f.id)
		}
		j.position = f.position
		j.staged = append(j.staged, domainCaptureFrame{root: f.root, id: f.id, position: f.position})
	}
	j.remaining -= len(frames)
	m.experiment.ids(int64(len(frames)))
	return nil
}

// A quiet-period commit sends immediately. During a burst, notification start
// times are spaced by minInterval; all hints accumulated before the send are
// covered. Dequeue before sending so commits during the round trip leave a new
// pending hint. This bridge is per writer process, not one global writer lock.
func startSnapshotFetchWakeups(ctx context.Context, url, channel string, minInterval time.Duration, m *snapshotFetchMetrics, wg *sync.WaitGroup, fail func(error)) (*snapshotBenchWakeups, error) {
	w := &snapshotBenchWakeups{pending: make(chan struct{}, 1), ready: make(chan struct{}, 1)}
	config, e := pgx.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	config.DialFunc = m.dial
	listener, e := pgx.ConnectConfig(ctx, config.Copy())
	if e != nil {
		return nil, e
	}
	if _, e = listener.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); e != nil {
		listener.Close(context.Background())
		return nil, e
	}
	sender, e := pgx.ConnectConfig(ctx, config.Copy())
	if e != nil {
		listener.Close(context.Background())
		return nil, e
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer listener.Close(context.Background())
		for {
			if _, e := listener.WaitForNotification(ctx); e != nil {
				if ctx.Err() == nil {
					fail(e)
				}
				return
			}
			w.received.Add(1)
			select {
			case w.ready <- struct{}{}:
			default:
				w.deliveryCoalesced.Add(1)
			}
		}
	}()
	go func() {
		defer wg.Done()
		defer sender.Close(context.Background())
		var last time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.pending:
			}
			if wait := time.Until(last.Add(minInterval)); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			select {
			case <-w.pending:
				w.batchMerges.Add(1)
			default:
			}
			last = time.Now()
			if _, e := sender.Exec(ctx, "SELECT pg_notify($1,'')", channel); e != nil {
				if ctx.Err() == nil {
					fail(e)
				}
				return
			}
			w.sent.Add(1)
		}
	}()
	return w, nil
}

// Notify mode reconciles even if a hint was lost, but avoids committing empty
// manifests. The last durable cut remains the previous boundary after an empty
// check. This adds an indexed EXISTS check to nonempty notification captures.
func captureSnapshotFetchCut(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, epoch int64, a snapshotBenchCut, m *domainCaptureMetrics) (snapshotBenchCut, bool, error) {
	if c.SnapshotWake == "poll" {
		z, e := captureDomainSnapshot(ctx, p, o, epoch, a, m)
		return z, e == nil, e
	}
	tx, e := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if e != nil {
		return snapshotBenchCut{}, false, e
	}
	defer tx.Rollback(context.Background())
	var text string
	m.queries.Add(1)
	if e = tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); e != nil {
		return snapshotBenchCut{}, false, e
	}
	z, e := parseSnapshotBenchCut(text)
	if e != nil {
		return z, false, e
	}
	var changed bool
	m.queries.Add(1)
	q := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot))", domainSnapshotCandidates(o, false))
	if c.SnapshotProbe == "indexed" {
		q = snapshotIndexedProbeQuery(o)
	} else if c.SnapshotProbe != "" {
		return z, false, fmt.Errorf("invalid snapshot probe %q", c.SnapshotProbe)
	}
	if e = tx.QueryRow(ctx, q, a.xmax, z.xmax, int64(0), a.xip, z.text).Scan(&changed); e != nil {
		return z, false, e
	}
	if !changed {
		m.experiment.skipped.Add(1)
		return z, false, tx.Commit(ctx)
	}
	m.queries.Add(1)
	if _, e = tx.Exec(ctx, "INSERT INTO "+o.clock+" VALUES($1,$2::text::pg_snapshot,$3::text::pg_snapshot)", epoch, a.text, z.text); e != nil {
		return z, false, e
	}
	e = tx.Commit(ctx)
	if e == nil {
		m.cuts.Add(1)
	}
	return z, e == nil, e
}

// EXISTS with an unknown ANY array can get a generic plan that scans the
// position index across retained history. Probe each formerly active xid
// separately instead. Its rows all have the same transactional visibility, so
// one journal row proves existence. The ordered, bounded LATERAL lookup and
// paired xid range avoid the broad equality estimate from single-xid history.
// Still-active xids are rejected before the lookup; an empty array does no work.
func snapshotIndexedProbeQuery(o replicationBenchObjects) string {
	return fmt.Sprintf(`SELECT EXISTS(
 SELECT 1 FROM(
  SELECT writer_xid FROM %s
   WHERE writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8 AND position>$3
  UNION ALL
  SELECT j.writer_xid FROM unnest($4::text[]::xid8[])h(xid)
   CROSS JOIN LATERAL(
    SELECT writer_xid FROM %s
     WHERE writer_xid>=h.xid AND writer_xid<=h.xid AND position>$3
     ORDER BY writer_xid,position LIMIT 1
   )j WHERE pg_visible_in_snapshot(h.xid,$5::text::pg_snapshot)
 )c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot)
)`, o.journal, o.journal)
}

func startSnapshotFetchExperiment(ctx context.Context, p *pgxpool.Pool, connString string, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, delivery *domainCaptureScheduler, fail func(error), wg *sync.WaitGroup) error {
	x := m.experiment
	if c.SnapshotFetch != "global" && c.SnapshotFetch != "fair" && !c.pipeline() {
		return fmt.Errorf("invalid snapshot fetch %q", c.SnapshotFetch)
	}
	if c.SnapshotWake != "poll" && c.SnapshotWake != "notify" {
		return fmt.Errorf("invalid wake %q", c.SnapshotWake)
	}
	if c.SnapshotWake == "notify" {
		w, err := startSnapshotFetchWakeups(ctx, connString, o.channel, time.Duration(c.NotifyBatchMS)*time.Millisecond, x, wg, fail)
		if err != nil {
			return err
		}
		x.wake = w
		m.onCommit = func(id int64) {
			if c.DropWakeEvery > 0 && id%int64(c.DropWakeEvery) == 0 {
				x.dropped.Add(1)
				return
			}
			w.signalCommit()
		}
	}
	var text string
	if err := p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
		return err
	}
	a, err := parseSnapshotBenchCut(text)
	if err != nil {
		return err
	}
	cuts := make(chan snapshotFetchCut, 64)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		timer := time.NewTicker(200 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			runtime.ReadMemStats(&ms)
			for old := x.heapPeak.Load(); int64(ms.HeapAlloc) > old; old = x.heapPeak.Load() {
				if x.heapPeak.CompareAndSwap(old, int64(ms.HeapAlloc)) {
					break
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := 10 * time.Millisecond
		if x.wake != nil {
			fallback := c.FallbackMS
			if fallback == 0 {
				fallback = 100
			}
			interval = time.Duration(fallback) * time.Millisecond
		}
		timer := time.NewTimer(0)
		defer timer.Stop()
		var ready <-chan struct{}
		if x.wake != nil {
			ready = x.wake.ready
		}
		for epoch := int64(1); ; epoch++ {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				x.fallback.Add(1)
			case <-ready:
				x.wakeCaptures.Add(1)
			}
			start := time.Now()
			z, published, e := captureSnapshotFetchCut(ctx, p, o, c, epoch, a, m)
			x.sql("capture", time.Since(start))
			if e != nil {
				if ctx.Err() == nil {
					fail(e)
				}
				return
			}
			if !published {
				epoch--
				timer.Reset(interval)
				continue
			}
			select {
			case cuts <- snapshotFetchCut{epoch: epoch, a: a, z: z}:
				a = z
			case <-ctx.Done():
				return
			}
			timer.Reset(interval)
		}
	}()
	if c.pipeline() {
		return startSnapshotPipeline(ctx, p, o, c, m, cuts, fail, wg)
	}
	var fair *snapshotRootFetchScheduler
	if c.SnapshotFetch == "fair" {
		fair = newSnapshotRootFetchScheduler(c, x)
		for i := 0; i < c.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					work, ok := fair.take(ctx)
					if !ok {
						return
					}
					pages, e := fetchSnapshotFairPage(ctx, p, o, work, m)
					if e != nil {
						if ctx.Err() == nil {
							fail(e)
						}
						return
					}
					for _, w := range work {
						frames := pages[w.job.root]
						if e = snapshotFetchPause(ctx, c, frames, x); e == nil {
							e = stageSnapshotRootPage(c, m, w, frames)
						}
						x.buffer(frames, -1)
						if e != nil {
							if ctx.Err() == nil {
								fail(e)
							}
							return
						}
						finished := w.job.remaining == 0
						if finished {
							// The whole root epoch is verified before any event is
							// recorded readable. The following epoch cannot run yet.
							domainCaptureDelay(c, w.job.staged)
							e = m.completeVerified(w.job.staged)
							x.ids(-int64(len(w.job.staged)))
							w.job.staged = nil
						}
						fair.complete(w, finished)
						if e != nil {
							if ctx.Err() == nil {
								fail(e)
							}
							return
						}
					}
				}
			}()
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var cut snapshotFetchCut
			select {
			case cut = <-cuts:
			case <-ctx.Done():
				return
			}
			if fair == nil {
				before := time.Now()
				frames, e := domainSnapshotFrames(ctx, p, o, cut.a, cut.z, nil, m, func(page []domainCaptureFrame) error {
					x.fetchQueries.Add(1)
					x.buffer(page, 1)
					x.sql("fetch", time.Since(before))
					e := snapshotFetchPause(ctx, c, page, x)
					before = time.Now()
					return e
				})
				if e == nil {
					if len(frames) == 0 {
						x.empty.Add(1)
					}
					e = domainCaptureRoute(ctx, c, m, delivery, frames)
				}
				if e != nil {
					if ctx.Err() == nil {
						fail(e)
					}
					return
				}
			} else {
				before := time.Now()
				m.queries.Add(1)
				x.discoveryQueries.Add(1)
				query := fmt.Sprintf("SELECT root_id,count(*) FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) GROUP BY root_id ORDER BY root_id", domainSnapshotCandidates(o, false))
				rows, e := p.Query(ctx, query, cut.a.xmax, cut.z.xmax, int64(0), cut.a.xip, cut.z.text)
				if e != nil {
					if ctx.Err() == nil {
						fail(e)
					}
					return
				}
				jobs := []*snapshotRootEpoch{}
				for rows.Next() {
					j := &snapshotRootEpoch{cut: cut}
					if e = rows.Scan(&j.root, &j.remaining); e != nil {
						break
					}
					jobs = append(jobs, j)
				}
				rows.Close()
				if e == nil {
					e = rows.Err()
				}
				x.sql("discovery", time.Since(before))
				if e != nil {
					if ctx.Err() == nil {
						fail(e)
					}
					return
				}
				if len(jobs) == 0 {
					x.empty.Add(1)
				}
				if e = fair.enqueueBatch(ctx, jobs); e != nil {
					return
				}
				m.rootTasks.Add(int64(len(jobs)))
			}
		}
	}()
	return nil
}

func loadSnapshotFetchConfig(c *domainCaptureConfig) error {
	if value := os.Getenv("FALCON_DOMAIN_CAPTURE_CONFIG"); value != "" {
		if e := json.Unmarshal([]byte(value), c); e != nil {
			return e
		}
	}
	*c = c.defaults()
	return nil
}
func logSnapshotFetchResult(b *testing.B, r domainCaptureResult) {
	data, e := json.Marshal(r)
	if e != nil {
		b.Fatal(e)
	}
	b.Logf("DOMAIN_SAMPLE %s", data)
	b.ReportMetric(float64(r.Queries)/r.WriteSeconds, "idle-queries/s")
}

// An idle operation observes one second. No source writes occur. This uses the
// same live publisher, fetch scheduler, and NOTIFY bridge as capacity samples.
func BenchmarkSnapshotFetchIdle(b *testing.B) {
	c := domainCaptureConfig{Name: "idle", Mode: "snapshot-routed"}.defaults()
	if err := loadSnapshotFetchConfig(&c); err != nil {
		b.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	connString, cleanup, _ := setupReplicationBenchPostgres(ctx, b)
	defer cleanup()
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		b.Fatal(err)
	}
	x := newSnapshotFetchMetrics(c)
	config.MaxConns = 5
	config.ConnConfig.DialFunc = x.dial
	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, p, o, c); err != nil {
		b.Fatal(err)
	}
	defer cleanupReplicationBenchObjects(context.Background(), p, connString, o, false)
	m := &domainCaptureMetrics{events: make([]domainCaptureEvent, 1), latest: map[[2]int64]int64{}, done: make(chan struct{}), experiment: x}
	s := newDomainCaptureScheduler()
	var wg sync.WaitGroup
	defer func() { stop(); wg.Wait() }()
	errs := make(chan error, 1)
	fail := func(e error) {
		select {
		case errs <- e:
		default:
		}
		stop()
	}
	if c.SnapshotFetch == "global" {
		domainCaptureWorkers(ctx, p, o, c, m, s, &wg, fail)
	}
	if err = startSnapshotFetchExperiment(ctx, p, connString, o, c, m, s, fail, &wg); err != nil {
		b.Fatal(err)
	}
	// Exclude initial registration, connection establishment, and first cut.
	time.Sleep(150 * time.Millisecond)
	initialQueries, initialCuts := m.queries.Load(), m.cuts.Load()
	b.ResetTimer()
	started := time.Now()
	x.start(started)
	timer := time.NewTimer(time.Duration(b.N) * time.Second)
	select {
	case <-timer.C:
	case e := <-errs:
		timer.Stop()
		b.Fatal(e)
	}
	elapsed := time.Since(started).Seconds()
	b.StopTimer()
	stop()
	wg.Wait()
	select {
	case e := <-errs:
		b.Fatal(e)
	default:
	}
	r := domainCaptureResult{Config: c, Operations: b.N, WriteSeconds: elapsed, Queries: m.queries.Load() - initialQueries, Cuts: m.cuts.Load() - initialCuts, Snapshot: x.result(m, elapsed)}
	logSnapshotFetchResult(b, r)
}
