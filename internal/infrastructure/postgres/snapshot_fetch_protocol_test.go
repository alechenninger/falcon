package postgres_test

import (
	"context"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotFairFetchProtocols(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 1, RootsPerTenant: 2, SnapshotFetch: "fair", SnapshotWake: "poll"}.defaults()
	o := newReplicationBenchObjects()
	if e = prepareDomainCapture(ctx, p, o, c); e != nil {
		t.Fatal(e)
	}
	m := &domainCaptureMetrics{events: make([]domainCaptureEvent, 268), latest: map[[2]int64]int64{}, done: make(chan struct{}), experiment: newSnapshotFetchMetrics(c)}
	appendRow := func(root int, id int64, tx interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	}) {
		payload := domainCapturePayload(id, c)
		m.events[id] = domainCaptureEvent{root: root, key: id, crc: crc32.ChecksumIEEE(payload)}
		if _, e := tx.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES($1,$2,$3)", root, id, payload); e != nil {
			t.Fatal(e)
		}
	}
	var text string
	if e = p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); e != nil {
		t.Fatal(e)
	}
	a, e := parseSnapshotBenchCut(text)
	if e != nil {
		t.Fatal(e)
	}
	pending, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer pending.Rollback(context.Background())
	appendRow(0, 1, pending) // allocates the lowest position, commits after cut 1
	tx, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for id := int64(2); id <= 261; id++ {
		appendRow(0, id, tx)
	}
	for id := int64(262); id <= 264; id++ {
		appendRow(1, id, tx)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	aborted, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	appendRow(0, 265, aborted)
	if e = aborted.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	z, e := captureDomainSnapshot(ctx, p, o, 1, a, m)
	if e != nil {
		t.Fatal(e)
	}
	s := newSnapshotRootFetchScheduler(c, m.experiment)
	if e = s.enqueue(ctx, &snapshotRootEpoch{root: 0, remaining: 260, cut: snapshotFetchCut{1, a, z}}); e != nil {
		t.Fatal(e)
	}
	if e = s.enqueue(ctx, &snapshotRootEpoch{root: 1, remaining: 3, cut: snapshotFetchCut{1, a, z}}); e != nil {
		t.Fatal(e)
	}
	first, ok := s.take(ctx)
	if !ok || len(first) != 1 || first[0].job.root != 0 || first[0].limit != 64 {
		t.Fatalf("heavy page not isolated/bounded: %+v", first)
	}
	// Root 1 remains schedulable while root 0's page is active.
	second, ok := s.take(ctx)
	if !ok || len(second) != 1 || second[0].job.root != 1 {
		t.Fatal("root 0 crowded out root 1")
	}
	fetchAndStage := func(work []snapshotRootWork) {
		pages, e := fetchSnapshotFairPage(ctx, p, o, work, m)
		if e != nil {
			t.Fatal(e)
		}
		for _, w := range work {
			frames := pages[w.job.root]
			if e = stageSnapshotRootPage(c, m, w, frames); e != nil {
				t.Fatal(e)
			}
			m.experiment.buffer(frames, -1)
			done := w.job.remaining == 0
			if done {
				if e = m.completeVerified(w.job.staged); e != nil {
					t.Fatal(e)
				}
				m.experiment.ids(-int64(len(w.job.staged)))
				w.job.staged = nil
			}
			s.complete(w, done)
		}
	}
	fetchAndStage(first)
	if !m.events[2].applied.IsZero() {
		t.Fatal("partial root epoch became readable")
	}
	fetchAndStage(second)
	if m.events[262].applied.IsZero() || !m.events[2].applied.IsZero() {
		t.Fatal("independent root completion failed")
	}
	for first[0].job.remaining > 0 {
		work, ok := s.take(ctx)
		if !ok {
			t.Fatal("lost continuation")
		}
		fetchAndStage(work)
	}
	if m.applied != 263 {
		t.Fatalf("wrong completed count %d", m.applied)
	}
	if e = pending.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	appendRow(1, 266, p)
	next, e := captureDomainSnapshot(ctx, p, o, 2, z, m)
	if e != nil {
		t.Fatal(e)
	}
	appendRow(1, 267, p)
	newest, e := captureDomainSnapshot(ctx, p, o, 3, next, m)
	if e != nil {
		t.Fatal(e)
	}
	work := []snapshotRootWork{
		{job: &snapshotRootEpoch{root: 0, remaining: 1, cut: snapshotFetchCut{2, z, next}}, limit: 1},
		{job: &snapshotRootEpoch{root: 1, remaining: 1, cut: snapshotFetchCut{3, next, newest}}, limit: 1},
	}
	pages, e := fetchSnapshotFairPage(ctx, p, o, work, m)
	if e != nil {
		t.Fatal(e)
	}
	if len(pages[0]) != 1 || pages[0][0].id != 1 || len(pages[1]) != 1 || pages[1][0].id != 267 {
		t.Fatalf("mixed epoch boundaries or late low position lost: %+v", pages)
	}
	// Reconstruct both per-root queries from durable manifests, not a live cut.
	var previous, replayed string
	if e = p.QueryRow(ctx, "SELECT previous_snapshot::text,snapshot::text FROM "+o.clock+" WHERE epoch=2").Scan(&previous, &replayed); e != nil {
		t.Fatal(e)
	}
	old, e := parseSnapshotBenchCut(previous)
	if e != nil {
		t.Fatal(e)
	}
	cut, e := parseSnapshotBenchCut(replayed)
	if e != nil {
		t.Fatal(e)
	}
	root := 0
	frames, e := domainSnapshotFrames(ctx, p, o, old, cut, &root, m, nil)
	if e != nil || len(frames) != 1 || frames[0].id != 1 {
		t.Fatalf("durable fair epoch replay failed: %v %v", frames, e)
	}
}

func TestSnapshotFetchMissingWakeAndEmptyCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, variant := range []struct{ fetch, probe, capture string }{{"global", "", ""}, {"fair", "", ""}, {"global", "indexed", ""}, {"fair", "indexed", ""}, {"pipeline-stream", "indexed", "single"}} {
		fetch := variant.fetch
		t.Run(fetch+"/"+variant.probe+"/"+variant.capture, func(t *testing.T) {
			c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 1, RootsPerTenant: 2, SnapshotFetch: fetch, SnapshotWake: "notify", SnapshotProbe: variant.probe, SnapshotCapture: variant.capture, NotifyBatchMS: 2, FallbackMS: 50, DropWakeEvery: 1, Fault: "rollback"}.defaults()
			o := newReplicationBenchObjects()
			if e = prepareDomainCapture(ctx, p, o, c); e != nil {
				t.Fatal(e)
			}
			m := &domainCaptureMetrics{events: domainCaptureTrace(1000, c), latest: map[[2]int64]int64{}, done: make(chan struct{}), experiment: newSnapshotFetchMetrics(c)}
			var initial string
			if e = p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&initial); e != nil {
				t.Fatal(e)
			}
			a, e := parseSnapshotBenchCut(initial)
			if e != nil {
				t.Fatal(e)
			}
			_, published, e := captureSnapshotFetchCut(ctx, p, o, c, 1, a, m)
			if e != nil || published {
				t.Fatalf("empty notification check published: %v %v", published, e)
			}
			runCtx, stop := context.WithCancel(ctx)
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
			s := newDomainCaptureScheduler()
			if fetch == "global" {
				domainCaptureWorkers(runCtx, p, o, c, m, s, &wg, fail)
			}
			if e = startSnapshotFetchExperiment(runCtx, p, url, o, c, m, s, fail, &wg); e != nil {
				t.Fatal(e)
			}
			// The rollback retry must signal only its successful commit. Swallow
			// that signal, proving reconciliation rather than a hint delivers it.
			if e = writeDomainCapture(runCtx, p, o, c, 1000, m); e != nil {
				t.Fatal(e)
			}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				m.mu.Lock()
				applied := !m.events[1000].applied.IsZero()
				m.mu.Unlock()
				if applied {
					break
				}
				select {
				case e := <-errs:
					t.Fatal(e)
				case <-deadline.C:
					t.Fatal("lost wake caused missing delivery")
				case <-tick.C:
				}
			}
			stop()
			wg.Wait()
			if m.experiment.dropped.Load() != 1 || m.experiment.wake.attempts.Load() != 0 || m.applied != 1 {
				t.Fatal("rollback or suppressed wake accounting incorrect")
			}
			var rows int
			if e = p.QueryRow(ctx, "SELECT count(*) FROM "+o.journal).Scan(&rows); e != nil || rows != 1 {
				t.Fatalf("rollback leaked rows: %d %v", rows, e)
			}
			var manifests int
			if e = p.QueryRow(ctx, "SELECT count(*) FROM "+o.clock).Scan(&manifests); e != nil || manifests != 2 {
				t.Fatalf("empty checks published epochs: %d %v", manifests, e)
			}
		})
	}
}
