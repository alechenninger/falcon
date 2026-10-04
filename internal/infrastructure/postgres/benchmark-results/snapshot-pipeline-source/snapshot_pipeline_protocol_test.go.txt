package postgres_test

import (
	"context"
	"errors"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pipelineTestFixture(t *testing.T, db *pgxpool.Pool, c domainCaptureConfig, n int) (replicationBenchObjects, *domainCaptureMetrics, *snapshotPipelineSink, snapshotBenchCut) {
	t.Helper()
	ctx := context.Background()
	o := newReplicationBenchObjects()
	if err := prepareDomainCapture(ctx, db, o, c); err != nil {
		t.Fatal(err)
	}
	m := &domainCaptureMetrics{events: make([]domainCaptureEvent, n+1), latest: map[[2]int64]int64{}, done: make(chan struct{}), experiment: newSnapshotFetchMetrics(c)}
	p := newSnapshotPipelineSink(c, m)
	var text string
	if err := db.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
		t.Fatal(err)
	}
	a, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	return o, m, p, a
}

func pipelineTestAppend(t *testing.T, ctx context.Context, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, o replicationBenchObjects, c domainCaptureConfig, m *domainCaptureMetrics, root int, id int64) {
	t.Helper()
	data := domainCapturePayload(id, c)
	if id < int64(len(m.events)) {
		m.events[id] = domainCaptureEvent{root: root, key: id, crc: crc32.ChecksumIEEE(data)}
	}
	if _, err := db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES($1,$2,$3)", root, id, data); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPipelineIncrementalAndFailedFetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, strategy := range []string{"pipeline-paged", "pipeline-stream", "pipeline-buffered"} {
		for _, broken := range []bool{false, true} {
			t.Run(strategy+map[bool]string{false: "-complete", true: "-failure"}[broken], func(t *testing.T) {
				c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: strategy, SnapshotWake: "poll", FetchPageRows: 128}.defaults()
				o, m, p, a := pipelineTestFixture(t, db, c, 260)
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for id := int64(1); id <= 260; id++ {
					pipelineTestAppend(t, ctx, tx, o, c, m, 0, id)
				}
				if err = tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
				if err != nil {
					t.Fatal(err)
				}
				staged := make(chan struct{}, 1)
				p.afterStage = func(task domainCaptureTask) {
					if task.frames[0].id == 1 {
						staged <- struct{}{}
					}
				}
				sentinel := errors.New("interrupted result stream")
				p.afterFetch = func(f domainCaptureFrame) error {
					if f.id == 1 {
						if strategy != "pipeline-buffered" {
							select {
							case <-staged:
							case <-ctx.Done():
								return ctx.Err()
							}
							if m.events[1].staged.IsZero() {
								return errors.New("first row was not staged before fetching second row")
							}
						}
						m.mu.Lock()
						readable := m.applied
						m.mu.Unlock()
						if readable != 0 {
							return errors.New("partial epoch became readable")
						}
					}
					if broken && f.id == 2 {
						return sentinel
					}
					return nil
				}
				workCtx, stop := context.WithCancel(ctx)
				var wg sync.WaitGroup
				failures := make(chan error, 1)
				p.start(workCtx, func(e error) { failures <- e; stop() }, &wg)
				defer func() { stop(); wg.Wait() }()
				err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{1, a, z}, p)
				if broken {
					if !errors.Is(err, sentinel) {
						t.Fatalf("expected interrupted stream, got %v", err)
					}
					stop()
					wg.Wait()
					if m.applied != 0 {
						t.Fatal("failed query published a root epoch")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-m.done:
				case err := <-failures:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				stop()
				wg.Wait()
				if m.applied != 260 {
					t.Fatalf("delivered %d/260", m.applied)
				}
				wantQueries := int64(3)
				if strategy == "pipeline-stream" {
					wantQueries = 1
				}
				if m.experiment.fetchQueries.Load() != wantQueries {
					t.Fatalf("paging did not immediately drain: %d queries", m.experiment.fetchQueries.Load())
				}
			})
		}
	}
}

func TestSnapshotPipelineRootProgressAndLateCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, strategy := range []string{"pipeline-paged", "pipeline-stream", "pipeline-buffered"} {
		t.Run(strategy, func(t *testing.T) {
			c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: strategy, SnapshotWake: "poll"}.defaults()
			o, m, p, a := pipelineTestFixture(t, db, c, 133)
			pending, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer pending.Rollback(context.Background())
			pipelineTestAppend(t, ctx, pending, o, c, m, 0, 1)
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pipelineTestAppend(t, ctx, tx, o, c, m, 1, 2)
			for id := int64(3); id <= 132; id++ {
				pipelineTestAppend(t, ctx, tx, o, c, m, 0, id)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			aborted, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pipelineTestAppend(t, ctx, aborted, o, c, m, 0, 999)
			if err = aborted.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
			if err != nil {
				t.Fatal(err)
			}
			blocked, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			p.beforeTask = func(task domainCaptureTask) {
				if task.root == 0 {
					once.Do(func() {
						close(blocked)
						select {
						case <-release:
						case <-ctx.Done():
						}
					})
				}
			}
			ready := make(chan [2]int64, 8)
			p.afterReady = func(root int, epoch int64) { ready <- [2]int64{int64(root), epoch} }
			workCtx, stop := context.WithCancel(ctx)
			var wg sync.WaitGroup
			failures := make(chan error, 1)
			p.start(workCtx, func(e error) { failures <- e; stop() }, &wg)
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
				stop()
				wg.Wait()
			}()
			waitReady := func(epoch int64) {
				t.Helper()
				select {
				case got := <-ready:
					if got != [2]int64{1, epoch} {
						t.Fatalf("quiet root was held behind busy root: %v", got)
					}
				case err := <-failures:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{1, a, z}, p); err != nil {
				t.Fatal(err)
			}
			waitReady(1)
			<-blocked
			if err = pending.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			pipelineTestAppend(t, ctx, db, o, c, m, 1, 133)
			end, err := captureDomainSnapshot(ctx, db, o, 2, z, m)
			if err != nil {
				t.Fatal(err)
			}
			if err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{2, z, end}, p); err != nil {
				t.Fatal(err)
			}
			waitReady(2)
			m.mu.Lock()
			applied := m.applied
			m.mu.Unlock()
			if applied != 2 {
				t.Fatalf("busy root published while blocked: %d", applied)
			}
			close(release)
			select {
			case <-m.done:
			case err := <-failures:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stop()
			wg.Wait()
			if m.applied != 133 || m.events[1].applied.Before(m.events[132].applied) {
				t.Fatal("late commit was skipped or root epochs reordered")
			}
		})
	}
}
