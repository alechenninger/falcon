package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotFetchPlanSparseAndEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	for _, plan := range []string{"guarded", "metadata", "ordered", "ordered-full", "bounded", "adaptive"} {
		for _, cover := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cover-%t", plan, cover), func(t *testing.T) {
				c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "poll", FetchPlan: plan, PositionCover: cover}.defaults()
				o, m, p, _ := pipelineTestFixture(t, db, c, 2)
				pending, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer pending.Rollback(context.Background())
				pipelineTestAppend(t, ctx, pending, o, c, m, 0, 1)
				if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT 0,0,$1 FROM generate_series(1,20000)", domainCapturePayload(0, c)); err != nil {
					t.Fatal(err)
				}
				var text string
				if err = db.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
					t.Fatal(err)
				}
				a, err := parseSnapshotBenchCut(text)
				if err != nil {
					t.Fatal(err)
				}
				// A new snapshot with no eligible committed journal rows must not
				// emit even the unresolved low-position transaction or old history.
				var emptyText string
				if err = db.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&emptyText); err != nil {
					t.Fatal(err)
				}
				empty, err := parseSnapshotBenchCut(emptyText)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				if err = fetchSnapshotOptimizedRows(ctx, db, o, snapshotFetchCut{1, a, empty}, c, m, func(f domainCaptureFrame) error { count++; return nil }); err != nil || count != 0 {
					t.Fatalf("empty delta returned %d rows: %v", count, err)
				}
				if err = pending.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				pipelineTestAppend(t, ctx, db, o, c, m, 0, 2)
				z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
				if err != nil {
					t.Fatal(err)
				}
				workCtx, stop := context.WithCancel(ctx)
				var wg sync.WaitGroup
				failures := make(chan error, 1)
				p.start(workCtx, func(e error) { failures <- e; stop() }, &wg)
				defer func() { stop(); wg.Wait() }()
				if err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{1, a, z}, p); err != nil {
					t.Fatal(err)
				}
				select {
				case <-m.done:
				case e := <-failures:
					t.Fatal(e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				stop()
				wg.Wait()
				if m.applied != 2 || m.events[1].applied.After(m.events[2].applied) {
					t.Fatal("sparse delta lost or reordered the old-position commit")
				}
				if plan == "adaptive" && p.x.fallbackCuts != 1 {
					t.Fatalf("wide position span did not select the sparse fallback: %d", p.x.fallbackCuts)
				}
			})
		}
	}
}
