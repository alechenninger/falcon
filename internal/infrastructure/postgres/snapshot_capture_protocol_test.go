package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotSingleCaptureProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	for _, mode := range []string{"auto", "force_generic_plan"} {
		for _, partitioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partitioned-%t", mode, partitioned), func(t *testing.T) {
				pc, err := pgxpool.ParseConfig(url)
				if err != nil {
					t.Fatal(err)
				}
				pc.ConnConfig.RuntimeParams["plan_cache_mode"] = mode
				db, err := pgxpool.NewWithConfig(ctx, pc)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed", SnapshotCapture: "single", FetchPlan: "guarded"}
				if partitioned {
					c.PartitionSpan, c.PartitionAhead = 64, 64
				}
				c = c.defaults()
				o, m, _, a := pipelineTestFixture(t, db, c, 6)
				capture := func(epoch int64, previous snapshotBenchCut, changed bool) snapshotBenchCut {
					t.Helper()
					z, published, err := captureSnapshotFetchCut(ctx, db, o, c, epoch, previous, m)
					if err != nil || published != changed {
						t.Fatalf("capture published=%t want=%t: %v", published, changed, err)
					}
					if changed {
						// A different connection must see exactly the returned manifest:
						// returned rows are not mistaken for a successful implicit commit.
						other, err := pgxpool.New(ctx, url)
						if err != nil {
							t.Fatal(err)
						}
						defer other.Close()
						var old, next string
						if err = other.QueryRow(ctx, "SELECT previous_snapshot::text,snapshot::text FROM "+o.clock+" WHERE epoch=$1", epoch).Scan(&old, &next); err != nil || old != previous.text || next != z.text {
							t.Fatalf("manifest not committed/mismatched: %v", err)
						}
					}
					return z
				}
				capture(1, a, false)
				late, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer late.Rollback(context.Background())
				pipelineTestAppend(t, ctx, late, o, c, m, 0, 1)
				pipelineTestAppend(t, ctx, db, o, c, m, 1, 2)
				aborted, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				pipelineTestAppend(t, ctx, aborted, o, c, m, 0, 3)
				if err = aborted.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				z := capture(1, a, true)
				frames, err := domainSnapshotFrames(ctx, db, o, a, z, nil, m, nil)
				if err != nil || len(frames) != 1 || frames[0].id != 2 {
					t.Fatalf("uncommitted/aborted membership: %+v %v", frames, err)
				}
				capture(2, z, false)
				if err = late.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				// The old-active branch must recover the lower-position late commit.
				next := capture(2, z, true)
				frames, err = domainSnapshotFrames(ctx, db, o, z, next, nil, m, nil)
				if err != nil || len(frames) != 1 || frames[0].id != 1 {
					t.Fatalf("late position lost: %+v %v", frames, err)
				}
				// A resolved old XID with no journal entry must not publish a cut.
				old, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var xid string
				if err = old.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid); err != nil {
					t.Fatal(err)
				}
				pipelineTestAppend(t, ctx, db, o, c, m, 1, 4)
				latest := capture(3, next, true)
				if err = old.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				capture(4, latest, false)
				var manifests int
				if err = db.QueryRow(ctx, "SELECT count(*) FROM "+o.clock).Scan(&manifests); err != nil || manifests != 4 {
					t.Fatalf("empty manifests: %d %v", manifests, err)
				}
				// Reconstruct cuts from durable storage, without any in-memory handoff.
				var previous, replayed string
				if err = db.QueryRow(ctx, "SELECT previous_snapshot::text,snapshot::text FROM "+o.clock+" WHERE epoch=2").Scan(&previous, &replayed); err != nil {
					t.Fatal(err)
				}
				ra, err := parseSnapshotBenchCut(previous)
				if err != nil {
					t.Fatal(err)
				}
				rz, err := parseSnapshotBenchCut(replayed)
				if err != nil {
					t.Fatal(err)
				}
				frames, err = domainSnapshotFrames(ctx, db, o, ra, rz, nil, m, nil)
				if err != nil || len(frames) != 1 || frames[0].id != 1 {
					t.Fatalf("durable replay: %+v %v", frames, err)
				}
				// A deferred trigger raises only at transaction commit. Scan must
				// report that failure rather than expose its already-returned row.
				function := o.clock + "_fail"
				_, err = db.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject manifest commit'; END $$; CREATE CONSTRAINT TRIGGER fail_commit AFTER INSERT ON %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s()`, function, o.clock, function))
				if err != nil {
					t.Fatal(err)
				}
				pipelineTestAppend(t, ctx, db, o, c, m, 1, 5)
				_, published, err := captureSnapshotFetchCut(ctx, db, o, c, 4, latest, m)
				if err == nil || published || m.cuts.Load() != 3 {
					t.Fatalf("commit failure published: %t cuts=%d err=%v", published, m.cuts.Load(), err)
				}
				if err = db.QueryRow(ctx, "SELECT count(*) FROM "+o.clock).Scan(&manifests); err != nil || manifests != 4 {
					t.Fatalf("failed commit persisted: %d %v", manifests, err)
				}
			})
		}
	}
}

func TestSnapshotSingleCaptureCommitDuringCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed", SnapshotCapture: "single", FetchPlan: "guarded"}.defaults()
	o, m, _, a := pipelineTestFixture(t, db, c, 2)
	pending, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Rollback(context.Background())
	pipelineTestAppend(t, ctx, pending, o, c, m, 0, 1)
	pipelineTestAppend(t, ctx, db, o, c, m, 1, 2)
	conn, err := db.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	// Only this diagnostic command pauses; the measured query has no sleep.
	query := strings.Replace(snapshotSingleCaptureQuery(o), "SELECT pg_current_snapshot() AS snapshot", "SELECT pg_current_snapshot() AS snapshot FROM pg_sleep(0.3)", 1)
	type result struct {
		text      string
		published bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		r.err = conn.QueryRow(ctx, query, int64(1), a.text, a.xmax, a.xip).Scan(&r.text, &r.published)
		done <- r
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err = db.QueryRow(ctx, "SELECT COALESCE((SELECT wait_event='PgSleep' FROM pg_stat_activity WHERE pid=$1),false)", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not enter diagnostic pause")
		}
		time.Sleep(time.Millisecond)
	}
	if err = pending.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || !r.published {
		t.Fatalf("capture: %+v", r)
	}
	z, err := parseSnapshotBenchCut(r.text)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := domainSnapshotFrames(ctx, db, o, a, z, nil, m, nil)
	if err != nil || len(frames) != 1 || frames[0].id != 2 {
		t.Fatalf("commit during command leaked into cut: %+v %v", frames, err)
	}
	next, published, err := captureSnapshotFetchCut(ctx, db, o, c, 2, z, m)
	if err != nil || !published {
		t.Fatalf("late next cut: %t %v", published, err)
	}
	frames, err = domainSnapshotFrames(ctx, db, o, z, next, nil, m, nil)
	if err != nil || len(frames) != 1 || frames[0].id != 1 {
		t.Fatalf("commit during command lost: %+v %v", frames, err)
	}
}
