package postgres_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func advancePartitionTestXid(t *testing.T, ctx context.Context, db *pgxpool.Pool, upper uint64) {
	t.Helper()
	for {
		xid, err := snapshotPartitionXid(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if xid >= upper {
			return
		}
	}
}

// A writer may allocate its XID before appending, or append and then delay
// commit. Neither elapsed time nor newer commits make its partition disposable.
func TestSnapshotPartitionRetentionAndLateWriter(t *testing.T) {
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
	for _, beforeAppend := range []bool{false, true} {
		t.Run(fmt.Sprintf("pause-before-append-%t", beforeAppend), func(t *testing.T) {
			c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "poll", FetchPlan: "guarded", PartitionSpan: 64, PartitionAhead: 8, PartitionGC: true}.defaults()
			o, m, p, a := pipelineTestFixture(t, db, c, 3)
			pending, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer pending.Rollback(context.Background())
			var xidText string
			if err = pending.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidText); err != nil {
				t.Fatal(err)
			}
			xid := mustPartitionXid(t, xidText)
			var old snapshotPartitionLeaf
			for _, leaf := range c.partitions.leaves {
				if xid >= leaf.lo && xid < leaf.hi {
					old = leaf
				}
			}
			if old.name == "" {
				t.Fatal("writer has no partition")
			}
			if !beforeAppend {
				pipelineTestAppend(t, ctx, pending, o, c, m, 0, 1)
			}
			advancePartitionTestXid(t, ctx, db, old.hi)
			pipelineTestAppend(t, ctx, db, o, c, m, 1, 2)
			z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
			if err != nil {
				t.Fatal(err)
			}
			ready := make(chan [2]int64, 8)
			p.afterReady = func(root int, epoch int64) { ready <- [2]int64{int64(root), epoch} }
			workCtx, stop := context.WithCancel(ctx)
			var wg sync.WaitGroup
			failures := make(chan error, 1)
			p.start(workCtx, func(e error) { failures <- e; stop() }, &wg)
			defer func() { stop(); wg.Wait() }()
			if err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{1, a, z}, p); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-ready:
				if r != [2]int64{1, 1} {
					t.Fatalf("quiet root: %v", r)
				}
			case e := <-failures:
				t.Fatal(e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = c.partitions.maintain(ctx, db, o); err != nil {
				t.Fatal(err)
			}
			var exists bool
			if err = db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", old.name).Scan(&exists); err != nil || !exists {
				t.Fatalf("unresolved writer partition was retired: %v", err)
			}
			if beforeAppend {
				pipelineTestAppend(t, ctx, pending, o, c, m, 0, 1)
			}
			if err = pending.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			pipelineTestAppend(t, ctx, db, o, c, m, 1, 3)
			end, err := captureDomainSnapshot(ctx, db, o, 2, z, m)
			if err != nil {
				t.Fatal(err)
			}
			// A completed writer alone is insufficient. Until its delta is applied,
			// the durable checkpoint remains the earlier, pinned snapshot.
			if err = c.partitions.maintain(ctx, db, o); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", old.name).Scan(&exists); err != nil || !exists {
				t.Fatalf("unapplied late write was retired: %v", err)
			}
			if err = runSnapshotPipelineCut(workCtx, db, o, snapshotFetchCut{2, z, end}, p); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				select {
				case <-ready:
				case e := <-failures:
					t.Fatal(e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			stop()
			wg.Wait()
			if m.applied != 3 {
				t.Fatalf("lost late writer: %d/3", m.applied)
			}
			if err = c.partitions.maintain(ctx, db, o); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", old.name).Scan(&exists); err != nil || exists {
				t.Fatalf("closed applied partition was not dropped: %v", err)
			}
			var epoch int64
			var snapshot string
			if err = db.QueryRow(ctx, "SELECT epoch,snapshot::text FROM "+o.clock+"_progress WHERE id=0").Scan(&epoch, &snapshot); err != nil || epoch != 2 || snapshot != end.text {
				t.Fatalf("durable retention checkpoint: %d %s %v", epoch, snapshot, err)
			}
		})
	}
}

func mustPartitionXid(t *testing.T, text string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestSnapshotPartitionCheckpointWaitsForAllRoots(t *testing.T) {
	m := newSnapshotPartitionManager(domainCaptureConfig{})
	first := snapshotFetchCut{epoch: 1, z: snapshotBenchCut{text: "100:200:"}}
	second := snapshotFetchCut{epoch: 2, z: snapshotBenchCut{text: "200:300:"}}
	m.seal(first, 2)
	m.rootReady(1)
	m.seal(second, 1)
	m.rootReady(2)
	if m.applied.epoch != 0 {
		t.Fatal("later/quiet root progress advanced global retention floor")
	}
	m.rootReady(1)
	if m.applied.epoch != 2 || m.applied.z.text != second.z.text {
		t.Fatal("completed epochs did not advance contiguous retention floor")
	}
}

func TestSnapshotPartitionCompoundKeySemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", PartitionSpan: 64, PartitionAhead: 8}.defaults()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, db, o, c); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, "CREATE UNIQUE INDEX ON "+o.journal+"(position)"); err == nil {
		t.Fatal("partition parent unexpectedly permits position-only uniqueness")
	}
	// Explicitly bypassing the sequence demonstrates the weaker constraint.
	// Normal writers never supply a position and retain one shared CACHE 1 sequence.
	for i := 0; i < 2; i++ {
		if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(position,root_id,batch_id,payload) VALUES(777,0,0,$1)", []byte("constraint-test")); err != nil {
			t.Fatal(err)
		}
	}
	var rows, positions, writers int
	if err = db.QueryRow(ctx, "SELECT count(*),count(DISTINCT position),count(DISTINCT writer_xid) FROM "+o.journal).Scan(&rows, &positions, &writers); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || positions != 1 || writers != 2 {
		t.Fatal("compound-key uniqueness semantics changed")
	}
}
