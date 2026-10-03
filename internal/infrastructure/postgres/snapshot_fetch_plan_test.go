package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotIndexedProbeLateCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", SnapshotWake: "notify", SnapshotProbe: "indexed", History: 50}.defaults()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, p, o, c); err != nil {
		t.Fatal(err)
	}
	var text string
	if err = p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
		t.Fatal(err)
	}
	a, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	appendRow := func(tx pgx.Tx, id int) {
		if _, err = tx.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,$1,$2)", id, make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Rollback(context.Background())
	appendRow(pending, 1)
	newer, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Rollback(context.Background())
	appendRow(newer, 2)
	if err = newer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	m := &domainCaptureMetrics{experiment: newSnapshotFetchMetrics(c)}
	z, published, err := captureSnapshotFetchCut(ctx, p, o, c, 1, a, m)
	if err != nil || !published {
		t.Fatalf("new journal transaction missed: %v %v", published, err)
	}
	if err = pending.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	next, published, err := captureSnapshotFetchCut(ctx, p, o, c, 2, z, m)
	if err != nil || !published {
		t.Fatalf("old active journal transaction missed: %v %v", published, err)
	}
	frames, err := domainSnapshotFrames(ctx, p, o, z, next, nil, m, nil)
	if err != nil || len(frames) != 1 || frames[0].id != 1 {
		t.Fatalf("wrong late delta: %+v %v", frames, err)
	}
	if _, published, err = captureSnapshotFetchCut(ctx, p, o, c, 3, next, m); err != nil || published {
		t.Fatalf("empty delta published: %v %v", published, err)
	}
}

// Opt-in query-plan diagnostics, outside the measured benchmark matrix.
func TestSnapshotFetchHistoryPlans(t *testing.T) {
	if os.Getenv("FALCON_SNAPSHOT_FETCH_PLANS") == "" {
		t.Skip("opt-in retained-history query plan diagnostic")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, History: 2000000}.defaults()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, p, o, c); err != nil {
		t.Fatal(err)
	}
	var text string
	if err = p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
		t.Fatal(err)
	}
	a, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if _, err = p.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES($1,$2,$3)", i, i+1, make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
	}
	if err = p.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
		t.Fatal(err)
	}
	z, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Rollback(context.Background())
	var xid string
	if err = pending.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid); err != nil {
		t.Fatal(err)
	}
	late, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Rollback(context.Background())
	if _, err = late.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,100,$1)", make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	// A later committed xid puts both older active xids explicitly in xip.
	if _, err = p.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,101,$1)", make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err = p.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
		t.Fatal(err)
	}
	active, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(active.xip) != 2 {
		t.Fatalf("expected two active xids: %+v", active)
	}
	if err = pending.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = late.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = p.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
		t.Fatal(err)
	}
	committed, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	onlyNoJournal := active
	onlyNoJournal.xip = []string{xid}
	conn, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	queries := []struct{ name, sql string }{
		{"exists", fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot))", domainSnapshotCandidates(o, false))},
		{"bounded", snapshotIndexedProbeQuery(o)},
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	for _, q := range queries {
		if _, err = conn.Conn().Prepare(ctx, q.name, q.sql); err != nil {
			t.Fatal(err)
		}
		for _, scenario := range []struct {
			name    string
			a, z    snapshotBenchCut
			changed bool
		}{{"nonempty", a, z, true}, {"empty", z, z, false}, {"active", active, active, false}, {"committed-no-journal", onlyNoJournal, committed, false}, {"late-journal", active, committed, true}} {
			for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
				if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 8; i++ {
					start := time.Now()
					var changed bool
					if err = conn.QueryRow(ctx, q.name, scenario.a.xmax, scenario.z.xmax, int64(0), scenario.a.xip, scenario.z.text).Scan(&changed); err != nil || changed != scenario.changed {
						t.Fatalf("%s %s: changed=%v error=%v", q.name, mode, changed, err)
					}
					t.Logf("READ %s %s %s %d %.3f ms", q.name, scenario.name, mode, i, float64(time.Since(start))/float64(time.Millisecond))
				}
				explain := fmt.Sprintf("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE %s(%s,%s,0,%s,%s)", q.name, quote(scenario.a.xmax), quote(scenario.z.xmax), quote("{"+strings.Join(scenario.a.xip, ",")+"}"), quote(scenario.z.text))
				rows, err := conn.Query(ctx, explain)
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
					var plan string
					if err = rows.Scan(&plan); err != nil {
						t.Fatal(err)
					}
					t.Logf("PLAN %s %s %s %s", q.name, scenario.name, mode, plan)
				}
				rows.Close()
				if err = rows.Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// Server-side slow-statement logging distinguishes database work from client
// scheduling/pool delays. This diagnostic intentionally runs outside the matrix.
func TestSnapshotFetchHistoryCaptureTrace(t *testing.T) {
	if os.Getenv("FALCON_SNAPSHOT_FETCH_TRACE") == "" {
		t.Skip("opt-in capture latency diagnostic")
	}
	if os.Getenv("FALCON_REPLICATION_BENCH_DATABASE_URL") != "" {
		t.Fatal("server logging diagnostic requires its own disposable PostgreSQL container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Exec(ctx, "ALTER SYSTEM SET log_min_duration_statement='10ms'"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	for _, fetch := range []string{"global", "fair"} {
		c := domainCaptureConfig{Name: "history-trace", Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, Rows: 1, Payload: 256, Keys: 4096, Skew: "pareto", Rate: 1000, History: 2000000, SnapshotFetch: fetch, SnapshotWake: "notify", NotifyBatchMS: 2, FallbackMS: 100}.defaults()
		config, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$", "-test.bench=^BenchmarkDomainCapture$", "-test.benchtime=20000x", "-test.count=1", "-test.timeout=2m", "-test.v")
		cmd.Env = append(os.Environ(), "FALCON_REPLICATION_BENCH_DATABASE_URL="+url, "FALCON_DOMAIN_CAPTURE_CONFIG="+string(config))
		output, err := cmd.CombinedOutput()
		t.Logf("TRACE BENCH %s\n%s", fetch, output)
		if err != nil {
			t.Fatal(err)
		}
	}
	podman := os.Getenv("PODMAN")
	if podman == "" {
		podman = "/opt/homebrew/bin/podman"
	}
	ids, err := exec.CommandContext(ctx, podman, "ps", "--filter", "label=falcon.benchmark=domain-capture", "--format", "{{.ID}}").Output()
	if err != nil || len(strings.Fields(string(ids))) != 1 {
		t.Fatalf("expected one diagnostic container: %q %v", ids, err)
	}
	logs, err := exec.CommandContext(ctx, podman, "logs", strings.TrimSpace(string(ids))).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SERVER LOG\n%s", logs)
}
