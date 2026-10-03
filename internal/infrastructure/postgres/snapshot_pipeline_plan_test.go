package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Opt-in diagnostics are outside measured samples. Both result membership and
// plans are checked with auto/custom/generic prepared plans, retained history,
// hundreds of writer XIDs, and a large TOASTed result set.
func TestSnapshotPipelinePlans(t *testing.T) {
	if os.Getenv("FALCON_SNAPSHOT_PIPELINE_PLANS") == "" {
		t.Skip("opt-in pipeline query plans")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	for fixture, c := range []domainCaptureConfig{
		{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, History: 2000000, Payload: 256},
		{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, Rows: 16, Payload: 1024},
	} {
		c = c.defaults()
		o := newReplicationBenchObjects()
		if err = prepareDomainCapture(ctx, db, o, c); err != nil {
			t.Fatal(err)
		}
		m := &domainCaptureMetrics{}
		var initial string
		if err = db.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&initial); err != nil {
			t.Fatal(err)
		}
		a, err := parseSnapshotBenchCut(initial)
		if err != nil {
			t.Fatal(err)
		}
		for first := 1; first <= 8192; first += 32 {
			if _, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO %s(root_id,batch_id,payload) SELECT n %% 1024,n,$3 FROM generate_series($1::int,$2::int)n", o.journal), first, first+31, domainCapturePayload(int64(first), c)); err != nil {
				t.Fatal(err)
			}
		}
		z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for arm, page := range []int{128, 2048, 0} {
			name := fmt.Sprintf("pipeline_plan_%d_%d", fixture, arm)
			q := snapshotPipelineQuery(o, page != 0)
			if _, err = conn.Conn().Prepare(ctx, name, q); err != nil {
				t.Fatal(err)
			}
			args := []any{a.xmax, z.xmax, int64(0), a.xip, z.text}
			literal := []string{quote(a.xmax), quote(z.xmax), "0", quote("{" + strings.Join(a.xip, ",") + "}"), quote(z.text)}
			if page != 0 {
				args = append(args, page)
				literal = append(literal, fmt.Sprint(page))
			}
			for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
				if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
					t.Fatal(err)
				}
				for rep := 0; rep < 6; rep++ {
					start := time.Now()
					rows, err := conn.Query(ctx, name, args...)
					if err != nil {
						t.Fatal(err)
					}
					n := 0
					for rows.Next() {
						n++
					}
					rows.Close()
					want := 8192
					if page != 0 {
						want = page
					}
					if err = rows.Err(); err != nil || n != want {
						t.Fatalf("plan membership %d/%d %v", n, want, err)
					}
					t.Logf("READ fixture=%d page=%d mode=%s rep=%d rows=%d elapsed=%s", fixture, page, mode, rep, n, time.Since(start))
				}
				var plan string
				explain := "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE " + name + "(" + strings.Join(literal, ",") + ")"
				if err = conn.QueryRow(ctx, explain).Scan(&plan); err != nil {
					t.Fatal(err)
				}
				t.Logf("PLAN fixture=%d page=%d mode=%s %s", fixture, page, mode, plan)
			}
		}
		conn.Release()
	}
}
