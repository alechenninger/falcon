package postgres_test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

// Opt-in diagnostics run outside measured samples. Prepare while the journal
// is small, then retain history, matching how a long-lived consumer grows.
func TestDomainCaptureCounterPlans(t *testing.T) {
	if os.Getenv("FALCON_DOMAIN_CAPTURE_PLANS") == "" {
		t.Skip("opt-in query plan diagnostic")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	connString, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, name := range []string{"join", "bounded"} {
		o := newReplicationBenchObjects()
		c := domainCaptureConfig{Mode: "counter-global"}.defaults()
		if err = prepareDomainCapture(ctx, p, o, c); err != nil {
			t.Fatal(err)
		}
		seed := func(first, last int) {
			if _, err = p.Exec(ctx, fmt.Sprintf(`INSERT INTO %s SELECT 0,n,n %% 64,0,decode(repeat('ab',256),'hex') FROM generate_series($1::int,$2::int)n`, o.journal), first, last); err != nil {
				t.Fatal(err)
			}
		}
		seed(1, 200)
		query := domainCounterPageQuery(o, name)
		if _, err = conn.Conn().Prepare(ctx, name, query); err != nil {
			t.Fatal(err)
		}
		read := func(lo, hi int64, phase string) {
			for i := 0; i < 10; i++ {
				start := time.Now()
				rows, err := conn.Query(ctx, name, []int32{0}, []int64{lo}, []int64{hi}, -1, int64(0))
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for rows.Next() {
					n++
				}
				rows.Close()
				if err = rows.Err(); err != nil || n != 50 {
					t.Fatalf("read %d %v", n, err)
				}
				t.Logf("%s %s read %d: %s", name, phase, i, time.Since(start))
			}
		}
		read(150, 200, "small")
		seed(201, 200000)
		read(199950, 200000, "retained")
		var generic, custom int64
		if err = conn.QueryRow(ctx, "SELECT generic_plans,custom_plans FROM pg_prepared_statements WHERE name=$1", name).Scan(&generic, &custom); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s generic=%d custom=%d", name, generic, custom)
		for _, cache := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
			if _, err = conn.Exec(ctx, "SET plan_cache_mode="+cache); err != nil {
				t.Fatal(err)
			}
			var plan string
			if err = conn.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE \""+name+"\"('{0}','{199950}','{200000}',-1,0)").Scan(&plan); err != nil {
				t.Fatal(err)
			}
			t.Logf("PLAN %s %s %s", name, cache, plan)
		}
		if _, err = conn.Exec(ctx, "SET plan_cache_mode=auto"); err != nil {
			t.Fatal(err)
		}
	}
}
