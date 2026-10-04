package postgres_test

// Supplemental statistics diagnostics are built and run after the sustained
// matrix. The frozen matrix binary and its eight hashed source files are kept.
import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotFetchPlanStatistics(t *testing.T) {
	if os.Getenv("FALCON_FETCH_PLAN_DIAGNOSTICS") == "" {
		t.Skip("opt-in fetch-plan statistics diagnostics")
	}
	for _, f := range []struct {
		name    string
		history int
		diverse bool
		entries int
	}{
		{"fresh-stale-stats", 0, false, 100000},
		{"history-stale-stats", 2000000, false, 100000},
		{"diverse-history", 2000000, true, 16},
	} {
		t.Run(f.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
			defer cleanup()
			db, err := pgxpool.New(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16}.defaults()
			o := newReplicationBenchObjects()
			if err = prepareDomainCapture(ctx, db, o, c); err != nil {
				t.Fatal(err)
			}
			// Isolate explicitly stale versus refreshed statistics. Autovacuum
			// remains enabled in the sustained matrix; only this fixture disables
			// it so background autoanalyze cannot erase the comparison.
			if _, err = db.Exec(ctx, "ALTER TABLE "+o.journal+" SET (autovacuum_enabled=false)"); err != nil {
				t.Fatal(err)
			}
			payload := domainCapturePayload(1, c)
			crc := crc32.ChecksumIEEE(payload)
			step := f.history
			if f.diverse {
				step = 1000 // 2,000 real committed historical writer XIDs
			}
			for first := 1; first <= f.history; first += step {
				if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%1024,0,$3 FROM generate_series($1::int,$2::int)n", first, min(first+step-1, f.history), payload); err != nil {
					t.Fatal(err)
				}
			}
			if f.history > 0 {
				if _, err = db.Exec(ctx, "ANALYZE "+o.journal); err != nil {
					t.Fatal(err)
				}
			}
			old, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Rollback(context.Background())
			var xid, text string
			if err = old.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
				t.Fatal(err)
			}
			a, err := parseSnapshotBenchCut(text)
			if err != nil {
				t.Fatal(err)
			}
			if err = old.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			for first := 1; first <= f.entries; first += 32 {
				if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%1024,n,$3 FROM generate_series($1::int,$2::int)n", first, min(first+31, f.entries), payload); err != nil {
					t.Fatal(err)
				}
			}
			z, err := captureDomainSnapshot(ctx, db, o, 1, a, &domainCaptureMetrics{})
			if err != nil {
				t.Fatal(err)
			}
			conn, err := db.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
			args := []any{a.xmax, z.xmax, int64(0), a.xip, z.text}
			literal := []string{quote(a.xmax), quote(z.xmax), "0", quote("{" + strings.Join(a.xip, ",") + "}"), quote(z.text)}
			for _, analyzed := range []bool{false, true} {
				if analyzed {
					if _, err = conn.Exec(ctx, "ANALYZE "+o.journal); err != nil {
						t.Fatal(err)
					}
				}
				fixture := f.name + map[bool]string{false: "/before-analyze", true: "/after-analyze"}[analyzed]
				var estimatedRows float64
				if err = conn.QueryRow(ctx, "SELECT reltuples::float8 FROM pg_class WHERE oid=$1::regclass", o.journal).Scan(&estimatedRows); err != nil {
					t.Fatal(err)
				}
				t.Logf("PLAN_STATS fixture=%s estimated_rows=%.0f actual_rows=%d", fixture, estimatedRows, f.history+f.entries)
				boundsName := fmt.Sprintf("stats_bounds_%t", analyzed)
				if _, err = conn.Conn().Prepare(ctx, boundsName, snapshotBoundsQuery(o)); err != nil {
					t.Fatal(err)
				}
				for _, plan := range []string{"control", "guarded", "metadata", "adaptive"} {
					q := snapshotPipelineQuery(o, false)
					if plan != "control" {
						q, err = snapshotOptimizedQuery(o, plan)
						if err != nil {
							t.Fatal(err)
						}
					}
					name := fmt.Sprintf("stats_%s_%t", plan, analyzed)
					if _, err = conn.Conn().Prepare(ctx, name, q); err != nil {
						t.Fatal(err)
					}
					for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
						if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
							t.Fatal(err)
						}
						readArgs, literals := append([]any{}, args...), append([]string{}, literal...)
						for rep := 0; rep < 6; rep++ {
							start := time.Now()
							if plan == "adaptive" {
								var lo, hi, n int64
								if err = conn.QueryRow(ctx, boundsName, args...).Scan(&lo, &hi, &n); err != nil || n != int64(f.entries) {
									t.Fatalf("statistics bounds %d/%d %v", n, f.entries, err)
								}
								readArgs = append(append([]any{}, args...), lo, hi)
								literals = append(append([]string{}, literal...), fmt.Sprint(lo), fmt.Sprint(hi))
							}
							rows, err := conn.Query(ctx, name, readArgs...)
							if err != nil {
								t.Fatal(err)
							}
							var firstRow time.Duration
							last, count := int64(0), 0
							for rows.Next() {
								var pos, id int64
								var root int
								var data []byte
								if err = rows.Scan(&pos, &root, &id, &data); err != nil {
									t.Fatal(err)
								}
								if count == 0 {
									firstRow = time.Since(start)
								}
								if pos <= last || id < 1 || id > int64(f.entries) || root != int(id%1024) || len(data) != c.Payload || crc32.ChecksumIEEE(data) != crc {
									t.Fatal("statistics fixture membership/order/payload mismatch")
								}
								last, count = pos, count+1
							}
							rows.Close()
							if err = rows.Err(); err != nil || count != f.entries {
								t.Fatalf("statistics rows %d/%d %v", count, f.entries, err)
							}
							v, _ := json.Marshal(map[string]any{"fixture": fixture, "plan": plan, "cover": false, "mode": mode, "repeat": rep, "rows": count, "first_ms": float64(firstRow) / float64(time.Millisecond), "total_ms": float64(time.Since(start)) / float64(time.Millisecond), "includes_bounds": plan == "adaptive"})
							t.Logf("PLAN_READ %s", v)
						}
						var explain string
						if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+name+"("+strings.Join(literals, ",")+")").Scan(&explain); err != nil {
							t.Fatal(err)
						}
						v, _ := json.Marshal(map[string]any{"fixture": fixture, "plan": plan, "cover": false, "mode": mode, "explain": json.RawMessage(explain)})
						t.Logf("PLAN_EXPLAIN %s", v)
						if plan == "adaptive" {
							if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+boundsName+"("+strings.Join(literal, ",")+")").Scan(&explain); err != nil {
								t.Fatal(err)
							}
							v, _ = json.Marshal(map[string]any{"fixture": fixture, "plan": "bounds", "cover": false, "mode": mode, "explain": json.RawMessage(explain)})
							t.Logf("PLAN_EXPLAIN %s", v)
						}
					}
				}
			}
		})
	}
}
