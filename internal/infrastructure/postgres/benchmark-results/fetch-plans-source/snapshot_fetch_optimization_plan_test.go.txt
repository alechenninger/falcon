package postgres_test

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

// Plan diagnostics are separately labelled and excluded from source throughput
// samples. Each fixture has a fresh database container, default durability,
// full-payload client reads, and server-side EXPLAIN ANALYZE records.
func TestSnapshotFetchPlanDiagnostics(t *testing.T) {
	if os.Getenv("FALCON_FETCH_PLAN_DIAGNOSTICS") == "" {
		t.Skip("opt-in fetch-plan diagnostics")
	}
	for _, fixture := range []struct {
		name      string
		history   int
		entries   int
		payload   int
		sparse    bool
		oldActive bool
	}{
		{"small-cut-history", 2000000, 16, 256, false, true},
		{"medium-cut-history", 2000000, 8192, 256, false, true},
		{"large-cut", 0, 100000, 256, false, false},
		{"large-payload", 0, 20000, 16384, false, false},
		{"sparse-late-commit", 2000000, 16, 256, true, true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
			defer cleanup()
			db, err := pgxpool.New(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, Rows: 1, Payload: fixture.payload}.defaults()
			o := newReplicationBenchObjects()
			if err = prepareDomainCapture(ctx, db, o, c); err != nil {
				t.Fatal(err)
			}
			var pending interface{ Commit(context.Context) error }
			if fixture.sparse {
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				if _, err = tx.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,1,$1)", domainCapturePayload(1, c)); err != nil {
					t.Fatal(err)
				}
				pending = tx
			}
			if fixture.history > 0 {
				if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%1024,0,$2 FROM generate_series(1,$1::int)n", fixture.history, domainCapturePayload(0, c)); err != nil {
					t.Fatal(err)
				}
			}
			// One old active XID without journal entries tests the parameterized
			// branch's empty result as well as transactions that remain active.
			if fixture.oldActive {
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				var xid string
				if err = tx.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid); err != nil {
					t.Fatal(err)
				}
				pendingEmpty := tx
				defer pendingEmpty.Rollback(context.Background())
				var text string
				if err = db.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
					t.Fatal(err)
				}
				if err = pendingEmpty.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(ctx, "UPDATE "+o.clock+" SET previous_snapshot=$1::text::pg_snapshot,snapshot=$1::text::pg_snapshot WHERE epoch=0", text); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err = db.Exec(ctx, "UPDATE "+o.clock+" SET previous_snapshot=pg_current_snapshot(),snapshot=pg_current_snapshot() WHERE epoch=0"); err != nil {
					t.Fatal(err)
				}
			}
			var text string
			if err = db.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
				t.Fatal(err)
			}
			a, err := parseSnapshotBenchCut(text)
			if err != nil {
				t.Fatal(err)
			}
			first := 1
			if pending != nil {
				if err = pending.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				first = 2
			}
			for id := first; id <= fixture.entries; id += 32 {
				end := min(fixture.entries, id+31)
				if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%1024,n,$3 FROM generate_series($1::int,$2::int)n", id, end, domainCapturePayload(1, c)); err != nil {
					t.Fatal(err)
				}
			}
			m := &domainCaptureMetrics{}
			z, err := captureDomainSnapshot(ctx, db, o, 1, a, m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(ctx, "ANALYZE "+o.journal); err != nil {
				t.Fatal(err)
			}
			conn, err := db.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
			args := []any{a.xmax, z.xmax, int64(0), a.xip, z.text}
			literals := []string{quote(a.xmax), quote(z.xmax), "0", quote("{" + strings.Join(a.xip, ",") + "}"), quote(z.text)}
			expectedCRC := crc32.ChecksumIEEE(domainCapturePayload(1, c))
			for _, cover := range []bool{false, true} {
				if cover {
					if _, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s_pkey; ALTER TABLE %s ADD PRIMARY KEY(position) INCLUDE(writer_xid)", o.journal, o.journal, o.journal)); err != nil {
						t.Fatal(err)
					}
				}
				for _, plan := range []string{"control", "guarded", "metadata", "ordered", "ordered-full", "bounded"} {
					if cover && plan != "ordered" {
						continue
					}
					q := snapshotPipelineQuery(o, false)
					if plan != "control" {
						q, err = snapshotOptimizedQuery(o, plan)
						if err != nil {
							t.Fatal(err)
						}
					}
					readArgs, literal := append([]any{}, args...), append([]string{}, literals...)
					if plan == "bounded" {
						var lo, hi, n int64
						if err = conn.QueryRow(ctx, snapshotBoundsQuery(o), args...).Scan(&lo, &hi, &n); err != nil || n != int64(fixture.entries) {
							t.Fatalf("bounds %d/%d: %v", n, fixture.entries, err)
						}
						readArgs = append(readArgs, lo, hi)
						literal = append(literal, fmt.Sprint(lo), fmt.Sprint(hi))
					}
					name := fmt.Sprintf("optimization_%s_%t", strings.ReplaceAll(plan, "-", "_"), cover)
					if _, err = conn.Conn().Prepare(ctx, name, q); err != nil {
						t.Fatal(err)
					}
					for _, mode := range []string{"auto", "force_generic_plan"} {
						if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
							t.Fatal(err)
						}
						for rep := 0; rep < 6; rep++ {
							start := time.Now()
							rows, err := conn.Query(ctx, name, readArgs...)
							if err != nil {
								t.Fatal(err)
							}
							count, last := 0, int64(0)
							var firstRow time.Duration
							for rows.Next() {
								var position, id int64
								var root int
								var data []byte
								if err = rows.Scan(&position, &root, &id, &data); err != nil {
									t.Fatal(err)
								}
								if count == 0 {
									firstRow = time.Since(start)
								}
								wantRoot := int(id % 1024)
								if fixture.sparse && id == 1 {
									wantRoot = 0
								}
								if position <= last || id < 1 || id > int64(fixture.entries) || root != wantRoot || len(data) != fixture.payload || crc32.ChecksumIEEE(data) != expectedCRC {
									t.Fatal("diagnostic membership/order/payload mismatch")
								}
								last, count = position, count+1
							}
							rows.Close()
							if err = rows.Err(); err != nil || count != fixture.entries {
								t.Fatalf("diagnostic rows %d/%d %v", count, fixture.entries, err)
							}
							v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "plan": plan, "cover": cover, "mode": mode, "repeat": rep, "rows": count, "first_ms": float64(firstRow) / float64(time.Millisecond), "total_ms": float64(time.Since(start)) / float64(time.Millisecond)})
							t.Logf("PLAN_READ %s", v)
						}
						var explain string
						if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+name+"("+strings.Join(literal, ",")+")").Scan(&explain); err != nil {
							t.Fatal(err)
						}
						v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "plan": plan, "cover": cover, "mode": mode, "explain": json.RawMessage(explain)})
						t.Logf("PLAN_EXPLAIN %s", v)
					}
				}
			}
		})
	}
}

func TestSnapshotFetchPlanCoverVisibility(t *testing.T) {
	if os.Getenv("FALCON_FETCH_PLAN_DIAGNOSTICS") == "" {
		t.Skip("opt-in covering-index visibility diagnostics")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := domainCaptureConfig{Mode: "snapshot-routed", History: 2000000}.defaults()
	o := newReplicationBenchObjects()
	if err = prepareDomainCapture(ctx, db, o, c); err != nil {
		t.Fatal(err)
	}
	var text string
	if err = db.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&text); err != nil {
		t.Fatal(err)
	}
	a, err := parseSnapshotBenchCut(text)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT 0,n,$1 FROM generate_series(1,32)n", domainCapturePayload(1, c)); err != nil {
		t.Fatal(err)
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
	if _, err = conn.Exec(ctx, "SET plan_cache_mode=force_generic_plan"); err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	literal := []string{quote(a.xmax), quote(z.xmax), "0", quote("{" + strings.Join(a.xip, ",") + "}"), quote(z.text)}
	for _, variant := range []string{"bounds", "position", "cover-unvacuumed", "cover-vacuumed"} {
		if variant == "cover-unvacuumed" {
			if _, err = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s_pkey; ALTER TABLE %s ADD PRIMARY KEY(position) INCLUDE(writer_xid)", o.journal, o.journal, o.journal)); err != nil {
				t.Fatal(err)
			}
		}
		if variant == "cover-vacuumed" {
			if _, err = conn.Exec(ctx, "VACUUM (ANALYZE) "+o.journal); err != nil {
				t.Fatal(err)
			}
		}
		q := snapshotMetadataQuery(o, true)
		if variant == "bounds" {
			q = snapshotBoundsQuery(o)
		}
		name := "visibility_" + strings.ReplaceAll(variant, "-", "_")
		if _, err = conn.Conn().Prepare(ctx, name, q); err != nil {
			t.Fatal(err)
		}
		var explain string
		if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+name+"("+strings.Join(literal, ",")+")").Scan(&explain); err != nil {
			t.Fatal(err)
		}
		v, _ := json.Marshal(map[string]any{"variant": variant, "explain": json.RawMessage(explain)})
		t.Logf("VISIBILITY_PLAN %s", v)
	}
}
