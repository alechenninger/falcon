package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Outside throughput measurements: verify the fused capture's probe does not
// acquire the broad retained-history scan avoided by the existing probe.
func TestSnapshotCapturePlans(t *testing.T) {
	if os.Getenv("FALCON_SNAPSHOT_CAPTURE_DIAGNOSTICS") == "" {
		t.Skip("opt-in capture plans")
	}
	for _, fixture := range []struct {
		name        string
		history, tx int
	}{{"small", 0, 0}, {"bulk-history", 2000000, 1}, {"diverse-history", 2000000, 2048}} {
		for _, partitioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partitioned-%t", fixture.name, partitioned), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
				defer cleanup()
				db, err := pgxpool.New(ctx, url)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, History: fixture.history, HistoryTx: fixture.tx}.defaults()
				if partitioned {
					c.PartitionSpan, c.PartitionAhead = 8192, 4
					c = c.defaults()
				}
				o := newReplicationBenchObjects()
				if err = prepareDomainCapture(ctx, db, o, c); err != nil {
					t.Fatal(err)
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
				conn, err := db.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Release()
				quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
				epoch := int64(1)
				for _, changed := range []bool{false, true} {
					if changed {
						if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,1,$1)", domainCapturePayload(1, c)); err != nil {
							t.Fatal(err)
						}
					}
					for _, mode := range []string{"auto", "force_generic_plan"} {
						if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
							t.Fatal(err)
						}
						name := fmt.Sprintf("capture_%s_%t", mode, changed)
						if _, err = conn.Conn().Prepare(ctx, name, snapshotSingleCaptureQuery(o)); err != nil {
							t.Fatal(err)
						}
						for rep := 0; rep < 6; rep++ {
							start := time.Now()
							var snapshot string
							var published bool
							if err = conn.QueryRow(ctx, name, epoch, a.text, a.xmax, a.xip).Scan(&snapshot, &published); err != nil || published != changed {
								t.Fatalf("capture %t want %t: %v", published, changed, err)
							}
							if conn.Conn().PgConn().TxStatus() != 'I' {
								t.Fatal("capture transaction left open")
							}
							v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "partitioned": partitioned, "changed": changed, "mode": mode, "repeat": rep, "total_ms": float64(time.Since(start)) / float64(time.Millisecond)})
							t.Logf("CAPTURE_PLAN_READ %s", v)
							epoch++
						}
						literals := []string{fmt.Sprint(epoch), quote(a.text), quote(a.xmax), quote("{" + strings.Join(a.xip, ",") + "}")}
						var explain string
						if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+name+"("+strings.Join(literals, ",")+")").Scan(&explain); err != nil {
							t.Fatal(err)
						}
						epoch++
						v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "partitioned": partitioned, "changed": changed, "mode": mode, "explain": json.RawMessage(explain)})
						t.Logf("CAPTURE_PLAN_EXPLAIN %s", v)
					}
				}
			})
		}
	}
}
