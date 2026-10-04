package postgres_test

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These client reads and EXPLAINs are diagnostic, never capacity samples.
// The dense fixture deliberately spans multiple small XID partitions.
func TestSnapshotPartitionPlans(t *testing.T) {
	if os.Getenv("FALCON_SNAPSHOT_PARTITION_DIAGNOSTICS") == "" {
		t.Skip("opt-in partition plans")
	}
	for _, fixture := range []struct {
		name                        string
		history, historyTx, entries int
		span                        int64
		ahead                       int
	}{
		{"small-cut", 0, 0, 16, 8192, 32},
		{"diverse-history", 2000000, 2048, 16, 8192, 32},
		{"bulk-history", 2000000, 1, 16, 8192, 32},
		{"dense-cut", 0, 0, 100000, 256, 64},
		{"many-leaves", 0, 0, 16, 8192, 512},
	} {
		for _, arm := range []string{"flat-position", "flat-compound", "partitioned"} {
			t.Run(fixture.name+"/"+arm, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
				defer cancel()
				url, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
				defer cleanup()
				db, err := pgxpool.New(ctx, url)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				c := domainCaptureConfig{Mode: "snapshot-routed", Tenants: 64, RootsPerTenant: 16, History: fixture.history, HistoryTx: fixture.historyTx}
				if arm == "flat-compound" {
					c.CompoundKey = true
				}
				if arm == "partitioned" {
					c.PartitionSpan, c.PartitionAhead = fixture.span, fixture.ahead
				}
				c = c.defaults()
				o := newReplicationBenchObjects()
				if err = prepareDomainCapture(ctx, db, o, c); err != nil {
					t.Fatal(err)
				}
				// Include one prior active writer with no journal rows. Its eventual
				// resolution exercises old-XID pruning, even for an empty result.
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
				payload := domainCapturePayload(1, c)
				crc := crc32.ChecksumIEEE(payload)
				for first := 1; first <= fixture.entries; first += 32 {
					if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%1024,n,$3 FROM generate_series($1::int,$2::int)n", first, min(first+31, fixture.entries), payload); err != nil {
						t.Fatal(err)
					}
				}
				z, err := captureDomainSnapshot(ctx, db, o, 1, a, &domainCaptureMetrics{})
				if err != nil {
					t.Fatal(err)
				}
				if c.partitions != nil {
					for _, leaf := range c.partitions.leaves {
						if _, err = db.Exec(ctx, "ANALYZE "+leaf.name); err != nil {
							t.Fatal(err)
						}
					}
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
				for _, plan := range []string{"any", "guarded"} {
					q := snapshotPipelineQuery(o, false)
					if plan == "guarded" {
						q, err = snapshotOptimizedQuery(o, "guarded")
						if err != nil {
							t.Fatal(err)
						}
					}
					for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
						if _, err = conn.Exec(ctx, "SET plan_cache_mode="+mode); err != nil {
							t.Fatal(err)
						}
						name := "partition_" + plan + "_" + mode
						start := time.Now()
						if _, err = conn.Conn().Prepare(ctx, name, q); err != nil {
							t.Fatal(err)
						}
						prepareMs := float64(time.Since(start)) / float64(time.Millisecond)
						for rep := 0; rep < 6; rep++ {
							start = time.Now()
							rows, err := conn.Query(ctx, name, args...)
							if err != nil {
								t.Fatal(err)
							}
							count, last := 0, int64(0)
							var first time.Duration
							for rows.Next() {
								var position, id int64
								var root int
								var data []byte
								if err = rows.Scan(&position, &root, &id, &data); err != nil {
									t.Fatal(err)
								}
								if count == 0 {
									first = time.Since(start)
								}
								if position <= last || id < 1 || id > int64(fixture.entries) || root != int(id%1024) || len(data) != c.Payload || crc32.ChecksumIEEE(data) != crc {
									t.Fatal("partition diagnostic payload/membership/order")
								}
								last, count = position, count+1
							}
							rows.Close()
							if err = rows.Err(); err != nil || count != fixture.entries {
								t.Fatalf("partition read %d/%d %v", count, fixture.entries, err)
							}
							v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "arm": arm, "plan": plan, "mode": mode, "repeat": rep, "rows": count, "span": c.PartitionSpan, "ahead": c.PartitionAhead, "prepare_ms": prepareMs, "first_ms": float64(first) / float64(time.Millisecond), "total_ms": float64(time.Since(start)) / float64(time.Millisecond)})
							t.Logf("PARTITION_PLAN_READ %s", v)
						}
						var explain string
						if err = conn.QueryRow(ctx, "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE "+name+"("+strings.Join(literals, ",")+")").Scan(&explain); err != nil {
							t.Fatal(err)
						}
						v, _ := json.Marshal(map[string]any{"fixture": fixture.name, "arm": arm, "plan": plan, "mode": mode, "span": c.PartitionSpan, "ahead": c.PartitionAhead, "explain": json.RawMessage(explain)})
						t.Logf("PARTITION_PLAN_EXPLAIN %s", v)
					}
				}
				var relations, indexes int
				if err = db.QueryRow(ctx, "SELECT count(*),COALESCE(sum((SELECT count(*) FROM pg_index WHERE indrelid=t.relid)),0)::int FROM pg_partition_tree($1::regclass)t WHERE isleaf", o.journal).Scan(&relations, &indexes); err != nil {
					t.Fatal(err)
				}
				if c.PartitionSpan == 0 {
					relations, indexes = 1, 3
				}
				t.Logf("PARTITION_FIXTURE name=%s arm=%s leaves=%d leaf_indexes=%d", fixture.name, arm, relations, indexes)
			})
		}
	}
}
