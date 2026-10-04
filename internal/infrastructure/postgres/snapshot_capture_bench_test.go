package postgres_test

// Single-request capture changes only how the immutable snapshot manifest is
// produced. Writers, notifications, fetch SQL, and root publication are shared.
import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A single command snapshot serves both pg_current_snapshot() and the probe.
// MATERIALIZED computes the cut once, not the journal delta: payloads remain on
// the separate streamed fetch path. Scalar cut references become InitPlans so
// the guarded probe retains indexed range bounds under generic planning.
// Only the publisher inserts manifests; this adds no writer-side clock lock.
func snapshotSingleCaptureQuery(o replicationBenchObjects) string {
	return fmt.Sprintf(`WITH cut AS MATERIALIZED (
 SELECT pg_current_snapshot() AS snapshot
), published AS (
 INSERT INTO %s(epoch,previous_snapshot,snapshot)
 SELECT $1::bigint,$2::text::pg_snapshot,snapshot FROM cut
 WHERE EXISTS(
  SELECT 1 FROM(
   SELECT writer_xid FROM %s
    WHERE writer_xid >= $3::text::xid8
     AND writer_xid < (SELECT pg_snapshot_xmax(snapshot) FROM cut)
     AND position>0
   UNION ALL
   SELECT j.writer_xid FROM unnest($4::text[]::xid8[])h(xid)
    CROSS JOIN LATERAL(
     SELECT writer_xid FROM %s
      WHERE writer_xid>=h.xid AND writer_xid<=h.xid AND position>0
      ORDER BY writer_xid,position LIMIT 1
    )j
    WHERE pg_visible_in_snapshot(h.xid,(SELECT snapshot FROM cut))
  )c WHERE pg_visible_in_snapshot(writer_xid,(SELECT snapshot FROM cut))
 ) RETURNING epoch
)
SELECT snapshot::text,EXISTS(SELECT 1 FROM published) FROM cut`, o.clock, o.journal, o.journal)
}

func captureSnapshotSingleRequest(ctx context.Context, p *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig, epoch int64, a snapshotBenchCut, m *domainCaptureMetrics) (snapshotBenchCut, bool, error) {
	if c.SnapshotWake != "notify" || c.SnapshotProbe != "indexed" {
		return snapshotBenchCut{}, false, fmt.Errorf("single capture requires notified capture with indexed probe")
	}
	var text string
	var published bool
	m.queries.Add(1)
	// QueryRow.Scan closes/drains its result, including ReadyForQuery and any
	// implicit-transaction commit error. No epoch is handed off on an error.
	err := p.QueryRow(ctx, snapshotSingleCaptureQuery(o), epoch, a.text, a.xmax, a.xip).Scan(&text, &published)
	if err != nil {
		return snapshotBenchCut{}, false, err
	}
	z, err := parseSnapshotBenchCut(text)
	if err != nil {
		return z, false, err
	}
	if published {
		m.cuts.Add(1)
	} else {
		m.experiment.skipped.Add(1)
	}
	return z, published, nil
}

func BenchmarkSnapshotCapture(b *testing.B) {
	c := domainCaptureConfig{Name: "capture", Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed", FetchPlan: "guarded"}.defaults()
	if err := loadSnapshotFetchConfig(&c); err != nil {
		b.Fatal(err)
	}
	runDomainCapture(b, c.defaults())
}
