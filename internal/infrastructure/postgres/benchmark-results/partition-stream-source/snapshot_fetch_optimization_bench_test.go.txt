package postgres_test

// Fetch-plan experiments retain the immutable snapshot membership and position
// ordering of the pipeline benchmark. No writer or production API changes.
import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The old-active branch is driven by individual visible XIDs. OFFSET 0 keeps
// the indexed, parameterized lookup as a separate subquery. Range bounds plus
// index ordering avoid the original generic-plan ANY-array position scan.
func snapshotGuardedCandidates(o replicationBenchObjects, metadata bool) string {
	columns := "position,writer_xid,root_id,batch_id,payload"
	if metadata {
		columns = "position,writer_xid"
	}
	return fmt.Sprintf(`SELECT %s FROM %s
 WHERE writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8 AND position>$3
 UNION ALL
 SELECT j.* FROM(
  SELECT xid FROM unnest($4::text[]::xid8[])h(xid)
   WHERE pg_visible_in_snapshot(xid,$5::text::pg_snapshot) OFFSET 0
 )h CROSS JOIN LATERAL(
  SELECT %s FROM %s
   WHERE writer_xid>=h.xid AND writer_xid<=h.xid AND position>$3
   ORDER BY writer_xid,position OFFSET 0
 )j`, columns, o.journal, columns, o.journal)
}

func snapshotMetadataQuery(o replicationBenchObjects, ordered bool) string {
	selection := fmt.Sprintf(`SELECT position FROM(%s)c
 WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot)
 ORDER BY position OFFSET 0`, snapshotGuardedCandidates(o, true))
	if ordered {
		selection = fmt.Sprintf(`SELECT position FROM %s
 WHERE position>$3 AND
  ((writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8) OR writer_xid=ANY($4::text[]::xid8[]))
  AND pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot)
 ORDER BY position OFFSET 0`, o.journal)
	}
	// Keep payload lookup after metadata ordering. A parameterized unique-key
	// lookup preserves the outer order, rather than joining then sorting payloads.
	return fmt.Sprintf(`SELECT j.position,j.root_id,j.batch_id,j.payload FROM(%s)ids
 CROSS JOIN LATERAL(
  SELECT position,root_id,batch_id,payload FROM %s WHERE position=ids.position OFFSET 0
 )j ORDER BY ids.position`, selection, o.journal)
}

func snapshotBoundsQuery(o replicationBenchObjects) string {
	return fmt.Sprintf(`SELECT COALESCE(min(position),0),COALESCE(max(position),0),count(*)
 FROM(%s)c WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot)`, snapshotGuardedCandidates(o, true))
}

func snapshotBoundedQuery(o replicationBenchObjects) string {
	return fmt.Sprintf(`SELECT position,root_id,batch_id,payload FROM %s
 WHERE position >= $6 AND position <= $7 AND position>$3 AND
  ((writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8) OR writer_xid=ANY($4::text[]::xid8[]))
  AND pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot)
 ORDER BY position`, o.journal)
}

func snapshotOptimizedQuery(o replicationBenchObjects, plan string) (string, error) {
	switch plan {
	case "guarded":
		return fmt.Sprintf(`SELECT position,root_id,batch_id,payload FROM(%s)c
 WHERE pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) ORDER BY position`, snapshotGuardedCandidates(o, false)), nil
	case "metadata":
		return snapshotMetadataQuery(o, false), nil
	case "ordered":
		return snapshotMetadataQuery(o, true), nil
	case "ordered-full":
		return fmt.Sprintf(`SELECT position,root_id,batch_id,payload FROM %s
 WHERE position>$3 AND
  ((writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8) OR writer_xid=ANY($4::text[]::xid8[]))
  AND pg_visible_in_snapshot(writer_xid,$5::text::pg_snapshot) ORDER BY position`, o.journal), nil
	case "bounded", "adaptive":
		return snapshotBoundedQuery(o), nil
	default:
		return "", fmt.Errorf("unknown snapshot fetch plan %q", plan)
	}
}

func fetchSnapshotOptimizedRows(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, cut snapshotFetchCut, c domainCaptureConfig, m *domainCaptureMetrics, row func(domainCaptureFrame) error) error {
	query, err := snapshotOptimizedQuery(o, c.FetchPlan)
	if err != nil {
		return err
	}
	args := []any{cut.a.xmax, cut.z.xmax, int64(0), cut.a.xip, cut.z.text}
	x := m.experiment.pipeline
	if c.FetchPlan == "bounded" || c.FetchPlan == "adaptive" {
		start := time.Now()
		m.queries.Add(1)
		m.experiment.fetchQueries.Add(1)
		var lo, hi, count int64
		err = db.QueryRow(ctx, snapshotBoundsQuery(o), args...).Scan(&lo, &hi, &count)
		m.experiment.sql("fetch", time.Since(start))
		if err != nil {
			return err
		}
		x.mu.Lock()
		x.boundsQueries++
		x.candidatePositions += count
		span := int64(0)
		if count > 0 {
			span = hi - lo + 1
			x.positionSpan += span
		}
		fallback := c.FetchPlan == "adaptive" && span > 4*count+4096
		if fallback {
			x.fallbackCuts++
			query = snapshotMetadataQuery(o, false)
		} else {
			x.boundedCuts++
			args = append(args, lo, hi)
		}
		x.mu.Unlock()
		if count == 0 {
			return nil
		}
	}
	start := time.Now()
	m.queries.Add(1)
	m.experiment.fetchQueries.Add(1)
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	position, count := int64(0), 0
	for rows.Next() {
		var f domainCaptureFrame
		if err = rows.Scan(&f.position, &f.root, &f.id, &f.payload); err != nil {
			return err
		}
		if count == 0 {
			x.mu.Lock()
			x.firstRow = append(x.firstRow, time.Since(start))
			x.mu.Unlock()
		}
		if f.position <= position {
			return fmt.Errorf("optimized fetch position regression")
		}
		position, count = f.position, count+1
		if err = snapshotFetchPause(ctx, c, []domainCaptureFrame{f}, m.experiment); err == nil {
			err = row(f)
		}
		if err != nil {
			return err
		}
	}
	rows.Close()
	m.experiment.sql("fetch", time.Since(start))
	return rows.Err()
}

func BenchmarkSnapshotFetchPlans(b *testing.B) {
	c := domainCaptureConfig{Name: "fetch-plans", Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed"}.defaults()
	if err := loadSnapshotFetchConfig(&c); err != nil {
		b.Fatal(err)
	}
	runDomainCapture(b, c.defaults())
}
