package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkSnapshotJournal(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSnapshotPipelined, replicationSequencePipelined, replicationLogicalSlot} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, workload, mode, replicationBenchRunOptions{writerCount: 16, fetchLimit: 128})
				})
			}
		})
	}
}

func BenchmarkSnapshotJournalAtRate(b *testing.B) {
	for _, rate := range []int{1000, 3000} {
		b.Run(fmt.Sprintf("At%d", rate), func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSnapshotPipelined, replicationSequencePipelined, replicationLogicalSlot} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256}, mode,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: rate})
				})
			}
		})
	}
}

func BenchmarkSnapshotJournalFaults(b *testing.B) {
	for _, fault := range []struct {
		name  string
		value sequenceBenchFault
	}{
		{"Rollback", sequenceBenchRollback}, {"BeforeInsert200ms", sequenceBenchBeforeInsertDelay}, {"AfterInsert200ms", sequenceBenchAfterInsertDelay},
	} {
		b.Run(fault.name, func(b *testing.B) {
			// Both use ordinary append/commit so the delay really holds a writer
			// transaction open; healthy capacity comparisons use both pipelines.
			for _, mode := range []replicationBenchMode{replicationSnapshotPoll, replicationSequencePoll} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sequenceFault: fault.value}, mode,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: 1000})
				})
			}
		})
	}
}

func BenchmarkSnapshotJournalKeys(b *testing.B) {
	for _, keys := range []int{256, 1} {
		b.Run(fmt.Sprintf("Keys%d", keys), func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSnapshotPipelined, replicationSequencePipelined} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sourceKeys: keys}, mode,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128})
				})
			}
		})
	}
}

func prepareSnapshotBenchSchema(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SEQUENCE %s AS BIGINT CACHE 1 NO CYCLE;
		CREATE TABLE %s (
			position BIGINT PRIMARY KEY DEFAULT nextval('%s'),
			writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id(),
			batch_id BIGINT NOT NULL,
			payload BYTEA NOT NULL
		);
		ALTER SEQUENCE %s OWNED BY %s.position;
		ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;
		CREATE INDEX ON %s (writer_xid, position);
		CREATE TABLE %s (
			epoch BIGINT PRIMARY KEY,
			previous_snapshot PG_SNAPSHOT NOT NULL,
			snapshot PG_SNAPSHOT NOT NULL,
			batch_count BIGINT NOT NULL
		);
		INSERT INTO %s VALUES (0, pg_current_snapshot(), pg_current_snapshot(), 0);
	`, objects.sequence, objects.journal, objects.sequence, objects.sequence, objects.journal,
		objects.journal, objects.journal, objects.clock, objects.clock))
	return err
}

// Only top-level xid8 values are used. The explicit column survives tuple
// freezing and makes range/exception lookup possible without consulting xmin.
type snapshotBenchCut struct {
	text string
	xmax string
	xip  []string
}

func parseSnapshotBenchCut(value string) (snapshotBenchCut, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return snapshotBenchCut{}, fmt.Errorf("invalid PostgreSQL snapshot %q", value)
	}
	cut := snapshotBenchCut{text: value, xmax: parts[1], xip: []string{}}
	if parts[2] != "" {
		cut.xip = strings.Split(parts[2], ",")
	}
	return cut, nil
}

type snapshotBenchReader struct {
	epoch int64
	cut   snapshotBenchCut
}

// The caller supplies its applied checkpoint. The latest durable manifest may
// be ahead of application after a crash, and must be replayed, not skipped.
func loadSnapshotBenchReader(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects, appliedEpoch int64) (*snapshotBenchReader, error) {
	var value string
	if err := pool.QueryRow(ctx, "SELECT snapshot::text FROM "+objects.clock+" WHERE epoch=$1", appliedEpoch).Scan(&value); err != nil {
		return nil, err
	}
	cut, err := parseSnapshotBenchCut(value)
	return &snapshotBenchReader{epoch: appliedEpoch, cut: cut}, err
}

// These sets are disjoint: old xip entries are below old xmax. New visible
// transactions are either in the new range or were active at the old cut.
// The final visibility predicate also supports replay of a durable old epoch
// after transactions excluded by that epoch have subsequently committed.
func snapshotBenchDeltaQuery(objects replicationBenchObjects) string {
	return fmt.Sprintf(`
		SELECT position, batch_id, payload FROM (
			SELECT position, writer_xid, batch_id, payload FROM %s
			WHERE writer_xid >= $1::text::xid8 AND writer_xid < $2::text::xid8 AND position > $3
			UNION ALL
			SELECT position, writer_xid, batch_id, payload FROM %s
			WHERE writer_xid = ANY($4::text[]::xid8[]) AND position > $3
		) AS candidates
		WHERE pg_visible_in_snapshot(writer_xid, $5::text::pg_snapshot)
		ORDER BY position LIMIT $6
	`, objects.journal, objects.journal)
}

func readSnapshotBenchPages(ctx context.Context, tx pgx.Tx, objects replicationBenchObjects, previous, next snapshotBenchCut,
	limit int, tracker *replicationBenchTracker, afterPage func(int) error) ([]sequenceBenchEntry, error) {
	var entries []sequenceBenchEntry
	var cursor int64 // Local to this epoch: late commits may have older positions.
	for {
		rows, err := tx.Query(ctx, snapshotBenchDeltaQuery(objects), previous.xmax, next.xmax, cursor, previous.xip, next.text, limit)
		if err != nil {
			return nil, err
		}
		count := 0
		for rows.Next() {
			var entry sequenceBenchEntry
			if err := rows.Scan(&entry.position, &entry.id, &entry.payload); err != nil {
				rows.Close()
				return nil, err
			}
			if entry.position <= cursor {
				rows.Close()
				return nil, fmt.Errorf("non-increasing epoch position %d", entry.position)
			}
			entries = append(entries, entry)
			cursor = entry.position
			count++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if tracker != nil {
			if count == 0 {
				tracker.recordEmptyFetch()
			} else {
				tracker.recordFetch(count)
			}
		}
		if afterPage != nil {
			if err := afterPage(len(entries)); err != nil {
				return nil, err
			}
		}
		if count < limit {
			return entries, nil
		}
	}
}

// A manifest is a durable ordering decision, not an applied-graph checkpoint.
// It stores both cuts so replay does not need a live exported SQL snapshot.
// Only one epoch producer is supported; a production leader needs fencing.
func (r *snapshotBenchReader) fetch(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects, limit int,
	tracker *replicationBenchTracker, publish func(int64, []sequenceBenchEntry) error, afterPage func(int) error) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	previous := r.cut
	var nextText, previousText string
	var expected int
	err = tx.QueryRow(ctx, "SELECT previous_snapshot::text, snapshot::text, batch_count FROM "+objects.clock+" WHERE epoch=$1", r.epoch+1).
		Scan(&previousText, &nextText, &expected)
	replay := err == nil
	if err != nil && err != pgx.ErrNoRows {
		return 0, err
	}
	if replay {
		previous, err = parseSnapshotBenchCut(previousText)
	} else {
		err = tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&nextText)
	}
	if err != nil {
		return 0, err
	}
	next, err := parseSnapshotBenchCut(nextText)
	if err != nil {
		return 0, err
	}
	entries, err := readSnapshotBenchPages(ctx, tx, objects, previous, next, limit, tracker, afterPage)
	if err != nil {
		return 0, err
	}
	if replay && len(entries) != expected {
		return 0, fmt.Errorf("epoch %d replay has %d batches, want %d (retention loss)", r.epoch+1, len(entries), expected)
	}
	if len(entries) == 0 && !replay {
		// Empty cuts contain no source changes and need no durable epoch. A
		// restart uses the older durable cut and can safely rediscover this span.
		r.cut = next
		return 0, nil
	}
	if !replay {
		_, err = tx.Exec(ctx, "INSERT INTO "+objects.clock+" (epoch, previous_snapshot, snapshot, batch_count) VALUES ($1, $2::text::pg_snapshot, $3::text::pg_snapshot, $4)",
			r.epoch+1, previous.text, next.text, len(entries))
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if err := publish(r.epoch+1, entries); err != nil {
		return 0, err // Leave applied progress unchanged; replay the manifest.
	}
	r.epoch++
	r.cut = next
	if tracker != nil {
		tracker.mu.Lock()
		tracker.measurements.snapshotEpochs++
		if replay {
			tracker.measurements.snapshotReplays++
		}
		tracker.measurements.snapshotExceptionMax = max(tracker.measurements.snapshotExceptionMax, len(previous.xip), len(next.xip))
		tracker.mu.Unlock()
	}
	return len(entries), nil
}

func startSnapshotBenchConsumer(ctx context.Context, pool *pgxpool.Pool, tracker *replicationBenchTracker, workload replicationBenchWorkload,
	checksum uint32, limit int, objects replicationBenchObjects, ready chan<- struct{}, workers *sync.WaitGroup, reportError func(error)) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		reader, err := loadSnapshotBenchReader(ctx, pool, objects, 0)
		if err != nil {
			reportError(err)
			return
		}
		publish := func(_ int64, entries []sequenceBenchEntry) error {
			// Decode every batch before publishing any completion. A production
			// graph would stage changes and expose the epoch after all shards apply.
			checksums := make([]uint64, len(entries))
			for i, entry := range entries {
				if entry.id == nil || len(entry.payload) != workload.rowsPerTx*workload.payloadSize {
					return fmt.Errorf("invalid snapshot batch at %d", entry.position)
				}
				for offset := 0; offset < len(entry.payload); offset += workload.payloadSize {
					checksums[i] += uint64(crc32.ChecksumIEEE(entry.payload[offset : offset+workload.payloadSize]))
				}
				if checksums[i] != uint64(checksum)*uint64(workload.rowsPerTx) {
					return fmt.Errorf("snapshot payload checksum at %d", entry.position)
				}
			}
			finished := time.Now()
			for i, entry := range entries {
				if err := tracker.recordApply(*entry.id, workload.rowsPerTx, checksums[i], workload.rowsPerTx, checksum, finished); err != nil {
					return err
				}
			}
			return nil
		}
		poller := newSnapshotBenchPoller(tracker.snapshotPollMode)
		defer poller.stop()
		first := true
		var wakeReady <-chan struct{}
		if tracker.snapshotWake != nil {
			wakeReady = tracker.snapshotWake.ready
		}
		for {
			poller.beforeScan()
			// Consume hints before capturing the cut. A commit during the scan
			// must leave a pending hint for the next scan, never be cleared after it.
			select {
			case <-wakeReady:
				tracker.snapshotWake.consumed.Add(1)
			default:
			}
			if tracker.snapshotWake != nil {
				tracker.snapshotWake.scans.Add(1)
			}
			count, err := reader.fetch(ctx, pool, objects, limit, tracker, publish, nil)
			if err != nil {
				if ctx.Err() == nil {
					reportError(err)
				}
				return
			}
			poller.afterScan()
			if first {
				close(ready)
				first = false
			}
			if count >= limit {
				continue
			}
			select {
			case <-wakeReady:
				tracker.snapshotWake.consumed.Add(1)
			case <-poller.c:
				if tracker.snapshotWake != nil {
					tracker.snapshotWake.polls.Add(1)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func TestSnapshotJournalProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	newObjects := func(t *testing.T) replicationBenchObjects {
		objects := newReplicationBenchObjects()
		if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
			t.Fatal(err)
		}
		if err := prepareSnapshotBenchSchema(ctx, pool, objects); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false); err != nil {
				t.Error(err)
			}
		})
		return objects
	}
	begin := func(t *testing.T) pgx.Tx {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tx.Rollback(context.Background()) })
		return tx
	}
	appendBatch := func(t *testing.T, tx pgx.Tx, objects replicationBenchObjects, id int64) int64 {
		if err := writeJournalBenchSource(ctx, tx, objects, replicationBenchWorkload{rowsPerTx: 1}, "value", id); err != nil {
			t.Fatal(err)
		}
		var position int64
		if err := tx.QueryRow(ctx, sequenceBenchAppendQuery(objects), id, []byte("value")).Scan(&position); err != nil {
			t.Fatal(err)
		}
		return position
	}
	commit := func(t *testing.T, tx pgx.Tx) {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	load := func(t *testing.T, objects replicationBenchObjects, epoch int64) *snapshotBenchReader {
		reader, err := loadSnapshotBenchReader(ctx, pool, objects, epoch)
		if err != nil {
			t.Fatal(err)
		}
		return reader
	}
	expect := func(t *testing.T, reader *snapshotBenchReader, objects replicationBenchObjects, ids ...int64) {
		called := false
		count, err := reader.fetch(ctx, pool, objects, 128, nil, func(epoch int64, entries []sequenceBenchEntry) error {
			called = true
			if len(entries) != len(ids) {
				t.Fatalf("epoch %d: got %d entries, want %d", epoch, len(entries), len(ids))
			}
			for i, id := range ids {
				if entries[i].id == nil || *entries[i].id != id {
					t.Fatalf("epoch %d entry %d: got %+v, want batch %d", epoch, i, entries[i], id)
				}
			}
			return nil
		}, nil)
		if err != nil || count != len(ids) || called != (len(ids) > 0) {
			t.Fatalf("fetch: count=%d publish=%v err=%v", count, called, err)
		}
	}

	t.Run("UncommittedAndAbortedGaps", func(t *testing.T) {
		objects := newObjects(t)
		reader := load(t, objects, 0)
		unrelated := begin(t)
		if _, err := unrelated.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}
		slow := begin(t)
		if position := appendBatch(t, slow, objects, 1); position != 1 {
			t.Fatal(position)
		}
		aborted := begin(t)
		appendBatch(t, aborted, objects, 2)
		if err := aborted.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		fast := begin(t)
		appendBatch(t, fast, objects, 3)
		commit(t, fast)
		expect(t, reader, objects, 3) // No wait or filler for positions 1 and 2.
		expect(t, reader, objects)    // Empty cuts retain the active exceptions.
		expect(t, reader, objects)
		commit(t, slow)
		expect(t, reader, objects, 1) // Older position becomes visible in epoch 2.
		expect(t, reader, objects)
		var rows int
		if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+objects.table).Scan(&rows); err != nil || rows != 2 {
			t.Fatalf("source rows=%d err=%v", rows, err)
		}
	})

	t.Run("AllocatedBeforeInsert", func(t *testing.T) {
		objects := newObjects(t)
		reader := load(t, objects, 0)
		slow := begin(t)
		if err := writeJournalBenchSource(ctx, slow, objects, replicationBenchWorkload{rowsPerTx: 1}, "value", 1); err != nil {
			t.Fatal(err)
		}
		var position int64
		if err := slow.QueryRow(ctx, "SELECT nextval('"+objects.sequence+"')").Scan(&position); err != nil {
			t.Fatal(err)
		}
		fast := begin(t)
		appendBatch(t, fast, objects, 2)
		commit(t, fast)
		expect(t, reader, objects, 2)
		if _, err := slow.Exec(ctx, "INSERT INTO "+objects.journal+" (position,batch_id,payload) VALUES ($1,1,$2)", position, []byte("value")); err != nil {
			t.Fatal(err)
		}
		commit(t, slow)
		expect(t, reader, objects, 1)
	})

	t.Run("StablePagesAndReplayAfterPublicationFailure", func(t *testing.T) {
		objects := newObjects(t)
		reader := load(t, objects, 0)
		ids := make([]int64, 260)
		for i := range ids {
			ids[i] = int64(i + 1)
			tx := begin(t)
			appendBatch(t, tx, objects, ids[i])
			commit(t, tx)
		}
		late := begin(t)
		appendBatch(t, late, objects, 261)
		committedBetweenPages := false
		publicationFailure := errors.New("crash after manifest commit, before application")
		_, err := reader.fetch(ctx, pool, objects, 128, nil, func(_ int64, entries []sequenceBenchEntry) error {
			if len(entries) != 260 {
				t.Fatalf("unstable snapshot: %d entries", len(entries))
			}
			return publicationFailure
		}, func(count int) error {
			if count == 128 && !committedBetweenPages {
				commit(t, late)
				newWriter := begin(t)
				appendBatch(t, newWriter, objects, 262)
				commit(t, newWriter)
				committedBetweenPages = true
			}
			return nil
		})
		if !errors.Is(err, publicationFailure) || reader.epoch != 0 || !committedBetweenPages {
			t.Fatalf("publication failure: epoch=%d page hook=%v err=%v", reader.epoch, committedBetweenPages, err)
		}
		// Replay must use the stored cut, excluding 261 and 262 even though
		// both are now physically visible to the new SQL transaction.
		reader = load(t, objects, 0)
		expect(t, reader, objects, ids...)
		expect(t, reader, objects, 261, 262)
		reader = load(t, objects, 1)
		expect(t, reader, objects, 261, 262)
		expect(t, reader, objects)
	})

	t.Run("KeyOrderDespiteInvertedXIDs", func(t *testing.T) {
		objects := newObjects(t)
		reader := load(t, objects, 0)
		olderXID := begin(t)
		var xidA, xidB string
		if err := olderXID.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidA); err != nil {
			t.Fatal(err)
		}
		newerXID := begin(t)
		workload := replicationBenchWorkload{rowsPerTx: 1, sourceKeys: 1}
		write := func(tx pgx.Tx, id int64, payload string) {
			if err := writeJournalBenchSource(ctx, tx, objects, workload, payload, id); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+objects.journal+" (batch_id,payload) VALUES ($1,$2)", id, []byte(payload)); err != nil {
				t.Fatal(err)
			}
		}
		write(newerXID, 1, "first")
		if err := newerXID.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidB); err != nil {
			t.Fatal(err)
		}
		commit(t, newerXID)
		write(olderXID, 2, "second")
		commit(t, olderXID)
		var inverted bool
		if err := pool.QueryRow(ctx, "SELECT $1::text::xid8 < $2::text::xid8", xidA, xidB).Scan(&inverted); err != nil || !inverted {
			t.Fatalf("expected inverted XID/mutation order: %s %s err=%v", xidA, xidB, err)
		}
		expect(t, reader, objects, 1, 2) // Sequence order, not XID order.
		var latest string
		if err := pool.QueryRow(ctx, "SELECT payload FROM "+objects.table).Scan(&latest); err != nil || latest != "second" {
			t.Fatalf("latest state=%q err=%v", latest, err)
		}
	})
}
