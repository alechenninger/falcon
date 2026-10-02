package postgres_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const journalBenchPollInterval = 10 * time.Millisecond

// Pipeline the commit-time append and COMMIT to remove one client round trip
// from the serialized portion, while leaving source writes outside it.
func BenchmarkChangeLogPipelined(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			runReplicationTransportBenchmark(b, workload, replicationJournalPipelined,
				replicationBenchRunOptions{writerCount: 16, fetchLimit: 128})
		})
	}
}

// Isolate commit serialization from journal storage and cursor-fetch costs.
// The source table is still replicated through pgoutput; only the counter is added.
func BenchmarkChangeLogCounterControl(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			runReplicationTransportBenchmark(b, workload, replicationCounterLogicalSlot,
				replicationBenchRunOptions{writerCount: 16})
		})
	}
}

// Unlike the notification-ID transport benchmark, these runs include the extra
// durable write and commit serialization required by an application change log.
func BenchmarkChangeLogTransport(b *testing.B) {
	for _, writers := range []int{16, 64} {
		b.Run(fmt.Sprintf("Writers%d", writers), func(b *testing.B) {
			runChangeLogBenchScenarios(b, writers, 0)
		})
	}
}

func BenchmarkChangeLogTransportAtRate(b *testing.B) {
	for _, rate := range []int{1000, 5000, 10000} {
		b.Run(fmt.Sprintf("Writers16At%dPerSec", rate), func(b *testing.B) {
			runChangeLogBenchScenarios(b, 16, rate)
		})
	}
}

func runChangeLogBenchScenarios(b *testing.B, writers, rate int) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			for _, mode := range []replicationBenchMode{
				replicationJournalPoll,
				replicationJournalNotifyInTx,
				replicationJournalNotifyAfterCommit,
				replicationJournalPipelined,
				replicationLogicalSlot,
			} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, workload, mode, replicationBenchRunOptions{
						writerCount:            writers,
						fetchLimit:             replicationNotifyBatchLimit,
						offeredWritesPerSecond: rate,
					})
				})
			}
		})
	}
}

func prepareJournalBenchSchema(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			position BIGINT NOT NULL
		) WITH (fillfactor = 70);
		INSERT INTO %s VALUES (1, 0);
		CREATE TABLE %s (
			position BIGINT PRIMARY KEY,
			batch_id BIGINT NOT NULL,
			payload BYTEA NOT NULL
		);
		-- Every entry repeats one random per-row payload for comparable transport
		-- checksums. Disable TOAST compression so Batch16 still transfers 16 KiB.
		ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;
	`, objects.clock, objects.clock, objects.journal, objects.journal))
	if err != nil {
		return fmt.Errorf("create journal and counter: %w", err)
	}
	return nil
}

func appendJournalBenchBatch(ctx context.Context, tx pgx.Tx, tracker *replicationBenchTracker,
	objects replicationBenchObjects, id int64, rowsPerTx int, payload string, notify bool) error {
	// Build the batch before taking the counter lock. One SQL statement avoids
	// an extra round trip between allocating a position and appending the log.
	batchPayload := []byte(strings.Repeat(payload, rowsPerTx))
	var position int64
	query := journalBenchAppendQuery(objects)
	args := []any{id, batchPayload}
	targets := []any{&position}
	if notify {
		// Notification delivery is transactional; execute it in the append's
		// RETURNING clause to avoid another round trip while holding the lock.
		query += ", pg_notify($3, position::text)"
		args = append(args, objects.channel)
		targets = append(targets, nil)
	}
	started := time.Now()
	err := tx.QueryRow(ctx, query, args...).Scan(targets...)
	tracker.recordJournalAppend(time.Since(started))
	if err != nil {
		return fmt.Errorf("append journal batch %d: %w", id, err)
	}
	return nil
}

func journalBenchAppendQuery(objects replicationBenchObjects) string {
	return fmt.Sprintf(`
		WITH next AS (
			UPDATE %s SET position = position + 1 WHERE id = 1
			RETURNING position
		)
		INSERT INTO %s (position, batch_id, payload)
		SELECT position, $1, $2 FROM next
		RETURNING position
	`, objects.clock, objects.journal)
}

func commitPipelinedJournalBenchBatch(ctx context.Context, tx pgx.Tx, tracker *replicationBenchTracker,
	objects replicationBenchObjects, id int64, rowsPerTx int, payload string) error {
	batch := &pgx.Batch{}
	batch.Queue(journalBenchAppendQuery(objects), id, []byte(strings.Repeat(payload, rowsPerTx)))
	batch.Queue("COMMIT")
	started := time.Now()
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	var position int64
	err := results.QueryRow().Scan(&position)
	tracker.recordJournalAppend(time.Since(started))
	if err != nil {
		return fmt.Errorf("pipelined append batch %d: %w", id, err)
	}
	if _, err := results.Exec(); err != nil {
		return fmt.Errorf("pipelined commit batch %d: %w", id, err)
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("finish commit pipeline: %w", err)
	}
	// For this variant commit duration covers the combined append/commit
	// pipeline. Write and operation-to-apply durations are comparable across modes.
	tracker.recordCommit(id, started, time.Now())
	return nil
}

func startJournalBenchConsumer(ctx context.Context, connString string, pool *pgxpool.Pool,
	tracker *replicationBenchTracker, workload replicationBenchWorkload, expectedChecksum uint32,
	mode replicationBenchMode, fetchLimit int, objects replicationBenchObjects, ready chan<- struct{},
	workers *sync.WaitGroup, reportError func(error)) error {
	wakeups := make(chan struct{}, 1)
	if mode == replicationJournalNotifyInTx || mode == replicationJournalNotifyAfterCommit {
		listener, err := pgx.Connect(ctx, connString)
		if err != nil {
			return fmt.Errorf("connect journal notification listener: %w", err)
		}
		// LISTEN is committed before the initial cursor query. Notifications carry
		// no cursor semantics; coalescing or losing them cannot skip log entries.
		if _, err := listener.Exec(ctx, "LISTEN "+objects.channel); err != nil {
			listener.Close(context.Background())
			return fmt.Errorf("listen for journal changes: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer listener.Close(context.Background())
			for {
				if _, err := listener.WaitForNotification(ctx); err != nil {
					if ctx.Err() == nil {
						reportError(fmt.Errorf("wait for journal notification: %w", err))
					}
					return
				}
				select {
				case wakeups <- struct{}{}:
				default:
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(journalBenchPollInterval)
		defer ticker.Stop()
		var cursor int64
		firstFetch := true
		for {
			next, count, err := fetchJournalBenchBatches(ctx, pool, tracker, objects.journal,
				cursor, fetchLimit, workload, expectedChecksum)
			if err != nil {
				if ctx.Err() == nil {
					reportError(err)
				}
				return
			}
			cursor = next
			if firstFetch {
				close(ready)
				firstFetch = false
			}
			if count == fetchLimit {
				continue // Catch up without a polling delay between pages.
			}
			select {
			case <-wakeups:
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func fetchJournalBenchBatches(ctx context.Context, pool *pgxpool.Pool, tracker *replicationBenchTracker,
	table string, cursor int64, limit int, workload replicationBenchWorkload,
	expectedChecksum uint32) (int64, int, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT position, batch_id, payload FROM %s
		WHERE position > $1 ORDER BY position LIMIT $2
	`, table), cursor, limit)
	if err != nil {
		return cursor, 0, fmt.Errorf("fetch journal after %d: %w", cursor, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var position, id int64
		var payload []byte
		if err := rows.Scan(&position, &id, &payload); err != nil {
			return cursor, count, fmt.Errorf("scan journal: %w", err)
		}
		if position != cursor+1 {
			return cursor, count, fmt.Errorf("journal gap or reorder: got %d after %d", position, cursor)
		}
		if len(payload) != workload.rowsPerTx*workload.payloadSize {
			return cursor, count, fmt.Errorf("batch %d journal payload has %d bytes, want %d",
				id, len(payload), workload.rowsPerTx*workload.payloadSize)
		}
		var checksumSum uint64
		for offset := 0; offset < len(payload); offset += workload.payloadSize {
			checksumSum += uint64(crc32.ChecksumIEEE(payload[offset : offset+workload.payloadSize]))
		}
		if err := tracker.recordApply(id, workload.rowsPerTx, checksumSum, workload.rowsPerTx,
			expectedChecksum, time.Now()); err != nil {
			return cursor, count, err
		}
		cursor = position // Advance only after validating the entire transaction.
		count++
	}
	if err := rows.Err(); err != nil {
		return cursor, count, fmt.Errorf("iterate journal: %w", err)
	}
	if count == 0 {
		tracker.recordEmptyFetch()
	} else {
		tracker.recordFetch(count)
	}
	return cursor, count, nil
}

func TestJournalBenchCursorRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	objects := newReplicationBenchObjects()
	if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
		t.Fatal(err)
	}
	if err := prepareJournalBenchSchema(ctx, pool, objects); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false); err != nil {
			t.Error(err)
		}
	}()

	workload := replicationBenchWorkload{rowsPerTx: 16, payloadSize: 32}
	payload := strings.Repeat("x", workload.payloadSize)
	const count = 260 // Cross two 128-transaction fetch boundaries.
	tracker := newReplicationBenchTracker(count)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := appendJournalBenchBatch(ctx, tx, tracker, objects, 999, workload.rowsPerTx, payload, false); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var position int64
	if err := pool.QueryRow(ctx, "SELECT position FROM "+objects.clock).Scan(&position); err != nil || position != 0 {
		t.Fatalf("rolled-back counter: position=%d err=%v", position, err)
	}

	// No notifications are sent. Batch IDs intentionally decrease while log
	// positions increase, so a request-ID cursor cannot pass this test.
	var nextID atomic.Int64
	var writers sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				index := nextID.Add(1)
				if index > count {
					return
				}
				if err := writeReplicationBenchBatch(ctx, pool, tracker, replicationJournalPoll,
					workload, payload, count+1-index, time.Time{}, objects); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	writers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	checksum := crc32.ChecksumIEEE([]byte(payload))
	var cursor int64
	for _, want := range []int{128, 128, 4, 0} {
		next, fetched, err := fetchJournalBenchBatches(ctx, pool, tracker, objects.journal,
			cursor, 128, workload, checksum)
		if err != nil || fetched != want || next != cursor+int64(want) {
			t.Fatalf("resume after %d: next=%d fetched=%d want=%d err=%v", cursor, next, fetched, want, err)
		}
		cursor = next
	}
	if len(tracker.samples().operationToApply) != count {
		t.Fatal("cursor recovery did not apply every committed batch")
	}
	pipelineTracker := newReplicationBenchTracker(1)
	if err := writeReplicationBenchBatch(ctx, pool, pipelineTracker, replicationJournalPipelined,
		workload, payload, count+1, time.Time{}, objects); err != nil {
		t.Fatal(err)
	}
	next, fetched, err := fetchJournalBenchBatches(ctx, pool, pipelineTracker, objects.journal,
		cursor, 128, workload, checksum)
	if err != nil || fetched != 1 || next != count+1 {
		t.Fatalf("pipelined transaction: next=%d fetched=%d err=%v", next, fetched, err)
	}
	cursor = next
	if len(pipelineTracker.samples().writeDurations) != 1 {
		t.Fatal("pipelined transaction was not fully committed and applied")
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+objects.journal+" VALUES ($1, $2, $3)",
		count+3, count+3, []byte(strings.Repeat(payload, workload.rowsPerTx))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fetchJournalBenchBatches(ctx, pool, tracker, objects.journal, cursor, 128, workload, checksum); err == nil {
		t.Fatal("cursor fetch accepted a missing position")
	}
}
