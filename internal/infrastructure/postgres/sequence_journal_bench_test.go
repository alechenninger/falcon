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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sequenceBenchGapGrace          = 100 * time.Millisecond
	sequenceBenchRepairLockTimeout = 10 * time.Millisecond
)

type sequenceBenchFault int

const (
	sequenceBenchNoFault sequenceBenchFault = iota
	sequenceBenchRollback
	sequenceBenchBeforeInsertDelay
	sequenceBenchAfterInsertDelay
)

func BenchmarkSequenceJournal(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSequencePoll, replicationSequencePipelined, replicationJournalPipelined, replicationLogicalSlot} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, workload, mode, replicationBenchRunOptions{writerCount: 16, fetchLimit: 128})
				})
			}
		})
	}
}

func BenchmarkSequenceJournalAtRate(b *testing.B) {
	for _, rate := range []int{1000, 3000} {
		b.Run(fmt.Sprintf("At%d", rate), func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSequencePipelined, replicationJournalPipelined, replicationLogicalSlot} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256}, mode,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: rate})
				})
			}
		})
	}
}

// Retain a latest-state table, upserting without a preceding SELECT or version
// condition. Only the journal has history. Hot keys still serialize naturally.
func BenchmarkSequenceJournalKeys(b *testing.B) {
	for _, keys := range []int{256, 1} {
		b.Run(fmt.Sprintf("Keys%d", keys), func(b *testing.B) {
			for _, mode := range []replicationBenchMode{replicationSequencePipelined, replicationJournalPipelined} {
				b.Run(mode.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sourceKeys: keys}, mode,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128})
				})
			}
		})
	}
}

func BenchmarkSequenceJournalFaults(b *testing.B) {
	for _, fault := range []struct {
		name  string
		value sequenceBenchFault
	}{
		{"Rollback", sequenceBenchRollback}, {"BeforeInsert200ms", sequenceBenchBeforeInsertDelay}, {"AfterInsert200ms", sequenceBenchAfterInsertDelay},
	} {
		b.Run(fault.name, func(b *testing.B) {
			runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sequenceFault: fault.value}, replicationSequencePoll,
				replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: 1000})
		})
	}
}

func prepareSequenceBenchSchema(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
  CREATE SEQUENCE %s AS BIGINT CACHE 1 NO CYCLE;
  CREATE TABLE %s (
   position BIGINT PRIMARY KEY DEFAULT nextval('%s'),
   batch_id BIGINT,
   payload BYTEA,
   CHECK ((batch_id IS NULL) = (payload IS NULL))
  );
  ALTER SEQUENCE %s OWNED BY %s.position;
  ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;
 `, objects.sequence, objects.journal, objects.sequence, objects.sequence, objects.journal, objects.journal))
	return err
}

func writeJournalBenchSource(ctx context.Context, tx pgx.Tx, objects replicationBenchObjects, workload replicationBenchWorkload, payload string, id int64) error {
	query := fmt.Sprintf(`INSERT INTO %s (batch_id, ordinal, payload)
  SELECT $1, ordinal, $3 FROM generate_series(1, $2) AS ordinal`, objects.table)
	key := id
	if workload.sourceKeys > 0 {
		key = (id-1)%int64(workload.sourceKeys) + 1
		query += " ON CONFLICT (batch_id, ordinal) DO UPDATE SET payload = EXCLUDED.payload"
	}
	_, err := tx.Exec(ctx, query, key, workload.rowsPerTx, payload)
	return err
}

func sequenceBenchAppendQuery(objects replicationBenchObjects) string {
	return "INSERT INTO " + objects.journal + " (batch_id, payload) VALUES ($1, $2) RETURNING position"
}

func writeSequenceBenchBatch(ctx context.Context, pool *pgxpool.Pool, tracker *replicationBenchTracker, mode replicationBenchMode,
	workload replicationBenchWorkload, payload string, id int64, objects replicationBenchObjects) error {
	// Keep operation-start timing outside retries. Replay the entire source+log
	// transaction after a repaired-position conflict; never commit source alone.
	batchPayload := []byte(strings.Repeat(payload, workload.rowsPerTx))
	injected := false
	for attempt := 0; attempt < 100; attempt++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		appendDuration, commitStarted, err := func() (time.Duration, time.Time, error) {
			tx, err := conn.Begin(ctx)
			if err != nil {
				return 0, time.Time{}, err
			}
			defer func() {
				if tx.Conn().PgConn().TxStatus() != 'I' {
					tx.Rollback(context.Background())
				}
			}()
			if err := writeJournalBenchSource(ctx, tx, objects, workload, payload, id); err != nil {
				return 0, time.Time{}, err
			}
			query := sequenceBenchAppendQuery(objects)
			args := []any{id, batchPayload}
			fault := sequenceBenchNoFault
			if !injected && id%1000 == 0 {
				fault = workload.sequenceFault
				injected = true
			}
			if fault == sequenceBenchBeforeInsertDelay {
				// A process pause after nextval but before its unique index entry exists.
				var position int64
				if err := tx.QueryRow(ctx, "SELECT nextval('"+objects.sequence+"')").Scan(&position); err != nil {
					return 0, time.Time{}, err
				}
				if err := pauseSequenceBenchWriter(ctx, 200*time.Millisecond); err != nil {
					return 0, time.Time{}, err
				}
				query = "INSERT INTO " + objects.journal + " (position, batch_id, payload) VALUES ($3, $1, $2) RETURNING position"
				args = append(args, position)
			}
			started := time.Now()
			var position int64
			if (mode == replicationSequencePipelined || mode == replicationSnapshotPipelined) && fault == sequenceBenchNoFault {
				batch := &pgx.Batch{}
				batch.Queue(query, args...)
				batch.Queue("COMMIT")
				results := tx.SendBatch(ctx, batch)
				err := results.QueryRow().Scan(&position)
				elapsed := time.Since(started)
				if err == nil {
					_, err = results.Exec()
				}
				closeErr := results.Close()
				if err == nil {
					err = closeErr
				}
				return elapsed, started, err // Commit timing includes append, as in counter pipeline.
			}
			if err := tx.QueryRow(ctx, query, args...).Scan(&position); err != nil {
				return 0, time.Time{}, err
			}
			elapsed := time.Since(started)
			if fault == sequenceBenchRollback {
				if err := tx.Rollback(ctx); err != nil {
					return 0, time.Time{}, err
				}
				tracker.mu.Lock()
				tracker.measurements.injectedRollbacks++
				tracker.mu.Unlock()
				return 0, time.Time{}, errSequenceBenchInjectedRollback
			}
			if fault == sequenceBenchAfterInsertDelay {
				if err := pauseSequenceBenchWriter(ctx, 200*time.Millisecond); err != nil {
					return 0, time.Time{}, err
				}
			}
			commitStarted := time.Now()
			return elapsed, commitStarted, tx.Commit(ctx)
		}()
		finished := time.Now()
		conn.Release()
		if err == nil {
			tracker.recordJournalAppend(appendDuration)
			tracker.recordCommit(id, commitStarted, finished)
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.Is(err, errSequenceBenchInjectedRollback) {
			continue
		}
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == objects.journal+"_pkey" {
			tracker.mu.Lock()
			tracker.measurements.sequenceRetries++
			tracker.mu.Unlock()
			continue
		}
		return fmt.Errorf("sequence batch %d: %w", id, err)
	}
	return fmt.Errorf("sequence batch %d exhausted retries", id)
}

var errSequenceBenchInjectedRollback = errors.New("injected sequence rollback")

func pauseSequenceBenchWriter(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type sequenceBenchReader struct {
	cursor   int64
	gap      int64
	gapSince time.Time
	grace    time.Duration
}

type sequenceBenchEntry struct {
	position int64
	id       *int64
	payload  []byte
}

func (r *sequenceBenchReader) fetch(ctx context.Context, pool *pgxpool.Pool, tracker *replicationBenchTracker,
	objects replicationBenchObjects, limit int, workload replicationBenchWorkload, expectedChecksum uint32) (int, bool, error) {
	rows, err := pool.Query(ctx, "SELECT position, batch_id, payload FROM "+objects.journal+" WHERE position > $1 ORDER BY position LIMIT $2", r.cursor, limit)
	if err != nil {
		return 0, false, err
	}
	// Release the query before repair. A repair can wait on an invisible unique
	// index entry, so it uses a bounded lock wait rather than blocking indefinitely.
	entries := make([]sequenceBenchEntry, 0, limit)
	for rows.Next() {
		var entry sequenceBenchEntry
		if err := rows.Scan(&entry.position, &entry.id, &entry.payload); err != nil {
			rows.Close()
			return 0, false, err
		}
		entries = append(entries, entry)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	if len(entries) == 0 {
		tracker.recordEmptyFetch()
		return 0, false, nil
	}
	tracker.recordFetch(len(entries))
	count := 0
	for _, entry := range entries {
		if entry.position != r.cursor+1 {
			next := r.cursor + 1
			if r.gap != next {
				r.gap = next
				r.gapSince = time.Now()
				tracker.mu.Lock()
				tracker.measurements.gapObservations++
				tracker.mu.Unlock()
			}
			if time.Since(r.gapSince) < r.grace {
				return count, false, nil
			}
			filled, timedOut, err := repairSequenceBenchGap(ctx, pool, objects, next)
			if err != nil {
				return count, false, err
			}
			tracker.mu.Lock()
			if filled {
				tracker.measurements.gapFills++
			}
			if timedOut {
				tracker.measurements.repairLockTimeouts++
			}
			tracker.mu.Unlock()
			return count, !timedOut, nil // Refetch; a conflict may mean the real row committed.
		}
		r.gap = 0
		if entry.id != nil {
			if len(entry.payload) != workload.rowsPerTx*workload.payloadSize {
				return count, false, fmt.Errorf("sequence payload length at %d", entry.position)
			}
			var checksum uint64
			for offset := 0; offset < len(entry.payload); offset += workload.payloadSize {
				checksum += uint64(crc32.ChecksumIEEE(entry.payload[offset : offset+workload.payloadSize]))
			}
			if err := tracker.recordApply(*entry.id, workload.rowsPerTx, checksum, workload.rowsPerTx, expectedChecksum, time.Now()); err != nil {
				return count, false, err
			}
		}
		r.cursor = entry.position // Includes permanent sentinels, never unresolved gaps.
		count++
	}
	return count, len(entries) == limit, nil
}

func repairSequenceBenchGap(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects, position int64) (filled, timedOut bool, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", sequenceBenchRepairLockTimeout.String()); err != nil {
		return false, false, err
	}
	tag, err := tx.Exec(ctx, "INSERT INTO "+objects.journal+" (position, batch_id, payload) VALUES ($1, NULL, NULL) ON CONFLICT (position) DO NOTHING", position)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, false, err
	}
	return tag.RowsAffected() == 1, false, nil
}

func startSequenceBenchConsumer(ctx context.Context, pool *pgxpool.Pool, tracker *replicationBenchTracker, workload replicationBenchWorkload,
	checksum uint32, limit int, objects replicationBenchObjects, ready chan<- struct{}, workers *sync.WaitGroup, reportError func(error)) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		reader := sequenceBenchReader{grace: sequenceBenchGapGrace}
		ticker := time.NewTicker(journalBenchPollInterval)
		defer ticker.Stop()
		first := true
		for {
			_, immediate, err := reader.fetch(ctx, pool, tracker, objects, limit, workload, checksum)
			if err != nil {
				if ctx.Err() == nil {
					reportError(err)
				}
				return
			}
			if first {
				close(ready)
				first = false
			}
			if immediate {
				continue
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func TestSequenceJournalProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	workload := replicationBenchWorkload{rowsPerTx: 1, payloadSize: 32}
	payload := strings.Repeat("x", 32)
	checksum := crc32.ChecksumIEEE([]byte(payload))
	newObjects := func(t *testing.T) replicationBenchObjects {
		objects := newReplicationBenchObjects()
		if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
			t.Fatal(err)
		}
		if err := prepareSequenceBenchSchema(ctx, pool, objects); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false); err != nil {
				t.Error(err)
			}
		})
		return objects
	}
	appendRow := func(t *testing.T, tx pgx.Tx, objects replicationBenchObjects, id int64) int64 {
		var position int64
		if err := tx.QueryRow(ctx, sequenceBenchAppendQuery(objects), id, []byte(payload)).Scan(&position); err != nil {
			t.Fatal(err)
		}
		return position
	}
	t.Run("RollbackAndResume", func(t *testing.T) {
		objects := newObjects(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if p := appendRow(t, tx, objects, 99); p != 1 {
			t.Fatal(p)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		tracker := newReplicationBenchTracker(260)
		for id := int64(1); id <= 260; id++ {
			if err := writeReplicationBenchBatch(ctx, pool, tracker, replicationSequencePipelined, workload, payload, id, time.Time{}, objects); err != nil {
				t.Fatal(err)
			}
		}
		reader := sequenceBenchReader{grace: time.Hour}
		if n, _, err := reader.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil || n != 0 || reader.cursor != 0 {
			t.Fatalf("passed unresolved gap: n=%d cursor=%d err=%v", n, reader.cursor, err)
		}
		reader.grace = 0
		for reader.cursor < 261 {
			if _, _, err := reader.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil {
				t.Fatal(err)
			}
		}
		if samples := tracker.samples(); samples.gapFills != 1 || len(samples.operationToApply) != 260 {
			t.Fatalf("wrong fill/application counts: fills=%d applied=%d", samples.gapFills, len(samples.operationToApply))
		}
		// A reader can resume from the sealed prefix, including a fill sentinel.
		restarted := sequenceBenchReader{cursor: reader.cursor}
		if n, _, err := restarted.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil || n != 0 {
			t.Fatalf("restart n=%d err=%v", n, err)
		}
	})
	t.Run("RepairWinsBeforeIndexInsert", func(t *testing.T) {
		objects := newObjects(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := writeJournalBenchSource(ctx, tx, objects, workload, payload, 1); err != nil {
			t.Fatal(err)
		}
		var position int64
		if err := tx.QueryRow(ctx, "SELECT nextval('"+objects.sequence+"')").Scan(&position); err != nil {
			t.Fatal(err)
		}
		filled, timedOut, err := repairSequenceBenchGap(ctx, pool, objects, position)
		if err != nil || !filled || timedOut {
			t.Fatalf("repair: filled=%v timeout=%v err=%v", filled, timedOut, err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+objects.journal+" VALUES ($1, $2, $3)", position, 1, []byte(payload))
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("delayed writer not rejected: %v", err)
		}
		if err := tx.Commit(ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
			t.Fatalf("source survived failed append: %v", err)
		}
		var count int
		if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+objects.table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rolled back source: count=%d err=%v", count, err)
		}
		// Retrying replays both writes under a new position.
		tracker := newReplicationBenchTracker(1)
		if err := writeReplicationBenchBatch(ctx, pool, tracker, replicationSequencePoll, workload, payload, 1, time.Time{}, objects); err != nil {
			t.Fatal(err)
		}
		reader := sequenceBenchReader{}
		if n, _, err := reader.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil || n != 2 || reader.cursor != 2 {
			t.Fatalf("retry apply n=%d cursor=%d err=%v", n, reader.cursor, err)
		}
	})
	t.Run("ExistingIndexInsertCannotBeFenced", func(t *testing.T) {
		objects := newObjects(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		position := appendRow(t, tx, objects, 1)
		tracker := newReplicationBenchTracker(2)
		tracker.recordOperationStart(1, time.Now(), time.Time{})
		if err := writeReplicationBenchBatch(ctx, pool, tracker, replicationSequencePoll, workload, payload, 2, time.Time{}, objects); err != nil {
			t.Fatal(err)
		}
		reader := sequenceBenchReader{grace: time.Hour}
		if n, _, err := reader.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil || n != 0 || reader.cursor != 0 {
			t.Fatalf("out-of-order visible row advanced cursor: n=%d err=%v", n, err)
		}
		filled, timedOut, err := repairSequenceBenchGap(ctx, pool, objects, position)
		if err != nil || filled || !timedOut {
			t.Fatalf("existing insert incorrectly fenced: filled=%v timeout=%v err=%v", filled, timedOut, err)
		}
		started := time.Now()
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		tracker.recordCommit(1, started, time.Now())
		if n, _, err := reader.fetch(ctx, pool, tracker, objects, 128, workload, checksum); err != nil || n != 2 || reader.cursor != 2 {
			t.Fatalf("ordered catch-up n=%d cursor=%d err=%v", n, reader.cursor, err)
		}
	})
}

func TestSequenceJournalKeyOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, operation := range []string{"Upsert", "DeleteReinsert"} {
		t.Run(operation, func(t *testing.T) {
			objects := newReplicationBenchObjects()
			if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			if err := prepareSequenceBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false); err != nil {
					t.Error(err)
				}
			}()
			if _, err := pool.Exec(ctx, "INSERT INTO "+objects.table+" VALUES (1,1,'initial')"); err != nil {
				t.Fatal(err)
			}
			first, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Rollback(ctx)
			if operation == "Upsert" {
				if _, err := first.Exec(ctx, "UPDATE "+objects.table+" SET payload='first' WHERE batch_id=1"); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := first.Exec(ctx, "DELETE FROM "+objects.table+" WHERE batch_id=1"); err != nil {
					t.Fatal(err)
				}
			}
			second, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Rollback(ctx)
			pid := second.Conn().PgConn().PID()
			type result struct {
				position int64
				err      error
			}
			done := make(chan result, 1)
			go func() {
				// No SELECT of current state or optimistic previous_version condition.
				_, err := second.Exec(ctx, "INSERT INTO "+objects.table+" VALUES (1,1,'second') ON CONFLICT (batch_id,ordinal) DO UPDATE SET payload=EXCLUDED.payload")
				var p int64
				if err == nil {
					err = second.QueryRow(ctx, sequenceBenchAppendQuery(objects), 2, []byte("second")).Scan(&p)
				}
				if err == nil {
					err = second.Commit(ctx)
				}
				done <- result{p, err}
			}()
			// Observe the actual database lock wait before allocating the first
			// revision, rather than using an assumed goroutine execution delay.
			deadline := time.Now().Add(5 * time.Second)
			for {
				var waiting bool
				if err := pool.QueryRow(ctx, "SELECT COALESCE(wait_event_type = 'Lock', false) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("second writer did not block on source mutation")
				}
				if err := pauseSequenceBenchWriter(ctx, time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			var p1 int64
			if err := first.QueryRow(ctx, sequenceBenchAppendQuery(objects), 1, []byte("first")).Scan(&p1); err != nil {
				t.Fatal(err)
			}
			if err := first.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-done:
				if result.err != nil || p1 != 1 || result.position != 2 {
					t.Fatalf("key order: first=%d second=%d err=%v", p1, result.position, result.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var value string
			if err := pool.QueryRow(ctx, "SELECT payload FROM "+objects.table+" WHERE batch_id=1").Scan(&value); err != nil || value != "second" {
				t.Fatalf("latest state=%q err=%v", value, err)
			}
		})
	}
}
