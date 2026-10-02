package postgres_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshotBenchWakeMode int

const (
	snapshotBenchPoll snapshotBenchWakeMode = iota
	snapshotBenchLocalWake
	snapshotBenchNotifyWake
)

func (m snapshotBenchWakeMode) String() string {
	return [...]string{"Poll", "LocalWake", "NotifyWake"}[m]
}

// One pending hint covers all commits preceding its consumption. The default
// sender has no batching delay; optional batching runs outside the write path.
type snapshotBenchWakeups struct {
	pending                           chan struct{}
	ready                             chan struct{}
	attempts, enqueued, coalesced     atomic.Int64
	sent, received, deliveryCoalesced atomic.Int64
	consumed, polls, scans            atomic.Int64
	batchMerges                       atomic.Int64
}

func (w *snapshotBenchWakeups) signalCommit() {
	w.attempts.Add(1)
	select {
	case w.pending <- struct{}{}:
		w.enqueued.Add(1)
	default:
		w.coalesced.Add(1)
	}
}

func (w *snapshotBenchWakeups) report(b *testing.B) {
	for unit, counter := range map[string]*atomic.Int64{
		"wake-attempts": &w.attempts, "wake-enqueued": &w.enqueued,
		"wake-coalesced": &w.coalesced, "notifies-sent": &w.sent,
		"notifies-received": &w.received, "delivery-coalesced": &w.deliveryCoalesced,
		"wakes-consumed": &w.consumed, "poll-wakes": &w.polls,
		"snapshot-scans":      &w.scans,
		"notify-batch-merges": &w.batchMerges,
	} {
		b.ReportMetric(float64(counter.Load()), unit)
	}
}

func startSnapshotBenchWakeups(ctx context.Context, connString, channel string, mode snapshotBenchWakeMode, batchDelay time.Duration,
	workers *sync.WaitGroup, reportError func(error)) (*snapshotBenchWakeups, error) {
	if mode == snapshotBenchPoll {
		return nil, nil
	}
	w := &snapshotBenchWakeups{pending: make(chan struct{}, 1)}
	w.ready = w.pending
	if mode == snapshotBenchLocalWake {
		return w, nil
	}
	w.ready = make(chan struct{}, 1)
	listener, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}
	// LISTEN must commit before the consumer's first journal scan. That scan
	// covers commits that precede listener registration.
	if _, err := listener.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		listener.Close(context.Background())
		return nil, err
	}
	sender, err := pgx.Connect(ctx, connString)
	if err != nil {
		listener.Close(context.Background())
		return nil, err
	}
	workers.Add(2)
	go func() {
		defer workers.Done()
		defer listener.Close(context.Background())
		for {
			if _, err := listener.WaitForNotification(ctx); err != nil {
				if ctx.Err() == nil {
					reportError(fmt.Errorf("snapshot LISTEN: %w", err))
				}
				return
			}
			w.received.Add(1)
			select {
			case w.ready <- struct{}{}:
			default:
				w.deliveryCoalesced.Add(1)
			}
		}
	}()
	go func() {
		defer workers.Done()
		defer sender.Close(context.Background())
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.pending:
				if batchDelay > 0 {
					// A bounded window starts at the first hint; new commits do not
					// extend it indefinitely under continuous load.
					timer := time.NewTimer(batchDelay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
					select {
					case <-w.pending:
						w.batchMerges.Add(1)
					default:
					}
				}
				// Dequeue BEFORE sending: commits during this round trip enqueue
				// another hint, including commits later than the notify transaction.
				if _, err := sender.Exec(ctx, "SELECT pg_notify($1, '')", channel); err != nil {
					if ctx.Err() == nil {
						reportError(fmt.Errorf("snapshot NOTIFY: %w", err))
					}
					return
				}
				w.sent.Add(1)
			}
		}
	}()
	return w, nil
}

func BenchmarkSnapshotWakeups(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			for _, wake := range []snapshotBenchWakeMode{snapshotBenchPoll, snapshotBenchLocalWake, snapshotBenchNotifyWake} {
				b.Run(wake.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, workload, replicationSnapshotPipelined,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, snapshotWakeMode: wake})
				})
			}
		})
	}
}

func BenchmarkSnapshotWakeupsAtRate(b *testing.B) {
	for _, rate := range []int{1000, 3000} {
		b.Run(fmt.Sprintf("At%d", rate), func(b *testing.B) {
			for _, wake := range []snapshotBenchWakeMode{snapshotBenchPoll, snapshotBenchLocalWake, snapshotBenchNotifyWake} {
				b.Run(wake.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256}, replicationSnapshotPipelined,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: rate, snapshotWakeMode: wake})
				})
			}
		})
	}
}

func BenchmarkSnapshotWakeupsFaults(b *testing.B) {
	for _, fault := range []struct {
		name  string
		value sequenceBenchFault
	}{
		{"Rollback", sequenceBenchRollback}, {"BeforeInsert200ms", sequenceBenchBeforeInsertDelay}, {"AfterInsert200ms", sequenceBenchAfterInsertDelay},
	} {
		b.Run(fault.name, func(b *testing.B) {
			for _, wake := range []snapshotBenchWakeMode{snapshotBenchPoll, snapshotBenchLocalWake, snapshotBenchNotifyWake} {
				b.Run(wake.String(), func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sequenceFault: fault.value}, replicationSnapshotPoll,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: 1000, snapshotWakeMode: wake})
				})
			}
		})
	}
}

var snapshotBenchWakeTestCases = []struct {
	name  string
	mode  snapshotBenchWakeMode
	delay time.Duration
}{
	{"LocalWake", snapshotBenchLocalWake, 0},
	{"NotifyWake", snapshotBenchNotifyWake, 0},
	{"NotifyBatch2ms", snapshotBenchNotifyWake, 2 * time.Millisecond},
}

func TestSnapshotWakeups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, variant := range snapshotBenchWakeTestCases {
		t.Run(variant.name, func(t *testing.T) {
			objects := newReplicationBenchObjects()
			if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			if err := prepareSnapshotBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			defer cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false)
			runCtx, stop := context.WithCancel(ctx)
			var workers sync.WaitGroup
			defer func() { stop(); workers.Wait() }()
			errs := make(chan error, 8)
			report := func(err error) { errs <- err }
			wake, err := startSnapshotBenchWakeups(runCtx, connString, objects.channel, variant.mode, variant.delay, &workers, report)
			if err != nil {
				t.Fatal(err)
			}
			tracker := newReplicationBenchTracker(3)
			tracker.snapshotWake = wake
			tracker.snapshotPollMode = snapshotBenchIdle100
			workload := replicationBenchWorkload{rowsPerTx: 1, payloadSize: 3, sequenceFault: sequenceBenchRollback}
			// An injected rollback retries internally. Only the successful commit
			// may wake the consumer, and its row must already be visible.
			if err := writeSequenceBenchBatch(runCtx, pool, tracker, replicationSnapshotPoll, workload, "abc", 1000, objects); err != nil {
				t.Fatal(err)
			}
			select {
			case <-wake.ready:
			case err := <-errs:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if wake.attempts.Load() != 1 {
				t.Fatalf("rollback notified: attempts=%d", wake.attempts.Load())
			}
			var committed int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+objects.journal).Scan(&committed); err != nil {
				t.Fatal(err)
			}
			if committed != 1 {
				t.Fatalf("notified before commit: rows=%d", committed)
			}
			// Deliberately swallow that hint. Initial scanning plus later fallback
			// polling must still apply every committed row, even without a bridge.
			ready := make(chan struct{})
			startSnapshotBenchConsumer(runCtx, pool, tracker, workload, crc32.ChecksumIEEE([]byte("abc")), 128, objects, ready, &workers, report)
			select {
			case <-ready:
			case err := <-errs:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			noWake := newReplicationBenchTracker(0)
			for _, id := range []int64{1001, 1002} {
				if err := writeSequenceBenchBatch(runCtx, pool, noWake, replicationSnapshotPoll, workload, "abc", id, objects); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-tracker.done:
			case err := <-errs:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if wake.polls.Load() == 0 {
				t.Fatal("missing hints were not recovered by polling")
			}
		})
	}
}

func TestSnapshotWakeupDuringScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, variant := range snapshotBenchWakeTestCases {
		t.Run(variant.name, func(t *testing.T) {
			objects := newReplicationBenchObjects()
			if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			if err := prepareSnapshotBenchSchema(ctx, pool, objects); err != nil {
				t.Fatal(err)
			}
			defer cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false)
			runCtx, stop := context.WithCancel(ctx)
			var workers sync.WaitGroup
			defer func() { stop(); workers.Wait() }()
			errs := make(chan error, 4)
			wake, err := startSnapshotBenchWakeups(runCtx, connString, objects.channel, variant.mode, variant.delay, &workers, func(err error) { errs <- err })
			if err != nil {
				t.Fatal(err)
			}
			awaitHint := func() {
				select {
				case <-wake.ready:
				case err := <-errs:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			tracker := newReplicationBenchTracker(2)
			tracker.snapshotWake = wake
			workload := replicationBenchWorkload{rowsPerTx: 1, payloadSize: 3}
			if err := writeSequenceBenchBatch(runCtx, pool, tracker, replicationSnapshotPoll, workload, "abc", 1, objects); err != nil {
				t.Fatal(err)
			}
			awaitHint() // Consume BEFORE snapshot capture.
			reader, err := loadSnapshotBenchReader(ctx, pool, objects, 0)
			if err != nil {
				t.Fatal(err)
			}
			var applied []int64
			publish := func(_ int64, entries []sequenceBenchEntry) error {
				for _, entry := range entries {
					applied = append(applied, *entry.id)
				}
				return nil
			}
			wrote := false
			count, err := reader.fetch(ctx, pool, objects, 1, nil, publish, func(_ int) error {
				if wrote {
					return nil
				}
				wrote = true
				// This commit is too late for the captured snapshot. Its hint must survive
				// the rest of the scan and wake the next scan, with no poll required.
				return writeSequenceBenchBatch(runCtx, pool, tracker, replicationSnapshotPoll, workload, "abc", 2, objects)
			})
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 || len(applied) != 1 || applied[0] != 1 {
				t.Fatalf("unstable first cut: %v", applied)
			}
			awaitHint()
			count, err = reader.fetch(ctx, pool, objects, 1, nil, publish, nil)
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 || len(applied) != 2 || applied[1] != 2 {
				t.Fatalf("lost commit during scan: %v", applied)
			}
		})
	}
}
