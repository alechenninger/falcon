package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshotBenchPollMode int

const (
	snapshotBenchTicker10 snapshotBenchPollMode = iota
	snapshotBenchIdle100
)

func (m snapshotBenchPollMode) String() string {
	return [...]string{"Ticker10ms", "Idle100ms"}[m]
}

const snapshotBenchIdleInterval = 100 * time.Millisecond

type snapshotBenchPoller struct {
	c      <-chan time.Time
	ticker *time.Ticker
	timer  *time.Timer
}

func newSnapshotBenchPoller(mode snapshotBenchPollMode) *snapshotBenchPoller {
	if mode == snapshotBenchTicker10 {
		ticker := time.NewTicker(journalBenchPollInterval)
		return &snapshotBenchPoller{c: ticker.C, ticker: ticker}
	}
	timer := time.NewTimer(snapshotBenchIdleInterval)
	timer.Stop() // Arm only after the first retrieval completes.
	return &snapshotBenchPoller{c: timer.C, timer: timer}
}

func (p *snapshotBenchPoller) beforeScan() {
	if p.timer != nil {
		// No idle timeout can accumulate during retrieval or publication.
		p.timer.Stop()
	}
}

func (p *snapshotBenchPoller) afterScan() {
	if p.timer != nil {
		// Every completed retrieval counts, including notification-triggered,
		// empty, and immediate catch-up scans. Go 1.23+ discards stale timer values.
		p.timer.Reset(snapshotBenchIdleInterval)
	}
}

func (p *snapshotBenchPoller) stop() {
	if p.timer != nil {
		p.timer.Stop()
	} else {
		p.ticker.Stop()
	}
}

type snapshotBenchIdleVariant struct {
	name  string
	poll  snapshotBenchPollMode
	delay time.Duration
}

var snapshotBenchIdleVariants = []snapshotBenchIdleVariant{
	{"Ticker10ms", snapshotBenchTicker10, 0},
	{"Idle100ms", snapshotBenchIdle100, 0},
	{"Idle100msBatch2ms", snapshotBenchIdle100, 2 * time.Millisecond},
}

func BenchmarkSnapshotIdleTimer(b *testing.B) {
	for _, workload := range []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	} {
		b.Run(workload.name, func(b *testing.B) {
			for _, variant := range snapshotBenchIdleVariants {
				b.Run(variant.name, func(b *testing.B) {
					runReplicationTransportBenchmark(b, workload, replicationSnapshotPipelined,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, snapshotWakeMode: snapshotBenchNotifyWake, snapshotPollMode: variant.poll, snapshotNotifyDelay: variant.delay})
				})
			}
		})
	}
}

func BenchmarkSnapshotIdleTimerAtRate(b *testing.B) {
	for _, rate := range []int{1000, 3000} {
		b.Run(fmt.Sprintf("At%d", rate), func(b *testing.B) {
			for _, variant := range snapshotBenchIdleVariants {
				b.Run(variant.name, func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256}, replicationSnapshotPipelined,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: rate,
							snapshotWakeMode: snapshotBenchNotifyWake, snapshotPollMode: variant.poll, snapshotNotifyDelay: variant.delay})
				})
			}
		})
	}
}

func BenchmarkSnapshotIdleTimerFaults(b *testing.B) {
	for _, fault := range []struct {
		name  string
		value sequenceBenchFault
	}{
		{"Rollback", sequenceBenchRollback}, {"BeforeInsert200ms", sequenceBenchBeforeInsertDelay}, {"AfterInsert200ms", sequenceBenchAfterInsertDelay},
	} {
		b.Run(fault.name, func(b *testing.B) {
			for _, variant := range snapshotBenchIdleVariants {
				b.Run(variant.name, func(b *testing.B) {
					runReplicationTransportBenchmark(b, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 256, sequenceFault: fault.value}, replicationSnapshotPoll,
						replicationBenchRunOptions{writerCount: 16, fetchLimit: 128, offeredWritesPerSecond: 1000,
							snapshotWakeMode: snapshotBenchNotifyWake, snapshotPollMode: variant.poll, snapshotNotifyDelay: variant.delay})
				})
			}
		})
	}
}

// One benchmark operation observes one second without source writes. Both paths
// keep the same live NOTIFY bridge; only their consumer countdown differs.
func BenchmarkSnapshotIdlePolling(b *testing.B) {
	for _, poll := range []snapshotBenchPollMode{snapshotBenchTicker10, snapshotBenchIdle100} {
		b.Run(poll.String(), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			connString, cleanup, _ := setupReplicationBenchPostgres(context.Background(), b)
			defer cleanup()
			pool, err := pgxpool.New(ctx, connString)
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()
			objects := newReplicationBenchObjects()
			defer cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, false)
			if err := prepareReplicationBenchSchema(ctx, pool, objects); err != nil {
				b.Fatal(err)
			}
			if err := prepareSnapshotBenchSchema(ctx, pool, objects); err != nil {
				b.Fatal(err)
			}
			var workers sync.WaitGroup
			defer func() { cancel(); workers.Wait() }()
			errs := make(chan error, 4)
			report := func(err error) {
				if ctx.Err() == nil {
					errs <- err
					cancel()
				}
			}
			wake, err := startSnapshotBenchWakeups(ctx, connString, objects.channel, snapshotBenchNotifyWake, 0, &workers, report)
			if err != nil {
				b.Fatal(err)
			}
			tracker := newReplicationBenchTracker(0)
			tracker.snapshotWake, tracker.snapshotPollMode = wake, poll
			ready := make(chan struct{})
			startSnapshotBenchConsumer(ctx, pool, tracker, replicationBenchWorkload{rowsPerTx: 1, payloadSize: 1}, 0,
				128, objects, ready, &workers, report)
			if err := waitForReplicationBenchReady(ctx, ready, errs); err != nil {
				b.Fatal(err)
			}
			initialScans := wake.scans.Load()
			b.ResetTimer()
			started := time.Now()
			for range b.N {
				timer := time.NewTimer(time.Second)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					b.Fatal(ctx.Err())
				}
			}
			elapsed := time.Since(started)
			b.StopTimer()
			cancel()
			workers.Wait()
			if err := takeReplicationBenchError(errs); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(wake.scans.Load()-initialScans)/elapsed.Seconds(), "idle-scans/s")
			b.ReportMetric(float64(wake.polls.Load()), "poll-wakes")
		})
	}
}

func TestPostgresCleanupAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connString, cleanup := setupPostgres(ctx, t)
	cancel()
	cleanup()
	// Reconnecting uses an independent context, so cancellation alone cannot
	// make this assertion pass while the old database remains alive.
	checkCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	conn, err := pgx.Connect(checkCtx, connString)
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("PostgreSQL remained reachable after cleanup with a canceled workload")
	}
}
