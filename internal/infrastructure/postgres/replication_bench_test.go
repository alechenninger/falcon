package postgres_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
)

const (
	replicationBenchWriters     = 16
	replicationNotifyBatchLimit = 128
)

var replicationBenchRunSequence atomic.Uint64

type replicationBenchObjects struct {
	table       string
	clock       string
	journal     string
	sequence    string
	channel     string
	publication string
	slot        string
}

func newReplicationBenchObjects() replicationBenchObjects {
	suffix := fmt.Sprintf("%x_%x", time.Now().UnixNano(), replicationBenchRunSequence.Add(1))
	return replicationBenchObjects{
		table:       "replication_bench_events_" + suffix,
		clock:       "replication_bench_clock_" + suffix,
		journal:     "replication_bench_journal_" + suffix,
		sequence:    "replication_bench_seq_" + suffix,
		channel:     "falcon_replication_bench_" + suffix,
		publication: "replication_bench_pub_" + suffix,
		slot:        "replication_bench_slot_" + suffix,
	}
}

func setupReplicationBenchPostgres(ctx context.Context, b *testing.B) (string, func(), bool) {
	if connString := os.Getenv("FALCON_REPLICATION_BENCH_DATABASE_URL"); connString != "" {
		return connString, func() {}, true
	}
	connString, cleanup := setupPostgres(ctx, b,
		testcontainers.WithCmdArgs(
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=20",
			"-c", "max_wal_senders=20",
			"-c", "shared_buffers=1GB",
			"-c", "effective_cache_size=3GB",
			"-c", "work_mem=4MB",
			"-c", "max_wal_size=4GB",
			// The Testcontainers postgres module disables fsync by default.
			// Explicitly restore durability for commit-contention measurements.
			"-c", "fsync=on",
			"-c", "synchronous_commit=on",
			"-c", "full_page_writes=on",
		),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Resources.Memory = 4 << 30
		}),
	)
	return connString, cleanup, false
}

type replicationBenchWorkload struct {
	name          string
	rowsPerTx     int
	payloadSize   int
	sourceKeys    int // Zero appends distinct rows; positive values upsert a bounded key set.
	sequenceFault sequenceBenchFault
}

type replicationBenchRunOptions struct {
	writerCount            int
	fetchLimit             int
	offeredWritesPerSecond int
	snapshotWakeMode       snapshotBenchWakeMode
	snapshotPollMode       snapshotBenchPollMode
	snapshotNotifyDelay    time.Duration
}

type replicationBenchMode int

const (
	replicationNotifyInTx replicationBenchMode = iota
	replicationNotifyAfterCommit
	replicationLogicalSlot
	replicationJournalPoll
	replicationJournalNotifyInTx
	replicationJournalNotifyAfterCommit
	replicationCounterLogicalSlot
	replicationJournalPipelined
	replicationSequencePoll
	replicationSequencePipelined
	replicationSnapshotPoll
	replicationSnapshotPipelined
)

func (m replicationBenchMode) String() string {
	switch m {
	case replicationNotifyInTx:
		return "NotifyInTransaction"
	case replicationNotifyAfterCommit:
		return "NotifyAfterCommit"
	case replicationJournalPoll:
		return "JournalPoll"
	case replicationJournalNotifyInTx:
		return "JournalNotifyInTransaction"
	case replicationJournalNotifyAfterCommit:
		return "JournalNotifyAfterCommit"
	case replicationCounterLogicalSlot:
		return "CounterLogicalSlot"
	case replicationJournalPipelined:
		return "JournalPollPipelined"
	case replicationSequencePoll:
		return "SequencePoll"
	case replicationSequencePipelined:
		return "SequencePollPipelined"
	case replicationSnapshotPoll:
		return "SnapshotPoll"
	case replicationSnapshotPipelined:
		return "SnapshotPollPipelined"
	default:
		return "LogicalSlot"
	}
}

func (m replicationBenchMode) usesJournal() bool {
	return m == replicationJournalPoll || m == replicationJournalNotifyInTx || m == replicationJournalNotifyAfterCommit || m == replicationJournalPipelined || m.usesSequence() || m.usesSnapshot()
}

func (m replicationBenchMode) usesCounter() bool {
	return (m.usesJournal() && !m.usesSequence() && !m.usesSnapshot()) || m == replicationCounterLogicalSlot
}

func (m replicationBenchMode) usesSnapshot() bool {
	return m == replicationSnapshotPoll || m == replicationSnapshotPipelined
}

func (m replicationBenchMode) usesSequence() bool {
	return m == replicationSequencePoll || m == replicationSequencePipelined
}

func (m replicationBenchMode) usesLogicalSlot() bool {
	return m == replicationLogicalSlot || m == replicationCounterLogicalSlot
}

func BenchmarkReplicationTransport(b *testing.B) {
	workloads := []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	}
	modes := []replicationBenchMode{
		replicationNotifyInTx,
		replicationNotifyAfterCommit,
		replicationLogicalSlot,
	}

	for _, workload := range workloads {
		workload := workload
		b.Run(workload.name, func(b *testing.B) {
			for _, mode := range modes {
				mode := mode
				fetchLimits := []int{0}
				if mode != replicationLogicalSlot {
					fetchLimits = []int{1, replicationNotifyBatchLimit}
				}
				for _, fetchLimit := range fetchLimits {
					fetchLimit := fetchLimit
					name := mode.String()
					if fetchLimit > 0 {
						name += fmt.Sprintf("/Fetch%d", fetchLimit)
					}
					b.Run(name, func(b *testing.B) {
						runReplicationTransportBenchmark(b, workload, mode, replicationBenchRunOptions{
							writerCount: replicationBenchWriters,
							fetchLimit:  fetchLimit,
						})
					})
				}
			}
		})
	}
}

func BenchmarkReplicationTransportAtRate(b *testing.B) {
	modes := []replicationBenchMode{
		replicationNotifyInTx,
		replicationNotifyAfterCommit,
		replicationLogicalSlot,
	}
	workloads := []replicationBenchWorkload{
		{name: "SingleRow", rowsPerTx: 1, payloadSize: 256},
		{name: "Batch16", rowsPerTx: 16, payloadSize: 1024},
	}

	runScenario := func(parent *testing.B, name string, workload replicationBenchWorkload, writerCount, offeredRate int) {
		parent.Run(name, func(b *testing.B) {
			for _, mode := range modes {
				mode := mode
				fetchLimits := []int{0}
				if mode != replicationLogicalSlot {
					fetchLimits = []int{1, replicationNotifyBatchLimit}
				}
				for _, fetchLimit := range fetchLimits {
					fetchLimit := fetchLimit
					name := mode.String()
					if fetchLimit > 0 {
						name += fmt.Sprintf("/Fetch%d", fetchLimit)
					}
					b.Run(name, func(b *testing.B) {
						runReplicationTransportBenchmark(b, workload, mode, replicationBenchRunOptions{
							writerCount:            writerCount,
							fetchLimit:             fetchLimit,
							offeredWritesPerSecond: offeredRate,
						})
					})
				}
			}
		})
	}

	runScenario(b, "SingleWriterAt100PerSec", workloads[0], 1, 100)
	for _, workload := range workloads {
		workload := workload
		runScenario(b, "SixteenWritersAt1000PerSec/"+workload.name,
			workload, replicationBenchWriters, 1000)
	}
}

func runReplicationTransportBenchmark(
	b *testing.B,
	workload replicationBenchWorkload,
	mode replicationBenchMode,
	options replicationBenchRunOptions,
) {
	setupCtx := context.Background()
	connString, cleanup, externalDatabase := setupReplicationBenchPostgres(setupCtx, b)
	defer cleanup()
	objects := newReplicationBenchObjects()

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		b.Fatalf("parse PostgreSQL config: %v", err)
	}
	poolConfig.MaxConns = int32(options.writerCount + 4)
	pool, err := pgxpool.NewWithConfig(setupCtx, poolConfig)
	if err != nil {
		b.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	defer func() {
		if err := cleanupReplicationBenchObjects(context.Background(), pool, connString, objects, mode.usesLogicalSlot()); err != nil {
			b.Logf("failed to clean up replication benchmark objects: %v", err)
		}
	}()

	if err := prepareReplicationBenchSchema(setupCtx, pool, objects); err != nil {
		b.Fatal(err)
	}
	if mode.usesCounter() {
		if err := prepareJournalBenchSchema(setupCtx, pool, objects); err != nil {
			b.Fatal(err)
		}
	}

	if mode.usesSequence() {
		if err := prepareSequenceBenchSchema(setupCtx, pool, objects); err != nil {
			b.Fatal(err)
		}
	}
	if mode.usesSnapshot() {
		if err := prepareSnapshotBenchSchema(setupCtx, pool, objects); err != nil {
			b.Fatal(err)
		}
	}

	var sharedBuffers, effectiveCacheSize, workMem, maxWALSize, version, fsync, synchronousCommit string
	if err := pool.QueryRow(setupCtx, `
		SELECT current_setting('shared_buffers'),
		       current_setting('effective_cache_size'),
		       current_setting('work_mem'),
		       current_setting('max_wal_size'), version(),
		       current_setting('fsync'), current_setting('synchronous_commit')
	`).Scan(&sharedBuffers, &effectiveCacheSize, &workMem, &maxWALSize, &version, &fsync, &synchronousCommit); err != nil {
		b.Fatalf("read PostgreSQL memory settings: %v", err)
	}
	memoryProfile := "container=4GiB"
	if externalDatabase {
		memoryProfile = "external database"
	}
	b.Logf("PostgreSQL memory profile: %s shared_buffers=%s effective_cache_size=%s work_mem=%s max_wal_size=%s; writers=%d, notification fetch limit=%d, offered writes/sec=%d, rows/transaction=%d, payload/row=%d bytes",
		memoryProfile, sharedBuffers, effectiveCacheSize, workMem, maxWALSize,
		options.writerCount, options.fetchLimit, options.offeredWritesPerSecond,
		workload.rowsPerTx, workload.payloadSize)
	b.Logf("%s; fsync=%s synchronous_commit=%s", version, fsync, synchronousCommit)
	if mode.usesCounter() {
		b.Logf("durable journal: one binary entry/transaction, counter UPDATE + log INSERT in one statement, ordered cursor fetch, idle poll interval=%s", journalBenchPollInterval)
	}
	if mode.usesSequence() {
		b.Logf("sequence journal: CACHE 1, gap grace=%s, repair lock timeout=%s, idle poll=%s, source keys=%d, fault=%d", sequenceBenchGapGrace, sequenceBenchRepairLockTimeout, journalBenchPollInterval, workload.sourceKeys, workload.sequenceFault)
	}
	if mode.usesSnapshot() {
		b.Logf("snapshot journal: explicit xid8 + index, repeatable-read pages, durable epoch manifests, idle poll=%s, source keys=%d, fault=%d", journalBenchPollInterval, workload.sourceKeys, workload.sequenceFault)
	}
	if mode == replicationJournalPipelined || mode == replicationSequencePipelined || mode == replicationSnapshotPipelined {
		b.Log("append and COMMIT share a protocol pipeline; commit timing covers the combined pipeline")
	}

	payloadBytes := make([]byte, workload.payloadSize)
	rng := rand.New(rand.NewSource(42))
	const payloadAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for index := range payloadBytes {
		payloadBytes[index] = payloadAlphabet[rng.Intn(len(payloadAlphabet))]
	}
	payload := string(payloadBytes)
	expectedPayloadChecksum := crc32.ChecksumIEEE(payloadBytes)

	runCtx, cancel := context.WithCancel(context.Background())
	var consumerWG sync.WaitGroup
	defer func() {
		cancel()
		consumerWG.Wait()
	}()

	consumerErrors := make(chan error, 1)
	reportConsumerError := func(err error) {
		if err == nil || runCtx.Err() != nil {
			return
		}
		select {
		case consumerErrors <- err:
		default:
		}
		cancel()
	}

	tracker := newReplicationBenchTracker(b.N)
	tracker.snapshotPollMode = options.snapshotPollMode
	if mode.usesSnapshot() {
		tracker.snapshotWake, err = startSnapshotBenchWakeups(runCtx, connString, objects.channel, options.snapshotWakeMode, options.snapshotNotifyDelay, &consumerWG, reportConsumerError)
		if err != nil {
			b.Fatalf("start snapshot wakeups: %v", err)
		}
		b.Logf("snapshot wakeup policy=%s; polling=%s; notification batch wait=%s", options.snapshotWakeMode, options.snapshotPollMode, options.snapshotNotifyDelay)
	}
	consumerReady := make(chan struct{})
	switch mode {
	case replicationNotifyInTx, replicationNotifyAfterCommit:
		if err := startNotifyBenchConsumer(runCtx, connString, pool, tracker, workload.rowsPerTx,
			expectedPayloadChecksum, options.fetchLimit, objects.table, objects.channel, consumerReady,
			&consumerWG, reportConsumerError); err != nil {
			b.Fatalf("start notification consumer: %v", err)
		}
	case replicationLogicalSlot, replicationCounterLogicalSlot:
		if err := startLogicalSlotBenchConsumer(setupCtx, runCtx, connString, tracker,
			workload.rowsPerTx, expectedPayloadChecksum, objects, consumerReady,
			&consumerWG, reportConsumerError); err != nil {
			b.Fatalf("start logical slot consumer: %v", err)
		}
	case replicationSequencePoll, replicationSequencePipelined:
		startSequenceBenchConsumer(runCtx, pool, tracker, workload, expectedPayloadChecksum, options.fetchLimit, objects, consumerReady, &consumerWG, reportConsumerError)
	case replicationSnapshotPoll, replicationSnapshotPipelined:
		startSnapshotBenchConsumer(runCtx, pool, tracker, workload, expectedPayloadChecksum, options.fetchLimit, objects, consumerReady, &consumerWG, reportConsumerError)
	case replicationJournalPoll, replicationJournalNotifyInTx, replicationJournalNotifyAfterCommit, replicationJournalPipelined:
		if err := startJournalBenchConsumer(runCtx, connString, pool, tracker, workload,
			expectedPayloadChecksum, mode, options.fetchLimit, objects, consumerReady,
			&consumerWG, reportConsumerError); err != nil {
			b.Fatalf("start journal consumer: %v", err)
		}
	}
	if err := waitForReplicationBenchReady(runCtx, consumerReady, consumerErrors); err != nil {
		b.Fatal(err)
	}
	if err := warmReplicationBenchConnections(runCtx, pool, objects.table, options.writerCount); err != nil {
		b.Fatalf("warm benchmark connections and query plans: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	writeStarted := time.Now()
	completed, writeElapsed, writeErr := runReplicationBenchWriters(
		runCtx, cancel, b.N, mode, workload, payload, pool, tracker,
		objects, options.writerCount, options.offeredWritesPerSecond,
	)

	if err := takeReplicationBenchError(consumerErrors); err != nil {
		b.Fatalf("replication consumer: %v", err)
	}
	if writeErr != nil {
		b.Fatalf("write workload: %v", writeErr)
	}
	if completed != b.N {
		b.Fatalf("completed %d writes, want %d", completed, b.N)
	}
	if err := waitForReplicationBenchDrain(runCtx, tracker, consumerErrors); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if workload.sourceKeys > 0 {
		var sourceRows int
		if err := pool.QueryRow(setupCtx, "SELECT COUNT(*) FROM "+objects.table).Scan(&sourceRows); err != nil {
			b.Fatal(err)
		}
		want := min(b.N, workload.sourceKeys) * workload.rowsPerTx
		if sourceRows != want {
			b.Fatalf("latest-state rows=%d, want %d", sourceRows, want)
		}
	}
	applyFinished := tracker.finishedAt()
	cancel()
	consumerWG.Wait()
	// The final transaction closes the drain channel before the consumer finishes
	// its epoch accounting. Join it before collecting the complete metrics.
	samples := tracker.samples()
	if err := takeReplicationBenchError(consumerErrors); err != nil {
		b.Fatalf("replication consumer shutdown: %v", err)
	}
	if len(samples.commitToApply) != b.N || len(samples.operationToApply) != b.N ||
		len(samples.remainingCatchUp) != b.N || len(samples.commitDurations) != b.N {
		b.Fatalf("recorded %d commit-to-apply, %d operation-to-apply, %d remaining-catch-up, and %d commit durations, want %d each",
			len(samples.commitToApply), len(samples.operationToApply),
			len(samples.remainingCatchUp), len(samples.commitDurations), b.N)
	}
	if options.offeredWritesPerSecond > 0 && (len(samples.scheduleLags) != b.N || len(samples.arrivalToApply) != b.N) {
		b.Fatalf("recorded %d scheduling delays, want %d", len(samples.scheduleLags), b.N)
	}
	if (mode.usesJournal() || mode.usesCounter()) && len(samples.journalAppendDurations) != b.N {
		b.Fatalf("recorded %d journal append durations, want %d", len(samples.journalAppendDurations), b.N)
	}

	applyElapsed := applyFinished.Sub(writeStarted)
	if writeElapsed <= 0 || applyElapsed <= 0 {
		b.Fatalf("invalid elapsed times: writes=%s apply=%s", writeElapsed, applyElapsed)
	}
	sort.Slice(samples.arrivalToApply, func(i, j int) bool { return samples.arrivalToApply[i] < samples.arrivalToApply[j] })
	sort.Slice(samples.operationToApply, func(i, j int) bool { return samples.operationToApply[i] < samples.operationToApply[j] })
	sort.Slice(samples.commitToApply, func(i, j int) bool { return samples.commitToApply[i] < samples.commitToApply[j] })
	sort.Slice(samples.remainingCatchUp, func(i, j int) bool { return samples.remainingCatchUp[i] < samples.remainingCatchUp[j] })
	sort.Slice(samples.commitDurations, func(i, j int) bool { return samples.commitDurations[i] < samples.commitDurations[j] })
	sort.Slice(samples.scheduleLags, func(i, j int) bool { return samples.scheduleLags[i] < samples.scheduleLags[j] })
	sort.Slice(samples.writeDurations, func(i, j int) bool { return samples.writeDurations[i] < samples.writeDurations[j] })
	sort.Slice(samples.journalAppendDurations, func(i, j int) bool { return samples.journalAppendDurations[i] < samples.journalAppendDurations[j] })
	sort.Ints(samples.transactionsPerFetch)

	sourceBytes := float64(completed) * float64(workload.rowsPerTx) * float64(workload.payloadSize)
	b.ReportMetric(float64(completed)/writeElapsed.Seconds(), "writes/s")
	b.ReportMetric(float64(completed)/applyElapsed.Seconds(), "applied-batches/s")
	b.ReportMetric(sourceBytes/(writeElapsed.Seconds()*1024*1024), "written-MiB/s")
	b.ReportMetric(sourceBytes/(applyElapsed.Seconds()*1024*1024), "applied-MiB/s")
	b.ReportMetric(float64(options.writerCount), "writers")
	if options.offeredWritesPerSecond > 0 {
		b.ReportMetric(float64(options.offeredWritesPerSecond), "offered-writes/s")
		b.ReportMetric(float64(replicationBenchPercentile(samples.arrivalToApply, 50))/float64(time.Microsecond), "p50-arrival-to-apply-us")
		b.ReportMetric(float64(replicationBenchPercentile(samples.arrivalToApply, 99))/float64(time.Microsecond), "p99-arrival-to-apply-us")
		b.ReportMetric(float64(samples.arrivalToApply[len(samples.arrivalToApply)-1])/float64(time.Microsecond), "max-arrival-to-apply-us")
		over50ms := len(samples.arrivalToApply) - sort.Search(len(samples.arrivalToApply), func(i int) bool { return samples.arrivalToApply[i] > 50*time.Millisecond })
		b.ReportMetric(float64(over50ms)/float64(b.N)*100, "over-50ms-arrival-pct")
		b.ReportMetric(float64(replicationBenchPercentile(samples.scheduleLags, 50))/float64(time.Microsecond), "p50-schedule-lag-us")
		b.ReportMetric(float64(replicationBenchPercentile(samples.scheduleLags, 99))/float64(time.Microsecond), "p99-schedule-lag-us")
	}
	b.ReportMetric(float64(replicationBenchPercentile(samples.operationToApply, 50))/float64(time.Microsecond), "p50-op-to-apply-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.operationToApply, 99))/float64(time.Microsecond), "p99-op-to-apply-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.commitToApply, 50))/float64(time.Microsecond), "p50-commit-to-apply-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.commitToApply, 99))/float64(time.Microsecond), "p99-commit-to-apply-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.remainingCatchUp, 50))/float64(time.Microsecond), "p50-remaining-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.remainingCatchUp, 99))/float64(time.Microsecond), "p99-remaining-us")
	b.ReportMetric(float64(samples.appliedBeforeCommit)/float64(b.N)*100, "applied-before-commit-pct")
	b.ReportMetric(float64(replicationBenchPercentile(samples.commitDurations, 50))/float64(time.Microsecond), "p50-commit-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.commitDurations, 99))/float64(time.Microsecond), "p99-commit-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.writeDurations, 50))/float64(time.Microsecond), "p50-write-us")
	b.ReportMetric(float64(replicationBenchPercentile(samples.writeDurations, 99))/float64(time.Microsecond), "p99-write-us")
	if mode.usesJournal() || mode.usesCounter() {
		b.ReportMetric(float64(replicationBenchPercentile(samples.journalAppendDurations, 50))/float64(time.Microsecond), "p50-journal-append-us")
		b.ReportMetric(float64(replicationBenchPercentile(samples.journalAppendDurations, 99))/float64(time.Microsecond), "p99-journal-append-us")
	}
	if mode.usesSnapshot() {
		b.ReportMetric(float64(samples.snapshotEpochs), "snapshot-epochs")
		b.ReportMetric(float64(samples.snapshotReplays), "snapshot-replays")
		b.ReportMetric(float64(samples.snapshotExceptionMax), "max-snapshot-exceptions")
		b.ReportMetric(float64(b.N)/float64(samples.snapshotEpochs), "avg-tx-per-epoch")
		if wake := tracker.snapshotWake; wake != nil {
			wake.report(b)
		}
	}
	if mode.usesSequence() || mode.usesSnapshot() {
		b.ReportMetric(float64(samples.gapObservations), "gap-observations")
		b.ReportMetric(float64(samples.gapFills), "gap-fills")
		b.ReportMetric(float64(samples.repairLockTimeouts), "repair-lock-timeouts")
		b.ReportMetric(float64(samples.sequenceRetries), "sequence-retries")
		b.ReportMetric(float64(samples.injectedRollbacks), "injected-rollbacks")
	}
	if mode.usesJournal() {
		b.ReportMetric(float64(samples.emptyFetches), "empty-fetches")
		b.ReportMetric(float64(options.fetchLimit), "fetch-limit")
	}
	if len(samples.transactionsPerFetch) > 0 {
		var totalFetched int
		for _, count := range samples.transactionsPerFetch {
			totalFetched += count
		}
		b.ReportMetric(float64(totalFetched)/float64(len(samples.transactionsPerFetch)), "avg-tx-per-fetch")
		b.ReportMetric(float64(replicationBenchIntPercentile(samples.transactionsPerFetch, 99)), "p99-tx-per-fetch")
	}
}

func prepareReplicationBenchSchema(ctx context.Context, pool *pgxpool.Pool, objects replicationBenchObjects) error {
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			batch_id BIGINT NOT NULL,
			ordinal INTEGER NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (batch_id, ordinal)
		)
	`, objects.table)); err != nil {
		return fmt.Errorf("create replication event table: %w", err)
	}
	if _, err := pool.Exec(ctx, "CREATE PUBLICATION "+objects.publication+" FOR TABLE "+objects.table); err != nil {
		return fmt.Errorf("create replication publication: %w", err)
	}
	return nil
}

func cleanupReplicationBenchObjects(
	ctx context.Context,
	pool *pgxpool.Pool,
	connString string,
	objects replicationBenchObjects,
	dropSlot bool,
) error {
	if dropSlot {
		var slotExists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_replication_slots WHERE slot_name = $1
			)
		`, objects.slot).Scan(&slotExists); err != nil {
			return fmt.Errorf("check benchmark slot before cleanup: %w", err)
		}
		if slotExists {
			conn, err := pgconn.Connect(ctx, replicationBenchConnString(connString))
			if err != nil {
				return fmt.Errorf("connect to drop benchmark slot: %w", err)
			}
			err = pglogrepl.DropReplicationSlot(ctx, conn, objects.slot,
				pglogrepl.DropReplicationSlotOptions{Wait: true})
			closeErr := conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("drop benchmark slot: %w", err)
			}
			if closeErr != nil {
				return fmt.Errorf("close slot cleanup connection: %w", closeErr)
			}
		}
	}
	if _, err := pool.Exec(ctx, "DROP PUBLICATION IF EXISTS "+objects.publication); err != nil {
		return fmt.Errorf("drop benchmark publication: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+objects.table); err != nil {
		return fmt.Errorf("drop benchmark table: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+objects.journal+", "+objects.clock); err != nil {
		return fmt.Errorf("drop benchmark journal and counter: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP SEQUENCE IF EXISTS "+objects.sequence); err != nil {
		return fmt.Errorf("drop benchmark sequence: %w", err)
	}
	return nil
}

func waitForReplicationBenchReady(ctx context.Context, ready <-chan struct{}, errs <-chan error) error {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ready:
		return nil
	case err := <-errs:
		return fmt.Errorf("replication consumer failed during startup: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for replication consumer startup")
	}
}

func warmReplicationBenchConnections(ctx context.Context, pool *pgxpool.Pool, table string, connectionCount int) error {
	query := fmt.Sprintf(`
		SELECT batch_id, payload
		FROM %s
		WHERE batch_id = ANY($1)
		ORDER BY batch_id, ordinal
	`, table)
	var workers sync.WaitGroup
	errs := make(chan error, connectionCount)
	for range connectionCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				errs <- fmt.Errorf("acquire warmup connection: %w", err)
				return
			}
			defer conn.Release()
			rows, err := conn.Query(ctx, query, []int64{})
			if err != nil {
				errs <- fmt.Errorf("warm fetch query: %w", err)
				return
			}
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				errs <- fmt.Errorf("finish warm fetch query: %w", err)
			}
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

type replicationBenchTiming struct {
	operationStarted time.Time
	scheduledAt      time.Time
	commitStarted    time.Time
	commitFinished   time.Time
	applyFinished    time.Time
	hasOperation     bool
	hasCommit        bool
	hasApply         bool
}

type replicationBenchSamples struct {
	operationToApply       []time.Duration
	arrivalToApply         []time.Duration
	commitToApply          []time.Duration
	remainingCatchUp       []time.Duration
	commitDurations        []time.Duration
	writeDurations         []time.Duration
	journalAppendDurations []time.Duration
	scheduleLags           []time.Duration
	transactionsPerFetch   []int
	appliedBeforeCommit    int
	emptyFetches           int
	gapObservations        int
	gapFills               int
	repairLockTimeouts     int
	sequenceRetries        int
	injectedRollbacks      int
	snapshotEpochs         int
	snapshotReplays        int
	snapshotExceptionMax   int
}

type replicationBenchTracker struct {
	snapshotPollMode snapshotBenchPollMode // Set before starting the consumer.
	snapshotWake     *snapshotBenchWakeups // Set before starting writers or consumer.
	mu               sync.Mutex
	events           map[int64]replicationBenchTiming
	completedIDs     map[int64]struct{}
	measurements     replicationBenchSamples
	expected         int
	applied          int
	finished         time.Time
	done             chan struct{}
}

func newReplicationBenchTracker(expected int) *replicationBenchTracker {
	return &replicationBenchTracker{
		events:       make(map[int64]replicationBenchTiming, expected),
		completedIDs: make(map[int64]struct{}, expected),
		measurements: replicationBenchSamples{
			operationToApply:     make([]time.Duration, 0, expected),
			commitToApply:        make([]time.Duration, 0, expected),
			remainingCatchUp:     make([]time.Duration, 0, expected),
			commitDurations:      make([]time.Duration, 0, expected),
			writeDurations:       make([]time.Duration, 0, expected),
			scheduleLags:         make([]time.Duration, 0, expected),
			transactionsPerFetch: make([]int, 0, expected),
		},
		expected: expected,
		done:     make(chan struct{}),
	}
}

func (t *replicationBenchTracker) recordOperationStart(id int64, started, scheduledAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !scheduledAt.IsZero() {
		t.measurements.scheduleLags = append(t.measurements.scheduleLags, started.Sub(scheduledAt))
	}

	event := t.events[id]
	event.operationStarted = started
	event.scheduledAt = scheduledAt
	event.hasOperation = true
	t.completeEventLocked(id, event)
}

func (t *replicationBenchTracker) recordCommit(id int64, started, finished time.Time) {
	t.mu.Lock()

	event := t.events[id]
	event.commitStarted = started
	event.commitFinished = finished
	event.hasCommit = true
	t.completeEventLocked(id, event)
	t.mu.Unlock()
	if t.snapshotWake != nil {
		t.snapshotWake.signalCommit()
	}
}

func (t *replicationBenchTracker) recordApply(
	id int64,
	rowCount int,
	checksumSum uint64,
	rowsPerTx int,
	expectedChecksum uint32,
	finished time.Time,
) error {
	if rowCount != rowsPerTx {
		return fmt.Errorf("batch %d applied %d rows, want %d", id, rowCount, rowsPerTx)
	}
	expectedChecksumSum := uint64(expectedChecksum) * uint64(rowsPerTx)
	if checksumSum != expectedChecksumSum {
		return fmt.Errorf("batch %d payload checksum mismatch: got %d, want %d", id, checksumSum, expectedChecksumSum)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.completedIDs[id]; ok {
		return fmt.Errorf("batch %d applied more than once", id)
	}
	event := t.events[id]
	if event.hasApply {
		return fmt.Errorf("batch %d applied more than once", id)
	}
	event.applyFinished = finished
	event.hasApply = true
	t.completedIDs[id] = struct{}{}
	t.applied++
	t.completeEventLocked(id, event)
	if t.applied == t.expected {
		t.finished = finished
		close(t.done)
	}
	return nil
}

func (t *replicationBenchTracker) completeEventLocked(id int64, event replicationBenchTiming) {
	if event.hasOperation && event.hasCommit && event.hasApply {
		if !event.scheduledAt.IsZero() {
			t.measurements.arrivalToApply = append(t.measurements.arrivalToApply, event.applyFinished.Sub(event.scheduledAt))
		}
		t.measurements.operationToApply = append(t.measurements.operationToApply,
			event.applyFinished.Sub(event.operationStarted))
		t.measurements.commitToApply = append(t.measurements.commitToApply,
			event.applyFinished.Sub(event.commitStarted))
		remaining := event.applyFinished.Sub(event.commitFinished)
		if remaining < 0 {
			remaining = 0
			t.measurements.appliedBeforeCommit++
		}
		t.measurements.remainingCatchUp = append(t.measurements.remainingCatchUp, remaining)
		t.measurements.commitDurations = append(t.measurements.commitDurations,
			event.commitFinished.Sub(event.commitStarted))
		t.measurements.writeDurations = append(t.measurements.writeDurations,
			event.commitFinished.Sub(event.operationStarted))
		delete(t.events, id)
		return
	}
	t.events[id] = event
}

func (t *replicationBenchTracker) recordFetch(transactionCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.measurements.transactionsPerFetch = append(t.measurements.transactionsPerFetch, transactionCount)
}

func (t *replicationBenchTracker) recordJournalAppend(elapsed time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.measurements.journalAppendDurations = append(t.measurements.journalAppendDurations, elapsed)
}

func (t *replicationBenchTracker) recordEmptyFetch() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.measurements.emptyFetches++
}

func (t *replicationBenchTracker) finishedAt() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.finished
}

func (t *replicationBenchTracker) samples() replicationBenchSamples {
	t.mu.Lock()
	defer t.mu.Unlock()
	return replicationBenchSamples{
		operationToApply:       append([]time.Duration(nil), t.measurements.operationToApply...),
		arrivalToApply:         append([]time.Duration(nil), t.measurements.arrivalToApply...),
		commitToApply:          append([]time.Duration(nil), t.measurements.commitToApply...),
		remainingCatchUp:       append([]time.Duration(nil), t.measurements.remainingCatchUp...),
		commitDurations:        append([]time.Duration(nil), t.measurements.commitDurations...),
		writeDurations:         append([]time.Duration(nil), t.measurements.writeDurations...),
		journalAppendDurations: append([]time.Duration(nil), t.measurements.journalAppendDurations...),
		scheduleLags:           append([]time.Duration(nil), t.measurements.scheduleLags...),
		transactionsPerFetch:   append([]int(nil), t.measurements.transactionsPerFetch...),
		appliedBeforeCommit:    t.measurements.appliedBeforeCommit,
		emptyFetches:           t.measurements.emptyFetches,
		gapObservations:        t.measurements.gapObservations,
		gapFills:               t.measurements.gapFills,
		repairLockTimeouts:     t.measurements.repairLockTimeouts,
		sequenceRetries:        t.measurements.sequenceRetries,
		injectedRollbacks:      t.measurements.injectedRollbacks,
		snapshotEpochs:         t.measurements.snapshotEpochs,
		snapshotReplays:        t.measurements.snapshotReplays,
		snapshotExceptionMax:   t.measurements.snapshotExceptionMax,
	}
}

func replicationBenchPercentile(samples []time.Duration, percentile int) time.Duration {
	index := (len(samples)*percentile+99)/100 - 1
	if index < 0 {
		index = 0
	}
	if index >= len(samples) {
		index = len(samples) - 1
	}
	return samples[index]
}

func replicationBenchIntPercentile(samples []int, percentile int) int {
	index := (len(samples)*percentile+99)/100 - 1
	if index < 0 {
		index = 0
	}
	if index >= len(samples) {
		index = len(samples) - 1
	}
	return samples[index]
}

func runReplicationBenchWriters(
	ctx context.Context,
	cancel context.CancelFunc,
	count int,
	mode replicationBenchMode,
	workload replicationBenchWorkload,
	payload string,
	pool *pgxpool.Pool,
	tracker *replicationBenchTracker,
	objects replicationBenchObjects,
	writerCount int,
	offeredWritesPerSecond int,
) (int, time.Duration, error) {
	if offeredWritesPerSecond > 0 {
		return runRateControlledReplicationBenchWriters(ctx, cancel, count, mode, workload,
			payload, pool, tracker, objects, writerCount, offeredWritesPerSecond)
	}
	started := time.Now()
	var nextID atomic.Int64
	var completed atomic.Int64
	var workers sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	for range writerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				id := nextID.Add(1)
				if id > int64(count) {
					return
				}
				if err := writeReplicationBenchBatch(ctx, pool, tracker, mode, workload,
					payload, id, time.Time{}, objects); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				completed.Add(1)
			}
		}()
	}
	workers.Wait()
	return int(completed.Load()), time.Since(started), firstErr
}

type replicationBenchWriteJob struct {
	id          int64
	scheduledAt time.Time
}

func runRateControlledReplicationBenchWriters(
	ctx context.Context,
	cancel context.CancelFunc,
	count int,
	mode replicationBenchMode,
	workload replicationBenchWorkload,
	payload string,
	pool *pgxpool.Pool,
	tracker *replicationBenchTracker,
	objects replicationBenchObjects,
	writerCount int,
	offeredWritesPerSecond int,
) (int, time.Duration, error) {
	started := time.Now()
	jobs := make(chan replicationBenchWriteJob, writerCount*2)
	var completed atomic.Int64
	var workers sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	for range writerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := writeReplicationBenchBatch(ctx, pool, tracker, mode, workload,
					payload, job.id, job.scheduledAt, objects); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				completed.Add(1)
			}
		}()
	}

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(jobs)
		for index := range count {
			scheduledAt := started.Add(time.Duration(index) * time.Second / time.Duration(offeredWritesPerSecond))
			if delay := time.Until(scheduledAt); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			select {
			case jobs <- replicationBenchWriteJob{id: int64(index + 1), scheduledAt: scheduledAt}:
			case <-ctx.Done():
				return
			}
		}
	}()

	workers.Wait()
	<-producerDone
	return int(completed.Load()), time.Since(started), firstErr
}

func writeReplicationBenchBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	tracker *replicationBenchTracker,
	mode replicationBenchMode,
	workload replicationBenchWorkload,
	payload string,
	id int64,
	scheduledAt time.Time,
	objects replicationBenchObjects,
) error {
	tracker.recordOperationStart(id, time.Now(), scheduledAt)
	if mode.usesSequence() || mode.usesSnapshot() {
		return writeSequenceBenchBatch(ctx, pool, tracker, mode, workload, payload, id, objects)
	}
	var tx pgx.Tx
	var err error
	if mode == replicationJournalPipelined {
		// SQL COMMIT through SendBatch does not release a pgxpool.Tx's pooled
		// connection. Own the connection explicitly for the pipelined variant.
		conn, acquireErr := pool.Acquire(ctx)
		if acquireErr != nil {
			return fmt.Errorf("acquire pipeline connection: %w", acquireErr)
		}
		defer conn.Release()
		tx, err = conn.Begin(ctx)
	} else {
		tx, err = pool.Begin(ctx)
	}
	if err != nil {
		return fmt.Errorf("begin batch %d: %w", id, err)
	}
	defer func() {
		if mode != replicationJournalPipelined || tx.Conn().PgConn().TxStatus() != 'I' {
			tx.Rollback(context.Background())
		}
	}()

	if err := writeJournalBenchSource(ctx, tx, objects, workload, payload, id); err != nil {
		return fmt.Errorf("insert batch %d: %w", id, err)
	}
	if mode == replicationJournalPipelined {
		return commitPipelinedJournalBenchBatch(ctx, tx, tracker, objects, id, workload.rowsPerTx, payload)
	}
	if mode.usesJournal() {
		if err := appendJournalBenchBatch(ctx, tx, tracker, objects, id, workload.rowsPerTx, payload,
			mode == replicationJournalNotifyInTx); err != nil {
			return err
		}
	}
	if mode == replicationCounterLogicalSlot {
		started := time.Now()
		_, err := tx.Exec(ctx, "UPDATE "+objects.clock+" SET position = position + 1 WHERE id = 1")
		tracker.recordJournalAppend(time.Since(started))
		if err != nil {
			return fmt.Errorf("increment control counter: %w", err)
		}
	}

	if mode == replicationNotifyInTx {
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", objects.channel, strconv.FormatInt(id, 10)); err != nil {
			return fmt.Errorf("notify batch %d in transaction: %w", id, err)
		}
	}

	commitStarted := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch %d: %w", id, err)
	}
	tracker.recordCommit(id, commitStarted, time.Now())

	if mode == replicationNotifyAfterCommit || mode == replicationJournalNotifyAfterCommit {
		if _, err := pool.Exec(ctx, "SELECT pg_notify($1, $2)", objects.channel, strconv.FormatInt(id, 10)); err != nil {
			return fmt.Errorf("notify batch %d after commit: %w", id, err)
		}
	}
	return nil
}

func startNotifyBenchConsumer(
	ctx context.Context,
	connString string,
	pool *pgxpool.Pool,
	tracker *replicationBenchTracker,
	rowsPerTx int,
	expectedChecksum uint32,
	fetchLimit int,
	table string,
	channel string,
	ready chan<- struct{},
	workers *sync.WaitGroup,
	reportError func(error),
) error {
	listener, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("connect notification listener: %w", err)
	}
	if _, err := listener.Exec(ctx, "LISTEN "+channel); err != nil {
		listener.Close(context.Background())
		return fmt.Errorf("listen on benchmark channel: %w", err)
	}

	notifications := make(chan int64, 4096)
	workers.Add(2)
	go func() {
		defer workers.Done()
		defer listener.Close(context.Background())
		for {
			notification, err := listener.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() == nil {
					reportError(fmt.Errorf("wait for notification: %w", err))
				}
				return
			}
			id, err := strconv.ParseInt(notification.Payload, 10, 64)
			if err != nil {
				reportError(fmt.Errorf("parse notification payload %q: %w", notification.Payload, err))
				return
			}
			select {
			case notifications <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	go consumeNotifyBenchBatches(ctx, pool, notifications, tracker, rowsPerTx,
		expectedChecksum, fetchLimit, table, workers, reportError)
	close(ready)
	return nil
}

func consumeNotifyBenchBatches(
	ctx context.Context,
	pool *pgxpool.Pool,
	notifications <-chan int64,
	tracker *replicationBenchTracker,
	rowsPerTx int,
	expectedChecksum uint32,
	fetchLimit int,
	table string,
	workers *sync.WaitGroup,
	reportError func(error),
) {
	defer workers.Done()
	for {
		var firstID int64
		select {
		case firstID = <-notifications:
		case <-ctx.Done():
			return
		}

		batchIDs := make([]int64, 1, fetchLimit)
		batchIDs[0] = firstID
	collect:
		for len(batchIDs) < fetchLimit {
			select {
			case id := <-notifications:
				batchIDs = append(batchIDs, id)
			default:
				break collect
			}
		}

		if err := fetchNotifyBenchBatches(ctx, pool, tracker, batchIDs, table, rowsPerTx, expectedChecksum); err != nil {
			if ctx.Err() == nil {
				reportError(err)
			}
			return
		}
	}
}

func fetchNotifyBenchBatches(
	ctx context.Context,
	pool *pgxpool.Pool,
	tracker *replicationBenchTracker,
	batchIDs []int64,
	table string,
	rowsPerTx int,
	expectedChecksum uint32,
) error {
	query := fmt.Sprintf(`
		SELECT batch_id, payload
		FROM %s
		WHERE batch_id = ANY($1)
		ORDER BY batch_id, ordinal
	`, table)
	rows, err := pool.Query(ctx, query, batchIDs)
	if err != nil {
		return fmt.Errorf("fetch notified batches: %w", err)
	}
	defer rows.Close()

	type batchResult struct {
		rowCount    int
		checksumSum uint64
		finishedAt  time.Time
	}
	results := make(map[int64]batchResult, len(batchIDs))
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			return fmt.Errorf("scan notified batch: %w", err)
		}
		result := results[id]
		result.rowCount++
		result.checksumSum += uint64(crc32.ChecksumIEEE([]byte(payload)))
		result.finishedAt = time.Now()
		results[id] = result
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate notified batches: %w", err)
	}

	if len(results) != len(batchIDs) {
		return fmt.Errorf("fetch returned rows for %d of %d notified batches", len(results), len(batchIDs))
	}
	tracker.recordFetch(len(batchIDs))
	for _, id := range batchIDs {
		result := results[id]
		if err := tracker.recordApply(id, result.rowCount, result.checksumSum,
			rowsPerTx, expectedChecksum, result.finishedAt); err != nil {
			return err
		}
	}
	return nil
}

func startLogicalSlotBenchConsumer(
	setupCtx context.Context,
	runCtx context.Context,
	connString string,
	tracker *replicationBenchTracker,
	rowsPerTx int,
	expectedChecksum uint32,
	objects replicationBenchObjects,
	ready chan<- struct{},
	workers *sync.WaitGroup,
	reportError func(error),
) error {
	replicationConn, err := pgconn.Connect(setupCtx, replicationBenchConnString(connString))
	if err != nil {
		return fmt.Errorf("connect replication stream: %w", err)
	}

	slot, err := pglogrepl.CreateReplicationSlot(setupCtx, replicationConn, objects.slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		replicationConn.Close(context.Background())
		return fmt.Errorf("create replication slot: %w", err)
	}
	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		replicationConn.Close(context.Background())
		return fmt.Errorf("parse slot consistent point %q: %w", slot.ConsistentPoint, err)
	}
	if err := pglogrepl.StartReplication(setupCtx, replicationConn, objects.slot, startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + objects.publication + "'",
			},
		}); err != nil {
		replicationConn.Close(context.Background())
		return fmt.Errorf("start logical replication: %w", err)
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := consumeLogicalSlotBench(runCtx, replicationConn, startLSN, tracker,
			rowsPerTx, expectedChecksum); err != nil && runCtx.Err() == nil {
			reportError(err)
		}
	}()
	close(ready)
	return nil
}

func consumeLogicalSlotBench(
	ctx context.Context,
	conn *pgconn.PgConn,
	startLSN pglogrepl.LSN,
	tracker *replicationBenchTracker,
	rowsPerTx int,
	expectedChecksum uint32,
) error {
	defer conn.Close(context.Background())
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	relationColumns := make(map[uint32]replicationBenchRelationColumns)
	appliedLSN := startLSN
	lastStatusUpdate := time.Now()
	var inTransaction bool
	var batchID int64
	var rowCount int
	var checksumSum uint64

	for {
		rawMessage, err := conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive replication message: %w", err)
		}
		if serverError, ok := rawMessage.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("replication server error: %s", serverError.Message)
		}
		copyData, ok := rawMessage.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse replication keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: appliedLSN}); err != nil {
					return fmt.Errorf("reply to replication keepalive: %w", err)
				}
				lastStatusUpdate = time.Now()
			}

		case pglogrepl.XLogDataByteID:
			data, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse replication data: %w", err)
			}
			message, err := pglogrepl.Parse(data.WALData)
			if err != nil {
				return fmt.Errorf("parse pgoutput message: %w", err)
			}

			switch message := message.(type) {
			case *pglogrepl.RelationMessage:
				relations[message.RelationID] = message
				columns, err := replicationBenchColumns(message)
				if err != nil {
					return err
				}
				relationColumns[message.RelationID] = columns
			case *pglogrepl.BeginMessage:
				inTransaction = true
				batchID = 0
				rowCount = 0
				checksumSum = 0
			case *pglogrepl.InsertMessage:
				if !inTransaction {
					return fmt.Errorf("received insert outside a transaction")
				}
				relation, ok := relations[message.RelationID]
				if !ok {
					return fmt.Errorf("received insert for unknown relation %d", message.RelationID)
				}
				columns, ok := relationColumns[message.RelationID]
				if !ok {
					return fmt.Errorf("received insert for relation %d without column metadata", message.RelationID)
				}
				id, payload, err := replicationBenchTuple(relation, columns, message.Tuple)
				if err != nil {
					return err
				}
				if rowCount == 0 {
					batchID = id
				} else if batchID != id {
					return fmt.Errorf("transaction contains batches %d and %d", batchID, id)
				}
				rowCount++
				checksumSum += uint64(crc32.ChecksumIEEE([]byte(payload)))
			case *pglogrepl.CommitMessage:
				if rowCount > 0 {
					if err := tracker.recordApply(batchID, rowCount, checksumSum,
						rowsPerTx, expectedChecksum, time.Now()); err != nil {
						return err
					}
				}
				inTransaction = false
				appliedLSN = message.TransactionEndLSN
				if time.Since(lastStatusUpdate) >= 10*time.Second {
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
						pglogrepl.StandbyStatusUpdate{WALWritePosition: appliedLSN}); err != nil {
						return fmt.Errorf("acknowledge applied transactions: %w", err)
					}
					lastStatusUpdate = time.Now()
				}
			}
		}
	}
}

type replicationBenchRelationColumns struct {
	batchID int
	payload int
}

func replicationBenchColumns(relation *pglogrepl.RelationMessage) (replicationBenchRelationColumns, error) {
	columns := replicationBenchRelationColumns{batchID: -1, payload: -1}
	for index, column := range relation.Columns {
		switch column.Name {
		case "batch_id":
			columns.batchID = index
		case "payload":
			columns.payload = index
		}
	}
	if columns.batchID < 0 || columns.payload < 0 {
		return columns, fmt.Errorf("relation %q is missing benchmark columns", relation.RelationName)
	}
	return columns, nil
}

func replicationBenchTuple(
	relation *pglogrepl.RelationMessage,
	columns replicationBenchRelationColumns,
	tuple *pglogrepl.TupleData,
) (int64, string, error) {
	if tuple == nil {
		return 0, "", fmt.Errorf("insert message has no tuple")
	}
	if columns.batchID >= len(tuple.Columns) || columns.payload >= len(tuple.Columns) {
		return 0, "", fmt.Errorf("tuple is missing columns from relation %q", relation.RelationName)
	}
	batchIDValue := tuple.Columns[columns.batchID]
	if batchIDValue.DataType != 't' {
		return 0, "", fmt.Errorf("unexpected batch_id wire format %q", batchIDValue.DataType)
	}
	id, err := strconv.ParseInt(string(batchIDValue.Data), 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse batch_id: %w", err)
	}
	payloadValue := tuple.Columns[columns.payload]
	if payloadValue.DataType != 't' {
		return 0, "", fmt.Errorf("unexpected payload wire format %q", payloadValue.DataType)
	}
	return id, string(payloadValue.Data), nil
}

func replicationBenchConnString(connString string) string {
	parsed, err := url.Parse(connString)
	if err != nil {
		return connString + "?replication=database"
	}
	query := parsed.Query()
	query.Set("replication", "database")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func waitForReplicationBenchDrain(
	ctx context.Context,
	tracker *replicationBenchTracker,
	consumerErrors <-chan error,
) error {
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	select {
	case <-tracker.done:
		return nil
	case err := <-consumerErrors:
		return fmt.Errorf("replication consumer failed before catch-up: %w", err)
	case <-ctx.Done():
		if err := takeReplicationBenchError(consumerErrors); err != nil {
			return err
		}
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for replication catch-up")
	}
}

func takeReplicationBenchError(consumerErrors <-chan error) error {
	select {
	case err := <-consumerErrors:
		return err
	default:
		return nil
	}
}
