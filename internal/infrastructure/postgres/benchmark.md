## Hydration Benchmark Results (Apple M4 Pro, PostgreSQL 18 in container)

Benchmarked hydration time from PostgreSQL at various scales. This measures the startup cost
after a node crash – the time to reload all tuples into memory before serving requests.

### Baseline Performance

| Scale  | Tuples | DB I/O | Hydrate | End-to-end | Tuples/sec |
| ------ | ------ | ------ | ------- | ---------- | ---------- |
| Small  | 19K    | 4ms    | 2ms     | 5ms        | 3.8M       |
| Medium | 237K   | 45ms   | 21ms    | 65ms       | 3.6M       |
| Large  | 2.7M   | 717ms  | 364ms   | 753ms      | 3.6M       |

### Large Scale Comparison: SELECT vs COPY vs Pipelined (2.7M tuples)

| Method                | Time      | Tuples/sec | Notes                               |
| --------------------- | --------- | ---------- | ----------------------------------- |
| **Pipelined Batched** | **535ms** | **5.0M**   | **FASTEST** - true parallelism      |
| SELECT Iterator       | 752ms     | 3.6M       | Serial DB read + hydrate            |
| SELECT Batched        | 822ms     | 3.3M       | Pre-loading adds overhead           |
| COPY (text)           | 821ms     | 3.3M       | Text parsing offsets protocol gains |
| SELECT DB-only        | 502ms     | 5.3M       | Pure DB read time                   |
| COPY DB-only          | 471ms     | 5.7M       | 6% faster - protocol is efficient   |

**Key findings:**

- **Batched pipelining is 29% faster** by overlapping DB I/O with hydration
- Per-tuple channel pipelining is slower due to channel overhead (~100-200ns/op)
- Batching amortizes channel overhead: ~270 channel ops vs 2.7M
- COPY protocol is 6% faster at DB level but text parsing negates gains

### Optimization Experiments (237K tuples)

| Approach                       | Time       | vs Baseline | Notes                           |
| ------------------------------ | ---------- | ----------- | ------------------------------- |
| **Pipelined Batched**          | **47.5ms** | **-25%**    | **FASTEST** - batch channel ops |
| Baseline (iterator)            | 59.4ms     | -           | Serial: DB read then hydrate    |
| Pre-alloc slice                | 64.8ms     | +9% slower  | Extra allocation overhead       |
| Pre-alloc slice + HydrateSlice | 63.6ms     | +7% slower  | Iterator overhead negligible    |
| Pre-alloc all (map + slice)    | 63.2ms     | +6% slower  | Map pre-sizing doesn't help     |
| Pipelined (per-tuple)          | 68.4ms     | +15% slower | Channel overhead dominates      |
| COPY                           | ~63ms      | +6% slower  | Text parsing overhead           |

**Why batched pipelining wins:**

- Per-tuple channel send/receive: ~100-200ns overhead per tuple
- Per-tuple hydration work: ~50ns (map lookup + bitmap add)
- Channel overhead was 2-4x the actual work!
- Batching (10K tuples/batch) reduces channel ops by 10,000x

**Conclusions:**

- **Batched pipelining achieves true parallelism** between DB I/O and hydration
- Per-tuple pipelining fails because channel overhead exceeds work per item
- Pre-allocation doesn't help because the bottleneck was serialization, not allocation
- COPY's protocol efficiency is offset by Go-side text parsing

**Remaining optimization opportunities:**

1. **Parallel connections**: Split load across N connections with `WHERE object_id % N = i`
2. **PostgreSQL tuning**: shared_buffers, effective_cache_size, work_mem
3. **Network optimization**: Unix socket vs TCP for local connections

**Projection at scale:**

- 100M tuple node: ~20 seconds to hydrate (at 5.0M tuples/sec)
- With sharding across 10 nodes: each shard is 10M tuples = ~2 seconds per shard

**Run benchmarks:**

```bash
go test -bench=BenchmarkHydration ./internal/infrastructure/postgres/...
go test -bench=BenchmarkHydrationOptimizations ./internal/infrastructure/postgres/...
go test -bench=BenchmarkLargeSELECTvsCOPY ./internal/infrastructure/postgres/...  # ~60s
```

## Replication Transport Benchmark

Compare transactional `NOTIFY` plus journal fetch, best-effort post-commit `NOTIFY` plus fetch, and a `pgoutput` logical replication slot:

```bash
go test -run '^$' -bench '^BenchmarkReplicationTransport$' -benchtime=10s -benchmem ./internal/infrastructure/postgres
go test -run '^$' -bench '^BenchmarkReplicationTransportAtRate$' -benchtime=30s -count=3 ./internal/infrastructure/postgres
```

`BenchmarkReplicationTransport` is a saturation comparison with 16 writers. `BenchmarkReplicationTransportAtRate` uses equal offered load for every transport: one writer at 100 writes/sec, and 16 writers at 1,000 writes/sec total. The 16-writer scenario runs with both workloads: one 256-byte row per transaction or 16 1-KiB rows. Notification variants are tested with fetch limits of 1 and 128; the slot consumer applies complete `pgoutput` transactions, while notification consumers fetch complete batches from the indexed table. Scheduler lag reports whether writers fall behind the offered rate. Polling fallback is intentionally excluded.

The local setup uses PostgreSQL 18 Alpine in a Testcontainer capped at 4 GiB. It enables `wal_level=logical`, sets `shared_buffers=1GB`, `effective_cache_size=3GB` (a planner estimate, not allocated memory), `work_mem=4MB`, and `max_wal_size=4GB`, and explicitly enables `fsync`, `synchronous_commit`, and `full_page_writes`. Testcontainers otherwise disables `fsync`; earlier runs that did not override this are not durable-commit measurements. Before timing, the benchmark waits for the notification listener or replication stream to be ready and warms pooled fetch connections with an empty indexed query. Timing includes consumer drain. Each run uses unique table, channel, publication, and slot names and drops them afterward.

Metrics include committed writes/sec, fully applied batches/sec, payload MiB/sec, p50/p99 operation-to-apply and commit-to-apply latency, commit duration, and remaining catch-up time per transaction (`max(0, apply time - commit finish)`). Remaining catch-up percentiles are calculated from per-transaction values; applied-before-commit percentage is reported separately. Notification runs also report average and p99 transactions per fetch. Consumers validate row counts and payload checksums, and duplicate batch application fails the run. This measures transport and decoding without including Falcon graph mutation costs.

For measurements at deployment RTT, point the benchmark at a dedicated external database from the same network location as the application:

```bash
FALCON_REPLICATION_BENCH_DATABASE_URL="$BENCH_DATABASE_URL" go test -run '^$' -bench '^BenchmarkReplicationTransportAtRate$' -benchtime=30s -count=3 ./internal/infrastructure/postgres
```

When `FALCON_REPLICATION_BENCH_DATABASE_URL` is set, no container is started and the external server's PostgreSQL settings are used unchanged. The server must support logical replication (`wal_level=logical`, at least one replication slot and WAL sender available); the supplied role needs permission to create tables/publications and logical replication slots. Use a dedicated database, since the benchmark creates and drops temporary objects. An interrupted process can leave those objects behind. The benchmark logs the server's memory settings; unlike the local setup, it does not impose a memory profile.

PostgreSQL delivers transactional notifications only after commit, and the writer does not wait for a listener to process them. However, queue exhaustion can make the transaction fail at commit. `NotifyAfterCommit` measures the alternative extra notification round trip, but it is not a reliable standalone protocol: a process crash between data commit and notification loses the wakeup. It needs a durable reconciliation path in production. The benchmark measures performance, not notification-loss recovery or queue-exhaustion behavior.

Use `-benchtime=1x` only as a wiring check. Use repeated sustained runs at the intended database topology for performance decisions. The local memory profile is a useful approximation, not a full RDS model for CPU, storage, or network limits; container-local results do not predict managed-service RTT or behavior.

## Application Change Log Benchmark

[October 1, 2026 measurements, conclusions, and raw samples](benchmark-results/change-log.md).

`BenchmarkChangeLogTransport` measures the complete write and consumption path for an application-maintained log. Every transport writes the same source rows. Journal variants additionally update a singleton transactional counter and append one durable `BYTEA` entry containing the complete transaction's ordered payloads. The counter and append share one SQL statement after the source writes; the counter lock remains held until commit. Transactional notifications are included in that statement, while post-commit notifications add a separate round trip after commit.

The consumer fetches `WHERE position > $cursor ORDER BY position LIMIT 128`, validates consecutive positions and every payload checksum, and advances its cursor only after a complete transaction. It does not look up notified request IDs. Notifications are coalesced wakeups; all journal variants also reconcile every 10 ms. Full pages are drained immediately. A committed `LISTEN` precedes the first fetch, which also warms the production cursor query before timing.

The workloads are one 256-byte source row or sixteen 1-KiB source rows per transaction. TOAST compression is disabled on journal payloads so the repeated per-row data in the larger workload is not artificially compressed. These are transport payloads, not Falcon's actual compact tuple encoding. The logical-slot baseline captures only the source table; journal variants deliberately include the additional durable log write. `BenchmarkChangeLogCounterControl` adds the counter update to the logical-slot baseline without writing or reading a journal, to distinguish serialization from log overhead.

`BenchmarkChangeLogPipelined` uses the same counter, journal, and polling consumer, but sends the append statement and `COMMIT` in a single `pgx.SendBatch` pipeline. Source writes still happen first. This removes the client round trip between append completion and sending commit, while preserving the same transaction and ordering semantics.

```bash
# Saturation at 16 and 64 concurrent writers; select Writers16 to restrict it.
go test -run '^$' -bench '^BenchmarkChangeLogTransport$' -benchtime=20s -count=3 -timeout=30m ./internal/infrastructure/postgres
# Counter-only control, with 16 writers.
go test -run '^$' -bench '^BenchmarkChangeLogCounterControl$' -benchtime=20s -count=3 ./internal/infrastructure/postgres
# Counter + journal with pipelined append and commit, with 16 writers.
go test -run '^$' -bench '^BenchmarkChangeLogPipelined$' -benchtime=20s -count=3 ./internal/infrastructure/postgres
# Equal offered load: 1,000, 5,000, and 10,000 transactions/sec, with 16 writers.
go test -run '^$' -bench '^BenchmarkChangeLogTransportAtRate$' -benchtime=20s -count=3 -timeout=30m ./internal/infrastructure/postgres
# Rollback, concurrency, cursor recovery without notifications, page boundaries, and gaps.
go test -run '^TestJournalBenchCursorRecovery$' ./internal/infrastructure/postgres
```

`FALCON_REPLICATION_BENCH_DATABASE_URL` works for these benchmarks too. External databases are not retuned. Each measured invocation uses a fresh schema; the log grows throughout the run and is dropped afterward. The benchmark has one consumer and excludes retention DDL, lease/checkpoint updates, graph application, preconditions, and simulated WAN latency.

In addition to the original metrics, `p50/p99-write-us` includes begin, source insertion, counter/append if present, and commit. It excludes the separate post-commit notification, whose cost remains in aggregate writer throughput. `p50/p99-journal-append-us` includes counter contention, log insertion, and the SQL round trip; for the counter-only control it measures just the counter statement. It is not an isolated server lock-wait metric. For the pipelined variant, commit timing and commit-to-apply timing start before the combined append/commit pipeline; compare write and operation-to-apply timing across modes instead of its commit metrics. Fetch counts include initial consumer warmup; successful transactions per fetch exclude empty queries. Saturation latency includes queueing for the counter; rate-controlled runs separately report scheduling lag when the offered rate exceeds capacity.

## Sequence Journal with Gap Repair

[Sequence measurements and ordering analysis](benchmark-results/sequence-journal.md).

`BenchmarkSequenceJournal` compares the counter pipeline and logical slot with two sequence journals: ordinary append/commit, and pipelined append/commit. Both use a logged `BIGINT` sequence with `CACHE 1`, a unique journal position, and one binary entry per complete transaction. Source mutations finish before position allocation. There is no singleton counter update and no application read of a previous key version.

The cursor query remains `WHERE position > $cursor ORDER BY position LIMIT 128`. The reader applies only a contiguous prefix, including permanent no-op gap sentinels. On observing a later committed position, it waits 100 ms for the missing position, then attempts a sentinel insert with a 10 ms lock timeout. A competing committed journal row wins normally. An uncommitted unique-index entry makes repair wait and possibly time out; repair cannot abort that writer. If the sentinel wins before the writer inserts, that writer's whole source+journal transaction must roll back and retry with a fresh sequence number. Sentinels never count as applied source transactions. Trailing allocations are not repaired until a higher committed position provides evidence of the gap.

`BenchmarkSequenceJournalAtRate` compares the sequence pipeline, counter pipeline, and slot at 1,000 and 3,000 offered transactions/sec. `p50/p99-arrival-to-apply-us` includes writer scheduling delay and any transaction retries, measuring latency from the intended arrival through complete consumer application. It supplements operation-to-apply latency; percentiles of schedule delay and operation latency cannot be added.

`BenchmarkSequenceJournalKeys` changes the source table to a bounded latest-state table with 256 keys or one hot key. Writers use `INSERT ... ON CONFLICT ... DO UPDATE`, with no preceding SELECT or version predicate, and then allocate a journal position. The source holds only latest state; history is in the separate journal. These checks model row contention, not Falcon's precise tuple insert/delete workload. `BenchmarkSequenceJournalFaults` injects one fault per 1,000 requested operations: a rolled-back append followed by a full retry, a 200 ms pause between explicit allocation and insert, or a 200 ms pause after journal insertion and before commit. Faults happen once per operation, including across retries. The split allocation in the pre-insert fault is deliberate; healthy writes allocate within the insert statement.

Metrics additionally include gap observations, successful sentinel fills, repair lock timeouts, repaired-position writer retries, and deliberately injected rollbacks. Sequence fetch statistics count rows fetched, including sentinels and later rows that may be fetched again while a preceding gap remains unresolved. They do not represent distinct applied transactions per fetch. Append duration records the successful attempt; operation and arrival latency include failed attempts. Pipeline commit timing includes append, as in the counter pipeline.

```bash
# Fixed transaction counts avoid repeated duration-calibration runs.
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSequenceJournal$' -benchtime=100000x -count=3 -timeout=20m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSequenceJournalAtRate$' -benchtime=30000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSequenceJournalKeys$' -benchtime=100000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSequenceJournalFaults$' -benchtime=20000x -count=1 -timeout=3m
go test ./internal/infrastructure/postgres -run '^TestSequenceJournal' -count=1 -timeout=3m
```

The recovery tests exercise pagination, rollback holes, restart, rejection of a delayed writer after repair, full source rollback, an already-inserted uncommitted row blocking repair, ordered replay of out-of-order commits, and same-key upsert/delete-reinsert ordering. This is a benchmark implementation, not a replacement production Store. It has one repair reader and omits durable leases/checkpoints, retention partitions, crash/failover recovery, bootstrap snapshot reconciliation, and graph application. Production retention must keep repaired positions fenced: deleting a sentinel while a paused writer can still insert that old position would invalidate the sealed prefix.

## Snapshot Journal with Consumer Epochs

[Snapshot measurements, protocol explanation, and raw results](benchmark-results/snapshot-journal.md).

`BenchmarkSnapshotJournal` compares a snapshot-delta journal with the sequence repair pipeline and logical slot. Writers use the same latest-state source mutations, followed by a journal append and commit pipeline. The snapshot journal adds an explicit top-level `writer_xid XID8 DEFAULT pg_current_xact_id()` and a `(writer_xid, position)` index. Sequence allocation still happens after source mutations with `CACHE 1`; XIDs identify transaction visibility, not mutation order.

The consumer uses a repeatable-read transaction to capture a PostgreSQL snapshot and fetch its delta in pages of 128, ordered by producer sequence position. Newly visible transactions are found through two disjoint indexed candidate sets: XIDs from the new range, and transactions that were in progress at the previous snapshot. An explicit snapshot-visibility predicate makes durable epoch replay possible even after previously excluded writers have committed. The position cursor resets for each epoch, so a late commit at an older position remains discoverable.

Each nonempty delta is recorded in a durable epoch manifest containing its previous and current snapshots and transaction count. The timed consumer waits for the manifest commit and validates all transaction payloads before reporting any application completion for the epoch. Empty cuts can advance the in-memory snapshot without a manifest because they contain no journal changes. There is one epoch producer; no writer obtains an epoch counter lock. This path includes the extra XID index and manifest writes, not just in-memory snapshot bookkeeping.

Restart starts from the caller's applied epoch, not the latest manifest. A manifest committed before publication is replayed from its stored cuts; a count mismatch fails rather than silently accepting retention loss. The benchmark does not persist the applied graph or its checkpoint. A production implementation needs staged graph application, shard-wide epoch publication, an applied checkpoint, leader fencing, and journal/manifest retention. Pages are bounded, but the complete epoch is buffered in client memory; large catch-up epochs require a bounded staging strategy.

```bash
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotJournal$' -benchtime=100000x -count=3 -timeout=15m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotJournalAtRate$' -benchtime=30000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotJournalFaults$' -benchtime=20000x -count=3 -timeout=10m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotJournalKeys$' -benchtime=100000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^TestSnapshotJournalProtocol$' -count=1 -timeout=2m
```

Fault comparisons use ordinary append/commit for both paths, injecting the same once-per-1,000-operation rollback or 200 ms pre/post-insert pause. The snapshot consumer never inserts repair sentinels and does not force paused writers to retry. Arrival latency includes retries and scheduling delay. `max-arrival-to-apply-us` and `over-50ms-arrival-pct` show whether sparse faults spread to unrelated operations. Snapshot metrics include applied epoch count, average transactions per epoch, replay count, and maximum snapshot exception count. Saturation and fault cases are repeated; fixed-rate and key-contention cases are single sustained checks.

Protocol tests cover uncommitted and aborted positions, delayed insertion at an older position, stable 128-row pages while writers commit, replay after manifest commit/publication failure, restart from an applied checkpoint, and per-key mutation order when XID order is inverted. Snapshots and journal queries come from the primary. Physical failover, restored databases, retention DDL, long-lived SQL snapshots, multiple producers, and production graph application are not exercised.

## Snapshot Journal with Coalesced Commit Wakeups

[Wakeup measurements, implementation, and raw results](benchmark-results/snapshot-wakeups.md).

`BenchmarkSnapshotWakeups`, `BenchmarkSnapshotWakeupsAtRate`, and `BenchmarkSnapshotWakeupsFaults` hold the snapshot protocol and 10 ms periodic poll constant while comparing `Poll`, `LocalWake`, and `NotifyWake`. Successful commits attempt a nonblocking send to a one-slot application queue. `LocalWake` delivers that hint directly to the consumer. `NotifyWake` uses a background sender with its own connection to issue `pg_notify` in a separate transaction, and a dedicated LISTEN connection coalesces delivery into the consumer's one-slot queue. Source transactions contain no NOTIFY, and writers do not wait for notification delivery or publish epochs. There is one bridge for the benchmark's 16 writers, modeling one writer process; additional writer processes would each need their own bridge.

Consumers take pending hints before capturing a snapshot. A commit during a scan leaves another hint for the next scan. LISTEN is committed before the initial scan; polling recovers hints omitted after commit. This experiment keeps the original periodic ticker, so it does not reduce idle polling frequency. Notifications may produce smaller epochs and additional empty scans. Sender or listener errors fail the benchmark to make problems visible; a production implementation would continue polling and reconnect.

```bash
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotWakeups$' -benchtime=100000x -count=3 -timeout=15m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotWakeupsAtRate$' -benchtime=30000x -count=3 -timeout=8m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotWakeupsFaults$' -benchtime=20000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^TestSnapshotWakeup' -count=1 -timeout=2m
```

Metrics count post-commit signal attempts, enqueued/coalesced hints, notifications sent/received, delivery coalescing, consumed hints, and poll wakeups. Queue counters include startup through shutdown; cancellation may leave unsent or unconsumed hints for rows already applied. Each attempted write is still checked for exactly-once application and complete payload integrity. Tests cover omitted hints recovered by polling, aborted attempts not notifying, and commits during pagination waking the next stable snapshot.

## Snapshot Idle Timer and Notification Batch Window

[Idle timer, batching measurements, and raw results](benchmark-results/snapshot-idle-timer.md).

`BenchmarkSnapshotIdleTimer`, `BenchmarkSnapshotIdleTimerAtRate`, and `BenchmarkSnapshotIdleTimerFaults` compare three PostgreSQL-notified policies: the independent `Ticker10ms` control, `Idle100ms` reset after every complete retrieval, and `Idle100msBatch2ms` with a bounded sender wait before notification. Source writes never wait for either timer. The consumer stops its idle timer during retrieval/publication; notification, empty, and immediate catch-up retrievals restart the countdown. Sender batching consumes additional pending hints before sending SQL, leaving commits during the round trip queued for another notification. The batch wait is configurable through `snapshotNotifyDelay`.

`BenchmarkSnapshotIdlePolling` observes no-write scan frequency; one benchmark operation is one second of observation. Additional metrics count total snapshot retrievals and notification batch merges. `poll-wakes` counts timer-triggered retrieval attempts, while `snapshot-scans` counts all retrieval attempts, including initial scanning. Complete retrievals may fetch multiple pages, so neither counter equals SQL query count.

```bash
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotIdlePolling$' -benchtime=3x -count=1 -timeout=1m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotIdleTimerAtRate$' -benchtime=30000x -count=3 -timeout=8m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotIdleTimer$' -benchtime=100000x -count=3 -timeout=15m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkSnapshotIdleTimerFaults$' -benchtime=20000x -count=1 -timeout=5m
go test ./internal/infrastructure/postgres -run '^Test(Snapshot|PostgresCleanup)' -count=1 -timeout=2m
```

Container cleanup uses an independent timeout context, so canceling writers/consumers cannot leave their database running. Cleanup failures fail the test/benchmark. Omitted-hint recovery tests use the 100 ms idle timer, and rollback/pagination tests exercise immediate notifications and the 2 ms batch variant. Existing earlier benchmark functions retain their 10 ms control for reproduction.
