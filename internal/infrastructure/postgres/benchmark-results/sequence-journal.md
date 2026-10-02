# Sequence journal with gap repair — October 1, 2026

The sequence journal removes the transaction-long global counter lock. Same-key order can be preserved without an application read of the previous version by finishing all actual source mutations before allocating the journal position. The throughput and latency measurements below use a separate latest-state source table and append-only journal; this is a benchmark implementation, not a production Store replacement.

## Ordering and correctness

The write protocol is:

1. Begin the source transaction and perform all tuple mutations.
2. After every mutation has completed, append one complete transaction record using `nextval()` from a logged `BIGINT` sequence with `CACHE 1`.
3. Commit the source and journal together. If gap repair has already claimed that position, roll back and retry the entire transaction under a new position.

An actual UPDATE/UPSERT already obtains a conflicting row lock. DELETE and insert uniqueness checks also make competing changes to the same key wait for a transaction outcome. Once transaction A has changed a key and reached journal allocation, B cannot finish changing that key until A commits or rolls back. With CACHE 1, B then allocates a later position. The existing source-table DML supplies this serialization; no preceding `SELECT` or `previous_version` constraint is necessary just to order captured changes. This is a deduction from [PostgreSQL row-lock behavior](https://www.postgresql.org/docs/current/explicit-locking.html) and [unique-index conflict handling](https://www.postgresql.org/docs/current/index-unique-checks.html), also exercised by the upsert and delete/reinsert regression tests.

Allocate only after *all* source mutations, not at transaction start and not as a DEFAULT in the source upsert. Expression evaluation can precede the row conflict wait. Perform no further source mutations or dependency reads after allocation. Apply the journal's actual mutation outcomes: an `INSERT ... DO NOTHING` or DELETE of an absent tuple must not be replayed as a change. If no-op intents must also be sequenced against future writes, use a persistent key lock/tombstone or a transaction advisory lock, including for absent keys. For an object-level rather than tuple-level order, all writers for that object must share a lock at that granularity. Multi-key writes still need consistent lock acquisition order and deadlock retries.

CACHE 1 is a correctness choice here. Larger session caches let a later caller obtain a smaller number reserved earlier by its connection, undermining monotonic key order even when the source mutations wait correctly. [PostgreSQL sequence documentation](https://www.postgresql.org/docs/current/sql-createsequence.html) describes this allocation behavior. There is still a short internal sequence synchronization point; it is not held through transaction commit.

Per-key serialization is different from a global counter: unrelated keys can progress concurrently. A very hot key necessarily limits conflicting-write capacity. A previous-version condition provides optimistic concurrency semantics, such as rejecting a stale read-modify-write; removing it may permit lost updates. Journal ordering alone does not replace that semantic guarantee. Tuple insert/delete operations can often use the source table's existing constraints instead. Falcon's current ID-mapping upsert can also serialize transactions referring to the same object; these transport checks exclude that mapping work.

## Gap repair and latency

The read watermark represents the highest contiguous prefix that has been applied or permanently fenced with a sentinel. It is neither sequence `last_value` nor the maximum visible journal position. The reader fetches positions greater than its cursor in ascending order and stops before the first unresolved hole. After a configurable grace period, it attempts a unique sentinel insert at the missing position.

There are two different cases:

- If the writer has allocated a number but has not inserted the journal's unique index entry, repair can win. The delayed append then fails, rolling back its source mutations. The writer retries the complete transaction.
- If the writer has already inserted its journal row but has not committed, repair waits for that transaction. A unique constraint does not abort the existing inserter. The benchmark uses a repair lock timeout and retries reading; the watermark remains blocked until the writer commits or rolls back.

This distinction follows [PostgreSQL's uniqueness checks](https://www.postgresql.org/docs/current/index-unique-checks.html). Kine's [polling reader](https://github.com/k3s-io/kine/blob/master/pkg/logstructured/sqllog/sql.go) also waits and attempts a fill; its [generic insert code](https://github.com/k3s-io/kine/blob/master/pkg/drivers/generic/generic.go) retries primary-key conflicts. The benchmark borrows this mechanism rather than reproducing Kine's historical-value schema, exact timing, or all recovery behavior. In particular it never skips an unresolved gap based only on elapsed time.

A 100 ms repair grace is not a bound on consistent-read latency. A stalled already-inserted transaction can hold up later committed positions across unrelated keys. For predictable tails, a production design needs bounded transaction duration, database-side enforcement, retry/idempotency rules, and a latency budget for repair and catch-up. PostgreSQL's role/session [transaction and idle-in-transaction timeouts](https://www.postgresql.org/docs/current/runtime-config-client.html) can terminate such transactions. Transaction termination behavior and application retries under those settings have not been benchmarked here. Wakeup notifications or more frequent polling can reduce healthy consumer delay but cannot let a scalar watermark pass an unresolved gap.

## Production integration

Keep the source as latest state and the journal as complete transaction records. Return the successful journal position as the write's consistency token; a read that requires it waits for the applied prefix to reach it. The current `Tx.Commit` interface returns only an error, so exposing that receipt needs an API decision. Do not publish a batch's watermark until all of its tuple changes have been applied.

Retain gap sentinels for as long as an old writer can still insert at that position. Dropping a partition can be a permanent fence if an insert into the removed range is rejected and that range is never recreated; a default partition accepting old positions would defeat that fence. Readers need durable checkpoints/retention leases and an explicit retained-through boundary. The benchmark has one repair reader, an in-memory cursor, and no partition retention.

Hydration also needs a defined snapshot protocol. A repeatable-read snapshot paired only with `MAX(position)` is insufficient: it can include position 12 while position 11 is still invisible, and later skipping everything through 12 would lose 11. A design must reconcile those snapshot-time holes, or use a bootstrap barrier with the necessary write coordination. Recording the visible positions/holes alongside the source snapshot is a candidate given the enforced per-key order, but that protocol is not implemented or validated by this benchmark. Crash/failover, restart from durable checkpoints, and multi-reader repair are also outside these measurements.

## Benchmark setup

All runs use the same local environment as the [counter measurements](change-log.md): PostgreSQL 18.6 Alpine on arm64 in a six-vCPU Podman VM, a 4 GiB database container, shared_buffers=1GB, fsync=on, synchronous_commit=on, full_page_writes=on, and wal_level=logical. One consumer fetches up to 128 transaction records with 10 ms polling, a 100 ms gap grace, and a 10 ms repair lock timeout. Each invocation starts with fresh tables and a fresh container.

Every path writes the same source payloads: one 256-byte row or sixteen 1-KiB rows per transaction. The journal adds one uncompressed binary record per transaction. Normal saturation runs append distinct source keys; separate hot-key checks upsert a bounded latest-state table without a prior-value read. Source DML is batched SQL, not Falcon's full per-tuple implementation. Graph application, preconditions, leases, retention DDL, deployment network latency, and managed-service storage/HA are excluded.

The 37 measured runs committed and applied **3,040,000 transactions**, representing **21,040,000 source row writes**. Individual timed runs lasted 10.0–60.6 seconds, with about 960 seconds of combined measured time. These totals exclude container setup, single-iteration calibration checks, and rolled-back attempts.

Saturation cases contain 100,000 committed and applied transactions each, repeated three times. Fixed-rate checks contain 30,000 transactions each; hot-key checks contain 100,000 each; fault checks contain 20,000 each. Fixed-rate/hot-key/fault cases are single sustained samples. Fixed counts avoid benchmark-duration calibration and keep comparisons at equal data volume. Medians below summarize individual run statistics, not pooled percentiles. Pipeline commit timing includes append; write/operation timing is comparable across paths. Fixed-rate arrival-to-apply latency includes intended-arrival scheduling delay and retries.

## Saturation, 16 writers

Medians of three 100,000-transaction runs per case. Latency starts when the writer begins the operation and ends when the consumer validates and applies the complete transaction.

| Path | 1 × 256 B tx/sec | 16 × 1 KiB tx/sec | Small p99 apply ms | Batch p99 apply ms |
| --- | ---: | ---: | ---: | ---: |
| Counter journal, pipelined | 3,584 | 1,719 | 25.63 | 61.02 |
| Sequence journal, ordinary append/commit | 6,368 | 2,867 | 12.90 | 24.89 |
| Sequence journal, pipelined | 8,176 | 3,130 | 12.30 | 25.23 |
| Logical replication slot | 7,961 | 4,653 | 2.84 | 16.73 |

The sequence pipeline increased throughput by **2.28× for small writes and 1.82× for larger batches** compared with the counter pipeline. Small-write sequence throughput was approximately the same as the slot baseline on this machine. For larger payloads, the extra durable journal remained an appreciable cost: the slot baseline was faster and had lower application latency. These are contemporaneous controls, replacing comparisons against earlier samples with different data volumes.

All twelve healthy sequence runs completed without sentinel fills, repair lock timeouts, or writer retries. They did observe transient out-of-order visibility: 121–394 gap observations per small-write run and 207–1,133 per larger-batch run. Those holes resolved before repair. Pipelined sequence throughput ranged from 7,974–8,198 small transactions/sec and 2,998–3,317 larger transactions/sec. The larger slot runs varied more, from 3,661–4,981/sec; no wait profiling was collected to attribute this variation.

Healthy sequence remaining catch-up p99 was about 10.2 ms for small writes and 13.0 ms for batches. The 10 ms poll interval contributes directly to those tails. A notification hint could improve healthy wakeup latency; this sequence variant does not benchmark notifications.

## Equal offered load, small writes

Single 30,000-transaction sample per path/rate, with 16 writers. All cases sustained approximately their offered rate. Arrival-to-apply includes writer scheduling delay, not just operation execution.

| Offered tx/sec | Path | p50 arrival-to-apply ms | p99 arrival-to-apply ms | p99 scheduling lag ms |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Sequence pipeline | 6.629 | 10.870 | 0.127 |
| 1,000 | Counter pipeline | 5.979 | 19.532 | 4.983 |
| 1,000 | Logical slot | 0.795 | 2.421 | 0.159 |
| 3,000 | Sequence pipeline | 5.732 | 10.726 | 0.062 |
| 3,000 | Counter pipeline | 6.024 | 23.722 | 11.996 |
| 3,000 | Logical slot | 0.834 | 1.794 | 0.073 |

The sequence path had little writer queueing at either rate. Its healthy tail mostly reflects polling and consumer work. The counter approached its small-write saturation capacity at 3,000/sec and showed more scheduling lag; its individual scheduling and operation percentiles must not be added to infer total latency. The measured arrival-to-apply percentile already captures their combined effect. These measure transport application, not end-to-end authorization reads or graph application.

## Gap faults, one per 1,000 operations

Single 20,000-transaction sample per fault at 1,000 offered small writes/sec, with ordinary sequence append/commit and the same cursor reader. Every operation eventually committed and applied. There are 20 injected faults per run, or 0.1% of requested writes.

| Fault | Committed tx/sec | Applied tx/sec | p99 arrival-to-apply ms | Fills | Repair lock timeouts | Repaired-position retries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Roll back an appended transaction; retry | 1,000 | 994.5 | 102.21 | 20 | 0 | 0 |
| Pause 200 ms after allocation, before insert | 990 | 989.8 | 99.25 | 19 | 0 | 19 |
| Pause 200 ms after insert, before commit | 990 | 989.4 | 191.55 | 0 | 150 | 0 |

The rollback case recorded 20 deliberately aborted attempts, distinct from repaired-position retries. Pre-insert repair won 19 times, forcing 19 full transaction retries. Post-insert repair never won: all 150 attempts timed out waiting on the uncommitted unique index entries, and the reader resumed when the writers committed. The final paused operation has no later committed row to expose its missing position, accounting for the 19 observed gaps in the pause cases. Its 200 ms pause also extends writer drain at the end of those fixed-count runs; the approximately 990/sec averages should not be interpreted as a steady-state capacity ceiling.

Write duration p99 remained about 0.93–1.00 ms in these runs: the slow writers themselves were only 0.1% of operations. Yet a single scalar prefix made many unrelated committed transactions wait, raising application p99 by roughly an order of magnitude. A smaller repair grace would help abandoned/pre-insert holes, with a different retry tradeoff. It cannot resolve already-inserted transactions by itself. Enforced transaction deadlines or a different consistency-token model are needed to bound that case.

## Latest-state key contention

Single 100,000-transaction sample per case, with 16 writers and one 256-byte source row per transaction. The source row is upserted by key without a preceding SELECT or previous-version condition; only the journal has history. Each run verifies that the source contains exactly the intended 256 rows or one row after all writes.

| Latest-state keys | Path | tx/sec | p99 operation-to-apply ms |
| ---: | --- | ---: | ---: |
| 256 | Sequence pipeline | 8,275 | 12.24 |
| 256 | Counter pipeline | 3,617 | 25.06 |
| 1 | Sequence pipeline | 2,322 | 70.12 |
| 1 | Counter pipeline | 2,201 | 66.31 |

With 256 keys, removing the counter retained the throughput benefit. With one hot key, both paths were limited by source-row serialization and had similar throughput and larger tails. This does not show that reading a version would itself cause contention: the source write already needs the same key's conflicting lock. The global counter adds contention between otherwise independent keys; a same-key version condition does not automatically recreate that global bottleneck. These are illustrative single checks rather than repeated capacity estimates.

The sequence approach is a strong candidate for throughput with the proposed latest-state-plus-journal model. Consistent-read tails remain sensitive to sparse slow transactions because of the single sealed prefix. For tight latency targets, preserve late allocation, use CACHE 1, limit transaction lifetimes, and evaluate the grace/polling policy against the actual workload. WAL had the lowest measured healthy latency; these experiments do not establish a bounded sequence-journal read SLO.

## Validation and reproduction

[Benchmark implementation](../sequence_journal_bench_test.go), [shared harness](../replication_bench_test.go), and [methods and commands](../benchmark.md#sequence-journal-with-gap-repair).

Correctness tests cover rollback holes, recovery across 128-row pages, restart, source rollback after a repaired-position conflict, full retry, blocking on an uncommitted unique entry, out-of-order commit replay, same-key upsert ordering, and delete/reinsert ordering. Every measured consumer validates all payload checksums and complete transaction counts and rejects duplicate application. Benchmark success requires every requested operation to have committed and applied.

The complete repository suite passed with `go test ./... -count=1 -timeout=5m`. The sequence protocol, key-order, and counter-recovery tests passed under `go test -race`, including after the fixed-rate timing and latest-state row-count checks were added. `gofmt` and `git diff --check` passed.

Raw measurements: [saturation](sequence-saturation.txt), [equal offered rate](sequence-at-rate.txt), [key contention](sequence-keys.txt), [gap faults](sequence-faults.txt), and [all 37 samples as CSV](sequence-samples.csv).
