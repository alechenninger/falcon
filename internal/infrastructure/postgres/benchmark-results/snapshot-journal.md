# Snapshot journal with consumer epochs — October 1, 2026

This benchmark explores using PostgreSQL transaction snapshots to discover committed journal batches without repairing missing producer sequence positions. Writers still update a separate latest-state table and append a journal record in the same transaction. Falcon snapshot time would represent consumer epochs, not producer sequence positions. This is a benchmark protocol, not a production Store implementation.

## How the implementation works

This belongs to the same family of protocols as PgQ. Its [batch query implementation](https://github.com/pgq/pgq/blob/master/functions/pgq.batch_event_sql.sql) also discovers events between recorded transaction snapshots using an XID range plus exceptions for previously active transactions. This benchmark uses the modern xid8/pg_snapshot functions and a smaller purpose-built schema; it does not benchmark PgQ itself.

[Schema](../snapshot_journal_bench_test.go#L78) adds an explicit top-level `writer_xid XID8 DEFAULT pg_current_xact_id()` and a `(writer_xid, position)` index to an immutable journal. The existing [writer](../sequence_journal_bench_test.go#L120) finishes its source mutations first, then allocates a sequence position and appends the complete transaction payload. Healthy comparisons pipeline append and commit. Source writes, journal writes, and their XID are committed atomically. The explicit XID column is used for discovery; source row locks and late `CACHE 1` sequence allocation establish ordering for conflicting mutations. XIDs themselves do not establish key mutation or commit order.

The [delta query](../snapshot_journal_bench_test.go#L142) uses two disjoint candidate sets between an old snapshot A and a new snapshot B:

1. Writer XIDs at least A.xmax and below B.xmax.
2. Writer XIDs in A's in-progress list.

The query selects only candidates completed by B according to `pg_visible_in_snapshot`. The [PostgreSQL transaction information functions](https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT) expose top-level XIDs, snapshot bounds, and the active list. Ordinary SQL row visibility supplies the committed-row requirement: aborted and currently uncommitted journal rows are absent. The visibility function alone is not a committed-status test. An XID in the old active list can commit much later; rechecking that exception discovers its journal even when its sequence position is lower than positions previously consumed. Newer XIDs excluded by B.xmax remain candidates in a later range. XIDs identify visibility and are not used to order the returned changes.

[Page fetching](../snapshot_journal_bench_test.go#L156) uses `ORDER BY position LIMIT 128` and a keyset position cursor that resets for each epoch. All pages share one [repeatable-read transaction](../snapshot_journal_bench_test.go#L205), including the snapshot capture. PostgreSQL keeps [that SQL snapshot stable across statements](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-REPEATABLE-READ). This prevents commits between pages from entering half of a recorded cut. The union allows candidate lookup through the XID index, rather than rescanning all retained payloads with a visibility predicate. This benchmark does not collect query plans or establish index scaling beyond the measured table sizes.

A nonempty delta produces a durable manifest containing its scalar epoch number, previous snapshot, current snapshot, and batch count. There is one epoch producer; writers never wait on an epoch counter. The manifest commit and payload validation are included in apply latency. The [publisher](../snapshot_journal_bench_test.go#L273) validates every transaction before recording a shared application-completion time for the entire epoch. A production graph would stage the changes and publish its watermark only after every relevant shard has applied the complete epoch. Intermediate subsets are not promised to represent complete database snapshots.

The timing comparison deliberately includes the extra XID index and durable manifest writes. These costs are absent from the sequence repair baseline, whose cursor is only in memory.

For example, when position 101 is uncommitted but 102 has committed, an epoch can contain 102 and exclude 101. When 101 commits, it is discovered and applied under a later epoch. If it aborts, no row or sentinel is needed. For conflicting actual source mutations, row/uniqueness locking prevents a later mutation on the same key from overtaking the uncommitted one; late sequence allocation preserves their mutation order within an epoch. No application SELECT of a previous version is added. No-op intent ordering and optimistic concurrency semantics require separate decisions, as described in the [sequence report](sequence-journal.md#ordering-and-correctness).

## Recovery, bootstrap, and retention

A durable manifest records an ordering decision, not an applied-graph checkpoint. [Restart](../snapshot_journal_bench_test.go#L129) begins at the caller's last applied epoch. If a manifest committed before application finished, the consumer replays it using the stored old/new cuts. The explicit visibility predicate excludes transactions that committed after that recorded cut, even though a fresh SQL transaction can now see their rows. A batch-count mismatch fails as possible retention loss. An empty cut can advance in-memory discovery without a manifest: replay from an older durable cut rediscovers the empty span safely. Each manifest stores its actual previous discovery cut, including any such empty advances.

Stored `pg_snapshot` values are visibility metadata, not live SQL snapshot handles and do not keep old tuple versions alive. Replay requires an immutable retained journal with explicit top-level XIDs; it does not use commit timestamps or call `pg_xact_status` for old transactions. Initial bootstrap in this experiment starts with empty source tables. Production hydration would capture source state and the discovery cut in the same repeatable-read snapshot, then begin deltas after that cut.

The protocol keeps Falcon's scalar StoreTime and snapshot-window representation by using epoch numbers internally. Falcon's architecture does not require client consistency tokens, and CurrentTime need not be a production API. Neither receipt mapping nor a public freshness-barrier API is a prerequisite for this design. Hydration and replicated progress still need to agree on the internal epoch boundaries.

A graph's applied checkpoint must be persisted or reconstructed independently of manifests. Leader fencing, multi-shard publication, bounded staging for large catch-up epochs, idempotent partial replay, retention leases, and primary failover need production designs. Every measured epoch is buffered in memory even though individual fetch pages are bounded. Long transactions can keep old journal partitions open for retention, even while unrelated apply progress continues. Dropping partitions based only on the largest observed producer position would lose late commits. Neither partition retention nor crash/failover is benchmarked.

All snapshots and journal fetches use the same primary. The tests do not validate using this metadata on physical read replicas or after restore/promotion.

## Measurement setup

Same local environment as the [sequence measurements](sequence-journal.md#benchmark-setup): Apple M4 Pro client, a six-vCPU/6-GiB Podman VM, PostgreSQL 18.6 arm64 in a 4-GiB container, shared_buffers=1GB, effective_cache_size=3GB, work_mem=4MB, max_wal_size=4GB, and fsync/synchronous_commit/full_page_writes enabled. Every invocation starts fresh objects and a fresh container. One consumer, 16 writers, 128-record pages, 10-ms polling, one 256-byte source row or sixteen 1-KiB rows per transaction. The sequence control uses 100-ms repair grace and 10-ms repair lock timeout. No host RAM configuration changes were made.

The 46 measured runs committed and applied **2,740,000 transactions**, representing **16,240,000 source row writes**. Timed runs lasted 10.0–47.6 seconds, with 983.4 seconds (16.4 minutes) of combined measured time. Saturation cases used 100,000 transactions each, repeated three times; fault cases used 20,000 each, repeated three times; fixed-rate cases used 30,000 each once; hot-key cases used 100,000 each once. Totals exclude container setup, single-iteration calibration, and aborted attempts. All consumers check complete transaction counts, payload checksums, and duplicate application. Fixed-rate arrival-to-apply includes scheduling delay and retries. Results exclude actual Falcon graph updates, ID mapping, preconditions, retained-history eviction, deployment RTT, RDS storage/HA, and production applied checkpoints. The logical slot captures only the source table; the journals also incur their extra durable writes. Medians are medians of run statistics, not pooled percentiles.

## Saturation results

Each cell is the median of three sustained 100,000-transaction runs at 16 writers. Apply latency starts at operation execution and ends at complete consumer publication.

| Path | 1 × 256 B tx/sec | 16 × 1 KiB tx/sec | Small p99 apply ms | Batch p99 apply ms |
| --- | ---: | ---: | ---: | ---: |
| Snapshot journal, pipelined | 7,523 | 2,910 | 19.84 | 69.40 |
| Sequence repair, pipelined | 7,856 | 3,199 | 12.45 | 24.72 |
| Logical replication slot | 7,658 | 4,739 | 3.01 | 17.35 |

The snapshot journal retained 95.8% of sequence throughput for small writes and 91.0% for the larger batches. Small throughput ranged from 7,404–7,566/sec; batch throughput from 2,852–3,078/sec. Healthy apply latency was higher: small p99 varied from 19.47–20.24 ms, batch p99 from 39.00–82.33 ms. This implementation preserves concurrent source writes, but it is not a healthy-latency improvement over the sequence cursor or WAL.

Average epochs contained about 74–76 small transactions or 32–33 larger transactions. Each larger epoch transfers and validates roughly half a MiB and includes a separate durable manifest commit. All snapshot saturation runs used zero sentinel fills, repair lock timeouts, or repaired-position retries. No wait/I/O profiling was collected, so the larger-batch tail variation cannot be assigned to a specific database bottleneck. The measurements combine snapshot lookup, page fetching, whole-epoch publication, durable manifest writes, payload/index overhead, and local VM/storage variability.

## Sparse writer faults

Three sustained 20,000-transaction runs per path/fault at 1,000 offered small transactions/sec. Both paths use ordinary append/commit. Each run injects one fault per 1,000 operations, exactly 20 faults (0.1%); retries retain the same operation timing. Values are medians of the three run statistics.

| Fault | Sequence p99 arrival-to-apply ms | Snapshot p99 arrival-to-apply ms | Sequence over 50 ms | Snapshot over 50 ms |
| --- | ---: | ---: | ---: | ---: |
| Rollback then retry | 97.99 | 12.05 | 5.565% | 0.000% |
| 200 ms before journal insert | 100.45 | 12.61 | 6.105% | 0.100% |
| 200 ms after journal insert | 191.59 | 12.61 | 14.490% | 0.100% |

Snapshot rollback p99 ranged from 11.57–12.22 ms, versus sequence repair 95.42–99.80 ms. Snapshot pre-insert pause p99 ranged from 11.73–43.71 ms, versus sequence repair 96.85–100.97 ms. Snapshot post-insert pause p99 ranged from 12.27–15.32 ms, versus sequence repair 191.58–191.62 ms. The pre-insert snapshot outlier also had 0.97% of operations above 50 ms, compared with 0.1% in its other two runs; no wait profiling was collected to attribute that extra variation.

The post-insert comparison is the clearest evidence of removing head-of-line blocking: the median share over 50 ms fell from 14.49% to 0.1%, and p99 fell about 15.2×. Paused writers still took roughly 200 ms to commit and become applicable; their delay stopped amplifying into many unrelated operations. Snapshot run maxima were about 204–211 ms for this fault. A query whose internal snapshot window requires a not-yet-applied epoch must still wait for application to reach it. These results do not promise a bounded foreground-read SLO.

All nine snapshot fault runs used zero gap fills, repair lock timeouts, or repaired-position retries. Each rollback run recorded exactly 20 deliberate aborted attempts before full retries. Sequence rollback controls filled 20 holes each; pre-insert controls filled 19 holes and forced 19 full retries each. Post-insert controls filled no holes and recorded 143–149 repair lock timeouts. The last paused operation has no higher committed position to reveal a gap to sequence repair, explaining 19 rather than 20 observed holes for pause controls.

Every requested operation ultimately committed and applied. Average committed rates remained approximately 1,000/sec for rollback and 990/sec for pause faults on both paths. The final 200-ms pause extends drain in the fixed-count runs; 990/sec is not a steady-state capacity ceiling.

## Healthy equal offered load

Single sustained 30,000-transaction run per case. All paths sustained approximately the offered rate. These checks measure intended-arrival through application and include writer scheduling delay.

| Offered tx/sec | Path | p50 arrival-to-apply ms | p99 arrival-to-apply ms | p99 scheduling lag ms |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Snapshot pipeline | 7.530 | 12.468 | 0.078 |
| 1,000 | Sequence pipeline | 5.838 | 10.985 | 0.104 |
| 1,000 | Logical slot | 0.729 | 3.912 | 0.368 |
| 3,000 | Snapshot pipeline | 7.395 | 13.076 | 0.067 |
| 3,000 | Sequence pipeline | 5.976 | 11.989 | 0.255 |
| 3,000 | Logical slot | 0.952 | 48.676 | 43.377 |

At these loads, snapshot publication added roughly 1–2 ms to healthy p99 compared with sequence repair. A 10-ms polling interval contributes to both paths. Healthy fixed-rate samples also had occasional spikes: the 1,000/sec snapshot run had a 116.82-ms maximum and 0.283% above 50 ms, while the 3,000/sec snapshot run had a 30.02-ms maximum and none above 50 ms. The WAL control had lower median latency but an anomalous 48.68-ms p99 at 3,000/sec in this single sample. This reinforces the need to preserve individual raw samples and distinguish protocol-induced blocking from unprofiled environment/consumer variation; it does not establish that WAL is inherently slower at that rate.

## Latest-state key contention

Single sustained 100,000-transaction run per case, with source UPSERTs and no previous-value SELECT. The benchmark verifies that the source contains precisely the requested latest-state key count afterward.

| Source keys | Path | tx/sec | p99 operation-to-apply ms |
| ---: | --- | ---: | ---: |
| 256 | Snapshot pipeline | 7,751 | 20.01 |
| 256 | Sequence pipeline | 7,709 | 12.48 |
| 1 | Snapshot pipeline | 2,115 | 72.57 |
| 1 | Sequence pipeline | 2,102 | 74.06 |

Both paths retain concurrent throughput across keys. One very hot source row serializes writers regardless of journal discovery method; neither design removes that necessary conflicting-write serialization. These single checks are illustrative, not repeated capacity estimates.

## Interpretation and next optimizations

The snapshot approach directly addresses sparse-writer head-of-line blocking while retaining most of the sequence journal's write throughput. It adds healthy consumer overhead and does not currently outperform WAL. The larger-payload saturation p99 is a material cost, not hidden by the favorable fault results. A deployment decision needs workload-specific freshness requirements and production graph/shard costs.

Candidate follow-ups, not included in these results:

- Separate epoch capture from delivery: persist small snapshot cuts promptly, then let consumers fetch immutable deltas independently. This could avoid making epoch size depend on a slow consumer, at the cost of a central fenced producer and manifest retention.
- Reduce polling or add notification hints to reduce healthy wakeup delay, while retaining snapshot reconciliation.
- Pipeline consumer metadata queries, or encapsulate capture plus manifest work server-side, to reduce round trips. Preserve the capture's relationship to the journal fetch snapshot.
- Remove still-active XIDs from the exception lookup, and consider coalescing nearby exceptions into a range. PgQ's [batch query builder](https://github.com/pgq/pgq/blob/master/functions/pgq.batch_event_sql.sql) provides an implementation reference; expanding the range also requires excluding transactions already visible in the previous cut.
- Use a server cursor or bounded staging for large catch-up epochs. Repeated limited queries can repeat candidate scans/sorts; the current measurements do not evaluate prolonged consumer outages or million-row deltas.
- Profile commit waits, query plans, WAL volume, and storage pressure before tuning the larger-payload path. No RAM A/B or wait attribution was performed here.

## Validation and reproduction

[Implementation and protocol tests](../snapshot_journal_bench_test.go), [shared writer and fault injection](../sequence_journal_bench_test.go), [shared harness](../replication_bench_test.go), and [methods/commands](../benchmark.md#snapshot-journal-with-consumer-epochs).

Protocol tests exercise an unrelated long transaction, uncommitted and aborted journal positions, a late insert at an older position, empty cuts retaining discovery exceptions, stable 128-row pages while transactions commit, manifest replay after publication failure, restart from an applied epoch, and inverted XID/key mutation order. Every measured run verifies complete transaction counts, source/journal payload checksums, and duplicate application. Snapshot consumers completed without sentinel repair or repaired-position retries.

The complete repository suite passed with `go test ./... -count=1 -timeout=5m`. Snapshot, sequence, key-order, and counter-recovery protocol tests passed under `go test -race`. `gofmt` and `git diff --check` passed.

Raw results: [saturation](snapshot-saturation.txt), [fixed-rate](snapshot-at-rate.txt), [writer faults](snapshot-faults.txt), [key contention](snapshot-keys.txt), and [all 46 samples as CSV](snapshot-samples.csv).
