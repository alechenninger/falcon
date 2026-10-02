# Application change log measurements — October 1, 2026

The application-maintained log worked correctly under sustained concurrent writes, but the global counter substantially reduced throughput relative to logical replication. Pipelining the append and commit helped: small-transaction capacity increased by 72% over ordinary journal polling. Increasing writer concurrency to 64 did not increase capacity in the sampled checks and made tail latency worse.

## Setup and scale

- Apple M4 Pro host, Go 1.27.1 on darwin/arm64, default GOMAXPROCS=14.
- PostgreSQL 18.6 Alpine in a six-vCPU Podman VM. Each database container capped at 4 GiB; shared_buffers=1GB, effective_cache_size=3GB, work_mem=4MB, max_wal_size=4GB.
- fsync=on, synchronous_commit=on, full_page_writes=on, wal_level=logical. Testcontainers normally disables fsync; that was discovered in smoke testing and explicitly overridden before these measurements. The smoke results are excluded.
- One consumer, indexed cursor fetches of up to 128 complete transactions, 10 ms polling reconciliation, checksum and consecutive-position validation. Notifications are wakeups, not an input list of transaction IDs.
- Every path writes the same append-only source rows. Journal variants also write a durable counter update and one complete binary log entry per transaction. Journal TOAST compression is disabled. Source insertion is batched SQL; this does not model the current store's per-tuple SQL calls or its complete authorization write workload.
- Main saturation and pipeline comparisons: three measured repetitions per case, `-benchtime=20s`. Fixed-rate checks: one repetition per case at 1,000 transactions/sec, `-benchtime=20s`. 64-writer checks: one repetition per case, `-benchtime=15s`.
- 44 measured runs, 2,536,835 committed/applied transactions and 17,904,665 source rows. Measured runs lasted 17.7–41.6 seconds and contained 23,977–189,067 transactions; combined measured time was 1,126 seconds. These totals exclude calibration and smoke runs.

Each measured invocation starts with fresh tables and a fresh database container. Data grows throughout the run. Source payloads are synthetic: one 256-byte row or sixteen 1-KiB rows per transaction, rather than Falcon's compact tuple representation. The measurements exclude graph application, preconditions, retention DDL, leases/checkpoints, multiple consumers, and deployment network latency. External databases can be selected with `FALCON_REPLICATION_BENCH_DATABASE_URL`; their configuration is left unchanged.

## Saturation with 16 writers

Values are medians across three independent measured invocations. Each p99 is computed within a run, then summarized by its median; these are not pooled percentiles. Throughput is committed transactions/sec. Latency runs from the writer starting an operation to the consumer validating and applying its complete transaction.

| Path | 1 × 256 B tx/sec | 16 × 1 KiB tx/sec | Small p99 apply ms | Batch p99 apply ms |
| --- | ---: | ---: | ---: | ---: |
| Logical replication | 7,796 | 3,705 | 2.95 | 23.87 |
| Counter only + logical replication | 2,007 | 1,682 | 32.42 | 43.17 |
| Counter + journal, polling | 2,044 | 1,307 | 37.38 | 63.22 |
| Counter + journal, transactional NOTIFY | 1,810 | 1,077 | 35.78 | 71.13 |
| Counter + journal, post-commit NOTIFY | 1,746 | 1,008 | 44.93 | 76.60 |
| Counter + journal, pipelined append/commit, polling | 3,515 | 1,920 | 26.36 | 64.25 |

Adding only the counter to the logical-replication baseline reduces small-transaction throughput from 7,796 to 2,007/sec. Ordinary journal polling is nearly identical at 2,044/sec. This isolates commit serialization and the extra counter statement as the dominant small-transaction penalty in this implementation, rather than journal fetching alone.

The pipeline sends the counter/append statement and COMMIT together after source writes, removing the client round trip between append completion and sending commit. Small-transaction throughput was consistent at 3,464–3,533/sec. Large-transaction throughput was more variable at 1,410–2,310/sec; its 1,920/sec median is a useful observation, not a precise capacity estimate. The experiment does not isolate the cause of that variability.

Transactional notification is folded into the append statement, avoiding another round trip under the counter lock. Its small-row saturation consumer fetched approximately one transaction per query, compared with about 20 for ordinary polling and 35 for pipelined polling. Post-commit notification moves its extra round trip outside the counter lock but adds aggregate writer work. Neither notification variant increased saturation capacity here.

## Offered load of 1,000 transactions/sec, 16 writers

All selected paths achieved approximately 1,000 committed and applied transactions/sec. These are single sustained checks per case, not repetition medians. Scheduling lag is the delay from the intended arrival time until a writer begins the operation. Operation-to-apply latency excludes that scheduling delay; the separate percentiles must not be added to estimate a combined p99.

| Workload | Path | p50 operation-to-apply ms | p99 operation-to-apply ms | p99 scheduling lag ms |
| --- | --- | ---: | ---: | ---: |
| 1 × 256 B | Transactional NOTIFY journal | 1.009 | 14.115 | 29.810 |
| 1 × 256 B | Pipelined polling journal | 6.316 | 11.250 | 1.840 |
| 1 × 256 B | Logical replication | 0.710 | 1.731 | 0.196 |
| 16 × 1 KiB | Transactional NOTIFY journal | 1.359 | 14.728 | 42.987 |
| 16 × 1 KiB | Pipelined polling journal | 6.513 | 14.516 | 16.951 |
| 16 × 1 KiB | Logical replication | 0.817 | 2.857 | 0.071 |

Notification lowers median latency compared with 10 ms polling. Both journal strategies still showed occasional writer scheduling delays; matching the average offered rate does not establish a tight latency bound. Logical replication had lower tail latency and much less scheduling lag in these checks.

## Concurrency check: 64 writers, small transactions

| Path | tx/sec | p99 operation-to-apply ms |
| --- | ---: | ---: |
| Ordinary polling journal | 1,849 | 164.90 |
| Pipelined polling journal | 3,297 | 91.59 |

For comparison, the 16-writer medians were 2,044/sec and 37.38 ms for ordinary polling, and 3,515/sec and 26.36 ms for pipelined polling. This single higher-concurrency check added queueing without raising capacity. More writers are not a solution to the global counter bottleneck on this topology.

## Implementation and validation

See [benchmark methods and commands](../benchmark.md#application-change-log-benchmark), [journal benchmark implementation](../journal_bench_test.go), and [shared harness](../replication_bench_test.go).

The recovery test verifies rollback of a counter allocation and log append, concurrent writers with request IDs unrelated to log order, recovery without notifications, pagination across two full 128-transaction pages, a pipelined commit, and rejection of a missing position. All measured consumers reject duplicates, gaps, reordered positions, incomplete payloads, and checksum mismatches. `go test ./... -count=1 -timeout=5m` and `go test -race ./internal/infrastructure/postgres -run '^TestJournalBenchCursorRecovery$' -count=1 -timeout=3m` passed.

Raw measurements: [saturation and counter control](change-log-saturation.txt), [pipelined commit](change-log-pipelined.txt), [fixed offered load](change-log-at-rate.txt), [64-writer concurrency](change-log-concurrency.txt), and [all individual samples as CSV](change-log-samples.csv).

To repeat the measured selections:

```bash
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkChangeLogCounterControl$' -benchtime=20s -count=3 -timeout=30m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkChangeLogTransport$/Writers16/(SingleRow|Batch16)/(JournalPoll|JournalNotifyInTransaction|JournalNotifyAfterCommit|LogicalSlot)$' -benchtime=20s -count=3 -timeout=30m
go test ./internal/infrastructure/postgres -run '^TestJournalBenchCursorRecovery$' -bench '^BenchmarkChangeLogPipelined$' -benchtime=20s -count=3 -timeout=10m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkChangeLogTransportAtRate$/Writers16At1000PerSec/(SingleRow|Batch16)/(JournalNotifyInTransaction|JournalPollPipelined|LogicalSlot)$' -benchtime=20s -count=1 -timeout=10m
go test ./internal/infrastructure/postgres -run '^$' -bench '^BenchmarkChangeLogTransport$/Writers64/SingleRow/(JournalPoll|JournalPollPipelined)$' -benchtime=15s -count=1 -timeout=5m
```
