# Snapshot journal with coalesced post-commit wakeups — October 1, 2026

This experiment changes only how the existing snapshot consumer wakes. The snapshot-delta query, stable repeatable-read pages, durable consumer epoch manifests, write pipeline, and 10 ms periodic polling remain unchanged. Writers do not produce snapshots or obtain epoch locks. This is benchmark code, not a production Store implementation.

## How wakeups work

After a source+journal transaction commits successfully, the [commit hook](../replication_bench_test.go#L722) makes a nonblocking attempt to enqueue one hint. A capacity-one queue coalesces commits while a hint is pending; there is no intentional batching delay. A hint requests a fresh scan, and carries no journal position, epoch, or consistency token.

Three paths use the same protocol:

- **Poll:** original 10 ms ticker, with no commit hint.
- **LocalWake:** the consumer directly receives the one-slot application hint. This isolates wakeup benefit within one process.
- **NotifyWake:** a background sender consumes application hints and sends `pg_notify` using a dedicated connection, after the source transaction has committed. A separate dedicated LISTEN connection delivers into a capacity-one consumer queue. This exercises the database-mediated process boundary, although all benchmark goroutines run in one client process. Writers neither send SQL notifications inside their transaction nor wait for notification delivery.

The [sender](../snapshot_wakeup_bench_test.go#L62) consumes its hint before the notification round trip. Commits during that round trip enqueue another hint. The [consumer](../snapshot_journal_bench_test.go#L309) consumes pending hints before capturing its next snapshot, including before immediate catch-up scans. It never clears pending hints after a scan: that could erase a commit that was too late for the captured cut. LISTEN commits before the initial scan, following [PostgreSQL's startup rule](https://www.postgresql.org/docs/current/sql-listen.html).

There is one notification bridge for this benchmark's 16 writer goroutines, modeling a single writer process. A deployment with several writer processes would have one independent bridge per process; the queues do not coalesce across processes. Epoch production remains a single consumer responsibility. Multiple bridges and epoch-producer failover are not exercised.

Application queues perform the coalescing. PostgreSQL itself does not fold identical notifications from different transactions; see [NOTIFY semantics](https://www.postgresql.org/docs/current/sql-notify.html). A low arrival rate can therefore produce nearly one notification per write. The separate notification transaction still costs a SQL round trip and database work. Sender/listener setup completes before timing; their runtime work is included. Each bridge adds two dedicated connections outside the writer pool.

## Polling and failure behavior

The 10 ms periodic ticker remains active independently of notifications. An idle consumer still attempts approximately 100 database scans/sec; successful hints do not reset this ticker. It sleeps between wakeups rather than spinning on CPU. Under load, notifications can add scans and smaller epochs instead of reducing database work. Full-page epochs continue immediately, as in the original protocol.

Notifications are freshness hints. The journal and snapshots establish correctness. A process can crash after committing but before sending its hint; the next fallback scan still discovers the row. The tests deliberately omit hints and verify recovery, and commit another write during pagination to verify that the next hint survives a stable cut. Notification connection failures deliberately fail the benchmark; production should retain polling while reconnecting and perform a fresh scan after LISTEN registration.

A later experiment can use a longer fallback interval, or an inactivity timer reset after scanning, to reduce idle queries. Increasing that interval increases detection delay for commits whose hints are lost. The interval is not a strict latency bound when scans, epoch commits, or application themselves are delayed. No longer interval is benchmarked here.

## Measurement setup

Same environment as [the earlier snapshot experiment](snapshot-journal.md#measurement-setup): Apple M4 Pro client, six-vCPU/6-GiB Podman VM, PostgreSQL 18.6 arm64 in a 4-GiB container, shared_buffers=1GB, effective_cache_size=3GB, work_mem=4MB, max_wal_size=4GB, with fsync, synchronous_commit, and full_page_writes enabled. Each run starts fresh database objects and a fresh container. There are 16 writers, one epoch producer, and 128-record fetch pages.

Healthy comparisons use append+commit pipelining. Fault comparisons use ordinary append and commit for all policies. Small writes carry one 256-byte row; batch writes carry sixteen 1-KiB rows. Fixed-rate arrival latency includes writer scheduling delay. Every committed transaction is checked for exactly-once application and full payload checksum integrity. Latency ends after durable epoch commit and payload validation; actual Falcon graph updates, ID mapping, retention DDL, failover, and deployment RTT are excluded.

Saturation uses 100,000 transactions per run, repeated three times. Healthy fixed-rate comparisons use 30,000 per run, repeated three times at each offered rate. Fault comparisons use 20,000 per run once per case. Medians are medians of individual run statistics, not percentiles pooled across runs. Fault checks are sustained single samples, and do not establish stable tail percentiles.

## Healthy equal offered load

Each row reports the median of three sustained 30,000-transaction runs. All paths sustained their offered rate.

| Offered tx/sec | Wake policy | p50 arrival→apply ms | p99 arrival→apply ms | p99 range ms | Mean tx/epoch |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1000 | Poll | 7.329 | 12.726 | 12.013–14.690 | 10.01 |
| 1000 | LocalWake | 2.139 | 3.672 | 3.447–6.796 | 1.21 |
| 1000 | NotifyWake | 2.336 | 4.156 | 3.519–4.484 | 1.32 |
| 3000 | Poll | 7.484 | 12.632 | 12.561–14.929 | 30.00 |
| 3000 | LocalWake | 3.494 | 6.534 | 6.150–9.611 | 6.04 |
| 3000 | NotifyWake | 4.390 | 7.711 | 6.672–10.831 | 7.51 |

At 1,000/sec, median p99 fell 71.1% with local wakeups and 67.3% with PostgreSQL notifications. At 3,000/sec the reductions were 48.3% and 39.0%. Scheduling p99 medians were below 0.15 ms for every case, so the improvements primarily reflect operation/application latency rather than relief of an offered-load scheduling backlog. Individual run tails still varied, as shown in the ranges.

The cost is more frequent epoch production. Polling grouped approximately 10 or 30 transactions per epoch at these rates; local wakeups grouped 1.21 or 6.04, and PostgreSQL notifications grouped 1.32 or 7.52. Manifest commit and validation remain on the apply path. This optimization removes much of the polling delay, not all consumer work.

At 1,000/sec, the bridge sent a median 29,925 notifications for 30,000 commits (0.25% coalesced at the sender). At 3,000/sec it sent 26,916 (10.28% coalesced). The consumer delivery queue coalesces further while scanning. Periodic poll wakeups still numbered roughly 2,980 over a 30-second notified run and 940 over a 10-second notified run: the fixed ticker continues to do work even when notifications are healthy.

## Maximum throughput

Each row reports the median of three 100,000-transaction runs with 16 continuously active writers. Apply latency starts at operation execution. Saturation paths have different achieved rates; the fixed-rate table is the cleaner comparison of latency at equal arrival rate.

| Payload / transaction | Wake policy | Tx/sec | Tx/sec range | p99 operation→apply ms | p99 range ms | Mean tx/epoch |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SingleRow | Poll | 8,101 | 7,939–8,125 | 18.961 | 18.442–19.636 | 81.04 |
| SingleRow | LocalWake | 7,807 | 7,527–7,853 | 14.262 | 13.534–14.286 | 30.10 |
| SingleRow | NotifyWake | 7,381 | 7,373–7,381 | 10.694 | 10.537–11.243 | 29.50 |
| Batch16 | Poll | 3,043 | 2,799–3,085 | 47.218 | 45.634–69.213 | 33.64 |
| Batch16 | LocalWake | 2,630 | 2,306–3,104 | 88.191 | 48.148–89.518 | 29.51 |
| Batch16 | NotifyWake | 2,558 | 2,356–2,884 | 88.518 | 52.190–109.147 | 29.39 |

For small writes, local wakeups retained 96.4% of polling throughput and the PostgreSQL bridge retained 91.1%, while both reduced saturation p99. The bridge sent a median 23,959 notifications for 100,000 commits, coalescing 76.0% of source hints at its sender. For larger batches it sent 24,660, coalescing 75.3%. Consumer-side coalescing further limits snapshot scans.

Larger-batch wakeups did not show a dependable benefit. Local and PostgreSQL paths retained 86.4% and 84.1% of polling throughput in these run medians, and their p99 medians rose from 47.2 ms to about 88 ms. Individual large-batch throughput and latency varied considerably, including a local run at 3,104/sec and 48.1 ms p99. These samples support the healthy small-write latency improvement, but do not support a general claim of improved latency under heavy payload saturation. No wait, I/O, checkpoint, or CPU profiles were collected to assign the large-batch variation to a specific cause.

Notified saturation epochs averaged approximately 30 transactions, versus approximately 81 small or 34 larger transactions with polling. Notifications increase the frequency of small-workload manifest commits. They do not eliminate page transfer, validation, epoch persistence, or shared database resource costs.

## Sparse writer faults

One sustained 20,000-transaction run per cell at 1,000 offered small transactions/sec, ordinary append/commit for every policy. Each run injects 20 faults (0.1% of operations). The figures are individual run statistics, not repeated medians.

| Fault | Wake policy | p99 arrival→apply ms | Max arrival→apply ms | Operations over 50 ms |
| --- | --- | ---: | ---: | ---: |
| Rollback | Poll | 11.833 | 103.675 | 0.470% |
| Rollback | LocalWake | 5.895 | 19.700 | 0.000% |
| Rollback | NotifyWake | 5.736 | 21.226 | 0.000% |
| BeforeInsert200ms | Poll | 12.285 | 215.448 | 0.100% |
| BeforeInsert200ms | LocalWake | 4.704 | 205.388 | 0.100% |
| BeforeInsert200ms | NotifyWake | 4.763 | 208.165 | 0.100% |
| AfterInsert200ms | Poll | 12.037 | 213.295 | 0.100% |
| AfterInsert200ms | LocalWake | 8.172 | 210.128 | 0.100% |
| AfterInsert200ms | NotifyWake | 3.599 | 205.185 | 0.100% |

All pause runs had exactly 0.100% of operations above 50 ms, matching the deliberately delayed writes, with maxima approximately 205–215 ms. There was no measured spread of those pauses to unrelated operations above that threshold. All policies used zero gap fills, repair lock timeouts, and repaired-position retries. Rollback runs recorded exactly 20 aborted attempts, while notified policies still made exactly 20,000 signal attempts: failed attempts did not notify.

The polling rollback sample had a 103.7-ms maximum and 0.47% above 50 ms despite no intentional writer pause. No profiles were collected to attribute that outlier; the snapshot protocol does not wait on rollback sequence holes. Single fault samples cannot establish a stable p99 ranking between local and PostgreSQL wakeups. Throughput remained approximately 1,000/sec for rollbacks and 990/sec for pauses; the final injected pause extends the fixed-count run, so the latter is not a capacity limit.

## Scale, validation, and artifacts

The 45 measured runs committed and applied **2,520,000 transactions**, representing **16,020,000 source row writes**. Measured runs lasted 10.0–43.4 seconds, totaling **988.9 seconds (16.5 minutes)**. These totals exclude startup, single-iteration benchmark calibration, and aborted attempts.

Raw samples: [saturation](snapshot-wakeups-saturation.txt), [fixed-rate](snapshot-wakeups-at-rate.txt), [faults](snapshot-wakeups-faults.txt), and [all samples as CSV](snapshot-wakeups-samples.csv). Every run passed the complete-transaction count, checksum, duplicate-application, and metric completeness checks. Raw-sample analysis also verified that notified runs made exactly one hint attempt per committed transaction and that enqueued plus coalesced hints equal commits.

The [integration tests](../snapshot_wakeup_bench_test.go#L203) exercise successful-commit-only signaling, committed visibility at receipt, omitted hints recovered by polling, and a commit during pagination whose hint survives to the next epoch. The original snapshot protocol tests continue to cover durable replay, delayed old positions, stable pages, and per-key ordering.

Validation passed:

- Full repository suite: `go test ./... -count=1` (PostgreSQL package 12.186 seconds).
- Race checks: `go test -race ./internal/infrastructure/postgres -run '^Test(Snapshot|SequenceJournal|Journal)' -count=1` (8.407 seconds).
- Formatting and `git diff --check`.

Reproduction commands and queue metric definitions are in [benchmark.md](../benchmark.md#snapshot-journal-with-coalesced-commit-wakeups). The 10 ms fallback is unchanged; the subsequent [idle-timer and batching experiment](snapshot-idle-timer.md) measures a longer activity-reset interval and an optional 2 ms sender window.

