# Snapshot idle timer and notification batching — October 1, 2026

This experiment changes the previous [coalesced notification benchmark](snapshot-wakeups.md) in two steps: replace its independent 10 ms ticker with a 100 ms inactivity timer, then optionally wait 2 ms in the background notification sender to collect more commits. The snapshot protocol, source+journal transactions, durable consumer epochs, and one-slot notification queues remain unchanged. This is benchmark code, not a production Store change.

## How the changes work

The [poller](../snapshot_idle_bench_test.go#L33) offers two modes. The existing `Ticker10ms` control runs independently of notifications. `Idle100ms` stops the timer before retrieval and starts a fresh 100 ms countdown after retrieval and publication finish. Every retrieval counts: a notification-triggered scan, an empty scan, and an immediate catch-up scan all reset the countdown. Full pages still continue immediately; they do not wait for the timer. A commit hint arriving during retrieval remains pending for the next cut, as before.

This makes polling an inactivity fallback. Continuous notification-triggered retrievals can keep it from firing entirely. With no notifications or source traffic, the consumer scans every approximately 100 ms plus retrieval time. A missed notification does not lose journal data. If other notifications cause fresh scans, they also discover the missed commit. If all traffic stops after an unnotified commit, its discovery waits for the remaining fallback interval and the next retrieval. The interval is not a strict apply-latency bound under consumer or database delays.

The optional `Idle100msBatch2ms` variant adds a tunable wait outside source transactions. After the sender takes its first hint, it waits a configured 2 ms, consumes any additional pending hint, and sends one SQL NOTIFY. New commits do not extend that window: this is bounded batching, not a wait for traffic to become quiet. Commits during the send round trip can enqueue another hint for the next notification. Writers do not sleep or wait for delivery. Each writer process would maintain its own bridge/window; this benchmark has one bridge for 16 writer goroutines.

The batching window controls notification frequency, not epoch size directly. The consumer independently captures its next snapshot and may already be retrieving because of a previous notification or catch-up. The experiment therefore records mean transactions per epoch, sender notification counts, total snapshot retrievals, and fallback wakeups rather than assuming one notification maps to one epoch.

The [consumer](../snapshot_journal_bench_test.go#L306) uses a timer whose stale values are excluded by the module's supported Go version. See [Go timer Reset semantics](https://pkg.go.dev/time#Timer.Reset). Neither this timer nor the previous ticker spins on CPU; the change reduces timer wakeups and database retrievals.

## Database cleanup and measurement setup

The first idle-only attempt exposed a cleanup context bug in the new harness. Its workload context was canceled before the test container termination used that same context. Those four temporary containers were explicitly removed, and that attempt's measurements were discarded. The [shared setup helper](../hydrate_bench_test.go#L403) now terminates containers using an independent context with a 30-second timeout and fails a test/benchmark on termination errors. A regression test cancels its workload before cleanup and verifies the old database is no longer reachable with an independent connection context.

Before sustained measurements, the entire Podman container inventory was empty. Measurements run sequentially, with a fresh database container for each sample and calibration, and cleanup checked between benchmark groups. No other PostgreSQL benchmark containers are intentionally kept running. The final inventory is checked after tests too.

The database configuration matches [the previous measurements](snapshot-wakeups.md#measurement-setup): Apple M4 Pro client, six-vCPU/6-GiB Podman VM, PostgreSQL 18.6 arm64 in a 4-GiB container, shared_buffers=1GB, effective_cache_size=3GB, work_mem=4MB, max_wal_size=4GB, with fsync, synchronous_commit, and full_page_writes enabled. There is one epoch producer, one NOTIFY bridge with two dedicated connections, 16 writers, and 128-record fetch pages.

Healthy comparisons pipeline append and COMMIT. Fault comparisons use ordinary append/commit. Small writes carry one 256-byte row; larger transactions carry sixteen 1-KiB rows. Fixed-rate latency begins at intended arrival, including writer scheduling delay. Apply completion includes durable epoch persistence and complete payload validation. Actual Falcon graph application, retained-history eviction, multiple bridges, reconnect/failover, and deployment RTT are excluded.

Saturation samples use 100,000 transactions each, repeated three times. Fixed-rate samples use 30,000 each, repeated three times per offered rate and policy. Fault samples use 20,000 each once per case. Medians are medians of individual run statistics, not pooled percentiles.

## No-write scan frequency

One three-second measured interval per policy, with a live notification bridge but no source writes. Startup and one-iteration calibration are excluded.

| Policy | Idle scans/sec | Poll wakeups in measured interval |
| --- | ---: | ---: |
| Ticker10ms | 99.99 | 300 |
| Idle100ms | 9.32 | 28 |

Idle database retrieval frequency fell about 90.7%. The reset timer waits 100 ms after each retrieval completes, so elapsed scan time and timer scheduling make the observed rate slightly lower than ten per second. This is a short functional check of idle frequency, not a CPU utilization measurement.

## Healthy equal offered load

Every policy below uses coalesced PostgreSQL NOTIFY after successful commit. Each row is the median of three sustained 30,000-transaction runs.

| Offered tx/sec | Policy | p50 arrival→apply ms | p99 arrival→apply ms | p99 range ms | Mean tx/epoch | Fallback wakeups | Notifications sent | Retrievals |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 | Ticker10ms | 2.293 | 3.644 | 3.362–5.508 | 1.290 | 2,980 | 29,934 | 23,267 |
| 1000 | Idle100ms | 2.347 | 3.867 | 3.548–4.428 | 1.331 | 0 | 29,962 | 22,544 |
| 1000 | Idle100msBatch2ms | 3.439 | 5.803 | 4.904–8.442 | 2.989 | 0 | 10,054 | 10,037 |
| 3000 | Ticker10ms | 4.544 | 8.037 | 7.178–106.598 | 7.831 | 933 | 26,173 | 3,835 |
| 3000 | Idle100ms | 4.506 | 8.637 | 8.374–9.899 | 7.736 | 0 | 26,419 | 3,879 |
| 3000 | Idle100msBatch2ms | 3.743 | 10.006 | 6.157–26.382 | 7.189 | 0 | 4,189 | 4,174 |

All paths sustained approximately their offered rate. Both idle-timer variants had zero fallback wakeups in every healthy measured run. The old ticker had about 99 poll wakeups/sec at 1,000 offered writes/sec and 93/sec at 3,000. Eliminating those wakeups did not eliminate that many complete retrievals: the timer-only policy reduced the median retrieval count by about 3.1% at 1,000/sec and increased it about 1.1% at 3,000/sec. The old timer was often initiating a useful retrieval that notifications now initiate instead.

Timer-only healthy p99 medians were 3.867 versus 3.644 ms at 1,000/sec, and 8.637 versus 8.037 ms at 3,000/sec. The ranges overlap at 1,000/sec. The 3,000/sec ticker control had one 106.598-ms p99 outlier; raw samples retain this result, and no wait/CPU profiles were collected to attribute it. The medians do not establish a statistically significant latency difference. The clear effect is the change in fallback wakeup frequency.

At 1,000/sec, the 2 ms batch window increased mean epoch size from 1.331 to 2.989 (about 2.25×), and cut notification count from 29,962 to 10,054 (66.4%). Median p50 rose about 1.09 ms and p99 about 1.94 ms relative to timer-only. This is the expected batching tradeoff at a light load.

At 3,000/sec, notification count fell from 26,419 to 4,189 (84.1%), but mean epoch size fell slightly from 7.736 to 7.189 rather than rising. Median p50 improved from 4.506 to 3.743 ms, while p99 worsened from 8.637 to 10.006 ms; batching p99 ranged from 6.157 to 26.382 ms. Fewer notifications do not guarantee larger epochs or a better tail: the producer also depends on consumer service time and when cuts are captured. Scheduling p99 medians were below 0.09 ms for every case; all original per-run metrics remain in the raw logs.

## Maximum throughput

Each row reports the median of three sustained 100,000-transaction runs with 16 continuously active writers. Latency starts at operation execution. Paths achieve different saturation rates; the fixed-rate table is the comparison at equal offered load.

| Payload / transaction | Policy | Tx/sec | Tx/sec range | p99 operation→apply ms | p99 range ms | Mean tx/epoch | Notifications | Fallback wakeups |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SingleRow | Ticker10ms | 7,206 | 7,107–7,210 | 11.747 | 11.074–12.286 | 29.76 | 23,961 | 1,112 |
| SingleRow | Idle100ms | 7,272 | 7,235–7,366 | 11.181 | 10.596–11.253 | 29.78 | 23,796 | 0 |
| SingleRow | Idle100msBatch2ms | 7,612 | 7,558–7,672 | 12.348 | 12.334–13.481 | 29.90 | 5,021 | 0 |
| Batch16 | Ticker10ms | 3,013 | 3,002–3,034 | 38.889 | 37.150–47.674 | 29.65 | 25,222 | 1,605 |
| Batch16 | Idle100ms | 2,999 | 2,923–3,129 | 44.336 | 36.557–57.425 | 29.52 | 25,878 | 0 |
| Batch16 | Idle100msBatch2ms | 2,341 | 2,226–2,527 | 109.824 | 106.337–126.116 | 28.99 | 9,064 | 0 |

The timer-only change left saturation throughput close to the control: +0.9% for small writes and −0.5% for larger batches. Every idle-timer saturation run had zero fallback wakeups. Complete retrieval counts and mean epoch size were almost unchanged: approximately 3,350–3,400 retrievals and 30 transactions per epoch. Thus removing the independent ticker does not remove an equivalent amount of necessary consumer work under sustained load.

The 2 ms window increased small-write throughput 4.7% relative to timer-only, from 7,272 to 7,612/sec. It cut median notification count 78.9%, from 23,796 to 5,021, but epoch size remained approximately 30. Median p99 rose from 11.181 to 12.348 ms. This supports a modest small-write capacity benefit, not an increase in epoch size at saturation.

Larger-batch batching regressed across all three samples. Median throughput fell 21.9% relative to timer-only, from 2,999 to 2,341/sec, and p99 rose from 44.336 to 109.824 ms. Notification count still fell about 65.0%, from 25,878 to 9,064, while mean epoch size remained about 29. There is no universal batching win in these measurements.

The larger-batch regression extended beyond the notification window: median p99 write completion rose from 17.645 to 43.033 ms, pipeline append/commit from 14.995 to 39.624 ms, and remaining post-commit catch-up from 31.623 to 63.522 ms. These are separate percentile statistics and cannot be summed to reconstruct end-to-end p99. The write path does not sleep for the configured sender delay. No wait, I/O, checkpoint, or CPU profiles were collected, so the measurements do not identify the cause of this regression or distinguish all workload phasing effects from environmental variation.

## Sparse writer faults

One sustained 20,000-transaction run per policy/fault at 1,000 offered small transactions/sec. All policies use ordinary append/commit. Each run injects 20 faults, or 0.1% of operations. These are individual run statistics, not repeated medians.

| Fault | Policy | p99 arrival→apply ms | Max arrival→apply ms | Operations above 50 ms | Fallback wakeups |
| --- | --- | ---: | ---: | ---: | ---: |
| Rollback | Ticker10ms | 3.672 | 15.019 | 0.000% | 1,984 |
| Rollback | Idle100ms | 4.926 | 16.281 | 0.000% | 0 |
| Rollback | Idle100msBatch2ms | 6.346 | 31.070 | 0.000% | 0 |
| BeforeInsert200ms | Ticker10ms | 6.921 | 207.002 | 0.100% | 2,004 |
| BeforeInsert200ms | Idle100ms | 4.496 | 206.282 | 0.100% | 1 |
| BeforeInsert200ms | Idle100msBatch2ms | 5.047 | 207.671 | 0.100% | 2 |
| AfterInsert200ms | Ticker10ms | 4.173 | 205.540 | 0.100% | 2,010 |
| AfterInsert200ms | Idle100ms | 4.574 | 208.385 | 0.100% | 2 |
| AfterInsert200ms | Idle100msBatch2ms | 5.274 | 209.713 | 0.100% | 2 |

All paused-writer samples had exactly 0.100% of operations above 50 ms, matching the deliberately paused operations, with maxima around 205–210 ms. No measured pause spread to unrelated operations above this threshold. No rollback sample had any operation above 50 ms. Every run used zero gap fills, repair lock timeouts, and repaired-position retries; rollback cases recorded exactly 20 aborted attempts.

Idle-timer rollback runs had zero fallback wakeups. Paused-writer runs had one or two idle wakeups during the final writer pause, when no other source traffic remained. This is expected fallback activity rather than polling on top of an actively notified stream. Omitted-hint tests separately verify eventual application with the 100 ms timer. These samples do not measure crash/reconnect recovery or establish fault-case p99 rankings from repeated runs.

## Scale and validation

The 45 sustained data samples committed and applied **2,520,000 transactions**, representing **16,020,000 source row writes**. Measured intervals lasted 10.0–44.9 seconds, totaling **989.9 seconds (16.5 minutes)**. Two additional no-write intervals observed idle scan frequency for three seconds each. Totals exclude setup, benchmark calibration, and aborted attempts. The initial idle-only attempt with failed cleanup is excluded entirely.

All data samples passed exactly-once transaction counts, complete payload checksum checks, and timing metric completeness. Raw-sample analysis verified one signal attempt per committed transaction, enqueued plus coalesced hints equal commits, zero healthy/saturation fallback wakes for both idle policies, and no container termination errors in any included raw log.

Artifacts: [fixed-rate log](snapshot-idle-at-rate.txt), [saturation log](snapshot-idle-saturation.txt), [fault log](snapshot-idle-faults.txt), [no-write log](snapshot-idle-no-writes.txt), and [all data samples as CSV](snapshot-idle-samples.csv).

The [notification tests](../snapshot_wakeup_bench_test.go#L203) cover immediate and 2 ms batched notifications, aborted attempts not signaling, committed visibility at receipt, omitted hints recovered with the 100 ms idle timer, and commits during stable pagination leaving a hint for the next cut. The [cleanup regression](../snapshot_idle_bench_test.go#L194) verifies cancellation cannot leave its old database reachable.

Final validation passed:

- Full repository suite: `go test ./... -count=1` (PostgreSQL package 13.669 seconds), including the canceled-workload cleanup regression.
- Race checks: `go test -race ./internal/infrastructure/postgres -run '^Test(Snapshot|SequenceJournal|Journal|PostgresCleanup)' -count=1` (9.952 seconds).
- Formatting and `git diff --check`.
- The Podman inventory was empty after each sustained benchmark group and after final tests. A mid-run inventory check showed only the current PostgreSQL benchmark container.

## Interpretation

The 100 ms inactivity fallback matches the requested behavior: no extra timer-triggered retrievals during healthy continuous notifications, approximately one-tenth the no-write scan rate, and healthy p99 medians close to the 10 ms control. Its tradeoff is a longer discovery delay for an unnotified commit when no other traffic triggers a scan. It is not a universal throughput optimization, because continuous source traffic still requires frequent retrievals and epoch publication.

Keep the notification batch wait configurable. A 2 ms window reduced notification traffic substantially and increased epoch size at 1,000 writes/sec, at the cost of healthy latency there. It gave a modest small-write capacity gain, but regressed larger-batch saturation and did not increase epoch size at higher load. These results support choosing a delay for a measured workload and latency budget, not enabling 2 ms universally.

Reproduction commands and metric definitions are in [benchmark.md](../benchmark.md#snapshot-idle-timer-and-notification-batch-window).
