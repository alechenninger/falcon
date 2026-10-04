# Single-request snapshot capture

This experiment compares the existing sequential publisher, a publisher using one atomic SQL statement, and a contemporaneous logical-slot baseline. Production Falcon's Store, MVCC protocol, and graph application are unchanged. The snapshot arms retain one globally ordered epoch timeline and independently progressing root application.

## Implementation

The sequential notified publisher sends `BEGIN REPEATABLE READ`, obtains `pg_current_snapshot()`, checks the guarded indexed existence probe, inserts a nonempty epoch manifest, and commits. A nonempty capture therefore has five sequential command exchanges; an empty capture has four. The instrumented query counter counts SELECT/INSERT calls, rather than BEGIN/COMMIT.

The new publisher sends one statement containing a materialized **single-row cut**, the same bounded existence test, a conditional manifest INSERT, and a result with the snapshot and a publication flag. All subqueries observe one command snapshot. The materialized CTE does not collect the journal delta or payloads. Scalar references to the cut become query parameters for indexed range access; formerly active XIDs retain individually bounded lookups. The statement uses ordinary SQL, without a stored function or trigger.

The call uses an implicit transaction. The client drains the result through transaction completion before handing off the epoch. A result row alone is insufficient proof of commit; the deferred-commit-error protocol test verifies that a failure prevents publication. Empty results preserve the preceding durable boundary and create no manifest. Late commits with older positions are still recovered through the previous snapshot's active-XID set. Payload fetching stays on a separate connection and uses the existing guarded streamed query and per-root FIFO workers.

See [capture statement and client call](../snapshot_capture_bench_test.go), [publisher dispatch](../snapshot_fetch_bench_test.go), [protocol tests](../snapshot_capture_protocol_test.go), and [plan diagnostics](../snapshot_capture_plan_test.go). The measured sources are preserved in a [hash-verified archive](snapshot-capture-source/manifest.json).

## Method

Each invocation starts a fresh PostgreSQL 18 Alpine container in the existing six-vCPU / 6 GiB Podman VM. The container limit is 4 GiB, shared buffers 1 GiB, and work memory 4 MiB; fsync, synchronous commit, and full-page writes remain enabled. The Go client runs on the M4 Pro host. The harness requires empty container inventories before and after every invocation; measured containers never run concurrently.

All arms have sixteen writers and the same latest-state upserts. Snapshot arms append an uncompressed 256 B or 16 KiB transaction journal record, have five consumer connections, two notification connections, four application workers, and a 32 MiB payload queue budget. WAL captures the same source rows without the additional journal. Demand ranks 1,024 roots with weights proportional to rank^-1.05. This is the globally ranked Pareto-shaped workload from the recent streaming experiments, rather than the earlier independently ranked tenant/root distribution.

The primary comparison holds notification spacing at 2 ms, with an immediate first notification after idle and a 100 ms reconciliation fallback. A separate spacing control changes only the minimum spacing to zero. Capacity uses 100k small or 50k large transactions; offered loads last twenty seconds; online rotation lasts forty seconds. Every measured configuration has three shuffled repetitions. History contains two million retained journal entries across 2,048 actual historical transactions, loaded and analyzed before measurement.

Freshness starts at the writer's commit acknowledgement and ends at ordered root readiness after successful fetch EOF. Arrival starts at intended arrival, including writer scheduling and execution. Capture timing covers the entire publisher call, including commit. The sink verifies complete payload checksums, exactly-once delivery, position ordering, and final latest state. It models private staging and root publication, rather than Falcon's graph or durable application recovery. Source/storage/network wait events are not sampled; these runs cannot establish a production tail bound or managed-service capacity.

Normal and race protocol checks cover empty captures, missing notifications, rollback/retry, a lower-position late commit, commits during capture, manifest replay, and failure at implicit commit. Existing pipeline and partition protocols, and the repository suite, also run. Plan diagnostics separately inspect empty/nonempty captures with automatic and forced generic planning, flat/partitioned schemas, and bulk/diverse two-million-row history. Smoke, calibration, protocol, race, suite, and diagnostic executions are excluded from measured tables.

## Results

**96 measured runs, 32 configurations, 3,639,000 source transactions.** Cells are medians of per-run statistics, with min–max ranges across three repetitions. These are not pooled percentiles or confidence intervals.

### Concurrent write capacity

| Arm | 256 B tx/sec | 16 KiB tx/sec | Small saturation fresh p99 (ms) | Large saturation fresh p99 (ms) |
| --- | --- | --- | --- | --- |
| Sequential capture | 7,595 [7,551–7,595] | 3,126 [2,981–3,209] | 6.70 [6.67–6.76] | 22.04 [18.72–24.29] |
| Single-request capture | 7,734 [7,667–7,795] | 3,135 [3,070–3,175] | 3.85 [3.82–3.86] | 12.71 [12.40–13.95] |
| WAL | 7,815 [7,742–7,833] | 5,188 [5,185–5,258] | 0.51 [0.50–0.54] | 4.51 [3.18–4.54] |

### Healthy freshness

All columns are commit-acknowledgement-to-root-readiness p99, in milliseconds. The spacing policy remains 2 ms.

| Arm | 50/sec small | 1k/sec small | 3k/sec small | 1k/sec large |
| --- | --- | --- | --- | --- |
| Sequential capture | 7.52 [7.35–8.07] | 2.95 [2.95–2.96] | 3.65 [3.56–3.71] | 5.26 [4.40–7.08] |
| Single-request capture | 4.55 [4.43–4.92] | 2.55 [2.54–2.55] | 2.73 [2.70–2.74] | 3.67 [2.87–26.32] |
| WAL | 0.44 [0.42–0.49] | 0.12 [0.11–0.13] | 0.14 [0.14–0.16] | 0.59 [0.51–0.66] |

### Arrival and writer latency

Arrival includes offered-load queuing and source execution; it is not just consumer freshness. The existing WAL harness does not record the quiet-root cohort, so those cells are unavailable.

**healthy-1000**

| Arm | Arrival p99 (ms) | Quiet arrival p99 (ms) | Write p99 (ms) | Write max (ms) | Fresh p999 (ms) |
| --- | --- | --- | --- | --- | --- |
| Sequential capture | 3.77 [3.76–3.77] | 3.76 [3.74–3.77] | 0.89 [0.88–0.89] | 11.31 [10.69–11.62] | 3.43 [3.41–3.81] |
| Single-request capture | 3.49 [3.38–3.71] | 3.48 [3.38–3.72] | 1.14 [0.92–1.29] | 14.30 [8.97–39.36] | 4.16 [2.76–7.92] |
| WAL | 1.22 [1.01–1.43] | — | 1.08 [0.92–1.32] | 20.27 [9.92–63.94] | 0.19 [0.17–0.61] |

**healthy-3000**

| Arm | Arrival p99 (ms) | Quiet arrival p99 (ms) | Write p99 (ms) | Write max (ms) | Fresh p999 (ms) |
| --- | --- | --- | --- | --- | --- |
| Sequential capture | 4.83 [4.66–4.96] | 4.83 [4.66–4.96] | 1.41 [1.32–1.53] | 9.59 [9.29–10.15] | 6.39 [4.36–6.70] |
| Single-request capture | 3.71 [3.71–3.74] | 3.75 [3.71–3.79] | 1.27 [1.21–1.35] | 23.12 [9.99–28.09] | 3.80 [3.65–4.02] |
| WAL | 1.18 [1.16–14.78] | — | 1.11 [1.10–4.24] | 11.49 [9.73–13.58] | 0.27 [0.23–1.11] |

**healthy-large-1000**

| Arm | Arrival p99 (ms) | Quiet arrival p99 (ms) | Write p99 (ms) | Write max (ms) | Fresh p999 (ms) |
| --- | --- | --- | --- | --- | --- |
| Sequential capture | 10.96 [9.72–20.19] | 10.99 [9.87–18.87] | 5.55 [5.36–5.87] | 95.01 [53.24–106.95] | 13.53 [11.74–25.57] |
| Single-request capture | 12.90 [9.85–1,143.61] | 12.14 [9.93–1,145.61] | 5.76 [5.26–31.93] | 111.59 [103.81–504.15] | 9.81 [8.75–460.51] |
| WAL | 4.16 [1.90–5.31] | — | 3.13 [1.40–3.30] | 29.81 [9.30–44.84] | 1.20 [0.96–1.51] |

### Capture stage and consumer work

Capture is the complete publisher call, including transaction completion. Instrumented queries exclude source SQL, bridge notifications, and the sequential control's BEGIN/COMMIT commands. Consumer wire bytes include the notification bridge and transaction-command traffic, but exclude writers. Stage percentiles must not be added to infer an end-to-end percentile.

**healthy-1000**

| Arm | Capture p50 (ms) | Capture p99 (ms) | Capture max (ms) | Fetch p99 (ms) | Queries/write | Wire bytes/write | Epochs/sec |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 0.96 [0.95–0.96] | 1.19 [1.18–1.23] | 11.29 [10.40–11.75] | 0.33 [0.32–0.33] | 1.960 [1.959–1.960] | 927 [927–927] | 490 [490–490] |
| Single-request capture | 0.37 [0.35–0.37] | 0.58 [0.53–0.61] | 8.68 [7.97–38.81] | 0.42 [0.41–0.46] | 0.979 [0.977–0.981] | 723 [723–724] | 489 [489–490] |

**healthy-3000**

| Arm | Capture p50 (ms) | Capture p99 (ms) | Capture max (ms) | Fetch p99 (ms) | Queries/write | Wire bytes/write | Epochs/sec |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 1.56 [1.46–1.56] | 2.00 [1.88–2.12] | 9.26 [8.87–10.62] | 0.55 [0.52–0.56] | 0.655 [0.655–0.656] | 510 [509–510] | 492 [491–492] |
| Single-request capture | 0.49 [0.48–0.49] | 0.75 [0.74–0.76] | 15.16 [8.66–27.44] | 0.56 [0.52–0.58] | 0.328 [0.328–0.328] | 442 [442–442] | 492 [492–492] |

**capacity-small**

| Arm | Capture p50 (ms) | Capture p99 (ms) | Capture max (ms) | Fetch p99 (ms) | Queries/write | Wire bytes/write | Epochs/sec |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 3.09 [3.09–3.12] | 4.03 [4.03–4.03] | 11.30 [11.15–18.09] | 1.30 [1.23–1.30] | 0.168 [0.168–0.169] | 364 [364–364] | 320 [318–321] |
| Single-request capture | 1.08 [1.06–1.08] | 1.64 [1.63–1.65] | 11.79 [11.42–20.81] | 1.13 [1.12–1.14] | 0.127 [0.126–0.128] | 361 [361–362] | 491 [490–491] |

**capacity-large**

| Arm | Capture p50 (ms) | Capture p99 (ms) | Capture max (ms) | Fetch p99 (ms) | Queries/write | Wire bytes/write | Epochs/sec |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 5.81 [5.76–5.99] | 16.43 [13.52–18.53] | 92.62 [91.77–117.60] | 4.47 [4.46–4.51] | 0.202 [0.196–0.209] | 16,527 [16,526–16,530] | 156 [153–162] |
| Single-request capture | 1.55 [1.51–1.78] | 9.91 [9.91–10.20] | 117.91 [100.42–123.07] | 3.59 [3.42–3.61] | 0.269 [0.268–0.278] | 16,568 [16,568–16,573] | 422 [415–424] |

### Immediate notification control

This changes only notification minimum spacing from 2 ms to zero. The first notification after idle is immediate under either policy.

| Capture | Spacing | Small capacity (tx/sec) | 1k/sec fresh p99 (ms) | 1k/sec arrival p99 (ms) | Capacity fresh p99 (ms) | Notifications/sec at capacity |
| --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 2 ms | 7,595 [7,551–7,595] | 2.95 [2.95–2.96] | 3.77 [3.76–3.77] | 6.70 [6.67–6.76] | 491 [491–491] |
| Sequential capture | 0 ms | 7,283 [7,235–7,286] | 2.19 [2.10–3.02] | 3.15 [2.94–4.17] | 7.05 [7.04–7.23] | 1,771 [1,759–1,787] |
| Single-request capture | 2 ms | 7,734 [7,667–7,795] | 2.55 [2.54–2.55] | 3.49 [3.38–3.71] | 3.85 [3.82–3.86] | 491 [491–491] |
| Single-request capture | 0 ms | 7,217 [7,161–7,218] | 0.94 [0.89–2.25] | 1.76 [1.70–317.31] | 3.08 [3.07–3.15] | 1,754 [1,733–1,762] |

### Two million retained journal rows

History spans 2,048 real historical transactions. Generic mode changes consumer planning only.

| Case | Arm | Fresh p99 (ms) | Quiet arrival p99 (ms) | Capture p99 (ms) | Capture max (ms) |
| --- | --- | --- | --- | --- | --- |
| history-auto | Sequential capture | 8.72 [3.11–8.98] | 125.22 [3.93–184.60] | 1.24 [1.16–1.29] | 57.01 [44.67–357.00] |
| history-auto | Single-request capture | 2.99 [2.52–3.04] | 6.34 [3.57–153.73] | 0.62 [0.61–1.42] | 121.68 [54.13–265.77] |
| history-generic | Sequential capture | 3.01 [2.96–3.02] | 4.25 [3.81–9.35] | 1.17 [1.17–1.26] | 51.94 [29.47–61.63] |
| history-generic | Single-request capture | 2.52 [2.47–2.54] | 3.71 [3.63–19.80] | 0.57 [0.52–0.57] | 52.34 [7.05–52.59] |

### Online partition rotation

40k small writes at 1k/sec, 8,192-XID ranges, initially four leaves, two future ranges of headroom, and the same 100 ms maintenance manager.

| Arm | Fresh p99 (ms) | Fresh p999 (ms) | Quiet arrival p99 (ms) | Capture p99 (ms) | Dropped leaves | Final leaves |
| --- | --- | --- | --- | --- | --- | --- |
| Sequential capture | 4.25 [3.19–7.01] | 5.48 [4.24–23.72] | 5.14 [4.01–195.92] | 2.02 [1.30–3.22] | 9 [9–9] | 3 [3–3] |
| Single-request capture | 2.79 [2.76–5.47] | 6.24 [4.34–8.25] | 3.85 [3.70–7.29] | 0.71 [0.62–3.68] | 9 [9–9] | 3 [3–3] |

### Sparse source faults

One rollback/retry or 200 ms post-append pause per 1,000 operations. Quiet roots are the lower-ranked 90% of roots; they exclude root 0, which receives the injected pauses.

| Fault | Arm | All fresh p99 (ms) | Quiet fresh p99 (ms) | Quiet arrival p99 (ms) | Arrival max (ms) |
| --- | --- | --- | --- | --- | --- |
| fault-rollback | Sequential capture | 2.96 [2.95–9.11] | 2.96 [2.95–9.13] | 3.79 [3.75–643.80] | 12.86 [12.52–793.59] |
| fault-rollback | Single-request capture | 2.76 [2.57–2.82] | 2.70 [2.56–2.88] | 4.23 [3.47–4.51] | 46.62 [24.98–62.43] |
| fault-after | Sequential capture | 3.09 [2.97–8.69] | 3.36 [2.97–8.57] | 4.71 [3.77–23.61] | 213.48 [211.26–214.55] |
| fault-after | Single-request capture | 2.77 [2.57–6.23] | 2.65 [2.58–6.27] | 8.05 [7.86–246.59] | 208.62 [207.69–558.04] |

### Capture plan diagnostics

Six warmed captures per combination, plus EXPLAIN ANALYZE. Empty probes include a formerly active transaction with no journal entry. Nonempty probes add one eligible transaction. These are diagnostics outside the measured matrix.

| Fixture | Partitioned | Changed | Planning | Client median (ms) | Server execution (ms) | Scanned rows |
| --- | --- | --- | --- | --- | --- | --- |
| small | False | False | auto | 0.504 | 0.066 | 0.0 |
| small | False | False | force_generic_plan | 0.285 | 0.060 | 0.0 |
| small | False | True | auto | 0.470 | 0.049 | 1.0 |
| small | False | True | force_generic_plan | 0.323 | 0.045 | 1.0 |
| small | True | False | auto | 0.576 | 0.099 | 0.0 |
| small | True | False | force_generic_plan | 0.344 | 0.083 | 0.0 |
| small | True | True | auto | 0.649 | 0.084 | 1.0 |
| small | True | True | force_generic_plan | 0.382 | 0.087 | 1.0 |
| bulk-history | False | False | auto | 0.389 | 0.051 | 0.0 |
| bulk-history | False | False | force_generic_plan | 0.193 | 0.059 | 0.0 |
| bulk-history | False | True | auto | 0.461 | 0.051 | 1.0 |
| bulk-history | False | True | force_generic_plan | 0.265 | 0.066 | 1.0 |
| bulk-history | True | False | auto | 0.901 | 0.099 | 0.0 |
| bulk-history | True | False | force_generic_plan | 0.337 | 0.086 | 0.0 |
| bulk-history | True | True | auto | 0.973 | 0.101 | 1.0 |
| bulk-history | True | True | force_generic_plan | 0.391 | 0.093 | 1.0 |
| diverse-history | False | False | auto | 0.389 | 0.045 | 0.0 |
| diverse-history | False | False | force_generic_plan | 0.184 | 0.049 | 0.0 |
| diverse-history | False | True | auto | 0.464 | 0.045 | 1.0 |
| diverse-history | False | True | force_generic_plan | 0.259 | 0.043 | 1.0 |
| diverse-history | True | False | auto | 1.098 | 0.122 | 0.0 |
| diverse-history | True | False | force_generic_plan | 0.348 | 0.087 | 0.0 |
| diverse-history | True | True | auto | 1.119 | 0.111 | 1.0 |
| diverse-history | True | True | force_generic_plan | 0.366 | 0.088 | 1.0 |

## Interpretation

**Single-request capture is worth retaining for this design.** It preserves snapshot membership, the durable manifest boundary, global epoch order, and independent root application while removing four sequential exchanges from a nonempty capture. At 1k small writes/sec, capture p50 falls from 0.96 to 0.37 ms and capture p99 from 1.19 to 0.58 ms. At 3k/sec, capture p99 falls from 2.00 to 0.75 ms. Lower publisher overhead is visible both in stage timings and in end-to-end freshness.

With the existing 2 ms notification spacing, healthy small-write freshness p99 improves by about 14% at 1k/sec (2.95 to 2.55 ms) and 25% at 3k/sec (3.65 to 2.73 ms). The 50/sec case improves from 7.52 to 4.55 ms. At saturation, small and large freshness p99 each improve by about 42%: 6.70 to 3.85 ms and 22.04 to 12.71 ms. These repeated saturation ranges do not overlap. The faster publisher creates smaller epochs more frequently: at small saturation it advances about 491 versus 320 epochs/sec; at large saturation about 422 versus 156. This also means more manifest commits and payload queries, rather than a universal reduction in every consumer cost. Future graph/MVCC measurements should account for that cadence.

Write capacity changes little. Small writes rise about 1.8%, from 7,595 to 7,734/sec; large writes stay around 3,130/sec, with overlapping ranges. Against the contemporaneous WAL control, the new path retains about 99% of small-write capacity and 60% of large-write capacity. The extra journal write remains present, and this change does not recover the large-payload throughput gap. Healthy WAL freshness p99 is still 0.12 ms at 1k small writes/sec and 0.14 ms at 3k/sec. No deployment-RTT experiment is included; fewer dependent exchanges should be useful at higher RTT, but these measurements quantify only the local topology.

**Wakeup spacing becomes a larger part of the remaining ordinary freshness delay.** With zero notification minimum spacing, single-request freshness p99 at 1k/sec is 0.94 ms [0.89–2.25], compared with 2.19 ms [2.10–3.02] for the sequential publisher with the same immediate-notification policy. This combines capture optimization with a separate wakeup policy and must not be credited to capture alone. The new variant's small-write capacity falls from 7,734 to 7,217/sec, about 6.7%, while notification frequency grows from roughly 491 to 1,754/sec. It reaches sub-millisecond median per-run p99 locally, rather than a demonstrated sub-millisecond tail bound. Intermediate spacing or consumer-aware coalescing deserves a separate experiment if that throughput/freshness balance is useful.

**Retention and fault correctness remain intact.** Forced-generic two-million-history freshness p99 improves from 3.01 to 2.52 ms. Automatic-history medians improve from 8.72 to 2.99 ms, but source/capture stalls remain in the ranges. All twenty-four diagnostic combinations examine zero rows for an empty probe or one eligible row for a nonempty probe, including bulk and diverse history and partitioned tables. Rotation retires nine leaves and ends with three in every sample; freshness p99 improves from 4.25 to 2.79 ms at the medians. Its p999 medians are 5.48 versus 6.24 ms, so maintenance-tail improvement is not uniform. Sparse rollback and post-append pause runs retain exactly-once delivery and final-state correctness without a gap-repair barrier.

**Shared database delays remain unresolved.** One large-payload single-request sample has freshness p99 of 26.32 ms and arrival p99 of 1.14 seconds; its source write and capture maxima are both near 500 ms, while payload fetch max is 5.42 ms. The immediate-notification sample with 2.25 ms freshness p99 has 317 ms arrival p99 and roughly 488 ms source/capture maxima. Other history, rotation, and fault samples also contain source/capture stalls. These observations identify stages, not the underlying wait event or device; they cannot be dismissed as unrelated environmental noise or attributed solely to this algorithm. None were discarded. Even with cleaner median freshness, the design does not yet demonstrate a tight production end-to-end SLO.

The next capture implementation should use the single atomic statement with the existing guarded streamed reader. Additional freshness work should separately test wakeup policy and diagnose write/capture stalls, preserving matched WAL controls. Production recovery still needs durable application/checkpoints, publisher fencing, replay, and protected retention; this experiment changes none of those requirements.

## Validation and reproduction

The audit verifies 96 measured runs across 32 configurations, three repetitions each, and 3,639,000 source transactions. Every measured invocation exited successfully, used the same frozen normal binary and eleven source hashes, and had empty before/after container inventories. Ten smoke samples and all calibration operations are excluded. Normal and race protocol checks, the repository suite, twenty-four plans, and 144 diagnostic captures passed. The source archive matches the measured hashes.

- [Runner](run-snapshot-capture.py), [report generator](report-snapshot-capture.py), [sample CSV](snapshot-capture-samples.csv), [plan CSV](snapshot-capture-explain.csv), [audit](snapshot-capture-audit.txt).
- [Atomic capture SQL](../snapshot_capture_bench_test.go), [protocol tests](../snapshot_capture_protocol_test.go), [missed-wakeup tests](../snapshot_fetch_protocol_test.go), [plan diagnostics](../snapshot_capture_plan_test.go).
- [Frozen source](snapshot-capture-source/manifest.json). Normal binary SHA-256: `ff68f996661b3131f787c6f48e616ff74b5d2b992cd6e2c1149601d90464f860`.

```sh
go test -c -o /tmp/falcon-snapshot-capture.test ./internal/infrastructure/postgres
go test -c -race -o /tmp/falcon-snapshot-capture-race.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py validate
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py validate --binary /tmp/falcon-snapshot-capture-race.test
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py smoke
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py diagnostics
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py suite
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-capture.py all
python3 internal/infrastructure/postgres/benchmark-results/report-snapshot-capture.py
```

Raw files use `snapshot-capture-{capacity,latency,spacing,history,maintenance,faults}.{txt,jsonl}`. Archive existing phase logs before rerunning: the runner appends, and the report rejects duplicate samples or mixed measured implementations.
