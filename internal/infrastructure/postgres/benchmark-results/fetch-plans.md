# Snapshot payload fetch plans

This follow-up compares the existing streamed snapshot query with explicit old-active-XID lookups, sorting narrow metadata before retrieving payloads, and a position scan bounded by the current cut's eligible entries. It retains one global snapshot clock and identical per-root application. Production Falcon is unchanged.

## Query variants

| Arm | How it finds and orders entries | Payload-stage SQL per nonempty cut |
| --- | --- | --- |
| Control | XID-range / old-active-array candidate UNION, then sort complete stored tuples by position | 1 |
| Guarded | Same new-XID range; explicit visible old-XID lookups through the `(writer_xid,position)` index; then sort complete stored tuples | 1 |
| Metadata | Guarded candidate discovery; sort only positions; retrieve each payload with a server-side unique-position lookup | 1 |
| Adaptive bounds | Guarded metadata discovery computes count/min/max position; scan that inclusive position interval, or use Metadata when the interval is sparse | 2 |

Guarded old-XID lookups return **all** journal entries for each eligible transaction. They have no one-row limit; the earlier bounded existence probe is a separate boolean check. The lateral lookup and paired XID range retain a parameterized index-access shape under generic plans. Still-active XIDs are filtered before lookup, and an empty old-active list drives no lookups.

Metadata uses an ordered subquery plus a lateral primary-key lookup. Its per-row payload lookups happen inside PostgreSQL, without per-row client queries. It reduces sort width, while adding another index/heap access for every delivered entry.

Adaptive bounds derive min/max/count from the eligible entries for the immutable captured snapshot. Both statements retain the same XID membership and snapshot visibility predicates. Bounds reset for every cut; older-position late commits remain included. When `max-min+1 > 4*count+4096`, it chooses Metadata instead of scanning that wide interval. This is an explicit initial heuristic, not a statistically fitted crossover. A position bound creates an efficient scan opportunity; the optimizer still chooses the actual access path, and some plans can sort even inside a bounded interval.

Diagnostics additionally compare unbounded position-ordered queries, with and without retrieving payloads afterward. They also replace the position primary-key index with `PRIMARY KEY(position) INCLUDE(writer_xid)`. This replaces the primary index rather than adding a fourth index. The separate index-capacity control measures that replacement's write cost using the original query.

## Methods and scope

Source writes, journal payloads, snapshot publication, notification policy, root FIFO queues, four application workers, and a 32 MiB payload budget match the previous pipeline experiment. Writers enqueue post-commit hints without waiting; the notification sender spaces starts by 2 ms, and capture has a 100 ms fallback. Capture retains the verified indexed existence probe. PostgreSQL 18 Alpine has six VM CPUs, 6 GiB VM RAM, a 4 GiB container limit, 1 GiB shared buffers, and 4 MiB work memory. Fsync, synchronous commit, and full-page writes are enabled. Sixteen writers, five consumer connections, and two notification connections are held identical.

Load ranks 1,024 roots with weights proportional to rank^-1.05, representing Pareto-shaped activity. Quiet roots are ranks 104–1,024. Main payloads are 256 B or 16 KiB per transaction. The matrix has three shuffled repetitions per configuration: source-idle backlog drain, concurrent source-write capacity, twenty-second healthy offered loads, two million retained entries with automatic and forced generic consumer plans, and primary-index write-cost controls. Writers always use their ordinary automatic planning; the forced-mode case changes consumer connections only.

Every measured invocation uses fresh containers and requires empty inventories before and after. No separate manual inventory polling is used. Source and binary hashes are saved per invocation. Smoke, protocol, race, full-suite, prepared-plan, and calibration runs are excluded from measured samples. The sustained retained-history fixture is a bulk transaction with one old XID; this skew is a diagnostic limitation, not a fitted production history distribution. Supplemental diagnostics separately use two million rows across 2,000 real committed writer transactions and compare stale with explicitly refreshed statistics. Autovacuum is disabled only in that supplemental fixture to isolate the statistics comparison; it stays enabled in the sustained matrix.

Epoch first-row latency starts before candidate discovery, so it includes the additional bounds statement. Payload statement first-row latency is recorded separately. Drain includes startup/capture/fetch/readiness after the source stops. Effective fetch bandwidth includes SQL, decoding, routing, and backpressure. Arrival starts at the intended write arrival; freshness starts at client commit acknowledgement. The sink validates full payload checksums, exactly-once delivery, position order, root publication, and final latest state. Real graph/MVCC application, retention, recovery, and managed-service network effects remain outside the measurements.

The previous pipeline implementation is preserved as [hash-verified source text](snapshot-pipeline-source/manifest.json), so its saved measurements remain attributable after these additive benchmark changes.

This report's measured implementation is also preserved as [hash-verified source text](fetch-plans-source/manifest.json), before the subsequent partitioned-stream experiment extended the shared benchmark files.

## Results

**120 measured samples, 40 configurations, 6,660,000 source transactions.** Values are medians of per-run statistics, with min–max ranges. These are not pooled percentiles or confidence intervals.

### Source-write capacity

| Arm | 256 B tx/sec | 16 KiB tx/sec | 16 KiB freshness p99 (ms) |
| --- | --- | --- | --- |
| Control | 7,308 [7,184–7,587] | 3,089 [2,940–3,090] | 22.34 [21.32–26.54] |
| Guarded | 7,526 [7,040–7,539] | 3,109 [3,063–3,123] | 22.60 [21.63–22.85] |
| Metadata | 7,535 [7,463–7,575] | 3,063 [2,925–3,203] | 24.43 [19.94–24.81] |
| Adaptive bounds | 7,014 [6,776–7,507] | 3,001 [2,922–3,042] | 23.77 [22.89–24.80] |

### Healthy freshness and arrival

| Arm | 1k/sec fresh p99 (ms) | 1k/sec quiet arrival p99 (ms) | 3k/sec fresh p99 (ms) | 3k/sec quiet arrival p99 (ms) |
| --- | --- | --- | --- | --- |
| Control | 2.98 [2.95–4.10] | 3.79 [3.77–42.03] | 9.18 [8.99–9.64] | 17.18 [15.84–358.70] |
| Guarded | 2.96 [2.95–3.57] | 3.79 [3.76–4.97] | 8.44 [7.52–9.12] | 16.13 [12.42–172.80] |
| Metadata | 2.99 [2.96–5.70] | 3.79 [3.75–201.81] | 6.85 [3.69–7.35] | 141.54 [4.88–403.37] |
| Adaptive bounds | 3.13 [3.11–3.13] | 3.90 [3.88–3.93] | 7.64 [4.11–8.74] | 9.55 [5.47–11.49] |

At 3k/sec, these maximum-duration summaries help distinguish source/capture stalls from payload startup. They are medians and ranges of each run's maximum, not p99 values.

| Arm | Append/commit max (ms) | Capture SQL max (ms) | Fetch SQL max (ms) | Epoch first row max (ms) |
| --- | --- | --- | --- | --- |
| Control | 14.11 [12.47–457.20] | 16.37 [12.88–457.21] | 3.11 [3.02–4.38] | 3.11 [3.02–4.33] |
| Guarded | 279.15 [14.39–422.96] | 279.43 [13.70–424.35] | 4.34 [4.07–4.37] | 4.33 [4.07–4.37] |
| Metadata | 425.62 [8.78–442.61] | 426.02 [9.97–442.12] | 3.78 [2.88–4.45] | 3.77 [2.89–4.45] |
| Adaptive bounds | 18.03 [12.60–22.86] | 18.91 [13.99–23.50] | 4.10 [3.92–4.47] | 5.30 [5.08–5.48] |

### Source-idle backlog drain

First-row latency includes bounds discovery for Adaptive. No root becomes readable until successful global fetch EOF and its own preceding application work.

**backlog-small**

| Arm | Drain (sec) | Epoch first row (ms) | Fetch MiB/sec | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Control | 0.217 [0.215–0.219] | 37.30 [37.08–38.55] | 122.6 [121.6–123.7] | 0.02 [0.02–0.03] |
| Guarded | 0.217 [0.215–0.217] | 36.56 [36.52–37.14] | 122.8 [122.2–123.4] | 0.02 [0.02–0.02] |
| Metadata | 0.219 [0.216–0.220] | 28.29 [27.26–29.17] | 121.4 [120.5–122.8] | 0.03 [0.02–0.04] |
| Adaptive bounds | 0.233 [0.233–0.237] | 53.48 [52.49–53.95] | 112.8 [111.4–113.2] | 0.03 [0.02–0.06] |

**backlog-large**

| Arm | Drain (sec) | Epoch first row (ms) | Fetch MiB/sec | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Control | 0.897 [0.892–0.901] | 5.27 [5.21–5.39] | 354.5 [352.3–356.7] | 0.39 [0.36–0.45] |
| Guarded | 0.893 [0.880–0.897] | 5.40 [5.18–5.44] | 356.3 [354.3–361.4] | 0.50 [0.41–0.58] |
| Metadata | 0.894 [0.889–0.894] | 4.53 [4.37–4.58] | 355.7 [355.1–357.9] | 0.38 [0.36–0.41] |
| Adaptive bounds | 0.895 [0.890–0.901] | 8.02 [7.38–8.22] | 355.3 [352.8–357.1] | 0.56 [0.39–0.84] |

**backlog-history**

| Arm | Drain (sec) | Epoch first row (ms) | Fetch MiB/sec | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Control | 0.228 [0.227–0.232] | 46.27 [45.27–47.33] | 117.1 [114.8–117.1] | 0.02 [0.02–0.02] |
| Guarded | 0.230 [0.230–0.230] | 45.19 [45.11–46.26] | 116.6 [116.0–116.8] | 0.03 [0.02–0.03] |
| Metadata | 0.224 [0.222–0.229] | 32.69 [32.34–32.72] | 118.8 [116.5–119.9] | 0.03 [0.02–0.06] |
| Adaptive bounds | 0.244 [0.238–0.246] | 60.55 [59.44–60.78] | 108.5 [108.4–111.8] | 0.02 [0.02–0.04] |

### Retained history and consumer planning

Two million old entries, then 1k offered small writes/sec. Generic mode applies to the consumer pool, including capture; writers retain automatic planning.

**history-auto**

| Arm | Write tx/sec | Freshness p99 (ms) | Quiet arrival p99 (ms) | Drain (sec) | Epoch first row p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| Control | 1,000 [1,000–1,000] | 3.02 [3.01–3.04] | 3.81 [3.81–3.87] | 0.002 [0.001–0.003] | 0.44 [0.43–0.45] |
| Guarded | 1,000 [1,000–1,000] | 3.06 [3.06–3.07] | 3.86 [3.86–3.88] | 0.002 [0.002–0.003] | 0.50 [0.49–0.50] |
| Metadata | 1,000 [1,000–1,000] | 3.19 [3.12–3.19] | 4.10 [3.90–5.02] | 0.001 [0.001–0.002] | 0.55 [0.54–0.58] |
| Adaptive bounds | 1,000 [1,000–1,000] | 3.27 [3.26–3.27] | 4.06 [4.04–4.06] | 0.001 [0.001–0.002] | 0.77 [0.76–0.79] |

**history-generic**

| Arm | Write tx/sec | Freshness p99 (ms) | Quiet arrival p99 (ms) | Drain (sec) | Epoch first row p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| Control | 1,000 [1,000–1,000] | 11,213.42 [10,725.30–11,278.55] | 11,216.77 [10,734.52–11,278.10] | 10.202 [10.149–10.483] | 196.27 [188.72–218.69] |
| Guarded | 1,000 [1,000–1,000] | 3.14 [2.93–3.55] | 21.79 [3.75–35.99] | 0.001 [0.001–0.002] | 0.31 [0.31–0.31] |
| Metadata | 1,000 [1,000–1,000] | 2.95 [2.95–2.95] | 3.79 [3.76–3.80] | 0.002 [0.001–0.002] | 0.33 [0.33–0.33] |
| Adaptive bounds | 1,000 [1,000–1,000] | 3.28 [3.08–13.61] | 4.15 [3.87–211.00] | 0.003 [0.002–0.003] | 0.51 [0.50–0.59] |

### Consumer cost

SQL includes capture, probe, manifest, bounds/payload statements, and separate notifications; BEGIN/COMMIT are excluded. Socket bytes include the consumer pool and notification bridge.

**healthy-1000**

| Arm | Fetch-stage SQL | All SQL/write | Wire bytes/write | Bounds calls | Sparse fallbacks |
| --- | --- | --- | --- | --- | --- |
| Control | 9,781 [9,683–9,799] | 2.446 [2.422–2.450] | 926 [920–927] | 0 [0–0] | 0 [0–0] |
| Guarded | 9,797 [9,764–9,803] | 2.450 [2.442–2.451] | 927 [925–928] | 0 [0–0] | 0 [0–0] |
| Metadata | 9,791 [9,460–9,798] | 2.448 [2.370–2.450] | 927 [907–927] | 0 [0–0] | 0 [0–0] |
| Adaptive bounds | 19,600 [19,594–19,618] | 2.940 [2.939–2.943] | 1,104 [1,103–1,104] | 9,800 [9,797–9,809] | 0 [0–0] |

**capacity-small**

| Arm | Fetch-stage SQL | All SQL/write | Wire bytes/write | Bounds calls | Sparse fallbacks |
| --- | --- | --- | --- | --- | --- |
| Control | 4,189 [4,180–4,277] | 0.236 [0.232–0.237] | 365 [364–365] | 0 [0–0] | 0 [0–0] |
| Guarded | 4,217 [4,214–4,237] | 0.234 [0.234–0.239] | 364 [364–366] | 0 [0–0] | 0 [0–0] |
| Metadata | 4,201 [4,184–4,211] | 0.234 [0.232–0.234] | 364 [364–364] | 0 [0–0] | 0 [0–0] |
| Adaptive bounds | 8,436 [8,436–8,480] | 0.277 [0.276–0.284] | 381 [381–384] | 4,218 [4,218–4,240] | 0 [0–0] |

**capacity-large**

| Arm | Fetch-stage SQL | All SQL/write | Wire bytes/write | Bounds calls | Sparse fallbacks |
| --- | --- | --- | --- | --- | --- |
| Control | 2,547 [2,476–2,555] | 0.348 [0.342–0.349] | 16,529 [16,527–16,529] | 0 [0–0] | 0 [0–0] |
| Guarded | 2,615 [2,501–2,640] | 0.351 [0.344–0.354] | 16,530 [16,527–16,531] | 0 [0–0] | 0 [0–0] |
| Metadata | 2,533 [2,524–2,644] | 0.346 [0.344–0.361] | 16,528 [16,528–16,533] | 0 [0–0] | 0 [0–0] |
| Adaptive bounds | 5,174 [5,114–5,210] | 0.404 [0.400–0.408] | 16,552 [16,551–16,554] | 2,587 [2,557–2,605] | 0 [0–0] |

**backlog-small**

| Arm | Fetch-stage SQL | All SQL/write | Wire bytes/write | Bounds calls | Sparse fallbacks |
| --- | --- | --- | --- | --- | --- |
| Control | 1 [1–1] | 0.000 [0.000–0.000] | 299 [299–299] | 0 [0–0] | 0 [0–0] |
| Guarded | 1 [1–1] | 0.000 [0.000–0.000] | 299 [299–299] | 0 [0–0] | 0 [0–0] |
| Metadata | 1 [1–1] | 0.000 [0.000–0.000] | 299 [299–299] | 0 [0–0] | 0 [0–0] |
| Adaptive bounds | 2 [2–2] | 0.000 [0.000–0.000] | 299 [299–299] | 1 [1–1] | 0 [0–0] |

### Covering primary-index write control

Both arms use the original streamed payload query. Cover replaces the position primary key with the same unique key plus included writer XID; the number of indexes stays fixed. No forced planning or explicit vacuum is used in these capacity runs.

| Index | 256 B tx/sec | 16 KiB tx/sec | 256 B WAL bytes/write | 256 B journal bytes/write |
| --- | --- | --- | --- | --- |
| Position PK | 7,511 [7,511–7,582] | 3,219 [3,192–3,237] | 1,167 [1,167–1,167] | 447 [447–447] |
| Position PK + included XID | 7,512 [7,430–7,591] | 3,113 [3,083–3,200] | 1,176 [1,176–1,177] | 456 [456–456] |


## Interpretation

The guarded old-XID lookup is the leading simple improvement to investigate for production. It uses the existing indexes, retains one payload query per cut, and avoids the reproduced generic-plan history scan. Its measured capacity is comparable to the control: the small-payload medians are 7,526 versus 7,308 tx/sec, with overlapping run ranges. That is evidence of no substantial throughput penalty here, not evidence of a reliable 3% speedup. Automatic planning and forced generic planning must be distinguished; the sustained retained-history tables report both.

The forced-generic sustained control fetches repeatedly scan retained history: freshness p99 is 11.21 seconds [10.73–11.28], and drain after writes stop is 10.20 seconds [10.15–10.48]. Guarded remains at 3.14 ms [2.93–3.55], and Metadata at 2.95 ms. Capture itself is fast in the slow control runs; approximately 160 ms per payload fetch backs up the captured-cut queue. Thus the multi-second penalty includes queue accumulation, rather than a single SQL call taking eleven seconds. Automatic planning remains fast with this same history. The forced-generic/skewed-history case is a reproducible failure mode, not the expected latency of the original query on every retained journal.

History shape matters. With two million entries spread across 2,000 committed writer XIDs, the generic control scans only sixteen eligible rows and takes 3.01 ms. Its plan launches two parallel workers for that tiny result; Guarded stays serial and takes 0.29 ms. Both take roughly 0.4 ms with custom planning. In the bulk-history/100k-new-entry diagnostic, explicit ANALYZE changes the generic control from a retained-history scan taking 253.24 ms to a selective plan taking 110.07 ms. The extreme scan is therefore neither inherent to snapshot membership nor inevitable with retention; skew, statistics, and planning mode determine it.

These are medians of six full client reads within each diagnostic fixture, separate from the three independently repeated sustained runs:

| Fixture and planning | Control (ms) | Guarded (ms) | Adaptive, including bounds (ms) |
| --- | --- | --- | --- |
| 2M bulk history, 16 new entries, generic | 150.98 | 0.31 | not measured in this fixture |
| 2M across 2,000 writer XIDs, 16 new entries, generic, analyzed | 3.01 | 0.29 | 0.45 |
| 100k dense entries, no statistics, custom | 103.24 | 103.33 | 114.40 |
| Same dense entries, analyzed, custom | 96.79 | 102.23 | 89.22 |
| 2M bulk history + 100k new entries, stale statistics, generic | 253.24 | 106.71 | 102.19 |
| Same history/cut, analyzed, generic | 110.07 | 107.88 | 125.30 |

The dense analyzed bound case is a limited positive result: 89.22 versus 96.79 ms including discovery, and first row 8.97 versus 14.21 ms. Stale statistics reverse that advantage, and the analyzed retained-history generic case can still choose a spilling sort inside the bounds. This makes bounds a possible specialized catch-up experiment rather than the preferred default from these measurements. Automatic prepared statements can change plans across the six reads; the saved EXPLAIN is the subsequent execution, so it need not describe every preceding read. Forced-custom and forced-generic results make that distinction explicit.

Narrow sorting helps first-row startup for a large inline-payload cut, but has not improved overall drain. For the 100k-entry small-payload backlog, first-row time falls from 37.30 to 28.29 ms; drain remains about 0.22 seconds. In the analyzed prepared-plan fixture, the control spills a 280-byte estimated-width sort to a 28,808 KiB disk workspace. Metadata sorts eight-byte positions in 3,073 KiB of memory, but then performs 100k additional primary-key lookups. In that main plan diagnostic, full client reads take 123.61 ms versus 107.75 ms under forced generic planning. Supplemental refreshed-statistics fixtures likewise show no overall Metadata advantage, though the precise startup costs differ. Avoiding the spill therefore does not make the entire query faster.

The large-payload case is less susceptible to the intuitive “sort every 16 KiB payload” cost. Payload storage is EXTERNAL, and PostgreSQL normally carries an 18-byte out-of-line TOAST pointer through sorting before reading the value for output. Its large-payload control sort fits in roughly 2 MiB; client payload transfer and validation dominate. This is consistent with the nearly identical 0.89–0.90 second drain times across arms. See [PostgreSQL's TOAST implementation](https://www.postgresql.org/docs/18/storage-toast.html).

Bounds can make an isolated payload SELECT start quickly, but the complete path also pays for finding bounds. Dense analyzed 100k-entry diagnostics show a sort-free payload scan beginning in about 0.4 ms; those particular diagnostic timings exclude bounds discovery. The end-to-end matrix includes it and shows slower small-backlog startup (53.48 versus 37.30 ms) and drain (0.233 versus 0.217 seconds). Bounds double fetch-stage SQL per nonempty cut. For a very old low-position late commit plus fresh rows, an unguarded bounds scan touches about two million entries and takes 402 ms; the adaptive variant detects this wide span and uses selective metadata lookup instead. The sparse fallback is tested explicitly, but is not triggered by the ordinary sustained matrix.

An unbounded scan in position-index order avoids sorting by doing more filtering work. In the 16-entry/two-million-history forced-generic diagnostic, it reads roughly two million entries and takes 161–226 ms, while the guarded query takes 0.31 ms. A sort-free plan is not by itself a good plan; selectivity and scanned history are decisive. The metadata-then-lookup unbounded plan also incurs about 176 ms of JIT compilation in this diagnostic, triggered by its high estimated cost ([PostgreSQL JIT decision policy](https://www.postgresql.org/docs/18/jit-decision.html)); the full-payload unbounded scan takes 161 ms without JIT. These timings include the server's default JIT policy, rather than isolating scan cost alone. The covering position index does not change that access problem. Before vacuum it still performs about two million heap visibility fetches; after vacuum it avoids those heap visits but scans the same index history, taking 157 ms. [Index-only scans depend on visibility-map coverage](https://www.postgresql.org/docs/18/indexes-index-only-scans.html).

The covering-index capacity control shows no small-payload throughput advantage: medians are 7,512 versus 7,511 tx/sec. With 16 KiB transactions, the cover median is 3,113 versus 3,219 tx/sec, with overlapping ranges. It adds about nine journal bytes and nine WAL bytes per small write in this fixture. There is no measured fetch benefit that justifies this extra index width yet.

Shared source/capture stalls remain a separate issue. Several healthy repetitions show hundreds of milliseconds in both append/commit and capture SQL while payload fetch maxima stay under 5 ms. The table preserves those arrival outliers. These measurements do not establish the cause of the stalls or show that a query variant prevents them. They also do not establish production graph/MVCC cost, crash recovery, or retention safety.

## Validation and provenance

The full Go suite and normal/race protocol checks pass. The protocol cases include streaming before EOF, partial-fetch failure without readiness publication, lower-position late commits, uncommitted/aborted entries, independent root progress, empty deltas, and sparse adaptive fallback. All 120 measured invocations exit successfully and have empty before/after container inventories. The [audit](fetch-plans-audit.txt) verifies one frozen eight-file implementation and binary across the measured matrix, all sample counts, exact fetched payload totals, and the prior source archive. It also records the resolved initial archive-package build failure. Prepared-plan diagnostics include 852 full client reads and 160 saved plans, plus four covering-visibility plans; none are counted as measured matrix samples.

## Code and reproduction

- [Fetch-query variants and adaptive bounds](../snapshot_fetch_optimization_bench_test.go).
- [Plan diagnostics](../snapshot_fetch_optimization_plan_test.go) and [statistics/history-diversity diagnostics](../snapshot_fetch_optimization_statistics_test.go).
- [Sparse/empty-delta protocol tests](../snapshot_fetch_optimization_protocol_test.go).
- [Incremental, interrupted-fetch, and root-progress tests](../snapshot_pipeline_protocol_test.go).
- [Common streamed application sink](../snapshot_pipeline_bench_test.go), [capture and wakeups](../snapshot_fetch_bench_test.go), and [source/load fixture](../domain_capture_bench_test.go).
- [Sequential runner](run-fetch-plans.py) and [report generator](report-fetch-plans.py).
- [Flattened measured samples](fetch-plans-samples.csv), [prepared-plan summary](fetch-plans-explain.csv), [full plans and client reads](fetch-plans-diagnostics.txt), [covering-index visibility plans](fetch-plans-diagnostics-cover.txt), and [statistics/history-diversity plans](fetch-plans-diagnostics-stats.txt).
- Raw `fetch-plans-{backlog,capacity,latency,history,index}.{jsonl,txt}`; smoke, suite, and protocol/race logs are separate.

```sh
export DOCKER_HOST=unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock
go test -c -o /tmp/falcon-snapshot-fetch-plans.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py validate
go test -c -race -o /tmp/falcon-snapshot-fetch-plans-race.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py validate --binary /tmp/falcon-snapshot-fetch-plans-race.test
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py suite
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py diagnostics
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py diagnostics-cover
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py all
python3 internal/infrastructure/postgres/benchmark-results/report-fetch-plans.py
# Supplemental diagnostics, after the measured matrix:
go test -c -o /tmp/falcon-snapshot-fetch-plans-extra.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-fetch-plans.py diagnostics-stats --binary /tmp/falcon-snapshot-fetch-plans-extra.test
python3 internal/infrastructure/postgres/benchmark-results/report-fetch-plans.py
```

The runner appends output. Archive old raw files before repeating phases; do not mix measured implementations. The suite log preserves an initial archive-package build failure and the successful rerun after archived Go files were renamed to `.go.txt`.
