# Ordering domains: capture throughput and freshness

This experiment compares two changes without modifying Falcon's store, MVCC windows, or query protocol:

1. Preserve global PostgreSQL snapshot cuts while allowing roots to become readable independently.
2. Allocate journal positions transactionally in writers, using a global, tenant, or root counter.

These are counter allocation scopes and delivery scopes, not an implementation of hierarchical shared-root certification. Directory generations, cross-root transactions, permission traversal, and MVCC window creation are deferred. The measurements establish what capture can gain from isolation; they do not establish the final hierarchical protocol's cost.

The latest state stays in a source table keyed by `(root_id, object_key, ordinal)`. Each transaction upserts one object (one or sixteen tuple rows). Journal implementations append a separate immutable entry containing the complete transaction payload. WAL decodes inserts and updates from that same source table. No preceding per-key SELECT or application version predicate is used.

## Implementations

- **Coupled snapshots:** capture a raw PostgreSQL snapshot, fetch its entire journal delta across roots, wait out any injected delivery stall, commit the manifest, then validate and record all entries as delivered at one common completion point. This models the previous benchmark's single common delivery boundary.
- **Independent root fetch:** commit the small `(epoch, previous_snapshot, snapshot)` manifest first. Discover affected roots and counts, then use four workers to fetch and deliver complete root epochs. Each root has one active task and an ordered queue. Capture continues independently of delivery.
- **Routed snapshots:** commit the same manifest first, then fetch global delta pages once and route the complete epoch's entries to independent root delivery queues. This keeps efficient batched SQL reads while removing the global delivery barrier. The router itself remains a shared fetch stage.
- **Writer counters:** upsert source rows, update the relevant counter, append the journal entry, and commit atomically. Append and COMMIT are pipelined. A committed head certifies a dense committed prefix; no consumer epoch assignment or gap repair is required. Read changed heads, fetch closed-prefix journal pages across domains using bounded lateral index ranges in one SQL query, then route to the same root delivery queues. Counter entries can become readable after each page; snapshots require the entire relevant root epoch.
- **WAL control:** consume `pgoutput` transaction commit records and route complete source transactions to the same root queues. It has no application journal. For this disposable benchmark the slot conservatively retains WAL until cleanup, rather than acknowledging delivery ahead of the root workers.

Root fetch and routing are both included because high root cardinality can multiply SQL round trips. Separating coordination from delivery is useful only if discovery and fetching remain affordable.

Here, a **manifest** is just a committed row containing `(epoch, previous_snapshot, snapshot)`. The snapshot pair defines which committed journal transactions belong to the epoch. Publishing it means committing that row; it contains no payload, per-root applied progress, or readiness flags. It establishes a durable capture boundary independently of consumer delivery.

**Complete root delivery** means obtaining every journal entry for that root in the epoch, waiting out any injected delivery stall, validating the payloads, and recording completion after earlier tasks for the same root. The benchmark uses this synthetic completion as its proxy for read readiness. It does not apply changes to Falcon's MVCC graph, publish root readiness watermarks, or exercise queries. In a production design, completing application and advancing a root's readiness watermark would make that epoch available for that root; committing the manifest alone would not. One root could be ready through epoch 42 while another remains at 41, despite both using the same globally ordered manifests. Roots with no changes would also need their coverage advanced; this benchmark measures completion only for roots with entries.

The snapshot delta uses stored top-level `xid8` values. It searches the new xid range plus transactions active at the previous cut, then filters using `pg_visible_in_snapshot` against the durable new cut. Journal rows must also be visible to the actual SQL query: an aborted transaction is never accepted just because its xid is no longer active. A late commit can have an older journal sequence number, so keyset pagination restarts for every epoch. Retention must preserve rows needed by outstanding manifests and root checkpoints.

## Measurement

PostgreSQL 18 Alpine runs in a fresh Podman container for each Go benchmark invocation, inside the same six-CPU, 6 GiB VM. The container has a 4 GiB memory limit; `shared_buffers=1GB`, `effective_cache_size=3GB`, `work_mem=4MB`, and `max_wal_size=4GB`. `fsync`, `synchronous_commit`, and `full_page_writes` are enabled. The client runs on the Apple M4 Pro host. These are relative measurements on local virtualized storage, not RDS capacity predictions.

Every variant has sixteen writer connections and a consumer budget of five SQL connections. Split/counter/WAL variants have four root delivery workers; the coupled baseline intentionally uses its original single consumer. There is one cut publisher in this experiment. Polling is the same 10 ms interval for all journal implementations; WAL streams continuously. Each root's task order is preserved. Queues are bounded to 4,096 tasks, and the snapshot manifest handoff is bounded to 64 cuts. Backpressure and drain time are measured.

Capacity samples use 100,000 single-row transactions or 50,000 sixteen-row transactions. Fixed-rate and fault samples run for 20 seconds of intended arrivals. Each configuration has three repetitions, interleaved in a reproducible shuffled order. Source object selection uses 4,096 keys per root, except an explicit hot-key control. Demand is uniform, nested rank-Pareto, or 80% concentrated on one root. Actual concentration is recorded in every sample; a nominal Pareto distribution does not imply equal load across tenants or roots.

- **Write throughput:** successful source commits divided by the measured writer interval. Delivery can lag it; apply throughput and post-write drain are reported separately.
- **Freshness:** complete, ordered root delivery minus the writer's COMMIT acknowledgement, clamped to zero when delivery beats the acknowledgement. This is not a database commit timestamp. The number of such early deliveries is recorded.
- **Arrival-to-readable:** complete delivery minus intended arrival, including offered-load queueing, source execution, commit waits, and capture/delivery. This is the useful overload and source-fault latency measure.
- **Write latency:** writer start through COMMIT acknowledgement, including retries.
- **Append/commit latency:** final journal append start through COMMIT acknowledgement, including counter waits. It is not a direct row-lock wait measurement.

All operations must be delivered exactly once with the full expected checksum and row payload. At the end of every sample, delivered latest values are compared to the actual source table, detecting same-key delivery reordering. Protocol tests additionally check invisible uncommitted heads, counter rollback without gaps, blocked shared domains versus independent domains, durable snapshot replay, multi-page fetching, late commits, aborted entries, and root queue isolation. The implementations and smoke benchmarks are checked under Go's race detector.

The runner checks `podman ps -a` before and after every invocation and records both inventories. A nonempty pre-run inventory aborts the matrix. Leaked benchmark-labelled containers are removed and the run fails. No other containers are removed. Go's one-operation calibration is excluded from measurements. An early full-size attempt exposed a shutdown-only cancellation error; it was fixed and that failed attempt was rerun, excluded from results.

## Reader query optimization

The initial counter query joined arrays of changed heads directly to the journal, sorted by `(domain, position)`, then applied a global LIMIT. A generic prepared plan could satisfy that ordering by scanning old journal entries and rejecting them in the join. Preparing on a small table and then growing it to 200,000 entries reproduced PostgreSQL selecting this plan automatically. Returning 50 new entries took approximately 22–27 ms in the client; `EXPLAIN ANALYZE` reported about 28 ms and 199,950 rejected old entries.

The final reader uses a bounded `LATERAL` range per changed domain, including the page cursor and an inner LIMIT, then sorts/limits those bounded results. The automatic generic plan took about 0.05 ms in the same `EXPLAIN ANALYZE`, and warmed client calls were about 0.25–0.47 ms. Cross-domain pagination is tested for a dense 550-entry prefix across four domains. This changes the physical access path, not the counter protocol.

Earlier counter samples using the unbounded join are retained in the raw logs but excluded from the main comparisons and rerun with the bounded query. Snapshot and WAL samples from before that optimization remain valid. Query-plan diagnostics were run with the capacity runner paused and its container already removed. See [query plans](domain-capture-plans.txt) and [diagnostic code](../domain_capture_plan_test.go).

## Results

**303 accepted samples across 101 configurations, each repeated three times: 18,090,000 successful source transactions.** All samples passed checksum, exactly-once delivery, and final latest-state checks. Tables show the median of three per-run statistics; brackets show the observed minimum and maximum, not confidence intervals. They do not pool all latency observations into a single percentile. Calibration and ten superseded counter samples are excluded.

### Global cuts versus delivery barriers

Independent root delivery has its clearest benefit when one root's delivery is paused, rather than when all roots are fast. At 1,000 intended transactions/sec, every thousandth transaction belongs to root 0 and injects a 200 ms delivery pause. The table excludes those injected transactions from the healthy cohorts and reports intended-arrival-to-readable p99, in milliseconds.

| Delivery implementation | Healthy writes: affected root | Sibling roots, same tenant | Other tenants |
| --- | --- | --- | --- |
| Coupled snapshot | 202.2 [201.7–202.5] | 202.0 [202.0–202.2] | 201.6 [201.6–201.7] |
| Separate root fetch | 201.8 [199.9–203.0] | 11.4 [11.3–11.6] | 11.9 [11.8–12.2] |
| Routed snapshot | 202.0 [199.7–202.1] | 11.4 [11.4–11.6] | 11.5 [11.4–11.6] |
| Global counter | 201.5 [199.3–202.9] | 12.5 [11.3–59.4] | 28.9 [11.3–64.5] |
| Tenant counter | 201.3 [200.8–201.9] | 11.0 [10.9–11.3] | 11.0 [10.9–11.3] |
| Root counter | 202.0 [201.5–202.7] | 11.1 [10.8–11.5] | 11.1 [10.8–11.5] |
| WAL | 191.8 [191.8–191.9] | 1.0 [0.9–1.1] | 1.0 [0.9–1.1] |

Routed snapshots reduce other-tenant median p99 from about 202 ms to 12 ms, while retaining a global cut and total epoch order. Even the **global counter** permits unrelated roots to proceed when delivery is independent; its source ordering does not require a global apply barrier. WAL can use the same isolation, and reaches about 1 ms for healthy other roots in this fault test.

A worker pool is necessary: restricting either split snapshot implementation to one delivery worker brings freshness p99 back to roughly 202–208 ms. Splitting the cut publisher alone does not create delivery capacity.

The routed implementation still fetches an entire global epoch before routing its payload. It isolates root delivery stalls, not a slow global fetch query, a stalled cut publisher, or backpressure after the bounded queues fill. Separate root fetch also isolates fetching, but its many small queries have a substantial cost at high cardinality.

### Counter scope and write throughput

Single-row transactions contain 256 bytes of payload. The multi-tenant workload has 64 tenants and 16 roots per tenant. Nested rank weights use exponent 1.05; the resulting Pareto trace places 78.2% of writes on the busiest 20% of roots and 27.8% on the busiest 1%. This is a concentrated workload rather than an equal distribution across domains.

The columns below are median successful source transactions/sec, with per-run ranges. The capacity workload is closed-loop with sixteen writers, not a fixed-rate latency workload.

| Implementation | Uniform: 1,024 roots | Pareto: 1,024 roots | 80% on one root |
| --- | --- | --- | --- |
| Coupled snapshot | 7,801 [7,620–7,924] | 7,853 [7,038–8,080] | 7,812 [7,481–8,056] |
| Separate root fetch | 6,749 [6,624–6,857] | 6,939 [5,776–6,997] | 6,863 [6,658–7,729] |
| Routed snapshot | 7,864 [7,794–7,975] | 7,913 [7,859–8,270] | 7,917 [7,629–7,999] |
| Global counter | 3,221 [3,196–3,233] | 3,336 [3,265–3,499] | 3,365 [3,184–3,519] |
| Tenant counter | 7,235 [7,060–7,816] | 7,030 [5,838–7,115] | 3,567 [3,534–3,617] |
| Root counter | 7,720 [7,421–7,799] | 7,324 [6,961–7,380] | 3,620 [3,410–3,680] |
| WAL | 7,653 [6,932–7,722] | 7,711 [7,664–7,840] | 7,556 [7,389–7,736] |

Scoping the counter recovers roughly **2.1–2.2×** the global counter's Pareto write throughput. Moving from 64 tenant clocks to 1,024 root clocks adds only about **4%** at the medians; the observed ranges overlap, so this is not evidence for a dependable throughput win from hierarchy on this particular many-tenant trace. Root clocks provide a stronger isolation boundary under faults, shown below.

When 80% of writes target one root but its object keys remain diverse, the root counter falls to about 3,620/sec. Snapshots and WAL retain approximately 7,600–7,900/sec. A root counter serializes every write in that root, including updates to independent tuples.

A single large tenant is different. Tenant-only time would still use one counter; root clocks can spread that tenant's writes:

| Roots in one tenant | One counter (also tenant-only) | Root counters | Routed snapshots | WAL |
| --- | --- | --- | --- | --- |
| 1 | 3,306 [3,234–3,362] | 3,410 [3,356–3,483] | 6,893 [6,783–8,011] | 7,435 [7,283–7,660] |
| 64 | 3,339 [3,323–3,448] | 7,148 [7,029–7,612] | 7,948 [7,567–8,027] | 7,560 [7,006–7,751] |
| 1024 | 3,280 [3,177–3,427] | 7,376 [7,285–8,053] | 7,911 [7,219–7,951] | 6,997 [6,960–7,331] |

Under uniform demand, 64 local clocks already recover most of the available throughput. This is the concrete capacity argument for considering local clocks inside a high-volume tenant; it does not include shared-root generation certification costs.

### Reader cost at cardinality

At saturation with uniform demand across 1,024 roots, separate root fetching produces median freshness p99 of **1,852 ms** (range 1,670–2,012 ms). Routed snapshots stay at **12.6 ms** (12.5–12.7 ms). Separate fetching makes about **1.015 consumer SQL queries per transaction**, versus **0.038** for routed snapshots, including capture/discovery queries. Its queue grows and post-write drain reaches about 0.53 seconds. Its source throughput is therefore not a claim that delivery remained fresh at that rate.

On Pareto load, multiple events for hot roots amortize those fetches: separate fetching reaches about 6,939 writes/sec with 20.6 ms freshness p99, compared with 7,913/sec and 12.7 ms for routed snapshots. Root cardinality without attention to SQL batching is not sufficient for a scalable consumer.

### Healthy fixed-rate latency

These samples contain no injected faults. Entries are median p99 `[minimum–maximum]`, in milliseconds. Freshness begins at the writer's COMMIT acknowledgement. Arrival includes scheduling, source execution and commit, then capture/delivery.

| Implementation | 1k/sec freshness | 1k/sec arrival | 3k/sec freshness | 3k/sec arrival |
| --- | --- | --- | --- | --- |
| Coupled snapshot | 11.4 [10.9–11.6] | 162.8 [33.2–279.2] | 10.7 [10.7–11.4] | 11.6 [11.6–230.6] |
| Separate root fetch | 11.7 [11.6–12.1] | 12.6 [12.6–14.3] | 13.9 [13.5–17.0] | 16.9 [14.8–278.5] |
| Routed snapshot | 10.9 [10.7–11.4] | 11.7 [11.6–74.5] | 11.0 [11.0–15.1] | 12.0 [11.9–590.7] |
| Global counter | 10.3 [9.8–10.4] | 11.4 [11.1–14.1] | 10.3 [10.2–10.3] | 32.5 [11.2–38.5] |
| Tenant counter | 10.3 [10.2–10.4] | 11.3 [11.3–12.0] | 10.3 [10.3–10.4] | 13.0 [11.1–1,135.4] |
| Root counter | 10.1 [9.9–10.7] | 17.3 [12.7–153.0] | 10.7 [10.7–10.7] | 12.8 [11.5–14.3] |
| WAL | 0.1 [0.1–0.2] | 1.4 [1.2–4.9] | 0.4 [0.2–0.5] | 6.1 [5.9–27.8] |

The 10 ms polling interval accounts for much of journal freshness. These runs do not compare commit wakeups or a tuned shorter poll interval. At 3,000/sec, root and tenant counters have about 12.8–13.0 ms median arrival p99; the global counter reaches about 32.5 ms near its source capacity. Routed snapshots are about 12.0 ms at the median.

**The healthy end-to-end tails are noisy.** Several samples have source-write stalls of hundreds of milliseconds, and some exceed a second. These generate offered-load queueing even when average commits/sec meets the target. For example, the 3k tenant-counter sample with 1,135 ms arrival p99 contains a 917 ms source operation; a routed snapshot sample with 591 ms arrival p99 contains a 434 ms source operation. The 1k coupled snapshot median is 163 ms, and all three repetitions have elevated arrival p99, despite 11.4 ms median freshness p99. Source wait events were not sampled, so these stalls cannot be attributed precisely to IO, lock waits, the client, or an external resource interruption. No samples were discarded for being slow. These results do not justify a precise small healthy-latency ranking or an intrinsic sub-millisecond claim.

### Stalled writer: narrower clocks and shared-pool spillover

Every thousandth transaction pauses for 200 ms **after** journal allocation and before COMMIT. It holds the selected counter row throughout that pause. The following p99 values measure intended arrival through readable delivery and exclude the injected operations from the healthy cohorts.

| Implementation | Healthy writes: affected root | Sibling roots, same tenant | Other tenants |
| --- | --- | --- | --- |
| Coupled snapshot | 11.5 [11.3–11.6] | 11.5 [11.3–11.7] | 11.5 [11.3–11.7] |
| Separate root fetch | 11.8 [11.7–12.1] | 11.9 [11.8–12.3] | 12.3 [12.2–12.9] |
| Routed snapshot | 11.8 [11.2–13.3] | 11.9 [11.2–13.2] | 11.9 [11.2–12.5] |
| Global counter | 221.3 [216.7–240.8] | 220.2 [215.3–258.7] | 220.0 [219.0–243.7] |
| Tenant counter | 258.3 [214.6–446.2] | 234.4 [208.8–458.3] | 207.8 [157.9–423.3] |
| Root counter | 199.0 [197.3–200.5] | 12.2 [11.7–18.0] | 18.1 [16.3–23.4] |
| WAL | 1.1 [1.1–1.1] | 1.1 [1.1–1.2] | 1.1 [1.0–1.2] |

The global counter spreads a held clock across all writers. A tenant counter confines the database row lock to one tenant, but the blocked requests can consume all sixteen shared writer slots and delay other tenants. Root counters keep the directly blocked set much smaller: sibling-root median p99 is about **12 ms** and other tenants about **18 ms**, while healthy writes on the affected root wait about **199 ms**. This is stronger evidence for a root-level QoS benefit than the small healthy-throughput difference from tenant clocks.

A pause **before** allocation leaves the counter available to other writers: healthy arrival p99 stays near 11–12 ms for journal implementations. This validates doing the counter update late. Snapshots exclude active writers from cuts and later recover their committed journal entries, without waiting on a sequence hole; healthy roots remain near 12 ms after an allocated-writer pause too.

Rollback tests preserve dense counter prefixes and complete delivery. Snapshot sequence allocations can leave holes, but visibility-based capture does not require filling them. Some rollback samples have large source stalls, so their arrival distributions are available in the CSV rather than summarized as a rollback latency improvement.

### Larger payloads and hot keys

The larger workload upserts sixteen 1 KiB source rows per transaction and journals their full 16 KiB payload. Source and journal working sets together exceed shared buffers. These are saturated samples, not 1k/sec healthy comparisons.

| Implementation | Transactions/sec | Freshness p99 (ms) |
| --- | --- | --- |
| Coupled snapshot | 2,323 [2,029–3,031] | 34.2 [24.7–68.3] |
| Separate root fetch | 2,054 [2,044–2,523] | 50.6 [34.7–52.5] |
| Routed snapshot | 2,943 [2,353–3,107] | 25.4 [25.1–44.3] |
| Global counter | 1,481 [1,452–1,621] | 12.1 [12.1–12.1] |
| Tenant counter | 2,975 [2,633–3,265] | 14.2 [14.2–14.3] |
| Root counter | 3,086 [2,511–3,091] | 15.0 [14.9–15.0] |
| WAL | 4,349 [4,162–5,158] | 3.4 [2.7–3.7] |

At the medians, routed snapshots, tenant counters and root counters are close to 2,900–3,100 transactions/sec. WAL reaches about 4,349/sec. The global counter is about 1,481/sec. Native counter positions avoid another durable epoch publication and permit page-prefix delivery; their measured freshness tail is lower than snapshot epoch delivery here. The throughput ranges are broad enough that small rankings should be treated cautiously.

With **one object key** and 80% of writes on its root, every tested design instead reaches about 2,400–2,600 writes/sec: global counter 2,497, tenant counter 2,528, root counter 2,552, routed snapshot 2,523, and WAL 2,494. Clock partitioning cannot remove the latest-state row lock. The journal implementations use late allocation after the source mutation, and the final-state validation verifies that updates to a key are delivered in the source's order.

### Two million retained journal entries

These samples preload two million historical entries, then deliver 20,000 new transactions at 1,000/sec. Historical entries are bulk loaded, already considered applied, and excluded from capture. They test physical table/index size and delta access; their xid distribution is not a replay of two million individual historical commits. No old partitions are dropped in this experiment.

| Implementation | Journal + indexes (MiB) | Freshness p99 (ms) | Arrival p99 (ms) |
| --- | --- | --- | --- |
| Coupled snapshot | 873 | 10.3 [10.1–10.6] | 11.2 [10.9–11.6] |
| Separate root fetch | 873 | 11.8 [11.3–12.2] | 13.7 [12.2–18.6] |
| Routed snapshot | 873 | 10.8 [10.2–15.0] | 11.7 [11.0–879.7] |
| Global counter | 692 | 10.4 [9.9–10.4] | 11.2 [11.2–47.8] |
| Tenant counter | 693 | 10.1 [10.0–10.2] | 11.2 [11.0–11.9] |
| Root counter | 693 | 10.2 [10.0–10.2] | 11.1 [11.0–11.6] |

The bounded counter query avoids the retained-history scan cliff. All counter scopes remain near 10 ms freshness and 11 ms median arrival p99; root/tenant ranges are tight here. Routed snapshots have a large arrival outlier while their median stays near 12 ms. Partitioning, deletion/vacuum cost, old-cut leases, publisher failover, and distributed consumer ownership still need separate experiments.

## What to pursue next

1. **Global cuts with batched fetch and independent root delivery** are the strongest snapshot candidate. They preserve global ordering, recover much of WAL's delivery isolation, and avoid the query amplification of one fetch per dirty root. A shared fetch stage and cut publisher remain potential bottlenecks; retention must tolerate a lagging root.
2. **Late root counters with batched domain reads** are worth deeper work for large tenants and QoS. They recover the global counter's throughput loss and contain held-clock faults better than tenant clocks. For this many-tenant Pareto trace, tenant counters already recover most healthy throughput. A very hot root remains a counter bottleneck.
3. Before comparing hierarchical MVCC protocols, try **writer admission per clock** so stalled-clock waiters do not monopolize connections, and **coalesced post-commit wakeups** to remove the common polling floor. Those are suggested follow-ups, not measured optimizations here. For large payloads, separating a thin ordering entry from payload insertion could further shorten the counter critical section; its extra write/read cost needs measurement.

The experiment supports isolating delivery immediately without giving up global checkpoint order. It supports local writer clocks where intra-tenant concurrency or fault isolation warrants them. It does not establish that full hierarchy beats tenant-only MVCC once dependency certification and snapshot-window bookkeeping are included.

## Validation and reproduction

`go test -count=1 ./...` passed. The protocol/integrity tests and all seven smoke implementations passed under the race detector. Checks cover counter visibility/rollback/lock isolation, dense pagination across domains, durable raw-cut replay, multi-page root deltas, late/aborted writers, root scheduling, duplicate/corrupt payload rejection, and empty epochs after completion. Every measured sample independently validates checksums and the final latest-state table.

The full performance matrix ended with `podman ps -a` equal to `[]`; every recorded pre/post invocation inventory was empty. Benchmark containers were never run concurrently. Existing unused anonymous volumes were not accumulating with the matrix. Cleanup uses a fresh background context rather than the workload's canceled context. Final validation containers are also cleaned up before delivery of this report.

Build the standalone binary, export a Docker-compatible Podman socket in `DOCKER_HOST`, and run phases sequentially:

```sh
go test -c -o /tmp/falcon-domain-bench.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-domain-capture.py capacity
python3 internal/infrastructure/postgres/benchmark-results/run-domain-capture.py latency
python3 internal/infrastructure/postgres/benchmark-results/run-domain-capture.py faults
python3 internal/infrastructure/postgres/benchmark-results/run-domain-capture.py controls
python3 internal/infrastructure/postgres/benchmark-results/run-domain-capture.py history
python3 internal/infrastructure/postgres/benchmark-results/summarize-domain-capture.py
python3 internal/infrastructure/postgres/benchmark-results/report-domain-capture.py
```

The recorded matrix additionally uses `repairs` to rerun the ten superseded counter samples. A fresh run of the final implementation does not need repairs. Raw phase logs retain calibration, attempted/failed invocations, timestamps, inventories and original per-run metrics. Scripts append results; remove only the intended generated phase output files before a complete rerun. The runner respects an exported `DOCKER_HOST`; `PODMAN` can override its default macOS CLI path.

- [Implementation](../domain_capture_bench_test.go): schema, source writers, durable cut capture, independent fetch/routing, counter prefix reads, WAL updates, scheduling and metrics.
- [Protocol tests](../domain_capture_protocol_test.go), [query-plan diagnostic](../domain_capture_plan_test.go).
- [All accepted samples](domain-capture-samples.csv), [median/range summary](domain-capture-summary.csv).
- [Raw capacity](domain-capture-capacity.txt), [counter reruns](domain-capture-repairs.txt), [fixed-rate latency](domain-capture-latency.txt), [faults](domain-capture-faults.txt), [controls](domain-capture-controls.txt), [retained history](domain-capture-history.txt), [query plans](domain-capture-plans.txt).
- [Repository validation](domain-capture-validation.txt), [race validation](domain-capture-race.txt), [race/smoke runs](domain-capture-smoke.txt).
