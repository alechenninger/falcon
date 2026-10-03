# Global snapshot epochs: wakeups and fair payload fetching

This follow-up compares a 2×2 matrix: global routed fetch versus fair batched root fetch, each with 10 ms polling or coalesced post-commit NOTIFY wakeups. All variants retain one globally ordered snapshot epoch timeline. Falcon's production store, graph, MVCC windows, and wire protocol are not changed.

## Protocol and implementation

Writers upsert the same latest-state tuple table and append the same complete transaction payload, xid8, root ID, and CACHE 1 sequence position in one transaction. The post-commit callback only places a hint in a capacity-one local channel; it performs no SQL and does not wait for consumer work.

The notification bridge has one dedicated sender and listener for the writer process. After a quiet period it sends immediately. During a burst, notification starts are spaced by at least 2 ms, merging pending hints. It dequeues before sending so a commit during the SQL round trip leaves a new hint. The listener wakes the single epoch publisher. Registration completes before the initial capture. A 100 ms fallback countdown is reset after every capture attempt and covers missed hints. Tests deliberately drop all hints and include rolled-back writes.

Polling captures and publishes at 10 ms after the preceding capture/handoff completes. Notification captures add an EXISTS check over indexed xid ranges in the same repeatable-read transaction; an empty check commits no manifest and leaves the preceding durable cut unchanged. A nonempty cut is committed before any payload fetching. Notification hints carry no durable progress.

This polling control uses a countdown after each capture, whereas the previous domain-capture benchmark used a fixed ticker. Compare wakeup treatments within this matrix; the older benchmark's polling numbers are not an exact control. The original EXISTS query can also get a poor generic plan with retained history; the separate bounded-probe follow-up below addresses that regression.

**Global routed fetch** reads all pages for an epoch, groups the complete payload by root, and hands it to four ordered root delivery workers. Its fetch stage remains serial and shared.

**Fair fetch** first discovers each affected root's exact count from the snapshot delta. Four workers then fetch pages from root queues. Each SQL batch contains up to 64 roots and 128 total entries, with a 2 MiB payload cap and a maximum of 64 entries from a single root. Queries use bounded LATERAL ranges with a separate cursor and snapshot pair for every root; roots in the same query can therefore be at different epochs. A root with at least 16 remaining entries, or more than half of observed entries after accumulating 16, gets its own batch. Other roots are batched together. After a page, its root returns to the back of the ready queue. At most one page is active per root.

Fair workers validate and stage changes page by page, releasing payloads after validation. They retain verified identifiers until the complete root epoch has arrived, then record a single readiness time. Earlier epochs for that root must finish first. This simulates private staging followed by publication; it does not implement graph staging or expose partial epochs. Quiet roots can finish while a heavy root has an incomplete epoch. Root tasks are atomically enqueued per discovered cut so workers can batch them.

Both algorithms keep global epoch order. The publisher-to-reader channel holds at most 64 cuts, and the root task queues hold at most 4,096 unfinished root epochs. These shared bounds can still transmit backpressure. Fair page payloads are bounded; staged identifiers and pending metadata can grow until the task limit. Global routed fetch retains the whole epoch payload, which has no byte bound in this baseline.

The shared discovery query, publisher, database resources, ownership/recovery, and retention remain potential sources of coupling. The benchmark measures readiness of roots with changes. Advancing coverage for unchanged roots and actual per-root MVCC readiness remain deferred.

The bridge fails the harness on connection errors. Production reconnect, publisher election, and failover are outside this experiment. Missing hints are covered by reconciliation; notifications do not establish ordering or replace the durable journal and cut manifest.

## Environment and measurement

Fresh PostgreSQL 18 Alpine containers run sequentially in the same six-CPU, 6 GiB Podman VM. Each container has a 4 GiB limit, 1 GiB shared buffers, 3 GiB effective cache size, 4 MiB work memory, and 4 GiB max WAL size. Fsync, synchronous commit, and full-page writes are enabled. The client runs on the Apple M4 Pro host.

All variants use sixteen writer connections and a five-connection consumer pool. Notification variants additionally use two dedicated bridge connections; their SQL calls and network traffic are included in consumer costs. The fair variant uses four fetch/stage workers; the global variant uses one payload reader and four delivery workers.

Capacity uses 100,000 single-row transactions or 50,000 sixteen-row transactions. Fixed-rate samples span 20 seconds, except the large dominant-root case spans 30 seconds. Demand is uniform, nested Pareto, or 80%/99% concentrated on one root; quiet-root samples are reported separately. Every configuration has three repetitions, shuffled within its phase with a recorded seed. Hashes of the implementation files are recorded per sample.

Freshness is complete root delivery minus COMMIT acknowledgement, clamped to zero when delivery precedes the acknowledgement. Arrival includes offered-load queueing and source execution. No PostgreSQL commit timestamp is inferred. Full checksums, exactly-once delivery, and final delivered values versus the latest-state source table are mandatory for every sample.

Network counters wrap actual socket reads/writes for the consumer pool and notification bridge, excluding connection establishment before measurement. They measure database/client traffic, not future Falcon MVCC RPC traffic. Logical retained-payload counters and sampled client heap describe this harness; they do not establish production memory overhead.

The runner records empty container inventories before and after every invocation, aborts on unexpected containers, and cleans only benchmark-labelled leaked containers on failure. No benchmark containers run concurrently.

## Results

**162 measured samples, 54 configurations, 8,502,000 source transactions.** All accepted source samples passed full-payload checksums, exactly-once delivery, and final latest-state checks. Values are medians of per-run statistics; brackets give minimum and maximum, not confidence intervals. Idle operations count seconds, not writes. Smoke and race runs and Go calibration are excluded.

### Source capacity

| Variant | Uniform, 256 B (tx/sec) | Pareto, 256 B (tx/sec) | Pareto, 16 KiB (tx/sec) |
| --- | --- | --- | --- |
| Global fetch / poll | 8,056 [7,941–8,074] | 7,603 [7,533–8,037] | 3,157 [2,724–3,173] |
| Global fetch / notify | 7,566 [7,505–7,574] | 7,630 [7,509–7,817] | 3,086 [3,050–3,088] |
| Fair fetch / poll | 7,917 [7,908–7,937] | 7,975 [7,974–7,989] | 3,237 [3,183–3,268] |
| Fair fetch / notify | 7,359 [7,327–7,482] | 7,487 [7,380–7,508] | 2,738 [2,668–3,154] |
| WAL | 7,598 [7,517–7,671] | 7,666 [6,864–7,836] | 5,089 [4,859–5,229] |

### Healthy freshness

Freshness starts at COMMIT acknowledgement. Arrival starts at the intended arrival and includes source queueing and execution.

| Variant | 50/sec freshness p99 (ms) | 1k/sec freshness p99 (ms) | 3k/sec freshness p99 (ms) | 3k/sec arrival p99 (ms) |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 18.59 [18.56–18.96] | 11.74 [11.62–11.89] | 12.15 [11.90–16.40] | 13.08 [12.71–19.80] |
| Global fetch / notify | 6.94 [6.86–7.09] | 2.96 [2.94–2.99] | 8.41 [3.65–8.53] | 11.93 [4.83–12.68] |
| Fair fetch / poll | 20.90 [20.43–21.69] | 12.04 [11.80–12.29] | 17.36 [12.68–18.08] | 154.39 [13.72–416.24] |
| Fair fetch / notify | 8.24 [8.16–8.79] | 3.25 [3.24–3.28] | 4.13 [4.08–4.16] | 5.36 [5.36–5.40] |
| WAL | — | 0.12 [0.11–0.12] | — | — |

### Quiet-root isolation

Every scenario uses 1,024 roots. Quiet roots are all roots other than root 0. Percentiles include only their writes. The fetch-stall scenario pauses the client payload-fetch stage for 200 ms on every thousandth source operation, assigned to root 0. This is an injected stall, not a measured PostgreSQL query fault.

**hot80-small**

| Variant | Quiet freshness p99 (ms) | Quiet arrival p99 (ms) | Source write p99 (ms) | Post-write drain (sec) |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 16.82 [12.12–16.98] | 300.88 [13.35–426.59] | 6.39 [1.56–6.51] | 0.010 [0.002–0.012] |
| Global fetch / notify | 7.97 [3.64–9.33] | 10.78 [4.81–414.64] | 4.80 [1.42–6.45] | 0.001 [0.001–0.008] |
| Fair fetch / poll | 17.20 [15.33–17.72] | 417.16 [206.98–516.43] | 5.64 [3.55–5.89] | 0.012 [0.009–0.013] |
| Fair fetch / notify | 9.03 [4.44–9.18] | 14.06 [5.71–465.29] | 4.85 [1.62–5.89] | 0.002 [0.002–0.002] |

**hot99-small**

| Variant | Quiet freshness p99 (ms) | Quiet arrival p99 (ms) | Source write p99 (ms) | Post-write drain (sec) |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 16.70 [16.53–16.70] | 22.46 [20.57–131.00] | 5.70 [5.43–6.23] | 0.012 [0.006–0.013] |
| Global fetch / notify | 7.42 [6.98–9.34] | 244.90 [16.05–374.64] | 5.88 [4.79–6.52] | 0.001 [0.001–0.003] |
| Fair fetch / poll | 17.08 [15.23–17.56] | 21.30 [17.87–117.89] | 5.46 [4.99–6.01] | 0.004 [0.002–0.007] |
| Fair fetch / notify | 4.57 [4.39–4.79] | 5.78 [5.60–5.96] | 1.51 [1.50–1.54] | 0.001 [0.001–0.002] |

**hot99-large**

| Variant | Quiet freshness p99 (ms) | Quiet arrival p99 (ms) | Source write p99 (ms) | Post-write drain (sec) |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 74.48 [33.33–76.27] | 4,097.29 [816.67–4,981.56] | 40.20 [23.59–47.61] | 0.011 [0.005–0.026] |
| Global fetch / notify | 14.33 [13.11–14.95] | 99.87 [75.98–127.47] | 10.57 [9.90–10.74] | 0.002 [0.001–0.005] |
| Fair fetch / poll | 43.47 [20.16–50.35] | 2,665.97 [98.58–4,734.71] | 27.31 [10.31–32.26] | 0.009 [0.004–0.039] |
| Fair fetch / notify | 17.58 [17.48–17.62] | 1,676.79 [1,368.14–1,887.84] | 12.55 [11.81–16.49] | 0.002 [0.002–0.002] |

**hot99-fetch-stall**

| Variant | Quiet freshness p99 (ms) | Quiet arrival p99 (ms) | Source write p99 (ms) | Post-write drain (sec) |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 210.41 [205.84–217.22] | 504.59 [206.49–530.85] | 2.57 [2.07–6.07] | 0.212 [0.209–0.212] |
| Global fetch / notify | 200.08 [200.03–200.10] | 201.06 [200.99–201.27] | 1.54 [1.44–6.55] | 0.204 [0.203–0.204] |
| Fair fetch / poll | 16.99 [12.61–17.74] | 255.77 [13.51–385.73] | 5.61 [1.26–5.81] | 0.213 [0.202–0.214] |
| Fair fetch / notify | 4.76 [4.69–7.68] | 6.59 [6.46–713.86] | 1.68 [1.64–2.81] | 0.204 [0.203–0.204] |

### Idle load

Each sample observes five measured seconds without source writes after startup. SQL counts include snapshot SELECT, EXISTS/manifest statements, payload queries, and notification SELECTs; BEGIN/COMMIT protocol commands are not counted. Wire traffic includes the consumer pool and NOTIFY sender/listener.

| Variant | SQL calls/sec | Committed epochs/sec | Consumer wire KiB/sec |
| --- | --- | --- | --- |
| Global fetch / poll | 201.4 [200.3–204.5] | 67.0 [66.8–68.2] | 50.40 [50.16–51.63] |
| Global fetch / notify | 19.2 [19.0–19.2] | 0.0 [0.0–0.0] | 4.78 [4.71–4.78] |
| Fair fetch / poll | 201.4 [199.6–211.2] | 67.2 [66.4–70.4] | 45.97 [45.15–47.81] |
| Fair fetch / notify | 18.8 [18.8–19.2] | 0.0 [0.0–0.0] | 4.68 [4.68–4.76] |

### Consumer cost and buffering

Peak retained payload counts live fetched payload bytes, including delivery queues; it excludes PGX transient buffers, slice/map overhead, and the precomputed validation fixture. Peak Go heap includes the entire client fixture and is sampled every 200 ms. Staged identifiers and metadata are additional to retained payload, available in raw samples.

**capacity-uniform**

| Variant | SQL calls/write | Consumer wire bytes/write | Peak payload MiB | Peak client heap MiB | WAL bytes/write |
| --- | --- | --- | --- | --- | --- |
| Global fetch / poll | 0.030 [0.030–0.030] | 307 [307–307] | 0.027 [0.027–0.028] | 50.8 [40.4–53.2] | 1,149 [1,149–1,150] |
| Global fetch / notify | 0.234 [0.234–0.234] | 365 [365–365] | 0.012 [0.011–0.013] | 42.5 [41.4–56.1] | 1,161 [1,161–1,162] |
| Fair fetch / poll | 0.051 [0.050–0.051] | 409 [409–409] | 0.026 [0.026–0.027] | 39.3 [32.2–47.0] | 1,149 [1,149–1,149] |
| Fair fetch / notify | 0.278 [0.276–0.280] | 500 [499–502] | 0.011 [0.011–0.013] | 42.1 [35.2–42.1] | 1,161 [1,161–1,161] |

**capacity-large**

| Variant | SQL calls/write | Consumer wire bytes/write | Peak payload MiB | Peak client heap MiB | WAL bytes/write |
| --- | --- | --- | --- | --- | --- |
| Global fetch / poll | 0.064 [0.063–0.065] | 16,445 [16,445–16,445] | 1.000 [1.000–1.047] | 22.7 [20.9–31.6] | 38,452 [38,452–38,455] |
| Global fetch / notify | 0.348 [0.347–0.348] | 16,529 [16,529–16,529] | 0.609 [0.547–0.656] | 21.7 [21.5–29.3] | 38,471 [38,471–38,472] |
| Fair fetch / poll | 0.084 [0.083–0.085] | 16,541 [16,540–16,542] | 1.000 [0.984–1.016] | 24.1 [23.7–27.0] | 38,453 [38,453–38,453] |
| Fair fetch / notify | 0.397 [0.395–0.409] | 16,679 [16,678–16,685] | 0.547 [0.547–0.969] | 24.1 [22.4–25.7] | 38,471 [38,471–38,471] |

**hot99-fetch-stall**

| Variant | SQL calls/write | Consumer wire bytes/write | Peak payload MiB | Peak client heap MiB | WAL bytes/write |
| --- | --- | --- | --- | --- | --- |
| Global fetch / poll | 0.089 [0.088–0.090] | 322 [322–322] | 0.026 [0.025–0.027] | 24.9 [20.0–27.4] | 1,148 [1,147–1,148] |
| Global fetch / notify | 0.692 [0.652–0.692] | 479 [469–479] | 0.052 [0.052–0.053] | 22.2 [22.1–25.3] | 1,177 [1,174–1,178] |
| Fair fetch / poll | 0.125 [0.125–0.129] | 339 [339–340] | 0.016 [0.016–0.016] | 27.2 [21.0–27.2] | 1,147 [1,146–1,147] |
| Fair fetch / notify | 0.992 [0.927–0.993] | 589 [570–589] | 0.007 [0.007–0.008] | 24.5 [20.6–26.4] | 1,184 [1,182–1,185] |

**hot99-large**

| Variant | SQL calls/write | Consumer wire bytes/write | Peak payload MiB | Peak client heap MiB | WAL bytes/write |
| --- | --- | --- | --- | --- | --- |
| Global fetch / poll | 0.095 [0.087–0.115] | 16,452 [16,450–16,457] | 1.031 [1.016–1.078] | 28.7 [27.2–33.3] | 39,296 [39,290–39,638] |
| Global fetch / notify | 1.133 [1.128–1.136] | 16,724 [16,723–16,725] | 0.500 [0.484–0.547] | 22.0 [21.6–26.6] | 39,724 [39,465–39,725] |
| Fair fetch / poll | 0.141 [0.117–0.177] | 16,473 [16,466–16,483] | 1.000 [0.953–1.000] | 29.2 [24.8–32.0] | 39,567 [39,538–39,635] |
| Fair fetch / notify | 1.159 [1.131–1.178] | 16,771 [16,763–16,777] | 0.547 [0.531–0.547] | 22.2 [22.1–22.7] | 39,646 [39,580–39,785] |

### Retained journal history

Two million historical rows are bulk loaded before measuring 20,000 writes at 1k/sec. This tests table/index size, not two million historical commit IDs. Retention deletion is not exercised.

| Variant | Freshness p99 (ms) | Arrival p99 (ms) | Fetch SQL p99 (ms) | SQL calls/write |
| --- | --- | --- | --- | --- |
| Global fetch / poll | 11.83 [11.71–11.89] | 12.81 [12.63–12.94] | 0.63 [0.53–0.63] | 0.274 [0.273–0.275] |
| Global fetch / notify | 67.37 [2.99–143.84] | 68.12 [3.80–408.49] | 0.45 [0.43–0.54] | 2.420 [2.264–2.448] |
| Fair fetch / poll | 12.28 [12.19–12.29] | 13.24 [13.23–13.32] | 0.59 [0.58–0.67] | 0.364 [0.363–0.364] |
| Fair fetch / notify | 4.18 [3.35–96.07] | 8.08 [4.16–96.79] | 0.54 [0.52–0.54] | 2.926 [2.902–2.939] |

### Notification coalescing control

Notification minimum spacing is 2 ms in the main experiment and zero in the control. The first send after an idle period is immediate in both. Compare the same Pareto capacity workload.

| Fetch | Spacing | Write tx/sec | Notifications/sec | Epochs/sec | Freshness p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| global | 2 ms | 7,630 [7,509–7,817] | 491 [491–492] | 322 [314–326] | 16.47 [12.96–16.72] |
| global | 0 ms | 7,277 [7,207–7,315] | 1,760 [1,749–1,762] | 304 [304–304] | 7.04 [6.99–13.34] |
| fair | 2 ms | 7,487 [7,380–7,508] | 491 [491–492] | 316 [312–316] | 7.75 [7.67–20.47] |
| fair | 0 ms | 7,187 [7,145–7,190] | 1,726 [1,725–1,728] | 302 [301–302] | 8.11 [8.09–8.21] |

### Bounded notification probe follow-up

An additional **18 samples and 840,000 source transactions** use `snapshot_probe=indexed`. They retain the same wakeups, global epochs, payload fetching, and source transactions, changing only the empty-snapshot probe. These samples have a separate recorded implementation hash and are not substituted into the original matrix above. The CSV includes both sets.

| Variant | 1k/sec freshness p99 (ms) | Pareto capacity (tx/sec) | 2m history freshness p99 (ms) | 2m history maximum capture (ms) |
| --- | --- | --- | --- | --- |
| Global fetch / notify | 2.92 [2.90–3.00] | 7,621 [7,563–7,634] | 3.05 [3.05–3.07] | 17.53 [6.44–19.17] |
| Fair fetch / notify | 3.29 [3.22–3.46] | 7,556 [7,477–7,719] | 3.38 [3.36–3.42] | 9.65 [9.40–9.93] |


## Interpretation

Coalesced notifications are the strongest general improvement here. At 1,000 writes/sec, healthy p99 falls from 11.74 to 2.96 ms with global fetch and from 12.04 to 3.25 ms with fair fetch. At 50/sec there is still a 7–8 ms p99 floor, so waking the publisher does not remove durable capture, fetch, and delivery costs. With no writes, the 100 ms fallback performs about 19 SQL calls/sec instead of 201, commits no empty epochs, and uses about one tenth of the consumer network traffic. This is not zero idle load.

Fair fetching provides a specific QoS benefit: a dominant root's payload-fetch stall no longer blocks every quiet root. With 99% of writes on root 0 and a 200 ms injected fetch-stage pause every 1,000 operations, quiet-root p99 is 200.08 ms for global/notify and 4.76 ms for fair/notify, about a 42× reduction. The quiet cohort contains roughly 600 writes per sample; its p99 estimates have less support than the all-write percentiles. The busy root still pays its own stall, and one fair/notify run has quiet **arrival** p99 of 714 ms despite freshness p99 below 8 ms. Fair scheduling isolates this fetch-stage delay; it does not isolate shared source writes, the publisher, the discovery query, or database resource pressure.

Fair fetch is not a universal throughput or latency improvement. Healthy 1k/sec p99 adds about 0.3 ms compared with global/notify. In the Pareto capacity runs, fair/notify delivers 7,487 tx/sec versus global/notify's 7,630; uniform capacity is 7,359 versus 7,566. The large-payload medians are 2,738 versus 3,086, an 11% reduction. WAL reaches 5,089 tx/sec for that workload and remains much faster for healthy freshness: about 0.12 ms at 1k/sec. Three repetitions and the reported ranges support broad tradeoffs, not precise rankings between similar small-payload capacity results.

The large dominant-root scenario is particularly important: global/notify has quiet-root freshness p99 of 14.33 ms and arrival p99 of 100 ms; fair/notify has 17.58 ms and 1.68 sec respectively. Some poll runs also fall below the offered 2,000 tx/sec. Fair fetch does not fix large write/commit stalls or guarantee a bound on end-to-end arrival latency. These samples are retained rather than excluded as outliers. Stage maxima in the raw data show long source-write and capture delays while payload query maxima remain below 9 ms; these runs do not include wait-event sampling sufficient to attribute the source stalls to a particular database resource.

### Cost of fairness and wakeups

For uniform small-payload capacity, fair/notify uses 0.278 consumer SQL calls/write and 500 consumer wire bytes/write versus global/notify's 0.234 and 365: about 19% more calls and 37% more database/client traffic. Relative to global/poll, global/notify increases SQL from 0.030 to 0.234 calls/write because it publishes smaller, more frequent cuts and performs the empty-snapshot probe and separate notification SQL. Notifications reduce idle work while increasing active bookkeeping. Those costs are outside the source transaction, but still compete for shared resources.

With 16 KiB transactions, additional fair-query traffic is a small fraction of payload bytes: 16,679 versus 16,529 bytes/write. Extra queries, discovery, and concurrent connection use still matter. Journal variants also append the transaction payload in addition to updating latest state; the WAL control avoids that extra journal write. This matrix does not identify a single limiting device or extrapolate to a particular RDS class.

Fair fetch structurally bounds a SQL batch to 128 entries and 2 MiB, and one root contributes at most 64 entries. Four workers can retain several batches at once. Healthy measured payload peaks are small for both approaches; the data do not establish a large production memory saving. The fair sink retains verified frame identifiers until a root epoch finishes, while a real implementation would also need private graph/MVCC staging. Client heap contains workload fixtures and is not a production sizing estimate. Future MVCC RPC bandwidth and window metadata are not measured.

At Pareto capacity, spacing notification starts by 2 ms reduces sends from roughly 1,750/sec to 491/sec. It raises source capacity by about 4–5% compared with zero spacing. Freshness is not uniformly improved: global zero-spacing median p99 is lower, with variation across runs, while fair zero-spacing p99 is similar. An immediate first send plus bounded burst coalescing is a useful policy; its spacing remains a throughput/freshness choice.

### Retained-history regression and bounded probe

The original notification implementation has a real retained-history regression. With two million old journal rows, global/notify per-run p99 ranges from 2.99 to 143.84 ms; fair/notify ranges from 3.35 to 96.07 ms. Fetch-query p99 remains below 0.6 ms. Rare publisher delays explain why a low capture-stage p99 can coexist with a large per-write freshness p99: a single delayed cut holds many committed writes.

The isolated server-side trace reproduces global/notify freshness p99 of 128.77 ms and logs four executions of the EXISTS probe lasting 162–199 ms. Prepared `EXPLAIN EXECUTE` then demonstrates the failure mechanism: a generic plan for the old-active-xid branch scans the journal's position primary-key index, applying the writer-xid array and snapshot visibility as filters. Empty arrays and committed transactions without journal rows can therefore scan the retained journal. The plan removes about two million rows. A custom plan uses the writer-xid index and avoids the scan. PostgreSQL's documentation explains the automatic choice between parameter-specific and generic plans and how to inspect it with [`EXPLAIN EXECUTE`](https://www.postgresql.org/docs/18/sql-prepare.html).

The optional `snapshot_probe=indexed` implementation keeps the new-xid range and replaces the old-xid ANY branch with `unnest` plus an ordered, bounded LATERAL lookup for each formerly active xid. Still-active xids are rejected by snapshot visibility before lookup. Each transaction's journal rows share visibility, so one matching row proves that it changed the journal; no payload or complete delta is needed for this existence check. Paired xid range bounds and index ordering avoid the broad equality/array estimate seen in this fixture. This changes the existence probe, not the cut boundaries, root progress semantics, or journal-retention requirements.

With default automatic planning and forced generic plans, the isolated original empty/active probes take roughly 130–185 ms; the bounded probes take about 0.2–0.4 ms and correctly include a formerly active transaction after commit. The snapshot test also covers a committed xid with no journal row. An ordering-only subquery plus empty-array guard was investigated first; it added parallel-query startup and did not demonstrate a bounded lookup for nonempty arrays, so the per-xid lookup is the implemented follow-up. Full query plans and slow-statement logs are retained below.

The eighteen additional pipeline samples assess retained history, healthy 1k/sec latency, and Pareto capacity with the bounded probe. With two million retained entries, all three global/notify runs have p99 of 3.05–3.07 ms and all three fair/notify runs have p99 of 3.36–3.42 ms. Healthy 1k/sec medians remain 2.92 and 3.29 ms; Pareto capacity remains 7,621 and 7,556 tx/sec respectively. Maximum capture times stay below 20 ms in those history runs. None replace the original matrix or its regressions. Bulk history has only one old xid, which exaggerates skew in planner statistics. A journal with many historical commit IDs may choose different plans. The example establishes a possible performance failure and a verified alternative for this fixture, not a universal planner guarantee.

### Recommendation

Continue with global snapshot epochs and coalesced notifications, using the bounded existence probe. Keep fair batched root fetching as a QoS option when preventing one root's fetch work from delaying others is worth its extra queries and scheduling. The global/notify path is the simpler healthy baseline; the fair path earns its complexity through fetch-stage isolation, not higher peak write throughput.

Before integrating with Falcon, the most useful remaining performance work is reducing discovery and per-cut SQL/round trips, adapting fetch batching to observed root pressure, and measuring shared-database stalls over longer runs with realistic retention and many historical xids. Actual unchanged-root coverage, graph staging, MVCC window costs, recovery/ownership, and retention are separate implementation questions. All benchmark variants here retain one global epoch domain.

## Code, validation, and reproduction

- [Writer and source workload](../domain_capture_bench_test.go): post-commit hint, sixteen writers, deterministic tuple/journal payloads, checksum and final-state verification.
- [Fetch and wakeup implementations](../snapshot_fetch_bench_test.go): notification bridge, durable capture, optional bounded probe, root batching, staging, and network/buffer metrics.
- [Protocol tests](../snapshot_fetch_protocol_test.go): paging, quiet-root progress during a heavy root epoch, no partial-root publication, snapshot exception handling, rollback, lost hints, and durable replay.
- [Probe tests and diagnostics](../snapshot_fetch_plan_test.go): generic-plan late-commit correctness, retained-history prepared plans, and isolated server slow-statement logging.
- [Sequential runner](run-snapshot-fetch.py) and [report generator](report-snapshot-fetch.py).
- [All sample metrics](snapshot-fetch-samples.csv); original raw samples: [idle](snapshot-fetch-idle.jsonl), [capacity](snapshot-fetch-capacity.jsonl), [latency](snapshot-fetch-latency.jsonl), [dominance](snapshot-fetch-dominance.jsonl), [history](snapshot-fetch-history.jsonl), [coalescing control](snapshot-fetch-notify-control.jsonl). Matching `.txt` files include complete benchmark output and container inventories.
- [Bounded-probe follow-up](snapshot-fetch-probe-fix.jsonl), [server trace](snapshot-fetch-plans.txt), [final prepared-plan diagnostics](snapshot-fetch-bounded-plans.txt), and [ordering-only probe diagnostic](snapshot-fetch-empty-plans.txt).
- [Test-suite output](snapshot-fetch-validation.txt), [race-detector protocol output](snapshot-fetch-race.txt), and [normal/race fetch and delivery smoke samples](snapshot-fetch-smoke.txt).

**Final validation:** `go test -count=1 ./...` passes. Race-detector protocol tests and normal/race fetch and delivery smoke cases pass. All 180 measured invocations have empty before/after container inventories, including the eighteen bounded-probe follow-ups; they contain 9,342,000 measured source transactions in total. The twelve idle samples contain no source writes. No sample was discarded for a slow result.

Every accepted source sample verifies full payload checksums, exactly-once delivery, and final source/latest-state agreement. Protocol tests exercise late commits, aborted transactions, multi-page roots, mixed root epochs in one SQL batch, missed notifications, and replay. PostgreSQL containers are isolated during measured phases; inventories must be empty before and after each invocation.

Run from the repository root with a working Podman/Docker endpoint configured in `DOCKER_HOST`:

```sh
go test -c -o /tmp/falcon-snapshot-fetch.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-fetch.py all
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-fetch.py probe-fix
python3 internal/infrastructure/postgres/benchmark-results/report-snapshot-fetch.py
go test -count=1 ./...
```

The runner appends output; archive/remove existing snapshot-fetch raw files before rerunning a complete matrix. Set `PODMAN` if its executable is elsewhere. The primary matrix records one frozen implementation hash; the separate probe follow-up records a second. Current benchmark code preserves the original probe when the option is empty, allowing the original treatment to be reproduced.
