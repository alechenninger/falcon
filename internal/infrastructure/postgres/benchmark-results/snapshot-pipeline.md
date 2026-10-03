# Snapshot fetching: incremental routing, pagination, and one streamed query

This benchmark compares four ways to fetch the same immutable journal delta for a durable global snapshot epoch: 128-row pages buffered until EOF, 128-row pages routed incrementally, 2,048-row pages routed incrementally, and one unbounded SELECT streamed through pgx and routed incrementally. No production Falcon implementation is changed.

## What is held identical

All arms use one global snapshot publisher, coalesced post-commit NOTIFY with 2 ms minimum spacing and 100 ms fallback, and the previously verified bounded existence probe. The publisher durably stores the cut before payload fetching; capture may proceed while the shared payload reader works.

The source transaction upserts latest state, then appends one complete journal entry containing the root, writer XID, sequence position, and transaction payload. There are sixteen writer connections, five prewarmed consumer-pool connections, and two notification-bridge connections.

The application sink is the same in all four arms: ordered per-root queues and four workers, one active task per root, the same payload checksum work, the same batching rule, a 32 MiB queued-payload budget, and a shared 65,536-task limit. Main samples route each row immediately. A separate batching control uses at most 32 rows / 256 KiB, with batch boundaries independent of SQL pages. Root queues relinquish consumed payload references; pending verified identifiers remain until publication.

Each worker validates and privately stages rows incrementally. Successful fetch EOF queues an epoch-completion marker behind each affected root's preceding rows. A root becomes readable after processing its own marker; it does not wait for other roots to finish application. A failed or interrupted fetch sends no completion markers. No root can advertise a complete epoch merely because one of its rows was processed; a later row may belong to that same root. Unchanged-root coverage and actual Falcon graph/MVCC staging are outside this benchmark.

The buffered control deliberately retains the whole epoch before feeding that same sink. Its whole-epoch payload buffer is outside the queue budget, while all arms use the same downstream budget. Source freshness measures publication, not private staging. The private-stage timestamp and fraction staged before EOF separately establish overlap.

The paged SQL uses position keyset pagination with immediate subsequent queries on full pages. Notifications never gate continuation. A sequence cursor resets for each epoch, so a transaction that commits late with an older position remains eligible for the next snapshot delta. The stream arm executes one finite SELECT for the captured delta and reads pgx `Rows.Next()`; it uses no SQL `DECLARE`/`FETCH` cursor and does not collect the result into an epoch-sized slice. All arms retain the same visibility predicate and ordering.

## Load and environment

Baseline load ranks all 1,024 roots with discrete weights proportional to `(rank)^-1.05`, a Zipf-like heavy-tailed workload used here to represent Pareto-shaped activity. This is a model choice, not a fitted production distribution. Unlike the preceding report's independently skewed tenant/root hierarchy, this shape directly ranks roots globally. The most active root gets about 15.5% of writes; the top 16 get about 49.6%; the top 64 get about 67.4%. Quiet-root percentiles cover the bottom 90% of roots, ranks 104–1,024. Payloads are 256 B or 16 KiB per transaction, with 4,096 object keys per root.

Capacity samples use 100k small or 50k large transactions. Healthy samples offer 50, 1k, and 3k small writes/sec for twenty seconds. Bursts offer two seconds of work at once. Separate application-stall samples inject 200 ms pauses on root zero every thousandth source operation. The backlog samples first commit 100k small or 20k large real source transactions, then start the consumer, producing one large captured delta. The backlog/history combination adds two million retained old journal entries. Every configuration has three repetitions in a deterministic shuffled order.

PostgreSQL 18 Alpine runs in the same Podman VM as the preceding experiments: six virtual CPUs, 6 GiB VM RAM, a 4 GiB container memory limit, 1 GiB shared buffers, 3 GiB effective-cache estimate, 4 MiB work memory, and 4 GiB maximum WAL size. Fsync, synchronous commit, and full-page writes are enabled. The Go client runs on the M4 Pro host. Every invocation requires an empty container inventory and verifies that it is empty again afterward; only benchmark-labelled leaked containers may be cleaned up by the runner.

These are synthetic complete-payload sinks. They validate checksums, exactly-once readiness, root ordering, and final latest state, but do not measure Falcon graph work, MVCC window publication/RPC overhead, retention deletion, failover, or recovery. The client heap includes its precomputed event fixture and is sampled every 200 ms; it is not production memory sizing. Read bandwidth is effective client consumption, not the physical storage ceiling.

## Results

**180 measured samples, 60 configurations, 9,612,000 source transactions.** Values are medians of per-run statistics; brackets give the minimum and maximum, not confidence intervals. Smoke tests, race tests, query-plan diagnostics, and Go calibration samples are excluded. All measured samples validate exactly-once complete-payload delivery and final state against the latest-state table.

### Concurrent write capacity

| Fetch | 256 B tx/sec | 16 KiB tx/sec | 16 KiB freshness p99 (ms) | 16 KiB fetch busy (%) |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 7,606 [7,599–7,608] | 3,059 [3,014–3,085] | 24.56 [21.54–26.99] | 41.1 [40.3–41.1] |
| Pipelined / 128 | 7,597 [7,541–7,629] | 3,132 [3,129–3,147] | 20.60 [20.07–21.22] | 42.4 [42.1–43.0] |
| Pipelined / 2,048 | 7,602 [7,601–7,641] | 3,061 [3,032–3,115] | 22.21 [20.71–22.65] | 41.4 [40.2–41.8] |
| Pipelined / stream | 7,560 [7,527–7,613] | 3,022 [3,011–3,068] | 23.66 [21.60–25.13] | 40.9 [40.5–42.0] |

Fetch busy is the fraction of elapsed time inside epoch fetching, including inline routing for pipelined arms. It is not device utilization. All arms run one SQL payload reader; source writing and capture share the database.

### Healthy freshness

| Fetch | 50/sec p99 (ms) | 1k/sec p99 (ms) | 3k/sec p99 (ms) | 3k/sec quiet p99 (ms) |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 7.03 [6.82–7.49] | 2.95 [2.95–2.96] | 6.00 [5.78–13.76] | 6.01 [5.70–13.84] |
| Pipelined / 128 | 7.12 [7.07–7.83] | 3.24 [2.96–6.33] | 4.54 [3.67–7.23] | 4.52 [3.67–7.26] |
| Pipelined / 2,048 | 7.45 [7.19–7.64] | 2.95 [2.94–2.97] | 6.50 [3.67–8.49] | 6.58 [3.67–8.49] |
| Pipelined / stream | 7.15 [6.97–8.32] | 2.96 [2.92–6.83] | 3.67 [3.66–8.48] | 3.65 [3.65–8.58] |

### Backlog drain and effective fetch capacity

The consumer starts after all source transactions commit. Drain time includes consumer startup, initial durable capture, fetching, routing, and publication. Effective fetch MiB/sec uses payload bytes divided by total epoch fetch duration, including decoding, routing, and any backpressure. It excludes source writing and root publication after EOF; it is not a storage-device bandwidth measurement. Backlog freshness includes the deliberate consumer-off interval and is not a healthy-latency statistic.

**backlog-small**

| Fetch | Drain (sec) | Fetch MiB/sec | Peak payload MiB | Staged before EOF (%) | First row p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 5.734 [5.726–5.842] | 4.3 [4.3–4.4] | 24.41 [24.41–24.41] | 0.0 [0.0–0.0] | 13.86 [13.62–14.24] |
| Pipelined / 128 | 5.794 [5.591–5.972] | 4.2 [4.1–4.4] | 0.03 [0.03–0.03] | 100.0 [100.0–100.0] | 13.78 [13.12–14.08] |
| Pipelined / 2,048 | 0.632 [0.621–0.643] | 39.8 [39.1–40.4] | 0.02 [0.02–0.23] | 100.0 [100.0–100.0] | 23.69 [23.17–25.06] |
| Pipelined / stream | 0.219 [0.218–0.222] | 121.2 [120.4–122.1] | 0.03 [0.02–0.04] | 100.0 [100.0–100.0] | 37.84 [37.83–39.94] |

**backlog-large**

| Fetch | Drain (sec) | Fetch MiB/sec | Peak payload MiB | Staged before EOF (%) | First row p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 1.414 [1.414–1.436] | 229.4 [226.0–229.6] | 312.50 [312.50–312.50] | 0.0 [0.0–0.0] | 3.86 [3.84–4.01] |
| Pipelined / 128 | 1.379 [1.366–1.576] | 229.1 [200.4–231.8] | 0.86 [0.73–1.83] | 100.0 [100.0–100.0] | 4.16 [3.96–4.76] |
| Pipelined / 2,048 | 0.936 [0.922–0.936] | 339.9 [339.1–344.5] | 0.30 [0.30–0.67] | 100.0 [100.0–100.0] | 4.86 [4.65–4.91] |
| Pipelined / stream | 0.883 [0.881–0.888] | 360.4 [357.8–360.8] | 0.41 [0.36–0.61] | 100.0 [100.0–100.0] | 5.28 [5.12–5.29] |

**backlog-history**

| Fetch | Drain (sec) | Fetch MiB/sec | Peak payload MiB | Staged before EOF (%) | First row p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 7.923 [7.909–7.993] | 3.1 [3.1–3.1] | 24.41 [24.41–24.41] | 0.0 [0.0–0.0] | 19.15 [19.09–19.20] |
| Pipelined / 128 | 8.068 [7.939–8.376] | 3.0 [2.9–3.1] | 0.03 [0.02–0.03] | 100.0 [100.0–100.0] | 19.18 [18.88–23.32] |
| Pipelined / 2,048 | 0.801 [0.787–0.811] | 31.3 [30.9–31.8] | 0.09 [0.03–0.21] | 100.0 [100.0–100.0] | 27.10 [26.69–27.57] |
| Pipelined / stream | 0.228 [0.224–0.230] | 116.6 [115.8–118.4] | 0.03 [0.02–0.03] | 100.0 [100.0–100.0] | 46.67 [44.66–46.83] |

### Bursts and slow application

Quiet roots are the lower-ranked 90% of roots (ranks 104–1,024), not every root except rank one. Bursts offer 6k small transactions or 2k large transactions every two seconds. Application stalls pause root 0 for 200 ms every 1,000th source operation, forced to that root; fetching itself is not deliberately stalled. Arrival starts at the intended arrival and includes source queueing.

**burst-small**

| Fetch | Quiet fresh p99 (ms) | Quiet arrival p99 (ms) | All fresh p99 (ms) | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 8.79 [8.71–8.95] | 815.33 [811.35–881.39] | 8.41 [7.74–8.65] | 0.01 [0.01–0.01] |
| Pipelined / 128 | 8.63 [8.59–9.06] | 813.64 [813.51–814.80] | 8.41 [8.13–8.81] | 0.01 [0.01–0.01] |
| Pipelined / 2,048 | 9.24 [8.90–9.51] | 814.73 [813.32–816.33] | 9.02 [8.27–9.16] | 0.01 [0.01–0.01] |
| Pipelined / stream | 9.06 [9.06–9.30] | 799.83 [793.56–812.69] | 8.59 [8.51–8.70] | 0.01 [0.01–0.01] |

**burst-large**

| Fetch | Quiet fresh p99 (ms) | Quiet arrival p99 (ms) | All fresh p99 (ms) | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 18.56 [17.93–19.74] | 666.05 [665.21–678.41] | 18.56 [17.87–19.96] | 0.58 [0.58–0.73] |
| Pipelined / 128 | 19.42 [17.68–19.66] | 664.31 [638.89–675.46] | 19.52 [18.16–19.72] | 0.33 [0.31–0.38] |
| Pipelined / 2,048 | 18.70 [18.61–19.24] | 652.82 [638.66–674.49] | 18.96 [18.81–19.02] | 0.23 [0.22–0.27] |
| Pipelined / stream | 18.82 [17.87–19.18] | 667.83 [663.02–696.22] | 18.82 [18.22–19.14] | 0.27 [0.27–0.30] |

**apply-stall-small**

| Fetch | Quiet fresh p99 (ms) | Quiet arrival p99 (ms) | All fresh p99 (ms) | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 5.42 [3.73–9.76] | 112.72 [5.12–394.19] | 184.36 [183.57–190.41] | 0.03 [0.03–0.06] |
| Pipelined / 128 | 3.73 [3.65–5.02] | 5.03 [4.84–232.52] | 183.66 [183.43–186.48] | 0.03 [0.03–0.05] |
| Pipelined / 2,048 | 4.00 [3.69–4.11] | 5.49 [4.92–5.76] | 183.19 [183.08–183.52] | 0.03 [0.03–0.03] |
| Pipelined / stream | 3.68 [3.66–3.69] | 4.88 [4.85–4.95] | 183.35 [183.09–183.41] | 0.03 [0.03–0.03] |

**apply-stall-large**

| Fetch | Quiet fresh p99 (ms) | Quiet arrival p99 (ms) | All fresh p99 (ms) | Peak payload MiB |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 10.66 [10.15–33.41] | 55.31 [49.97–1,971.18] | 166.55 [162.79–178.53] | 1.23 [0.98–1.98] |
| Pipelined / 128 | 10.39 [10.18–30.67] | 73.65 [66.64–1,508.87] | 165.45 [163.98–173.18] | 0.98 [0.98–1.48] |
| Pipelined / 2,048 | 10.44 [10.29–11.65] | 79.18 [62.17–164.44] | 166.76 [163.31–172.41] | 1.00 [0.98–1.20] |
| Pipelined / stream | 10.49 [10.01–17.64] | 64.63 [55.60–2,419.56] | 164.09 [163.40–177.37] | 0.98 [0.98–1.94] |

### Consumer cost

Counts include snapshot capture/probe/manifest, fetch, and separate notification SQL; BEGIN/COMMIT protocol commands are excluded. Wire bytes include the consumer pool and notification bridge, excluding prewarmed pool connection establishment. Backlog startup additionally establishes the notification connections after the writers stop. Payload peaks exclude pgx transient buffers, staging identifiers, and graph state.

**capacity-small**

| Fetch | Fetch statements | All SQL/write | Wire bytes/write | Peak staged IDs | Peak Go heap MiB |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 4,217 [4,185–4,236] | 0.233 [0.232–0.234] | 365 [364–365] | 47 [36–59] | 55.1 [45.6–61.3] |
| Pipelined / 128 | 4,226 [4,218–4,243] | 0.234 [0.234–0.234] | 365 [365–365] | 37 [36–51] | 71.7 [47.1–106.1] |
| Pipelined / 2,048 | 4,181 [4,175–4,238] | 0.232 [0.231–0.234] | 364 [364–365] | 39 [37–45] | 57.1 [45.6–68.5] |
| Pipelined / stream | 4,178 [4,173–4,196] | 0.232 [0.232–0.232] | 364 [364–364] | 53 [50–54] | 45.8 [44.8–72.0] |

**capacity-large**

| Fetch | Fetch statements | All SQL/write | Wire bytes/write | Peak staged IDs | Peak Go heap MiB |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 2,555 [2,527–2,596] | 0.349 [0.345–0.351] | 16,529 [16,528–16,530] | 31 [31–69] | 29.9 [26.7–42.2] |
| Pipelined / 128 | 2,587 [2,529–2,588] | 0.350 [0.346–0.350] | 16,530 [16,528–16,530] | 37 [36–42] | 25.7 [25.5–30.4] |
| Pipelined / 2,048 | 2,587 [2,509–2,647] | 0.349 [0.344–0.355] | 16,530 [16,528–16,532] | 35 [33–38] | 28.4 [25.8–33.4] |
| Pipelined / stream | 2,605 [2,605–2,605] | 0.352 [0.351–0.353] | 16,530 [16,530–16,530] | 35 [34–84] | 28.5 [26.9–30.2] |

**backlog-small**

| Fetch | Fetch statements | All SQL/write | Wire bytes/write | Peak staged IDs | Peak Go heap MiB |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 782 [782–782] | 0.009 [0.009–0.009] | 302 [302–302] | 100,000 [100,000–100,000] | 65.9 [61.1–88.8] |
| Pipelined / 128 | 782 [782–782] | 0.009 [0.009–0.009] | 302 [302–302] | 100,000 [100,000–100,000] | 42.9 [41.5–54.2] |
| Pipelined / 2,048 | 49 [49–49] | 0.001 [0.001–0.001] | 299 [299–299] | 100,000 [100,000–100,000] | 40.9 [39.5–42.3] |
| Pipelined / stream | 1 [1–1] | 0.000 [0.000–0.000] | 299 [299–299] | 100,000 [100,000–100,000] | 37.4 [31.6–42.1] |

**backlog-large**

| Fetch | Fetch statements | All SQL/write | Wire bytes/write | Peak staged IDs | Peak Go heap MiB |
| --- | --- | --- | --- | --- | --- |
| Buffered / 128 | 157 [157–157] | 0.009 [0.009–0.009] | 16,430 [16,430–16,430] | 17,995 [17,993–18,000] | 333.6 [327.5–335.6] |
| Pipelined / 128 | 157 [157–157] | 0.009 [0.009–0.009] | 16,430 [16,430–16,430] | 20,000 [20,000–20,000] | 14.6 [13.6–15.0] |
| Pipelined / 2,048 | 10 [10–10] | 0.001 [0.001–0.001] | 16,428 [16,428–16,428] | 20,000 [20,000–20,000] | 14.7 [11.4–15.0] |
| Pipelined / stream | 1 [1–1] | 0.001 [0.001–0.001] | 16,428 [16,428–16,428] | 20,000 [20,000–20,000] | 14.8 [14.0–15.3] |

### Retained history

Two million old entries are loaded before measurement. This bulk history has one old writer XID; measured source transactions each have their own XID.

| Fetch | Freshness p99 (ms) | Quiet p99 (ms) | Capture max (ms) | First row p99 (ms) |
| --- | --- | --- | --- | --- |
| Buffered / 128 | 3.03 [3.02–3.03] | 3.02 [3.02–3.03] | 14.16 [11.51–16.94] | 0.44 [0.44–0.44] |
| Pipelined / 128 | 3.03 [3.02–5.91] | 3.03 [3.02–5.92] | 47.54 [9.52–447.15] | 0.45 [0.45–0.49] |
| Pipelined / 2,048 | 3.03 [3.03–3.04] | 3.02 [3.02–3.04] | 12.76 [11.35–16.73] | 0.44 [0.44–0.46] |
| Pipelined / stream | 3.04 [3.03–3.21] | 3.04 [3.03–3.22] | 11.00 [10.62–15.34] | 0.45 [0.45–0.46] |

### Application batching control

These additional samples change the common application batch from one row to at most 32 rows / 256 KiB. SQL page boundaries never flush batches. All four fetch arms share the same batch rule within each comparison.

| Fetch | Large write capacity (tx/sec) | Small backlog drain (sec) | Small backlog fetch MiB/sec |
| --- | --- | --- | --- |
| Buffered / 128 | 3,050 [2,825–3,140] | 5.670 [5.662–5.717] | 4.4 [4.3–4.4] |
| Pipelined / 128 | 3,005 [3,002–3,218] | 5.773 [5.723–6.013] | 4.3 [4.1–4.3] |
| Pipelined / 2,048 | 3,049 [2,912–3,060] | 0.608 [0.592–0.615] | 41.7 [41.5–42.8] |
| Pipelined / stream | 3,015 [2,802–3,078] | 0.181 [0.178–0.183] | 149.1 [148.0–151.5] |


## Interpretation

**Use one streamed query per captured epoch as the next aggregate-fetch baseline.** It preserves the global snapshot domain, overlaps private root application with fetching, and removes repeated candidate discovery, ordering, and query round trips within a large delta. On the 100k-entry small backlog, median drain falls from 5.794 seconds with incremental 128-row pages to 0.219 seconds with streaming: about 26× faster. Incremental 2,048-row pages take 0.632 seconds. With two million retained entries, the corresponding drain times are 8.068, 0.801, and 0.228 seconds. The stream executes one payload statement rather than 782 or 49 for these 100k-entry cuts.

**Pipelining alone primarily changes memory and overlap.** Keeping 128-row SQL pages while routing incrementally does not materially accelerate the small backlog. It does eliminate the whole-epoch payload buffer: for the 20k-entry / 16 KiB backlog, buffered delivery peaks at 312.5 MiB, versus a median 0.41 MiB for streaming, with a 0.36–0.61 MiB range. Nearly all rows in the pipelined backlog runs are privately staged before EOF. This is distinct from readiness; none becomes readable before successful EOF. The benchmark still retains identifiers until root publication, and production graph staging will have its own memory requirements.

**Larger pages are a useful middle ground for large payloads.** The 16 KiB backlog drains in 1.379 seconds with incremental 128-row pages, 0.936 seconds with 2,048-row pages, and 0.883 seconds with streaming. Effective stream consumption reaches 360 MiB/sec in this source-idle backlog. The much smaller difference between large pages and streaming here means pagination is less costly once payload transfer dominates. These capacities include client decoding/routing, exclude concurrent source writing, and do not establish a physical storage limit.

**Steady-state writes and healthy freshness barely change.** Small-payload capacity remains about 7.5–7.6k transactions/sec; large-payload capacity remains about 3.0–3.15k. Streaming is not the write-throughput winner. At 1k offered small writes/sec, median p99 freshness is about 3 ms for every arm. The 3k/sec stream median is 3.67 ms, but its 3.66–8.48 ms range overlaps other arms. Source writes and capture share the database, and large-payload fetch occupies about 41–42% of elapsed time. Incremental arms never wait on the 32 MiB payload budget in this matrix; the buffered large-backlog control spends 11–14 ms waiting while feeding its retained payload to the sink after EOF. These observations support removing inefficient backlog queries; they do not show that fetching is the current saturation bottleneck or that a faster isolated reader raises peak source throughput.

**Independent application protects quiet roots from a slow root in this workload.** Under the 200 ms root-zero application pauses at 3k small writes/sec, streaming has median quiet-root freshness p99 of 3.68 ms and quiet arrival p99 of 4.88 ms, while all-root freshness p99 is 183.35 ms. Incremental paged arms have similar quiet-root results. This is achieved with globally ordered fetching and no fair SQL scheduler. It supports the hypothesis that aggregate fetching plus independent root application is sufficient for ordinary Pareto-shaped load at the tested rates.

It does not establish a general isolation guarantee. A root still waits for the shared epoch fetch to reach EOF, and a full shared queue can backpressure that fetcher. A fetch/query stall, an extremely large cut, or an expensive application sink could still delay other roots. This matrix injects application pauses, whereas the preceding [fair-fetch experiment](snapshot-fetch.md) also injected payload-fetch pauses. Those are different failure modes, and the root distribution and quiet cohort have changed; their numbers are not direct paired controls.

**Arrival latency exposes shared-source delays that post-commit freshness hides.** Bursts have quiet arrival p99 near 0.8 seconds for small payloads and 0.65–0.67 seconds for large payloads, despite post-commit freshness around 9 and 19 ms. In the large application-stall runs, all arms have roughly 10–11 ms median quiet freshness, but some repetitions have quiet arrival p99 between 1.5 and 2.4 seconds. One paged-128 and one stream repetition finish below the offered 1.5k writes/sec. The history paged-128 arm also has one quiet-arrival p99 of 1.03 seconds. All these samples remain in the tables. We did not collect wait-event traces sufficient to attribute their source/capture delays to a particular database resource; a scheduler change cannot be credited with fixing them.

**Application batching modestly improves isolated stream drain, without a demonstrated steady-state gain.** With the common batch changed from one row to at most 32 rows / 256 KiB, streamed small-backlog drain falls from 0.219 to 0.181 seconds, and effective consumption rises from 121 to 149 MiB/sec. Larger pages improve slightly, from 0.632 to 0.608 seconds; 128-row paging remains about 5.8 seconds. Large-payload write-capacity medians with batching are 3,050 / 3,005 / 3,049 / 3,015 tx/sec across buffered, 128-page, 2,048-page, and stream arms, with wider ranges down to 2,802 tx/sec. There is no consistent capacity improvement. Small cuts often reach EOF before accumulating 32 rows, so batching also reduces early staging in those runs. This is a useful optimization to retain as a control, not evidence for making larger application batches the default.

### Query startup and prepared-plan hazard

Streaming pgx rows does not guarantee that PostgreSQL emits the first row immediately. The plans in the separate [diagnostic log](snapshot-pipeline-plans.txt) contain sorts before output. In the 100k-entry small backlog, the stream's first-row delay is about 38 ms, even though its complete drain is only 219 ms. Paginated first-row statistics cover many statements, whereas the stream statistic covers one statement per epoch.

The retained-history diagnostic uses two million old entries plus 8,192 fresh entries from 256 writer transactions. These are server-side `EXPLAIN (ANALYZE, BUFFERS)` times, outside the measured matrix:

| Query | Automatic plan startup / execution (ms) | Forced generic startup / execution (ms) |
| --- | --- | --- |
| 128-row first page | 1.79 / 1.82 | 130.59 / 133.62 |
| 2,048-row first page | 2.26 / 2.48 | 130.37 / 133.34 |
| Entire streamed delta | 2.26 / 2.70 | 142.93 / 148.41 |

The forced generic plan scans the old-active-XID branch through the position primary-key index and filters the retained history even when that branch's XID array is empty. Its parallel workers together filter approximately two million entries. The automatic/custom plans use the writer-XID index for this fixture. No diagnostic plan writes temporary blocks, but larger cuts can behave differently. The separate large-payload fixture has no retained history and does not exhibit this generic-plan scan.

The main matrix uses PostgreSQL's default automatic plan selection. Its 2m-history median p99 stays near 3 ms; the forced-plan regression above is a reproduced risk, not a claim that these measured samples used that bad plan. The earlier bounded existence-probe fix does not fix the payload SELECT. A next implementation experiment should give the payload candidate query the same attention: avoid empty old-active branches and seek bounded lookups for nonempty XID lists, then test automatic and forced generic plans. PostgreSQL documents parameter-specific versus generic planning and inspection with [`EXPLAIN EXECUTE`](https://www.postgresql.org/docs/18/sql-prepare.html).

### Validation and limits

The [full Go suite](snapshot-pipeline-validation.txt) passes, as do the [race-enabled protocol tests](snapshot-pipeline-race.txt). Protocol tests explicitly demonstrate private application before fetch finishes, immediate continuation after full pages, no readiness after an interrupted fetch, a quiet root progressing through later epochs while another root is held, late commits with lower sequence positions, and exclusion of rolled-back entries. This is readiness validation, not an implemented retry/recovery protocol. The [sample audit](snapshot-pipeline-audit.txt) checks all 180 successful invocations, empty container inventories before and after every invocation, and identical source/binary hashes that match the saved implementation.

Three repetitions and twenty-second healthy runs support the large backlog differences, not precise rankings of similar write capacities or a long-term p99 guarantee. Historical entries share one old XID, payloads have fixed sizes within a run, and the application sink does checksum/identifier work rather than Falcon graph/MVCC work. Independent-root window memory, wire cost, unchanged-root coverage, recovery, and retention remain separate implementation questions. Production Falcon code and APIs are unchanged.

## Code and reproduction

- [Common incremental sink and fetchers](../snapshot_pipeline_bench_test.go).
- [Pipeline protocol tests](../snapshot_pipeline_protocol_test.go).
- [Prepared-plan diagnostics](../snapshot_pipeline_plan_test.go).
- [Shared publisher, notifications, and bounded existence probe](../snapshot_fetch_bench_test.go).
- [Source transactions, workload generation, and final-state validation](../domain_capture_bench_test.go).
- [Sequential runner](run-snapshot-pipeline.py), [report generator](report-snapshot-pipeline.py), and [all sample metrics](snapshot-pipeline-samples.csv).
- Raw samples and full logs: `snapshot-pipeline-{capacity,latency,burst,apply-stall,history,backlog,batch32}.{jsonl,txt}`. Smoke, race, validation, and plan outputs are separate.

```sh
export DOCKER_HOST=unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock
go test -c -o /tmp/falcon-snapshot-pipeline.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-snapshot-pipeline.py all
python3 internal/infrastructure/postgres/benchmark-results/report-snapshot-pipeline.py
FALCON_SNAPSHOT_PIPELINE_PLANS=1 /tmp/falcon-snapshot-pipeline.test -test.run '^TestSnapshotPipelinePlans$' -test.v
```

The runner appends output. Archive existing pipeline raw files before repeating a full phase; do not mix implementations under one result file.
