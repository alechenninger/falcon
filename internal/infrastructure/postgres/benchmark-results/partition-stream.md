# XID-partitioned streamed journal fetch

This experiment tests whether physical XID-range partitions can support cheap journal retention while keeping the streamed, routed snapshot consumer competitive. It preserves one global snapshot epoch domain, the existing streamed pipeline, and independent root application. Production Falcon is unchanged.

## Design

Writers update latest state and append the journal in the same transaction. A shared PostgreSQL sequence with `CACHE 1` allocates `position` after the latest-state upsert obtains its per-key lock. `writer_xid` is the transaction's native 64-bit PostgreSQL XID. Neither allocation represents commit order. Snapshot visibility determines epoch membership; position determines delivery order within that epoch. Late commits with lower positions are still included because position bounds reset for every captured cut.

The physical journal is partitioned by `RANGE(writer_xid)`. These buckets span many logical epochs; a writer does not need to discover its eventual commit epoch or update its entry after commit. XID allocation includes other writing transactions in the database, so a bucket is not a fixed row count or wall-clock duration.

| Arm | Primary key | Other journal indexes | Attached leaves at startup |
| --- | --- | --- | --- |
| Flat / position PK | `position` | `(writer_xid,position)`, `(root_id,writer_xid,position)` | One flat table |
| Flat / compound PK | `(writer_xid,position)` | `position`, `(root_id,writer_xid,position)` | One flat table |
| XID / 8,192 | `(writer_xid,position)` | `position`, `(root_id,writer_xid,position)` | 32 future leaves, plus 16 history leaves when preloaded |
| XID / 32,768 | `(writer_xid,position)` | `position`, `(root_id,writer_xid,position)` | Same leaf counts, wider XID ranges |

The flat compound control isolates the key change from partitioning. There are three journal indexes in every arm, with the same key sets. PostgreSQL requires partition columns in a partitioned primary/unique key. Consequently, the partitioned schema does not enforce global uniqueness of position alone. The shared sequence supplies distinct values for ordinary writers; explicit reused positions or sequence resets require separate protection. The protocol test deliberately demonstrates this weaker database constraint. [PostgreSQL partition constraints](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-LIMITATIONS).

That compound constraint should not be reused as a sequence-gap-repair fence: a repair inserted under a different XID would not conflict with the pending writer's position. This snapshot protocol determines membership through transaction visibility and does not fill sequence gaps.

The main fetch query is the earlier guarded query: a new-XID range plus explicit indexed lookups for eligible old-active XIDs, followed by `ORDER BY position`. Partition pruning bounds candidate storage, while snapshot visibility remains the correctness filter. Rows feed root queues immediately as pgx receives them. A root becomes readable only after successful global fetch EOF and completion of its own preceding FIFO work. Physical partition order and the compound primary-key order do not replace position ordering.

## Live retention

The rotating arm starts with four future partitions and runs one background manager every 100 ms. The manager observes XID allocation, maintains two partition ranges of headroom, and persists the highest contiguous epoch completed by **all** roots. It retires a partition only when its upper XID bound is no greater than that checkpoint snapshot's `xmin`. This conservatively excludes unresolved old writers and work not yet acknowledged by a root. PostgreSQL defines snapshot xmin as the boundary below which transactions have committed and are visible, or aborted. [Snapshot functions](https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-PG-SNAPSHOT).

New leaves are created separately, given matching range checks, and attached. Old leaves are detached concurrently and then dropped. This avoids row-by-row deletion; it still incurs DDL, locks, plan invalidation, and checkpoint writes. Concurrent detach can wait for transactions using the parent. It runs outside an explicit transaction and requires no default partition. Interrupted detach needs production recovery via `FINALIZE`. [PostgreSQL partition maintenance](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-MAINTENANCE), [ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html).

This is a benchmark retention implementation, not a complete production recovery protocol. The sink verifies payloads and latest state in memory; storing its progress models the checkpoint cost but does not make that synthetic sink crash durable. Production reclamation must follow durable application or durable replay storage and account for every required consumer and protected read/checkpoint lease. Manager leadership/fencing, restart reconciliation, interrupted DDL, and recovery after source failure are not implemented here. A long assigned-XID transaction or lagging root can pin retention even while healthy roots advance their windows.

The tested manager allocates an XID to observe allocation progress and advances the reclamation floor only through acknowledged Falcon epochs. Idle lifecycle behavior, unrelated database XID bursts, provisioning after a manager outage, and epoch-manifest retention need production design beyond these steady-write experiments. The small experimental bucket sizes intentionally exercise rollover; they are not proposed production sizing defaults.

Provisioning needs enough headroom, and should be able to continue while retirement waits for an active parent-table user. The benchmark uses one serial manager, so a sufficiently long detach wait could also delay creation. The measured short streams do not establish availability under arbitrarily long transactions or streams.

## Methods

The matrix uses sixteen writers, five consumer connections, two notification connections, four root workers, 1,024 roots with rank^-1.05 activity, and a 32 MiB payload queue budget. The rotating manager adds one connection. Latest-state writes, payload checksumming, streamed routing, root FIFO application, notifications, and the indexed existence probe are identical. Notification sends have 2 ms minimum spacing with a 100 ms capture fallback. Small transactions carry one 256 B payload; large transactions carry sixteen 1 KiB tuples in one 16 KiB journal entry.

PostgreSQL 18 Alpine runs in the existing six-CPU / 6 GiB Podman VM, with a 4 GiB container memory limit, 1 GiB shared buffers, 4 MiB work memory, and durability enabled (`fsync`, synchronous commit, full-page writes). Automatic planning, autovacuum, parallelism, and JIT retain ordinary settings except explicitly forced generic consumer cases. Writers always retain automatic planning.

Each configuration has three shuffled repetitions in a fresh isolated database: concurrent capacity, twenty-second offered loads, source-idle backlog drain, two million retained entries across 2,048 real historical transactions, a deliberately skewed one-XID history control, and forty-second live rotation. Initial loading, partition creation, XID alignment, and explicit history statistics collection occur before timing. Both flat controls use the same historical transactions and XID alignment. Live maintenance costs remain inside measured runs. Smaller excluded smoke runs also stress rotation with 256-XID buckets.

History uses sixteen older ranges, with widths of 160 XIDs for the diverse fixture or 128 for the one-XID fixture; the named 8,192/32,768 spans apply to live ranges. The diverse preload has about 977 journal entries per transaction. It populates journal history, rather than a corresponding historical latest-state dataset. The substantial-history reclamation follow-up repeats the automatic and generic offered loads with four future leaves and background GC, including a wider-live-range automatic control.

Diagnostic fixtures separately compare custom, generic, and automatic prepared plans, including 100k eligible rows spanning multiple partitions and 512 attached leaves. They use fresh analyzed statistics and uniform root IDs; the measured load uses Pareto-shaped roots. Six reads per diagnostic combination validate complete payloads and order. These diagnostics are outside capacity measurements and are not a long-running statistics-aging test.

Freshness measures client commit acknowledgement to root readability. Arrival starts at the intended write arrival and includes writer queuing. Drain starts when the source stops; backlog runs deliberately start their consumer afterward. Fetch bandwidth includes SQL, decoding, routing, and backpressure. The sink checks every expected payload, exactly-once delivery, position order, no premature readability, and final latest state. Real graph/MVCC costs and managed-service network/storage remain outside scope. The historical dataset does not establish behavior beyond RAM capacity.

The runner requires empty container inventories before and after every invocation, captures source/binary hashes, and aborts on a leak. Containers run sequentially; no other PostgreSQL workloads or heavy compilation run during the measured matrix. Calibration, smoke, protocol, race, full-suite, and diagnostic runs are excluded from measured results. The previous fetch-plan implementation is preserved in its [source archive](fetch-plans-source/manifest.json).

The frozen implementation for this experiment is saved as [hash-verified source text](partition-stream-source/manifest.json).

## Results

**126 measured runs, 42 configurations, 5,190,000 source transactions.** Cells are medians of per-run statistics with min–max ranges across three repetitions; these are not pooled percentiles or confidence intervals.

### Concurrent capacity

| Arm | 256 B writes/sec | 16 KiB writes/sec | 16 KiB freshness p99 (ms) |
| --- | --- | --- | --- |
| Flat / position PK | 7,605 [7,546–7,620] | 3,037 [2,728–3,102] | 23.01 [20.58–27.94] |
| Flat / compound PK | 7,617 [7,503–7,625] | 2,983 [2,940–3,201] | 23.60 [20.86–25.33] |
| XID / 8,192 | 7,446 [7,347–7,467] | 3,012 [2,961–3,145] | 27.04 [22.64–29.77] |
| XID / 32,768 | 7,427 [7,280–7,705] | 2,986 [2,970–3,011] | 31.56 [26.21–34.36] |

### Healthy offered load

| Arm | 1k/sec fresh p99 (ms) | 1k/sec quiet arrival p99 (ms) | 3k/sec fresh p99 (ms) | 3k/sec quiet arrival p99 (ms) |
| --- | --- | --- | --- | --- |
| Flat / position PK | 2.96 [2.94–2.96] | 3.80 [3.72–3.80] | 5.72 [3.68–9.07] | 16.51 [4.86–345.35] |
| Flat / compound PK | 2.96 [2.94–3.74] | 3.76 [3.75–4.74] | 3.67 [3.66–5.15] | 4.89 [4.87–340.49] |
| XID / 8,192 | 9.30 [4.21–14.73] | 20.84 [5.10–477.42] | 5.56 [5.48–6.99] | 7.29 [7.10–10.47] |
| XID / 32,768 | 6.29 [4.22–6.86] | 117.17 [5.15–354.65] | 6.70 [5.43–15.69] | 19.55 [6.80–190.00] |

Maximum durations at 3k/sec distinguish source/capture stalls from fetching. These are medians of each run's maximum, not p99.

| Arm | Append/commit max (ms) | Capture max (ms) | Fetch max (ms) |
| --- | --- | --- | --- |
| Flat / position PK | 311.02 [9.38–431.03] | 311.54 [10.92–430.47] | 3.07 [3.02–4.05] |
| Flat / compound PK | 10.41 [9.87–447.16] | 10.73 [10.69–448.58] | 2.97 [2.80–3.22] |
| XID / 8,192 | 10.72 [10.27–13.67] | 16.08 [14.72–23.74] | 8.78 [8.78–8.97] |
| XID / 32,768 | 38.58 [8.13–282.35] | 33.67 [21.67–283.57] | 10.28 [8.99–10.76] |

### Source-idle backlog

Freshness is deliberately excluded here: the source commits its entire backlog before the consumer starts.

**backlog-small**

| Arm | Drain (sec) | First row (ms) | Fetch MiB/sec | Peak queued payload MiB |
| --- | --- | --- | --- | --- |
| Flat / position PK | 0.214 [0.213–0.215] | 36.47 [36.14–36.53] | 124.2 [123.5–124.6] | 0.02 [0.02–0.03] |
| Flat / compound PK | 0.213 [0.210–0.216] | 36.48 [36.22–37.30] | 124.7 [123.2–126.9] | 0.03 [0.02–0.03] |
| XID / 8,192 | 0.222 [0.218–0.224] | 36.87 [36.37–37.30] | 123.3 [122.0–126.0] | 0.03 [0.02–0.03] |
| XID / 32,768 | 0.223 [0.222–0.228] | 37.21 [37.01–37.51] | 122.2 [119.8–123.1] | 0.02 [0.02–0.03] |

**backlog-large**

| Arm | Drain (sec) | First row (ms) | Fetch MiB/sec | Peak queued payload MiB |
| --- | --- | --- | --- | --- |
| Flat / position PK | 0.894 [0.892–0.927] | 5.38 [4.98–5.48] | 355.2 [342.6–356.4] | 0.47 [0.27–0.64] |
| Flat / compound PK | 0.887 [0.885–0.893] | 5.43 [5.37–6.23] | 358.2 [355.9–359.2] | 0.44 [0.42–0.50] |
| XID / 8,192 | 0.891 [0.883–0.893] | 5.87 [5.86–5.97] | 359.4 [358.5–362.5] | 0.42 [0.34–0.45] |
| XID / 32,768 | 0.888 [0.877–0.899] | 5.96 [5.85–6.03] | 360.7 [356.8–365.0] | 0.41 [0.38–0.48] |

### Two million retained rows across 2,048 real transactions

**history-auto**

| Arm | Freshness p99 (ms) | Quiet arrival p99 (ms) | First row p99 (ms) | Fetch SQL/write |
| --- | --- | --- | --- | --- |
| Flat / position PK | 3.08 [3.08–3.12] | 3.90 [3.90–3.98] | 0.52 [0.51–0.53] | 0.489 [0.488–0.490] |
| Flat / compound PK | 3.11 [3.08–4.55] | 3.92 [3.89–6.93] | 0.52 [0.52–0.52] | 0.488 [0.481–0.490] |
| XID / 8,192 | 6.55 [6.53–15.97] | 9.34 [9.13–545.30] | 2.07 [2.07–2.07] | 0.484 [0.467–0.488] |
| XID / 32,768 | 5.55 [5.04–9.51] | 9.02 [5.93–753.18] | 2.03 [2.00–2.05] | 0.487 [0.462–0.489] |

**history-generic**

| Arm | Freshness p99 (ms) | Quiet arrival p99 (ms) | First row p99 (ms) | Fetch SQL/write |
| --- | --- | --- | --- | --- |
| Flat / position PK | 2.92 [2.92–2.93] | 3.71 [3.68–3.79] | 0.30 [0.30–0.31] | 0.490 [0.489–0.490] |
| Flat / compound PK | 3.10 [3.05–5.62] | 11.39 [3.84–304.88] | 0.32 [0.32–0.32] | 0.485 [0.466–0.487] |
| XID / 8,192 | 4.46 [4.18–14.97] | 26.36 [19.20–314.63] | 0.69 [0.68–0.70] | 0.486 [0.464–0.486] |
| XID / 32,768 | 4.41 [3.94–4.63] | 14.90 [4.92–19.45] | 1.02 [1.02–1.06] | 0.486 [0.485–0.489] |

### Bulk-history generic-plan stress control

Two million retained rows from one old XID, then 1k/sec offered load. This isolates the previously observed query-shape pathology.

| Arm | Freshness p99 (ms) | First row p99 (ms) | Drain (sec) |
| --- | --- | --- | --- |
| flat-any | 10,769.19 [10,717.18–11,263.75] | 197.45 [182.71–215.17] | 10.126 [9.988–10.286] |
| flat-guarded | 2.94 [2.91–2.95] | 0.31 [0.31–0.31] | 0.002 [0.002–0.003] |
| xid-any | 15,379.83 [15,145.35–15,499.67] | 277.50 [252.17–280.41] | 14.572 [14.479–14.741] |
| xid-guarded | 3.63 [3.60–3.67] | 0.65 [0.64–0.67] | 0.003 [0.001–0.003] |

### Background rotation and retention

40k small writes at 1k/sec, with no preload. Static starts with 32 future buckets; rotating starts with four and maintains two buckets of headroom. This comparison includes that different metadata working set, durable progress writes, DDL, and one additional manager connection.

| Arm | Fresh p99 (ms) | Fresh p999 (ms) | Quiet arrival p99 (ms) | Journal MiB | Dropped | Final leaves |
| --- | --- | --- | --- | --- | --- | --- |
| flat-compound | 3.01 [2.99–3.07] | 3.43 [3.29–4.66] | 3.80 [3.79–4.12] | 17.0 [17.0–17.0] | 0 [0–0] | 0 [0–0] |
| static-8192 | 4.27 [4.26–4.28] | 8.76 [8.26–8.82] | 5.18 [5.17–5.18] | 18.4 [18.4–18.4] | 0 [0–0] | 32 [32–32] |
| rotating-8192 | 3.16 [3.12–3.17] | 3.97 [3.87–4.75] | 3.95 [3.93–3.98] | 1.5 [1.5–1.5] | 9 [9–9] | 3 [3–3] |

| Rotation | Count | p99 duration (ms) | Max duration (ms) |
| --- | --- | --- | --- |
| Create | 8 [8–8] | 5.38 [5.14–5.49] | 5.38 [5.14–5.49] |
| Detach | 9 [9–9] | 2.14 [2.14–2.50] | 2.14 [2.14–2.50] |
| Drop | 9 [9–9] | 3.46 [3.32–4.20] | 3.46 [3.32–4.20] |
| Checkpoint | 400 [400–400] | 0.57 [0.57–0.77] | 1.19 [1.10–2.66] |

| Rotating 8,192 | Manager SQL/write | Durable checkpoints | Created leaves | Reclaimed MiB |
| --- | --- | --- | --- | --- |
| 40k live writes | 0.021 [0.021–0.021] | 400 [400–400] | 8 [8–8] | 16.3 [16.3–16.4] |

### Reclaiming substantial history during writes

Two million retained rows across 2,048 real transactions, then 1k/sec writes with rotation and reclamation enabled. Automatic cases run 20k writes; the generic case runs 10k, matching the corresponding static history controls. GC can first persist progress and retire old history after the first fully completed epoch.

| Arm / planning | Fresh p99 (ms) | Fresh p999 (ms) | Quiet arrival p99 (ms) | Reclaimed MiB | Final journal MiB | Detach max (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| rotating-8192 / auto | 3.26 [3.25–3.29] | 8.89 [6.07–10.30] | 4.11 [4.05–4.18] | 838.9 [838.9–839.0] | 1.7 [1.7–1.7] | 2.83 [2.58–3.10] |
| rotating-32768 / auto | 3.21 [3.20–3.21] | 10.03 [8.00–10.57] | 4.05 [4.03–4.10] | 838.7 [838.7–838.7] | 1.7 [1.7–1.7] | 2.84 [2.35–2.87] |
| rotating-8192 / generic | 3.08 [3.05–3.19] | 10.90 [9.88–11.59] | 4.01 [3.97–4.05] | 835.3 [835.3–835.3] | 0.9 [0.9–0.9] | 2.75 [2.28–2.96] |

### Write amplification

Journal size sums all attached leaf relations (including their indexes and TOAST). Measured WAL excludes preload and initial partition DDL but includes live rotation.

| Arm | Small WAL bytes/write | Small journal bytes/write | Large WAL bytes/write | Large journal bytes/write |
| --- | --- | --- | --- | --- |
| Flat / position PK | 1,167 [1,167–1,167] | 447 [446–447] | 38,595 [38,594–38,597] | 17,307 [17,306–17,313] |
| Flat / compound PK | 1,167 [1,167–1,167] | 447 [447–447] | 38,594 [38,594–38,594] | 17,306 [17,306–17,307] |
| XID / 8,192 | 1,165 [1,165–1,166] | 466 [465–466] | 38,593 [38,592–38,594] | 17,347 [17,347–17,350] |
| XID / 32,768 | 1,165 [1,165–1,165] | 456 [455–456] | 38,593 [38,592–38,593] | 17,327 [17,326–17,329] |

### Prepared-plan diagnostics

Six client reads per combination; client median includes decoding and checksumming. Prepare is a separate cold statement-parse measurement, and EXPLAIN is one subsequent execution. Automatic planning can change after the fifth execution, so its EXPLAIN need not describe all six reads. The following table uses the guarded query with forced generic planning.

| Fixture | Arm | Prepare (ms) | Full read (ms) | EXPLAIN planning (ms) | Scanned rows | Executed scans / scan nodes | JIT (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small-cut | flat-position | 0.25 | 0.25 | 0.00 | 16.0 | 1/2 | 0.00 |
| small-cut | flat-compound | 0.24 | 0.24 | 0.00 | 16.0 | 1/2 | 0.00 |
| small-cut | partitioned | 0.25 | 0.29 | 0.01 | 16.0 | 1/33 | 0.00 |
| diverse-history | flat-position | 0.25 | 0.26 | 0.01 | 16.0 | 1/2 | 0.00 |
| diverse-history | flat-compound | 0.24 | 0.25 | 0.00 | 16.0 | 1/2 | 0.00 |
| diverse-history | partitioned | 0.30 | 0.42 | 0.01 | 16.0 | 1/49 | 0.00 |
| bulk-history | flat-position | 0.30 | 0.32 | 0.01 | 16.0 | 1/2 | 0.00 |
| bulk-history | flat-compound | 0.31 | 0.34 | 0.01 | 16.0 | 1/2 | 0.00 |
| bulk-history | partitioned | 0.39 | 0.39 | 0.01 | 16.0 | 1/49 | 0.00 |
| dense-cut | flat-position | 0.37 | 108.30 | 0.02 | 100000.0 | 1/2 | 0.00 |
| dense-cut | flat-compound | 0.38 | 110.74 | 0.01 | 100000.0 | 1/2 | 0.00 |
| dense-cut | partitioned | 0.35 | 111.49 | 0.03 | 100000.0 | 13/77 | 0.00 |
| many-leaves | flat-position | 0.26 | 0.26 | 0.00 | 16.0 | 1/2 | 0.00 |
| many-leaves | flat-compound | 0.26 | 0.25 | 0.01 | 16.0 | 1/2 | 0.00 |
| many-leaves | partitioned | 0.31 | 1.73 | 0.10 | 16.0 | 1/513 | 0.00 |

The bulk-history plan stress isolates why the guarded lookup remains necessary.

| Arm | Query | Full read (ms) | Scanned rows | JIT (ms) |
| --- | --- | --- | --- | --- |
| flat-position | any | 152.91 | 2000032.0 | 17.30 |
| flat-position | guarded | 0.32 | 16.0 | 0.00 |
| partitioned | any | 207.51 | 2000033.0 | 179.48 |
| partitioned | guarded | 0.39 | 16.0 | 0.00 |


## Interpretation

**XID-range partitioning is viable for this snapshot protocol and is most useful for cheap retention. Keeping few leaves attached preserves freshness much better than precreating many future leaves.** It retains the global snapshot time domain and the same independently progressing root application.

Small-write capacity is 7,446/sec with 8,192-XID partitions versus 7,605/sec with the original flat table, about 2% lower in this matrix. Large-write medians are all around 3k/sec with overlapping ranges. Source-idle large-payload drain remains about 0.89 seconds across every arm; small-payload drain changes from 0.214 to 0.222 seconds. There is no compelling throughput win here, but the bulk streamed fetch remains competitive.

The forty-second live comparison is especially useful: flat compound freshness p99 is 3.01 ms, static 32-leaf partitioning is 4.27 ms, and rotation ending with three leaves is 3.16 ms. Their p999 values are 3.43, 8.76, and 3.97 ms. Rotation reduces the final journal from about 18.39 MiB static to 1.47 MiB, retiring nine leaves. Median per-run maximum times are 5.38 ms for creation/attach, 2.14 ms for detach, and 3.46 ms for drop. It adds about 0.021 manager SQL statements per write and 400 durable checkpoints over forty seconds. This demonstrates the combined small-working-set / checkpoint / DDL configuration; it does not isolate the incremental cost of DDL from the benefit of fewer leaves.

The substantial-history reclamation runs remove roughly 835–839 MiB while writers continue. Ordinary freshness returns close to the flat baseline after the old leaves are retired. Maintenance and plan invalidation remain visible in the rarer tail; the table reports p999 alongside p99. These tests establish successful online reclamation with verified delivery, rather than full recovery or availability guarantees.

Pruning does what we wanted: the guarded diagnostic visits only sixteen eligible rows, whether history is flat or partitioned. The remaining cost is not proportional to retained row count in these plans. **Attached partition count still matters:** with 512 leaves, a warm generic guarded read has a 1.73 ms client median versus 0.26 ms flat; forced custom is 11.87 ms versus 0.36 ms flat. Even runtime-pruned plans carry metadata/executor work, and cold executions can be slower. Keep the partition count bounded through sensible range sizing, modest provisioning headroom, and reclamation. A pinned retention floor can defeat that bound.

The guarded old-XID lookup remains essential. In the intentionally skewed generic-plan workload, partitioned original-`ANY` freshness p99 is 15.38 seconds versus 3.63 ms guarded. The corresponding flat values are 10.77 seconds and 2.94 ms. Its diagnostic scans about two million old rows and spends substantial time in JIT, whereas guarded lookup examines sixteen. Partitioning does not substitute for an appropriate query shape.

Position sorting is still required. In the dense diagnostic, the generic guarded plan spills a roughly 28 MiB sort with 4 MiB work memory in both schemas. Full client reads are 108.30 ms flat and 111.49 ms partitioned. Physical XID order and the compound key cannot serve as global position order. This remains a potential large-epoch optimization, but ordinary small-cut setup is the more immediate cost exposed here.

The Pareto offered-load matrix retains occasional source/capture stalls lasting hundreds of milliseconds. Quiet-root arrival can be much worse than post-commit freshness. Those samples and ranges are preserved; pruning or independent root application does not establish a bound on those shared database/source delays. These short local runs cannot establish a production tail-latency guarantee or performance beyond RAM capacity.

I would continue with **guarded streamed fetching, XID-range storage, a small attached working set, and background reclamation**. Compare automatic and generic consumer planning as explicit configurations; the tested generic plans reduce setup cost, but schema churn and large cuts still warrant validation. The next deeper measurements should exercise sustained rotation at capacity, larger-than-memory retained history, aging statistics, stalled retention floors, and provisioning during long-running parent users. Adaptive position bounds are a lower priority given the earlier results and the strong bulk-stream performance retained here.

## Reproduction and code

- [Runner](run-partition-stream.py), [report generator](report-partition-stream.py), [sample CSV](partition-stream-samples.csv), [plan summary CSV](partition-stream-explain.csv), [audit](partition-stream-audit.txt).
- [Partition schema and manager](../snapshot_partition_bench_test.go), [retention / late-writer / compound-key tests](../snapshot_partition_protocol_test.go), [prepared-plan diagnostics](../snapshot_partition_plan_test.go).
- [Shared benchmark workload](../domain_capture_bench_test.go), [streamed root pipeline](../snapshot_pipeline_bench_test.go), [guarded query](../snapshot_fetch_optimization_bench_test.go).

```sh
go test -c -o /tmp/falcon-snapshot-partitions.test ./internal/infrastructure/postgres
go test -c -race -o /tmp/falcon-snapshot-partitions-race.test ./internal/infrastructure/postgres
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py smoke
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py validate
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py validate --binary /tmp/falcon-snapshot-partitions-race.test
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py diagnostics
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py suite
python3 internal/infrastructure/postgres/benchmark-results/run-partition-stream.py all
python3 internal/infrastructure/postgres/benchmark-results/report-partition-stream.py
```

Raw measured files use `partition-stream-{capacity,latency,backlog,history,skew,maintenance,reclaim}.{txt,jsonl}`. Every raw invocation records its configuration, source hashes, binary hash, exit status, and before/after container inventories. Archive existing phase logs before repeating runs; the runner appends rather than overwriting them, and the report audit rejects mixed implementations or duplicate samples.
