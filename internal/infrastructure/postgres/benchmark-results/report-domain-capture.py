#!/usr/bin/env python3
"""Generate the comparison tables from accepted, complete repetitions."""
import collections,json,pathlib,statistics
HERE=pathlib.Path(__file__).resolve().parent
rows=[]
for phase in ['capacity','repairs','latency','faults','controls','history']:
    for line in (HERE/f'domain-capture-{phase}.jsonl').read_text().splitlines():
        r=json.loads(line)
        if r['config']['mode'].startswith('counter-') and r['config'].get('counter_fetch')!='bounded':continue
        rows.append(r)
groups=collections.defaultdict(list)
for r in rows:groups[(r['config']['name'],r['config']['mode'])].append(r)
assert len(rows)==303 and all(len(rs)==3 for rs in groups.values())
MODES=['snapshot-coupled','snapshot-roots','snapshot-routed','counter-global','counter-tenant','counter-root','wal']
LABEL={'snapshot-coupled':'Coupled snapshot','snapshot-roots':'Separate root fetch','snapshot-routed':'Routed snapshot','counter-global':'Global counter','counter-tenant':'Tenant counter','counter-root':'Root counter','wal':'WAL'}
def median(rs,fn):return statistics.median(fn(r) for r in rs)
def range_value(rs,fn,decimals=1):
    values=[fn(r) for r in rs]
    return f'{statistics.median(values):,.{decimals}f} [{min(values):,.{decimals}f}–{max(values):,.{decimals}f}]'
def table(headers,data):
    return '| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+''.join('| '+' | '.join(map(str,row))+' |\n' for row in data)+'\n'
text='''## Results

**303 accepted samples across 101 configurations, each repeated three times: 18,090,000 successful source transactions.** All samples passed checksum, exactly-once delivery, and final latest-state checks. Tables show the median of three per-run statistics; brackets show the observed minimum and maximum, not confidence intervals. They do not pool all latency observations into a single percentile. Calibration and ten superseded counter samples are excluded.

### Global cuts versus delivery barriers

Independent root delivery has its clearest benefit when one root's delivery is paused, rather than when all roots are fast. At 1,000 intended transactions/sec, every thousandth transaction belongs to root 0 and injects a 200 ms delivery pause. The table excludes those injected transactions from the healthy cohorts and reports intended-arrival-to-readable p99, in milliseconds.

'''
data=[]
for mode in MODES:
    rs=groups['pareto-delivery',mode]
    data.append([LABEL[mode]]+[range_value(rs,lambda r,c=c:r['CohortArrivals'][c]['p99_ms']) for c in ['affected-root','sibling-roots','other-tenants']])
text+=table(['Delivery implementation','Healthy writes: affected root','Sibling roots, same tenant','Other tenants'],data)
text+='''Routed snapshots reduce other-tenant median p99 from about 202 ms to 12 ms, while retaining a global cut and total epoch order. Even the **global counter** permits unrelated roots to proceed when delivery is independent; its source ordering does not require a global apply barrier. WAL can use the same isolation, and reaches about 1 ms for healthy other roots in this fault test.

A worker pool is necessary: restricting either split snapshot implementation to one delivery worker brings freshness p99 back to roughly 202–208 ms. Splitting the cut publisher alone does not create delivery capacity.

The routed implementation still fetches an entire global epoch before routing its payload. It isolates root delivery stalls, not a slow global fetch query, a stalled cut publisher, or backpressure after the bounded queues fill. Separate root fetch also isolates fetching, but its many small queries have a substantial cost at high cardinality.

### Counter scope and write throughput

Single-row transactions contain 256 bytes of payload. The multi-tenant workload has 64 tenants and 16 roots per tenant. Nested rank weights use exponent 1.05; the resulting Pareto trace places 78.2% of writes on the busiest 20% of roots and 32.7% on the busiest 1%. This is a concentrated workload rather than an equal distribution across domains.

The columns below are median successful source transactions/sec, with per-run ranges. The capacity workload is closed-loop with sixteen writers, not a fixed-rate latency workload.

'''
data=[]
for mode in MODES:
    data.append([LABEL[mode]]+[range_value(groups[name,mode],lambda r:r['WriteTPS'],0) for name in ['multi-uniform','multi-pareto','multi-hot-root']])
text+=table(['Implementation','Uniform: 1,024 roots','Pareto: 1,024 roots','80% on one root'],data)
text+='''Scoping the counter recovers roughly **2.1–2.2×** the global counter's Pareto write throughput. Moving from 64 tenant clocks to 1,024 root clocks adds only about **4%** at the medians; the observed ranges overlap, so this is not evidence for a dependable throughput win from hierarchy on this particular many-tenant trace. Root clocks provide a stronger isolation boundary under faults, shown below.

When 80% of writes target one root but its object keys remain diverse, the root counter falls to about 3,620/sec. Snapshots and WAL retain approximately 7,600–7,900/sec. A root counter serializes every write in that root, including updates to independent tuples.

A single large tenant is different. Tenant-only time would still use one counter; root clocks can spread that tenant's writes:

'''
data=[]
for root_count in [1,64,1024]:
    name=f'one-tenant-{root_count}'
    data.append([root_count]+[range_value(groups[name,mode],lambda r:r['WriteTPS'],0) for mode in ['counter-global','counter-root','snapshot-routed','wal']])
text+=table(['Roots in one tenant','One counter (also tenant-only)','Root counters','Routed snapshots','WAL'],data)
text+='''Under uniform demand, 64 local clocks already recover most of the available throughput. This is the concrete capacity argument for considering local clocks inside a high-volume tenant; it does not include shared-root generation certification costs.

### Reader cost at cardinality

At saturation with uniform demand across 1,024 roots, separate root fetching produces median freshness p99 of **1,852 ms** (range 1,670–2,012 ms). Routed snapshots stay at **12.6 ms** (12.5–12.7 ms). Separate fetching makes about **1.015 consumer SQL queries per transaction**, versus **0.038** for routed snapshots, including capture/discovery queries. Its queue grows and post-write drain reaches about 0.53 seconds. Its source throughput is therefore not a claim that delivery remained fresh at that rate.

On Pareto load, multiple events for hot roots amortize those fetches: separate fetching reaches about 6,939 writes/sec with 20.6 ms freshness p99, compared with 7,913/sec and 12.7 ms for routed snapshots. Root cardinality without attention to SQL batching is not sufficient for a scalable consumer.

### Healthy fixed-rate latency

These samples contain no injected faults. Entries are median p99 `[minimum–maximum]`, in milliseconds. Freshness begins at the writer's COMMIT acknowledgement. Arrival includes scheduling, source execution and commit, then capture/delivery.

'''
data=[]
for mode in MODES:
    a=groups['pareto-at-1000',mode];b=groups['pareto-at-3000',mode]
    data.append([LABEL[mode],range_value(a,lambda r:r['Freshness']['p99_ms']),range_value(a,lambda r:r['Arrival']['p99_ms']),range_value(b,lambda r:r['Freshness']['p99_ms']),range_value(b,lambda r:r['Arrival']['p99_ms'])])
text+=table(['Implementation','1k/sec freshness','1k/sec arrival','3k/sec freshness','3k/sec arrival'],data)
text+='''The 10 ms polling interval accounts for much of journal freshness. These runs do not compare commit wakeups or a tuned shorter poll interval. At 3,000/sec, root and tenant counters have about 12.8–13.0 ms median arrival p99; the global counter reaches about 32.5 ms near its source capacity. Routed snapshots are about 12.0 ms at the median.

**The healthy end-to-end tails are noisy.** Several samples have source-write stalls of hundreds of milliseconds, and some exceed a second. These generate offered-load queueing even when average commits/sec meets the target. For example, the 3k tenant-counter sample with 1,135 ms arrival p99 contains a 917 ms source operation; a routed snapshot sample with 591 ms arrival p99 contains a 434 ms source operation. The 1k coupled snapshot median is 163 ms, and all three repetitions have elevated arrival p99, despite 11.4 ms median freshness p99. Source wait events were not sampled, so these stalls cannot be attributed precisely to IO, lock waits, the client, or an external resource interruption. No samples were discarded for being slow. These results do not justify a precise small healthy-latency ranking or an intrinsic sub-millisecond claim.

### Stalled writer: narrower clocks and shared-pool spillover

Every thousandth transaction pauses for 200 ms **after** journal allocation and before COMMIT. It holds the selected counter row throughout that pause. The following p99 values measure intended arrival through readable delivery and exclude the injected operations from the healthy cohorts.

'''
data=[]
for mode in MODES:
    rs=groups['pareto-after',mode]
    data.append([LABEL[mode]]+[range_value(rs,lambda r,c=c:r['CohortArrivals'][c]['p99_ms']) for c in ['affected-root','sibling-roots','other-tenants']])
text+=table(['Implementation','Healthy writes: affected root','Sibling roots, same tenant','Other tenants'],data)
text+='''The global counter spreads a held clock across all writers. A tenant counter confines the database row lock to one tenant, but the blocked requests can consume all sixteen shared writer slots and delay other tenants. Root counters keep the directly blocked set much smaller: sibling-root median p99 is about **12 ms** and other tenants about **18 ms**, while healthy writes on the affected root wait about **199 ms**. This is stronger evidence for a root-level QoS benefit than the small healthy-throughput difference from tenant clocks.

A pause **before** allocation leaves the counter available to other writers: healthy arrival p99 stays near 11–12 ms for journal implementations. This validates doing the counter update late. Snapshots exclude active writers from cuts and later recover their committed journal entries, without waiting on a sequence hole; healthy roots remain near 12 ms after an allocated-writer pause too.

Rollback tests preserve dense counter prefixes and complete delivery. Snapshot sequence allocations can leave holes, but visibility-based capture does not require filling them. Some rollback samples have large source stalls, so their arrival distributions are available in the CSV rather than summarized as a rollback latency improvement.

### Larger payloads and hot keys

The larger workload upserts sixteen 1 KiB source rows per transaction and journals their full 16 KiB payload. Source and journal working sets together exceed shared buffers. These are saturated samples, not 1k/sec healthy comparisons.

'''
data=[]
for mode in MODES:
    rs=groups['batch16-pareto',mode]
    data.append([LABEL[mode],range_value(rs,lambda r:r['WriteTPS'],0),range_value(rs,lambda r:r['Freshness']['p99_ms'])])
text+=table(['Implementation','Transactions/sec','Freshness p99 (ms)'],data)
text+='''At the medians, routed snapshots, tenant counters and root counters are close to 2,900–3,100 transactions/sec. WAL reaches about 4,349/sec. The global counter is about 1,481/sec. Native counter positions avoid another durable epoch publication and permit page-prefix delivery; their measured freshness tail is lower than snapshot epoch delivery here. The throughput ranges are broad enough that small rankings should be treated cautiously.

With **one object key** and 80% of writes on its root, every tested design instead reaches about 2,400–2,600 writes/sec: global counter 2,497, tenant counter 2,528, root counter 2,552, routed snapshot 2,523, and WAL 2,494. Clock partitioning cannot remove the latest-state row lock. The journal implementations use late allocation after the source mutation, and the final-state validation verifies that updates to a key are delivered in the source's order.

### Two million retained journal entries

These samples preload two million historical entries, then deliver 20,000 new transactions at 1,000/sec. Historical entries are bulk loaded, already considered applied, and excluded from capture. They test physical table/index size and delta access; their xid distribution is not a replay of two million individual historical commits. No old partitions are dropped in this experiment.

'''
data=[]
for mode in MODES[:-1]:
    rs=groups['history-2m',mode]
    data.append([LABEL[mode],f'{median(rs,lambda r:r["JournalBytes"])/2**20:,.0f}',range_value(rs,lambda r:r['Freshness']['p99_ms']),range_value(rs,lambda r:r['Arrival']['p99_ms'])])
text+=table(['Implementation','Journal + indexes (MiB)','Freshness p99 (ms)','Arrival p99 (ms)'],data)
text+='''The bounded counter query avoids the retained-history scan cliff. All counter scopes remain near 10 ms freshness and 11 ms median arrival p99; root/tenant ranges are tight here. Routed snapshots have a large arrival outlier while their median stays near 12 ms. Partitioning, deletion/vacuum cost, old-cut leases, publisher failover, and distributed consumer ownership still need separate experiments.

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
'''
path=HERE/'domain-capture.md'
prior=path.read_text().split('## Results\n',1)[0]
# Use the measured concentration rather than a nominal distribution label.
share=median(groups['multi-pareto','counter-root'],lambda r:r['RootTop1Share'])*100
text=text.replace('32.7% on the busiest 1%',f'{share:.1f}% on the busiest 1%')
path.write_text(prior+text)
print('Generated report for',len(rows),'samples and',len(groups),'complete configurations')
