#!/usr/bin/env python3
"""Audit matched capture samples, export CSV, and render comparative tables."""
import argparse, collections, csv, hashlib, json, pathlib, runpy, statistics

HERE = pathlib.Path(__file__).resolve().parent
PHASES = ["capacity", "latency", "spacing", "history", "maintenance", "faults"]
ARMS = ["multi", "single", "wal"]
LABEL = {"multi": "Sequential capture", "single": "Single-request capture", "wal": "WAL"}

def flatten(value, prefix=""):
    out = {}
    for key, item in value.items():
        if isinstance(item, dict): out.update(flatten(item, prefix+key+"."))
        elif not isinstance(item, list): out[prefix+key] = item
    return out

def nodes(node):
    yield node
    for child in node.get("Plans", []): yield from nodes(child)

def table(headers, rows):
    return "| "+" | ".join(headers)+" |\n| "+" | ".join("---" for _ in headers)+" |\n"+"".join("| "+" | ".join(map(str, row))+" |\n" for row in rows)+"\n"

def export_csv(path, rows):
    with path.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=sorted({key for row in rows for key in row}))
        writer.writeheader(); writer.writerows(rows)

def plans():
    reads = collections.defaultdict(list); explains = {}
    for line in (HERE/"snapshot-capture-diagnostics.txt").read_text().splitlines():
        if "CAPTURE_PLAN_READ " in line:
            r = json.loads(line.split("CAPTURE_PLAN_READ ", 1)[1])
            reads[r["fixture"],r["partitioned"],r["changed"],r["mode"]].append(r)
        elif "CAPTURE_PLAN_EXPLAIN " in line:
            r = json.loads(line.split("CAPTURE_PLAN_EXPLAIN ", 1)[1])
            explains[r["fixture"],r["partitioned"],r["changed"],r["mode"]] = r["explain"][0]
    result = []
    for key, rs in reads.items():
        p = explains[key]; ns = list(nodes(p["Plan"]))
        scans = [n for n in ns if n["Node Type"] in ["Index Scan","Index Only Scan","Seq Scan","Bitmap Heap Scan"]]
        result.append(dict(fixture=key[0],partitioned=key[1],changed=key[2],mode=key[3],reads=len(rs),
            client_ms=statistics.median(r["total_ms"] for r in rs),planning_ms=p["Planning Time"],execution_ms=p["Execution Time"],
            scanned_rows=sum((n.get("Actual Rows",0)+n.get("Rows Removed by Filter",0))*n.get("Actual Loops",0) for n in scans),
            scan_nodes=len(scans),executed_scans=sum(n.get("Actual Loops",0)>0 for n in scans),
            temp_written_blocks=p["Plan"].get("Temp Written Blocks",0)))
    export_csv(HERE/"snapshot-capture-explain.csv", result)
    return result

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--partial",action="store_true"); args = ap.parse_args()
    samples = [json.loads(line) for phase in PHASES if (HERE/f"snapshot-capture-{phase}.jsonl").exists()
               for line in (HERE/f"snapshot-capture-{phase}.jsonl").read_text().splitlines()]
    groups = collections.defaultdict(list)
    for r in samples:
        assert r["container_inventory"] == [] and r["config"]["skew"] == "pareto-roots"
        assert r["Freshness"]["count"] == r["operations"]
        assert r["Write"]["count"] == r["operations"]
        if r["arm"] != "wal":
            p = r["snapshot"]["pipeline"]
            assert p["FetchedRows"] == r["operations"]
            assert p["FetchedBytes"] == r["operations"]*r["config"]["rows"]*r["config"]["payload"]
            assert r["config"]["snapshot_capture"] == r["arm"]
        groups[r["config"]["name"],r["arm"]].append(r)
    expected = runpy.run_path(str(HERE/"run-snapshot-capture.py"))
    if not args.partial:
        cases = [(phase,i,arm,c,n,rep) for phase in PHASES for i,(arm,c,n,rep) in enumerate(expected["matrix"](phase))]
        assert len(samples) == len(cases) == 96 and len(groups) == 32
        assert sum(r["operations"] for r in samples) == 3639000
        assert all(len(rs)==3 and {r["repeat"] for r in rs}=={0,1,2} for rs in groups.values())
        actual = {(r["phase"],r["index"]):r for r in samples}; assert len(actual)==len(cases)
        for phase,i,arm,c,n,rep in cases:
            r=actual[phase,i]
            assert (r["arm"],r["requested_config"],r["operations"],r["repeat"]) == (arm,c,n,rep)
        assert len({r["binary_sha256"] for r in samples})==1
        assert len({json.dumps(r["implementation"],sort_keys=True) for r in samples})==1
        manifest=json.loads((HERE/"snapshot-capture-source"/"manifest.json").read_text())
        assert manifest==samples[0]["implementation"]
        for name,sha in manifest.items():
            assert hashlib.sha256((HERE.parent/name).read_bytes()).hexdigest()==sha
            assert hashlib.sha256((HERE/"snapshot-capture-source"/(name+".txt")).read_bytes()).hexdigest()==sha
        for phase in PHASES:
            lines=(HERE/f"snapshot-capture-{phase}.txt").read_text().splitlines()
            starts=[json.loads(l[4:]) for l in lines if l.startswith("RUN ")]
            ends=[json.loads(l[4:]) for l in lines if l.startswith("END ")]
            assert len(starts)==len(ends)==len(expected["matrix"](phase))
            for start,end in zip(starts,ends):
                assert start["container_inventory"]==end["container_inventory"]==[] and end["exit"]==0
                assert start["index"]==end["index"]
        for phase in ["smoke","validate","diagnostics","suite"]:
            lines=(HERE/f"snapshot-capture-{phase}.txt").read_text().splitlines()
            starts=[json.loads(l[4:]) for l in lines if l.startswith("RUN ")]
            ends=[json.loads(l[4:]) for l in lines if l.startswith("END ")]
            assert starts and len(starts)==len(ends)
            assert all(s["container_inventory"]==e["container_inventory"]==[] and e["exit"]==0 for s,e in zip(starts,ends))
    export_csv(HERE/"snapshot-capture-samples.csv",[flatten({k:v for k,v in r.items() if k not in ["implementation","requested_config"]}) for r in samples])
    def stat(name,arm,fn,digits=2):
        rs=groups.get((name,arm))
        if not rs:return "—"
        vs=[fn(r) for r in rs]
        if any(v is None for v in vs):return "—"
        return f"{statistics.median(vs):,.{digits}f} [{min(vs):,.{digits}f}–{max(vs):,.{digits}f}]"
    def fresh(r):return r["Freshness"]["p99_ms"]
    def pipe(r):return r["snapshot"]["pipeline"]
    body=f"## Results\n\n**{len(samples)} measured runs, {len(groups)} configurations, {sum(r['operations'] for r in samples):,} source transactions.** Cells are medians of per-run statistics, with min–max ranges across three repetitions. These are not pooled percentiles or confidence intervals.\n\n"
    body+="### Concurrent write capacity\n\n"+table(["Arm","256 B tx/sec","16 KiB tx/sec","Small saturation fresh p99 (ms)","Large saturation fresh p99 (ms)"],[
        [LABEL[a],stat("capacity-small",a,lambda r:r["WriteTPS"],0),stat("capacity-large",a,lambda r:r["WriteTPS"],0),stat("capacity-small",a,fresh),stat("capacity-large",a,fresh)] for a in ARMS])
    body+="### Healthy freshness\n\nAll columns are commit-acknowledgement-to-root-readiness p99, in milliseconds. The spacing policy remains 2 ms.\n\n"+table(["Arm","50/sec small","1k/sec small","3k/sec small","1k/sec large"],[
        [LABEL[a]]+[stat(n,a,fresh) for n in ["healthy-50","healthy-1000","healthy-3000","healthy-large-1000"]] for a in ARMS])
    body+="### Arrival and writer latency\n\nArrival includes offered-load queuing and source execution; it is not just consumer freshness. The existing WAL harness does not record the quiet-root cohort, so those cells are unavailable.\n\n"
    for name in ["healthy-1000","healthy-3000","healthy-large-1000"]:
        body+=f"**{name}**\n\n"+table(["Arm","Arrival p99 (ms)","Quiet arrival p99 (ms)","Write p99 (ms)","Write max (ms)","Fresh p999 (ms)"],[
            [LABEL[a],stat(name,a,lambda r:r["Arrival"]["p99_ms"]),stat(name,a,lambda r:r["CohortArrivals"].get("quiet-roots",{}).get("p99_ms")),stat(name,a,lambda r:r["Write"]["p99_ms"]),stat(name,a,lambda r:r["Write"]["max_ms"]),stat(name,a,lambda r:r["Freshness"]["p999_ms"])] for a in ARMS])
    body+="### Capture stage and consumer work\n\nCapture is the complete publisher call, including transaction completion. Instrumented queries exclude source SQL, bridge notifications, and the sequential control's BEGIN/COMMIT commands. Consumer wire bytes include the notification bridge and transaction-command traffic, but exclude writers. Stage percentiles must not be added to infer an end-to-end percentile.\n\n"
    for name in ["healthy-1000","healthy-3000","capacity-small","capacity-large"]:
        body+=f"**{name}**\n\n"+table(["Arm","Capture p50 (ms)","Capture p99 (ms)","Capture max (ms)","Fetch p99 (ms)","Queries/write","Wire bytes/write","Epochs/sec"],[
            [LABEL[a],stat(name,a,lambda r:r["snapshot"]["CaptureSQL"]["p50_ms"]),stat(name,a,lambda r:r["snapshot"]["CaptureSQL"]["p99_ms"]),stat(name,a,lambda r:r["snapshot"]["CaptureSQL"]["max_ms"]),stat(name,a,lambda r:r["snapshot"]["FetchSQL"]["p99_ms"]),stat(name,a,lambda r:r["Queries"]/r["operations"],3),stat(name,a,lambda r:(r["snapshot"]["ReadWireBytes"]+r["snapshot"]["WriteWireBytes"])/r["operations"],0),stat(name,a,lambda r:r["Cuts"]/r["snapshot"]["ElapsedSeconds"],0)] for a in ARMS[:2]])
    body+="### Immediate notification control\n\nThis changes only notification minimum spacing from 2 ms to zero. The first notification after idle is immediate under either policy.\n\n"+table(["Capture","Spacing","Small capacity (tx/sec)","1k/sec fresh p99 (ms)","1k/sec arrival p99 (ms)","Capacity fresh p99 (ms)","Notifications/sec at capacity"],[
        [LABEL[a],spacing,stat(capacity,a,lambda r:r["WriteTPS"],0),stat(healthy,a,fresh),stat(healthy,a,lambda r:r["Arrival"]["p99_ms"]),stat(capacity,a,fresh),stat(capacity,a,lambda r:r["snapshot"]["NotifySent"]/r["snapshot"]["ElapsedSeconds"],0)] for a in ARMS[:2] for spacing,capacity,healthy in [("2 ms","capacity-small","healthy-1000"),("0 ms","immediate-capacity","immediate-1000")]])
    body+="### Two million retained journal rows\n\nHistory spans 2,048 real historical transactions. Generic mode changes consumer planning only.\n\n"+table(["Case","Arm","Fresh p99 (ms)","Quiet arrival p99 (ms)","Capture p99 (ms)","Capture max (ms)"],[
        [name,LABEL[a],stat(name,a,fresh),stat(name,a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:r["snapshot"]["CaptureSQL"]["p99_ms"]),stat(name,a,lambda r:r["snapshot"]["CaptureSQL"]["max_ms"])] for name in ["history-auto","history-generic"] for a in ARMS[:2]])
    body+="### Online partition rotation\n\n40k small writes at 1k/sec, 8,192-XID ranges, initially four leaves, two future ranges of headroom, and the same 100 ms maintenance manager.\n\n"+table(["Arm","Fresh p99 (ms)","Fresh p999 (ms)","Quiet arrival p99 (ms)","Capture p99 (ms)","Dropped leaves","Final leaves"],[
        [LABEL[a],stat("rotation-1000",a,fresh),stat("rotation-1000",a,lambda r:r["Freshness"]["p999_ms"]),stat("rotation-1000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat("rotation-1000",a,lambda r:r["snapshot"]["CaptureSQL"]["p99_ms"]),stat("rotation-1000",a,lambda r:r["partitions"]["Dropped"],0),stat("rotation-1000",a,lambda r:r["partitions"]["FinalLeaves"],0)] for a in ARMS[:2]])
    body+="### Sparse source faults\n\nOne rollback/retry or 200 ms post-append pause per 1,000 operations. Quiet roots are the lower-ranked 90% of roots; they exclude root 0, which receives the injected pauses.\n\n"+table(["Fault","Arm","All fresh p99 (ms)","Quiet fresh p99 (ms)","Quiet arrival p99 (ms)","Arrival max (ms)"],[
        [name,LABEL[a],stat(name,a,fresh),stat(name,a,lambda r:r["Cohorts"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:r["Arrival"]["max_ms"])] for name in ["fault-rollback","fault-after"] for a in ARMS[:2]])
    ps=plans()
    if not args.partial:assert len(ps)==24 and all(p["reads"]==6 and p["scanned_rows"]<=1 for p in ps)
    body+="### Capture plan diagnostics\n\nSix warmed captures per combination, plus EXPLAIN ANALYZE. Empty probes include a formerly active transaction with no journal entry. Nonempty probes add one eligible transaction. These are diagnostics outside the measured matrix.\n\n"+table(["Fixture","Partitioned","Changed","Planning","Client median (ms)","Server execution (ms)","Scanned rows"],[
        [p["fixture"],p["partitioned"],p["changed"],p["mode"],f"{p['client_ms']:.3f}",f"{p['execution_ms']:.3f}",p["scanned_rows"]] for p in ps])
    path=HERE/"snapshot-capture.md"
    if path.exists():
        old=path.read_text();intro=old.split("## Results",1)[0]
        tail="## Interpretation"+old.split("## Interpretation",1)[1] if "## Interpretation" in old else ""
    else:intro="# Single-request snapshot capture\n\n";tail=""
    path.write_text(intro+body+tail)
    audit=dict(measured_runs=len(samples),configurations=len(groups),source_transactions=sum(r["operations"] for r in samples),
        repetitions=3,plan_combinations=len(ps),diagnostic_captures=sum(p["reads"] for p in ps),
        normal_binary_sha256=sorted({r["binary_sha256"] for r in samples}),all_checked_inventories_empty=True,partial=args.partial)
    (HERE/"snapshot-capture-audit.txt").write_text(json.dumps(audit,indent=2)+"\n")
    print(json.dumps(audit,indent=2))

if __name__=="__main__":main()
