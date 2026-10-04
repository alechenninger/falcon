#!/usr/bin/env python3
"""Validate fetch-plan samples and render statistics plus EXPLAIN summaries."""
import argparse, collections, csv, json, pathlib, statistics

HERE = pathlib.Path(__file__).resolve().parent
PHASES = ["backlog", "capacity", "latency", "history", "index"]
ARMS = ["control", "guarded", "metadata", "adaptive"]
LABEL = dict(control="Control", guarded="Guarded", metadata="Metadata", adaptive="Adaptive bounds")

def nodes(n):
    yield n
    for c in n.get("Plans", []):
        yield from nodes(c)

def flatten(v, prefix=""):
    out = {}
    for k, value in v.items():
        if isinstance(value, dict):
            out.update(flatten(value, prefix+k+"."))
        elif not isinstance(value, list):
            out[prefix+k] = value
    return out

def table(headers, rows):
    return "| "+" | ".join(headers)+" |\n| "+" | ".join(["---"]*len(headers))+" |\n"+"".join("| "+" | ".join(map(str,r))+" |\n" for r in rows)+"\n"

def diagnostic_summary():
    reads = collections.defaultdict(list)
    plans = {}
    logs=[HERE/"fetch-plans-diagnostics.txt",HERE/"fetch-plans-diagnostics-stats.txt"]
    lines=[line for p in logs if p.exists() for line in p.read_text().splitlines()]
    for line in lines:
        if "PLAN_READ " in line:
            r = json.loads(line.split("PLAN_READ ",1)[1]);reads[r["fixture"],r["plan"],r["cover"],r["mode"]].append(r)
        elif "PLAN_EXPLAIN " in line:
            r = json.loads(line.split("PLAN_EXPLAIN ",1)[1]);plans[r["fixture"],r["plan"],r["cover"],r["mode"]] = r["explain"][0]
    rows = []
    for key, rs in reads.items():
        p = plans[key];ns = list(nodes(p["Plan"]));sorts=[n for n in ns if n["Node Type"] == "Sort"]
        scans=[n for n in ns if n["Node Type"] in ["Index Scan","Index Only Scan","Seq Scan","Bitmap Heap Scan"]]
        rows.append(dict(fixture=key[0],plan=key[1],cover=key[2],mode=key[3],
                         includes_bounds=rs[0].get("includes_bounds",False),
                         read_ms=statistics.median(r["total_ms"] for r in rs),
                         read_min_ms=min(r["total_ms"] for r in rs),read_max_ms=max(r["total_ms"] for r in rs),
                         first_ms=statistics.median(r["first_ms"] for r in rs),
                         first_min_ms=min(r["first_ms"] for r in rs),first_max_ms=max(r["first_ms"] for r in rs),
                         execution_ms=p["Execution Time"],startup_ms=p["Plan"]["Actual Startup Time"],
                         estimated_cost=p["Plan"]["Total Cost"],jit_ms=p.get("JIT",{}).get("Timing",{}).get("Total",0),
                         scanned_rows=sum((n.get("Actual Rows",0)+n.get("Rows Removed by Filter",0))*n.get("Actual Loops",1) for n in scans),
                         sort_method=";".join(n.get("Sort Method","") for n in sorts),
                         sort_kib=sum(n.get("Sort Space Used",0) for n in sorts),
                         sort_width=";".join(str(n["Plan Width"]) for n in sorts),
                         temp_written_blocks=p["Plan"].get("Temp Written Blocks",0),
                         heap_fetches=sum(n.get("Heap Fetches",0)*n.get("Actual Loops",1) for n in scans)))
    with (HERE/"fetch-plans-explain.csv").open("w") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    return rows

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--partial",action="store_true");args=ap.parse_args()
    samples=[]
    for phase in PHASES:
        p=HERE/f"fetch-plans-{phase}.jsonl"
        if p.exists():samples.extend(json.loads(l) for l in p.read_text().splitlines())
    groups=collections.defaultdict(list)
    for r in samples:
        assert r["snapshot"]["pipeline"]["FetchedRows"]==r["operations"]
        assert r["container_inventory"]==[] and r["config"]["skew"]=="pareto-roots"
        groups[r["config"]["name"],r["arm"]].append(r)
    if not args.partial:
        assert len(samples)==120 and len(groups)==40,(len(samples),len(groups))
        assert all(len(g)==3 and {r["repeat"] for r in g}=={0,1,2} for g in groups.values())
        assert len({json.dumps(r["implementation"],sort_keys=True) for r in samples})==1
        assert len({r["binary_sha256"] for r in samples})==1
    flats=[flatten({k:v for k,v in r.items() if k not in ["implementation","requested_config"]}) for r in samples]
    with (HERE/"fetch-plans-samples.csv").open("w") as f:
        w=csv.DictWriter(f,fieldnames=sorted({k for r in flats for k in r}));w.writeheader();w.writerows(flats)
    def stat(name,arm,fn,digits=2):
        rs=groups.get((name,arm))
        if not rs:return "—"
        vs=[fn(r) for r in rs]
        return f"{statistics.median(vs):,.{digits}f} [{min(vs):,.{digits}f}–{max(vs):,.{digits}f}]"
    def pipe(r):return r["snapshot"]["pipeline"]
    body=f"## Results\n\n**{len(samples)} measured samples, {len(groups)} configurations, {sum(r['operations'] for r in samples):,} source transactions.** Values are medians of per-run statistics, with min–max ranges. These are not pooled percentiles or confidence intervals.\n\n"
    body+="### Source-write capacity\n\n"+table(["Arm","256 B tx/sec","16 KiB tx/sec","16 KiB freshness p99 (ms)"],[
        [LABEL[a],stat("capacity-small",a,lambda r:r["WriteTPS"],0),stat("capacity-large",a,lambda r:r["WriteTPS"],0),stat("capacity-large",a,lambda r:r["Freshness"]["p99_ms"])] for a in ARMS])
    body+="### Healthy freshness and arrival\n\n"+table(["Arm","1k/sec fresh p99 (ms)","1k/sec quiet arrival p99 (ms)","3k/sec fresh p99 (ms)","3k/sec quiet arrival p99 (ms)"],[
        [LABEL[a],stat("healthy-1000",a,lambda r:r["Freshness"]["p99_ms"]),stat("healthy-1000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat("healthy-3000",a,lambda r:r["Freshness"]["p99_ms"]),stat("healthy-3000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"])] for a in ARMS])
    body+="At 3k/sec, these maximum-duration summaries help distinguish source/capture stalls from payload startup. They are medians and ranges of each run's maximum, not p99 values.\n\n"+table(["Arm","Append/commit max (ms)","Capture SQL max (ms)","Fetch SQL max (ms)","Epoch first row max (ms)"],[
        [LABEL[a],stat("healthy-3000",a,lambda r:r["AppendCommit"]["max_ms"]),stat("healthy-3000",a,lambda r:r["snapshot"]["CaptureSQL"]["max_ms"]),stat("healthy-3000",a,lambda r:r["snapshot"]["FetchSQL"]["max_ms"]),stat("healthy-3000",a,lambda r:pipe(r)["EpochFirstRow"]["max_ms"])] for a in ARMS])
    body+="### Source-idle backlog drain\n\nFirst-row latency includes bounds discovery for Adaptive. No root becomes readable until successful global fetch EOF and its own preceding application work.\n\n"
    for name in ["backlog-small","backlog-large","backlog-history"]:
        body+=f"**{name}**\n\n"+table(["Arm","Drain (sec)","Epoch first row (ms)","Fetch MiB/sec","Peak payload MiB"],[
            [LABEL[a],stat(name,a,lambda r:r["DrainSeconds"],3),stat(name,a,lambda r:pipe(r)["EpochFirstRow"]["p99_ms"]),stat(name,a,lambda r:pipe(r)["PayloadMiBPerFetchSecond"],1),stat(name,a,lambda r:r["snapshot"]["PeakBufferedPayload"]/1048576)] for a in ARMS])
    body+="### Retained history and consumer planning\n\nTwo million old entries, then 1k offered small writes/sec. Generic mode applies to the consumer pool, including capture; writers retain automatic planning.\n\n"
    for name in ["history-auto","history-generic"]:
        body+=f"**{name}**\n\n"+table(["Arm","Write tx/sec","Freshness p99 (ms)","Quiet arrival p99 (ms)","Drain (sec)","Epoch first row p99 (ms)"],[
            [LABEL[a],stat(name,a,lambda r:r["WriteTPS"],0),stat(name,a,lambda r:r["Freshness"]["p99_ms"]),stat(name,a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:r["DrainSeconds"],3),stat(name,a,lambda r:pipe(r)["EpochFirstRow"]["p99_ms"])] for a in ARMS])
    body+="### Consumer cost\n\nSQL includes capture, probe, manifest, bounds/payload statements, and separate notifications; BEGIN/COMMIT are excluded. Socket bytes include the consumer pool and notification bridge.\n\n"
    for name in ["healthy-1000","capacity-small","capacity-large","backlog-small"]:
        body+=f"**{name}**\n\n"+table(["Arm","Fetch-stage SQL","All SQL/write","Wire bytes/write","Bounds calls","Sparse fallbacks"],[
            [LABEL[a],stat(name,a,lambda r:r["snapshot"]["FetchQueries"],0),stat(name,a,lambda r:(r["Queries"]+r["snapshot"]["NotifySent"])/r["operations"],3),stat(name,a,lambda r:(r["snapshot"]["ReadWireBytes"]+r["snapshot"]["WriteWireBytes"])/r["operations"],0),stat(name,a,lambda r:pipe(r)["BoundsQueries"],0),stat(name,a,lambda r:pipe(r)["FallbackCuts"],0)] for a in ARMS])
    body+="### Covering primary-index write control\n\nBoth arms use the original streamed payload query. Cover replaces the position primary key with the same unique key plus included writer XID; the number of indexes stays fixed. No forced planning or explicit vacuum is used in these capacity runs.\n\n"+table(["Index","256 B tx/sec","16 KiB tx/sec","256 B WAL bytes/write","256 B journal bytes/write"],[
        [label,stat("index-capacity-small",a,lambda r:r["WriteTPS"],0),stat("index-capacity-large",a,lambda r:r["WriteTPS"],0),stat("index-capacity-small",a,lambda r:r["WALBytes"]/r["operations"],0),stat("index-capacity-small",a,lambda r:r["JournalBytes"]/r["operations"],0)] for a,label in [("control","Position PK"),("cover-control","Position PK + included XID")]])
    diagnostic_summary()
    p=HERE/"fetch-plans.md";old=p.read_text();prefix=old.split("## Results",1)[0];tail="\n## Interpretation"+old.split("## Interpretation",1)[1];p.write_text(prefix+body+tail)
    print(f"{len(samples)} samples / {len(groups)} configurations / {sum(r['operations'] for r in samples):,} operations")
    for name in ["backlog-small","backlog-large","backlog-history","capacity-small","capacity-large","healthy-1000","history-generic"]:
        for a in ARMS:
            if (name,a) in groups:print(name,a,"TPS",stat(name,a,lambda r:r["WriteTPS"],0),"fresh",stat(name,a,lambda r:r["Freshness"]["p99_ms"]),"drain",stat(name,a,lambda r:r["DrainSeconds"],3))

if __name__ == "__main__":main()
