#!/usr/bin/env python3
"""Rebuild sample CSV and report tables; preserve manually written interpretation."""
import argparse, collections, csv, json, pathlib, statistics

HERE=pathlib.Path(__file__).resolve().parent
PHASES=["idle","capacity","latency","dominance","history","notify-control"]
LABEL={"global-poll":"Global fetch / poll","global-notify":"Global fetch / notify",
       "fair-poll":"Fair fetch / poll","fair-notify":"Fair fetch / notify","wal":"WAL"}
ORDER=list(LABEL)

def variant(r):
    c=r["config"]
    return "wal" if c["mode"]=="wal" else c["snapshot_fetch"]+"-"+c["snapshot_wake"]

def flatten(value, prefix=""):
    out={}
    for k,v in value.items():
        key=prefix+k
        if isinstance(v,dict):
            out.update(flatten(v,key+"."))
        elif not isinstance(v,list):
            out[key]=v
    return out

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--partial",action="store_true")
    args=parser.parse_args()
    rows=[]
    for phase in PHASES:
        p=HERE/f"snapshot-fetch-{phase}.jsonl"
        if p.exists():
            rows.extend(json.loads(line) for line in p.read_text().splitlines())
    followup=[]
    p=HERE/"snapshot-fetch-probe-fix.jsonl"
    if p.exists():
        followup=[json.loads(line) for line in p.read_text().splitlines()]
    groups=collections.defaultdict(list)
    for r in rows:
        groups[r["config"]["name"],variant(r)].append(r)
    extra_groups=collections.defaultdict(list)
    for r in followup:
        extra_groups[r["config"]["name"],variant(r)].append(r)
    if not args.partial:
        assert len(rows)==162, len(rows)
        assert all(len(rs)==3 for rs in groups.values())
        assert len({json.dumps(r["implementation"],sort_keys=True) for r in rows})==1
        if followup:
            assert len(followup)==18, len(followup)
            assert all(len(rs)==3 for rs in extra_groups.values())
            assert all(r["config"]["snapshot_probe"]=="indexed" for r in followup)
            assert len({json.dumps(r["implementation"],sort_keys=True) for r in followup})==1
    flat=[dict(flatten({k:v for k,v in r.items() if k not in ["implementation","requested_config"]}),variant=variant(r)) for r in rows+followup]
    columns=sorted({k for r in flat for k in r})
    with (HERE/"snapshot-fetch-samples.csv").open("w") as f:
        w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(flat)
    def value(name,v,fn,d=1,source=groups):
        rs=source.get((name,v))
        if not rs:return "—"
        values=[fn(r) for r in rs]
        return f"{statistics.median(values):,.{d}f} [{min(values):,.{d}f}–{max(values):,.{d}f}]"
    def table(headers,data):
        return "| "+" | ".join(headers)+" |\n| "+" | ".join(["---"]*len(headers))+" |\n"+"".join("| "+" | ".join(map(str,r))+" |\n" for r in data)+"\n"
    body="## Results\n\n"
    count=sum(r["operations"] for r in rows if r["phase"]!="idle")
    body+=f"**{len(rows)} measured samples, {len(groups)} configurations, {count:,} source transactions.** "
    body+="All accepted source samples passed full-payload checksums, exactly-once delivery, and final latest-state checks. "
    body+="Values are medians of per-run statistics; brackets give minimum and maximum, not confidence intervals. Idle operations count seconds, not writes. Smoke and race runs and Go calibration are excluded.\n\n"
    body+="### Source capacity\n\n"
    body+=table(["Variant","Uniform, 256 B (tx/sec)","Pareto, 256 B (tx/sec)","Pareto, 16 KiB (tx/sec)"],
                [[LABEL[v]]+[value(n,v,lambda r:r["WriteTPS"],0) for n in ["capacity-uniform","capacity-pareto","capacity-large"]] for v in ORDER])
    body+="### Healthy freshness\n\nFreshness starts at COMMIT acknowledgement. Arrival starts at the intended arrival and includes source queueing and execution.\n\n"
    body+=table(["Variant","50/sec freshness p99 (ms)","1k/sec freshness p99 (ms)","3k/sec freshness p99 (ms)","3k/sec arrival p99 (ms)"],
                [[LABEL[v]]+[value(f"healthy-{rate}",v,lambda r:r["Freshness"]["p99_ms"],2) for rate in [50,1000,3000]]+
                 [value("healthy-3000",v,lambda r:r["Arrival"]["p99_ms"],2)] for v in ORDER])
    body+="### Quiet-root isolation\n\nEvery scenario uses 1,024 roots. Quiet roots are all roots other than root 0. Percentiles include only their writes. The fetch-stall scenario pauses the client payload-fetch stage for 200 ms on every thousandth source operation, assigned to root 0. This is an injected stall, not a measured PostgreSQL query fault.\n\n"
    for n in ["hot80-small","hot99-small","hot99-large","hot99-fetch-stall"]:
        body+=f"**{n}**\n\n"
        body+=table(["Variant","Quiet freshness p99 (ms)","Quiet arrival p99 (ms)","Source write p99 (ms)","Post-write drain (sec)"],
                    [[LABEL[v],value(n,v,lambda r:r["Cohorts"]["quiet-roots"]["p99_ms"],2),
                      value(n,v,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"],2),
                      value(n,v,lambda r:r["Write"]["p99_ms"],2),value(n,v,lambda r:r["DrainSeconds"],3)] for v in ORDER[:-1]])
    body+="### Idle load\n\nEach sample observes five measured seconds without source writes after startup. SQL counts include snapshot SELECT, EXISTS/manifest statements, payload queries, and notification SELECTs; BEGIN/COMMIT protocol commands are not counted. Wire traffic includes the consumer pool and NOTIFY sender/listener.\n\n"
    body+=table(["Variant","SQL calls/sec","Committed epochs/sec","Consumer wire KiB/sec"],
                [[LABEL[v],value("idle",v,lambda r:r["Queries"]/r["WriteSeconds"],1),
                  value("idle",v,lambda r:r["Cuts"]/r["WriteSeconds"],1),
                  value("idle",v,lambda r:(r["snapshot"]["ReadWireBytes"]+r["snapshot"]["WriteWireBytes"])/r["WriteSeconds"]/1024,2)] for v in ORDER[:-1]])
    body+="### Consumer cost and buffering\n\nPeak retained payload counts live fetched payload bytes, including delivery queues; it excludes PGX transient buffers, slice/map overhead, and the precomputed validation fixture. Peak Go heap includes the entire client fixture and is sampled every 200 ms. Staged identifiers and metadata are additional to retained payload, available in raw samples.\n\n"
    for n in ["capacity-uniform","capacity-large","hot99-fetch-stall","hot99-large"]:
        body+=f"**{n}**\n\n"
        body+=table(["Variant","SQL calls/write","Consumer wire bytes/write","Peak payload MiB","Peak client heap MiB","WAL bytes/write"],
                    [[LABEL[v],value(n,v,lambda r:(r["Queries"]+r["snapshot"]["NotifySent"])/r["operations"],3),
                      value(n,v,lambda r:(r["snapshot"]["ReadWireBytes"]+r["snapshot"]["WriteWireBytes"])/r["operations"],0),
                      value(n,v,lambda r:r["snapshot"]["PeakBufferedPayload"]/1048576,3),
                      value(n,v,lambda r:r["snapshot"]["PeakHeapBytes"]/1048576,1),
                      value(n,v,lambda r:r["WALBytes"]/r["operations"],0)] for v in ORDER[:-1]])
    body+="### Retained journal history\n\nTwo million historical rows are bulk loaded before measuring 20,000 writes at 1k/sec. This tests table/index size, not two million historical commit IDs. Retention deletion is not exercised.\n\n"
    body+=table(["Variant","Freshness p99 (ms)","Arrival p99 (ms)","Fetch SQL p99 (ms)","SQL calls/write"],
                [[LABEL[v],value("history-2m",v,lambda r:r["Freshness"]["p99_ms"],2),
                  value("history-2m",v,lambda r:r["Arrival"]["p99_ms"],2),
                  value("history-2m",v,lambda r:r["snapshot"]["FetchSQL"]["p99_ms"],2),
                  value("history-2m",v,lambda r:(r["Queries"]+r["snapshot"]["NotifySent"])/r["operations"],3)] for v in ORDER[:-1]])
    body+="### Notification coalescing control\n\nNotification minimum spacing is 2 ms in the main experiment and zero in the control. The first send after an idle period is immediate in both. Compare the same Pareto capacity workload.\n\n"
    body+=table(["Fetch","Spacing","Write tx/sec","Notifications/sec","Epochs/sec","Freshness p99 (ms)"],
                [[f,spacing,value(n,f+"-notify",lambda r:r["WriteTPS"],0),
                  value(n,f+"-notify",lambda r:r["snapshot"]["NotifySent"]/r["WriteSeconds"],0),
                  value(n,f+"-notify",lambda r:r["Cuts"]/r["WriteSeconds"],0),
                  value(n,f+"-notify",lambda r:r["Freshness"]["p99_ms"],2)]
                 for f in ["global","fair"] for n,spacing in [("capacity-pareto","2 ms"),("capacity-notify0","0 ms")]])
    if followup:
        extra_count=sum(r["operations"] for r in followup)
        body+="### Bounded notification probe follow-up\n\n"
        body+=f"An additional **{len(followup)} samples and {extra_count:,} source transactions** use `snapshot_probe=indexed`. "
        body+="They retain the same wakeups, global epochs, payload fetching, and source transactions, changing only the empty-snapshot probe. These samples have a separate recorded implementation hash and are not substituted into the original matrix above. The CSV includes both sets.\n\n"
        body+=table(["Variant","1k/sec freshness p99 (ms)","Pareto capacity (tx/sec)","2m history freshness p99 (ms)","2m history maximum capture (ms)"],
                    [[LABEL[v],value("healthy-1000",v,lambda r:r["Freshness"]["p99_ms"],2,extra_groups),
                      value("capacity-pareto",v,lambda r:r["WriteTPS"],0,extra_groups),
                      value("history-2m",v,lambda r:r["Freshness"]["p99_ms"],2,extra_groups),
                      value("history-2m",v,lambda r:r["snapshot"]["CaptureSQL"]["max_ms"],2,extra_groups)] for v in ["global-notify","fair-notify"]])
    p=HERE/"snapshot-fetch.md"
    existing=p.read_text()
    prefix=existing.split("## Results",1)[0]
    tail="\n## Interpretation"+existing.split("## Interpretation",1)[1] if "## Interpretation" in existing else ""
    p.write_text(prefix+body+tail)
    print(f"{len(rows)} samples; {count:,} source writes; groups={len(groups)}")
    for n in ["capacity-pareto","healthy-1000","hot99-fetch-stall"]:
        for v in ORDER[:-1]:
            rs=groups.get((n,v))
            if rs:
                metric="quiet-roots" if n=="hot99-fetch-stall" else None
                print(n,v,len(rs),value(n,v,lambda r:r["WriteTPS"],0),
                      value(n,v,lambda r:r["Cohorts"][metric]["p99_ms"] if metric else r["Freshness"]["p99_ms"],2))

if __name__=="__main__":main()
