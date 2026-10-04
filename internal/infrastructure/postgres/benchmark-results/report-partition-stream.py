#!/usr/bin/env python3
"""Audit partitioned streaming samples and render measured tables and plan summaries."""
import argparse, collections, csv, hashlib, json, pathlib, statistics

HERE = pathlib.Path(__file__).resolve().parent
PHASES = ["capacity", "latency", "backlog", "history", "skew", "maintenance", "reclaim"]
ARMS = ["flat-position", "flat-compound", "xid-8192", "xid-32768"]
LABEL = {"flat-position": "Flat / position PK", "flat-compound": "Flat / compound PK",
         "xid-8192": "XID / 8,192", "xid-32768": "XID / 32,768"}

def nodes(n):
    yield n
    for child in n.get("Plans", []):
        yield from nodes(child)

def flatten(v, prefix=""):
    out = {}
    for k, value in v.items():
        if isinstance(value, dict):
            out.update(flatten(value, prefix+k+"."))
        elif not isinstance(value, list):
            out[prefix+k] = value
    return out

def table(headers, rows):
    return "| "+" | ".join(headers)+" |\n| "+" | ".join(["---"]*len(headers))+" |\n"+"".join("| "+" | ".join(map(str, r))+" |\n" for r in rows)+"\n"

def diagnostics():
    reads = collections.defaultdict(list)
    explains = {}
    path = HERE/"partition-stream-diagnostics.txt"
    if not path.exists():
        return []
    for line in path.read_text().splitlines():
        if "PARTITION_PLAN_READ " in line:
            r = json.loads(line.split("PARTITION_PLAN_READ ", 1)[1])
            reads[r["fixture"], r["arm"], r["plan"], r["mode"]].append(r)
        elif "PARTITION_PLAN_EXPLAIN " in line:
            r = json.loads(line.split("PARTITION_PLAN_EXPLAIN ", 1)[1])
            explains[r["fixture"], r["arm"], r["plan"], r["mode"]] = r["explain"][0]
    summaries = []
    for key, rs in reads.items():
        p = explains[key]
        ns = list(nodes(p["Plan"]))
        scans = [n for n in ns if n["Node Type"] in ["Index Scan", "Index Only Scan", "Seq Scan", "Bitmap Heap Scan"]]
        sorts = [n for n in ns if n["Node Type"] == "Sort"]
        summaries.append(dict(fixture=key[0], arm=key[1], plan=key[2], mode=key[3],
            samples=len(rs), rows=rs[0]["rows"], prepare_ms=rs[0]["prepare_ms"],
            first_ms=statistics.median(r["first_ms"] for r in rs),
            total_ms=statistics.median(r["total_ms"] for r in rs),
            total_min_ms=min(r["total_ms"] for r in rs), total_max_ms=max(r["total_ms"] for r in rs),
            planning_ms=p["Planning Time"], execution_ms=p["Execution Time"],
            startup_ms=p["Plan"]["Actual Startup Time"],
            scanned_rows=sum((n.get("Actual Rows", 0)+n.get("Rows Removed by Filter", 0))*n.get("Actual Loops", 1) for n in scans),
            scan_nodes=len(scans), executed_scans=sum(n.get("Actual Loops", 0)>0 for n in scans),
            subplans_removed=sum(n.get("Subplans Removed", 0) for n in ns),
            jit_ms=p.get("JIT", {}).get("Timing", {}).get("Total", 0),
            sort_method=";".join(n.get("Sort Method", "") for n in sorts),
            sort_kib=sum(n.get("Sort Space Used", 0) for n in sorts),
            temp_written_blocks=p["Plan"].get("Temp Written Blocks", 0)))
    if summaries:
        with (HERE/"partition-stream-explain.csv").open("w") as f:
            w = csv.DictWriter(f, fieldnames=list(summaries[0])); w.writeheader(); w.writerows(summaries)
    return summaries

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--partial", action="store_true"); args = ap.parse_args()
    samples = [json.loads(line) for phase in PHASES if (HERE/f"partition-stream-{phase}.jsonl").exists()
               for line in (HERE/f"partition-stream-{phase}.jsonl").read_text().splitlines()]
    groups = collections.defaultdict(list)
    for r in samples:
        assert r["snapshot"]["pipeline"]["FetchedRows"] == r["operations"]
        assert r["snapshot"]["pipeline"]["FetchedBytes"] == r["operations"]*r["config"]["rows"]*r["config"]["payload"]
        assert r["container_inventory"] == [] and r["config"]["skew"] == "pareto-roots"
        assert r["ApplyBeforeAck"] == 0
        groups[r["config"]["name"], r["arm"]].append(r)
    if not args.partial:
        assert len(samples) == 126 and len(groups) == 42, (len(samples), len(groups))
        assert sum(r["operations"] for r in samples) == 5190000
        assert all(len(g) == 3 and {r["repeat"] for r in g} == {0, 1, 2} for g in groups.values())
        assert len({json.dumps(r["implementation"], sort_keys=True) for r in samples}) == 1
        assert len({r["binary_sha256"] for r in samples}) == 1
        for name, sha in samples[0]["implementation"].items():
            assert hashlib.sha256((HERE.parent/name).read_bytes()).hexdigest() == sha, name
        for phase in PHASES:
            raw = (HERE/f"partition-stream-{phase}.txt").read_text().splitlines()
            starts = [json.loads(l[4:]) for l in raw if l.startswith("RUN ")]
            ends = [json.loads(l[4:]) for l in raw if l.startswith("END ")]
            count = sum(r["phase"] == phase for r in samples)
            assert len(starts) == len(ends) == count, (phase, count)
            assert all(s["container_inventory"] == [] and e["container_inventory"] == [] and e["exit"] == 0 for s, e in zip(starts, ends))
            assert [s["index"] for s in starts] == [e["index"] for e in ends]
    if samples:
        flats = [flatten({k:v for k,v in r.items() if k not in ["implementation", "requested_config"]}) for r in samples]
        with (HERE/"partition-stream-samples.csv").open("w") as f:
            w = csv.DictWriter(f, fieldnames=sorted({k for r in flats for k in r})); w.writeheader(); w.writerows(flats)
    def stat(name, arm, fn, digits=2):
        rs = groups.get((name, arm))
        if not rs: return "—"
        values = [fn(r) for r in rs]
        return f"{statistics.median(values):,.{digits}f} [{min(values):,.{digits}f}–{max(values):,.{digits}f}]"
    def pipe(r): return r["snapshot"]["pipeline"]
    body = f"## Results\n\n**{len(samples)} measured runs, {len(groups)} configurations, {sum(r['operations'] for r in samples):,} source transactions.** Cells are medians of per-run statistics with min–max ranges across three repetitions; these are not pooled percentiles or confidence intervals.\n\n"
    body += "### Concurrent capacity\n\n"+table(["Arm", "256 B writes/sec", "16 KiB writes/sec", "16 KiB freshness p99 (ms)"], [
        [LABEL[a], stat("capacity-small",a,lambda r:r["WriteTPS"],0), stat("capacity-large",a,lambda r:r["WriteTPS"],0), stat("capacity-large",a,lambda r:r["Freshness"]["p99_ms"])] for a in ARMS])
    body += "### Healthy offered load\n\n"+table(["Arm", "1k/sec fresh p99 (ms)", "1k/sec quiet arrival p99 (ms)", "3k/sec fresh p99 (ms)", "3k/sec quiet arrival p99 (ms)"], [
        [LABEL[a], stat("healthy-1000",a,lambda r:r["Freshness"]["p99_ms"]), stat("healthy-1000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]), stat("healthy-3000",a,lambda r:r["Freshness"]["p99_ms"]), stat("healthy-3000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"])] for a in ARMS])
    body += "Maximum durations at 3k/sec distinguish source/capture stalls from fetching. These are medians of each run's maximum, not p99.\n\n"+table(["Arm", "Append/commit max (ms)", "Capture max (ms)", "Fetch max (ms)"], [
        [LABEL[a],stat("healthy-3000",a,lambda r:r["AppendCommit"]["max_ms"]),stat("healthy-3000",a,lambda r:r["snapshot"]["CaptureSQL"]["max_ms"]),stat("healthy-3000",a,lambda r:r["snapshot"]["FetchSQL"]["max_ms"])] for a in ARMS])
    body += "### Source-idle backlog\n\nFreshness is deliberately excluded here: the source commits its entire backlog before the consumer starts.\n\n"
    for name in ["backlog-small", "backlog-large"]:
        body += f"**{name}**\n\n"+table(["Arm", "Drain (sec)", "First row (ms)", "Fetch MiB/sec", "Peak queued payload MiB"], [
            [LABEL[a],stat(name,a,lambda r:r["DrainSeconds"],3),stat(name,a,lambda r:pipe(r)["EpochFirstRow"]["p99_ms"]),stat(name,a,lambda r:pipe(r)["PayloadMiBPerFetchSecond"],1),stat(name,a,lambda r:r["snapshot"]["PeakBufferedPayload"]/1048576)] for a in ARMS])
    body += "### Two million retained rows across 2,048 real transactions\n\n"
    for name in ["history-auto", "history-generic"]:
        body += f"**{name}**\n\n"+table(["Arm", "Freshness p99 (ms)", "Quiet arrival p99 (ms)", "First row p99 (ms)", "Fetch SQL/write"], [
            [LABEL[a],stat(name,a,lambda r:r["Freshness"]["p99_ms"]),stat(name,a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:pipe(r)["EpochFirstRow"]["p99_ms"]),stat(name,a,lambda r:r["snapshot"]["FetchQueries"]/r["operations"],3)] for a in ARMS])
    body += "### Bulk-history generic-plan stress control\n\nTwo million retained rows from one old XID, then 1k/sec offered load. This isolates the previously observed query-shape pathology.\n\n"+table(["Arm", "Freshness p99 (ms)", "First row p99 (ms)", "Drain (sec)"], [
        [a,stat("bulk-history-generic",a,lambda r:r["Freshness"]["p99_ms"]),stat("bulk-history-generic",a,lambda r:pipe(r)["EpochFirstRow"]["p99_ms"]),stat("bulk-history-generic",a,lambda r:r["DrainSeconds"],3)] for a in ["flat-any", "flat-guarded", "xid-any", "xid-guarded"]])
    body += "### Background rotation and retention\n\n40k small writes at 1k/sec, with no preload. Static starts with 32 future buckets; rotating starts with four and maintains two buckets of headroom. This comparison includes that different metadata working set, durable progress writes, DDL, and one additional manager connection.\n\n"+table(["Arm", "Fresh p99 (ms)", "Fresh p999 (ms)", "Quiet arrival p99 (ms)", "Journal MiB", "Dropped", "Final leaves"], [
        [a,stat("maintenance-1000",a,lambda r:r["Freshness"]["p99_ms"]),stat("maintenance-1000",a,lambda r:r["Freshness"]["p999_ms"]),stat("maintenance-1000",a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat("maintenance-1000",a,lambda r:r["JournalBytes"]/1048576,1),stat("maintenance-1000",a,lambda r:(r.get("partitions")or{}).get("Dropped",0),0),stat("maintenance-1000",a,lambda r:(r.get("partitions")or{}).get("FinalLeaves",0),0)] for a in ["flat-compound", "static-8192", "rotating-8192"]])
    body += table(["Rotation", "Count", "p99 duration (ms)", "Max duration (ms)"], [
        [k,stat("maintenance-1000","rotating-8192",lambda r,k=k:r["partitions"][k]["count"],0),stat("maintenance-1000","rotating-8192",lambda r,k=k:r["partitions"][k]["p99_ms"]),stat("maintenance-1000","rotating-8192",lambda r,k=k:r["partitions"][k]["max_ms"])] for k in ["Create", "Detach", "Drop", "Checkpoint"]])
    body += table(["Rotating 8,192", "Manager SQL/write", "Durable checkpoints", "Created leaves", "Reclaimed MiB"], [[
        "40k live writes",stat("maintenance-1000","rotating-8192",lambda r:r["partitions"]["SQL"]/r["operations"],3),stat("maintenance-1000","rotating-8192",lambda r:r["partitions"]["Checkpoints"],0),stat("maintenance-1000","rotating-8192",lambda r:r["partitions"]["Created"],0),stat("maintenance-1000","rotating-8192",lambda r:r["partitions"]["ReclaimedBytes"]/1048576,1)]])
    body += "### Reclaiming substantial history during writes\n\nTwo million retained rows across 2,048 real transactions, then 1k/sec writes with rotation and reclamation enabled. Automatic cases run 20k writes; the generic case runs 10k, matching the corresponding static history controls. GC can first persist progress and retire old history after the first fully completed epoch.\n\n"+table(["Arm / planning", "Fresh p99 (ms)", "Fresh p999 (ms)", "Quiet arrival p99 (ms)", "Reclaimed MiB", "Final journal MiB", "Detach max (ms)"], [
        [a+" / "+mode,stat(name,a,lambda r:r["Freshness"]["p99_ms"]),stat(name,a,lambda r:r["Freshness"]["p999_ms"]),stat(name,a,lambda r:r["CohortArrivals"]["quiet-roots"]["p99_ms"]),stat(name,a,lambda r:r["partitions"]["ReclaimedBytes"]/1048576,1),stat(name,a,lambda r:r["JournalBytes"]/1048576,1),stat(name,a,lambda r:r["partitions"]["Detach"]["max_ms"])]
        for name,a,mode in [("history-reclaim-auto","rotating-8192","auto"),("history-reclaim-auto","rotating-32768","auto"),("history-reclaim-generic","rotating-8192","generic")]])
    body += "### Write amplification\n\nJournal size sums all attached leaf relations (including their indexes and TOAST). Measured WAL excludes preload and initial partition DDL but includes live rotation.\n\n"+table(["Arm", "Small WAL bytes/write", "Small journal bytes/write", "Large WAL bytes/write", "Large journal bytes/write"], [
        [LABEL[a],stat("capacity-small",a,lambda r:r["WALBytes"]/r["operations"],0),stat("capacity-small",a,lambda r:r["JournalBytes"]/r["operations"],0),stat("capacity-large",a,lambda r:r["WALBytes"]/r["operations"],0),stat("capacity-large",a,lambda r:r["JournalBytes"]/r["operations"],0)] for a in ARMS])
    plans = diagnostics()
    if not args.partial:
        assert len(plans) == 90 and all(p["samples"] == 6 for p in plans), len(plans)
        manifest = json.loads((HERE/"partition-stream-source"/"manifest.json").read_text())
        assert manifest == samples[0]["implementation"]
        for name, sha in manifest.items():
            assert hashlib.sha256((HERE/"partition-stream-source"/(name+".txt")).read_bytes()).hexdigest() == sha
        checks = {}
        for phase in ["validate", "suite", "diagnostics", "smoke"]:
            lines = (HERE/f"partition-stream-{phase}.txt").read_text().splitlines()
            starts = [json.loads(l[4:]) for l in lines if l.startswith("RUN ")]
            ends = [json.loads(l[4:]) for l in lines if l.startswith("END ")]
            assert len(starts) == len(ends) and starts
            assert all(s["container_inventory"] == [] and e["container_inventory"] == [] and e["exit"] == 0 for s,e in zip(starts,ends))
            checks[phase] = len(starts)
        validations = [json.loads(l[4:]) for l in (HERE/"partition-stream-validate.txt").read_text().splitlines() if l.startswith("RUN ")]
        assert any("race" in r["binary"] and r["implementation"] == manifest for r in validations)
        assert any("race" not in r["binary"] and r["implementation"] == manifest for r in validations)
        (HERE/"partition-stream-audit.txt").write_text(
            f"Measured runs: {len(samples)}\nConfigurations: {len(groups)}; three distinct repetitions each\n"
            f"Source transactions: {sum(r['operations'] for r in samples):,}\n"
            "All measured source/binary hashes identical\n"
            f"Binary SHA256: {samples[0]['binary_sha256']}\n"
            f"{len(manifest)} current and archived source hashes match measured implementation\n"
            "All 126 measured invocations have exit 0 and empty before/after container inventories\n"
            "Every measured sample delivered the exact expected entry and payload-byte counts; no premature apply acknowledgement\n"
            f"Diagnostics: {len(plans)} plans / {sum(p['samples'] for p in plans)} verified full client reads\n"
            f"Excluded invocation audit: {json.dumps(checks,sort_keys=True)}; all exit 0 and inventories empty\n"
            "Frozen normal and race protocol validation confirmed\n")
    body += "### Prepared-plan diagnostics\n\nSix client reads per combination; client median includes decoding and checksumming. Prepare is a separate cold statement-parse measurement, and EXPLAIN is one subsequent execution. Automatic planning can change after the fifth execution, so its EXPLAIN need not describe all six reads. The following table uses the guarded query with forced generic planning.\n\n"
    selected = [p for p in plans if p["plan"] == "guarded" and p["mode"] == "force_generic_plan"]
    body += table(["Fixture", "Arm", "Prepare (ms)", "Full read (ms)", "EXPLAIN planning (ms)", "Scanned rows", "Executed scans / scan nodes", "JIT (ms)"], [
        [p["fixture"],p["arm"],f'{p["prepare_ms"]:.2f}',f'{p["total_ms"]:.2f}',f'{p["planning_ms"]:.2f}',p["scanned_rows"],f'{p["executed_scans"]}/{p["scan_nodes"]}',f'{p["jit_ms"]:.2f}'] for p in selected])
    body += "The bulk-history plan stress isolates why the guarded lookup remains necessary.\n\n"+table(["Arm", "Query", "Full read (ms)", "Scanned rows", "JIT (ms)"], [
        [p["arm"],p["plan"],f'{p["total_ms"]:.2f}',p["scanned_rows"],f'{p["jit_ms"]:.2f}'] for p in plans
        if p["fixture"] == "bulk-history" and p["mode"] == "force_generic_plan" and p["arm"] != "flat-compound"])
    report = HERE/"partition-stream.md"
    if report.exists():
        old = report.read_text(); prefix = old.split("## Results",1)[0]
        tail = "\n## Interpretation"+old.split("## Interpretation",1)[1] if "## Interpretation" in old else ""
        report.write_text(prefix+body+tail)
    else:
        report.write_text("# XID-partitioned streamed journal fetch\n\n"+body)
    print(f"{len(samples)} measured runs / {len(groups)} configurations / {sum(r['operations'] for r in samples):,} source transactions; {len(plans)} plans")
    for name, arm in groups:
        print(name,arm,"TPS",stat(name,arm,lambda r:r["WriteTPS"],0),"fresh",stat(name,arm,lambda r:r["Freshness"]["p99_ms"]),"drain",stat(name,arm,lambda r:r["DrainSeconds"],3))

if __name__ == "__main__": main()
