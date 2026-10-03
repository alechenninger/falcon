#!/usr/bin/env python3
"""Validate pipeline samples and render comparison tables, preserving commentary."""
import argparse, collections, csv, json, pathlib, statistics

HERE = pathlib.Path(__file__).resolve().parent
PHASES = ["capacity", "latency", "burst", "apply-stall", "history", "backlog", "batch32"]
ARMS = ["buffered128", "paged128", "paged2048", "stream"]
LABEL = {"buffered128": "Buffered / 128", "paged128": "Pipelined / 128", "paged2048": "Pipelined / 2,048", "stream": "Pipelined / stream"}

def flatten(v, prefix=""):
    out = {}
    for k, value in v.items():
        if isinstance(value, dict):
            out.update(flatten(value, prefix+k+"."))
        elif not isinstance(value, list):
            out[prefix+k] = value
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partial", action="store_true")
    args = ap.parse_args()
    samples = []
    for phase in PHASES:
        file = HERE/f"snapshot-pipeline-{phase}.jsonl"
        if file.exists():
            samples.extend(json.loads(line) for line in file.read_text().splitlines())
    groups = collections.defaultdict(list)
    for r in samples:
        groups[r["config"]["name"], r["arm"]].append(r)
        assert r["snapshot"]["pipeline"]["FetchedRows"] == r["operations"]
        assert r["config"]["skew"] == "pareto-roots"
        assert r["container_inventory"] == []
    if not args.partial:
        assert len(samples) == 180, len(samples)
        assert len(groups) == 60, len(groups)
        assert all(len(g) == 3 and {r["repeat"] for r in g} == {0, 1, 2} for g in groups.values())
        assert len({json.dumps(r["implementation"], sort_keys=True) for r in samples}) == 1
        assert len({r["binary_sha256"] for r in samples}) == 1
    flats = [flatten({k: v for k, v in r.items() if k not in ["implementation", "requested_config"]}) for r in samples]
    columns = sorted({k for r in flats for k in r})
    with (HERE/"snapshot-pipeline-samples.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(flats)
    def value(name, arm, fn, digits=2):
        rows = groups.get((name, arm))
        if not rows:
            return "—"
        v = [fn(r) for r in rows]
        return f"{statistics.median(v):,.{digits}f} [{min(v):,.{digits}f}–{max(v):,.{digits}f}]"
    def table(headers, rows):
        return "| "+" | ".join(headers)+" |\n| "+" | ".join(["---"]*len(headers))+" |\n"+"".join("| "+" | ".join(map(str, row))+" |\n" for row in rows)+"\n"
    def pipe(r):
        return r["snapshot"]["pipeline"]
    count = sum(r["operations"] for r in samples)
    body = f"## Results\n\n**{len(samples)} measured samples, {len(groups)} configurations, {count:,} source transactions.** "
    body += "Values are medians of per-run statistics; brackets give the minimum and maximum, not confidence intervals. Smoke tests, race tests, query-plan diagnostics, and Go calibration samples are excluded. All measured samples validate exactly-once complete-payload delivery and final state against the latest-state table.\n\n"
    body += "### Concurrent write capacity\n\n"
    body += table(["Fetch", "256 B tx/sec", "16 KiB tx/sec", "16 KiB freshness p99 (ms)", "16 KiB fetch busy (%)"], [
        [LABEL[a], value("capacity-small", a, lambda r: r["WriteTPS"], 0), value("capacity-large", a, lambda r: r["WriteTPS"], 0), value("capacity-large", a, lambda r: r["Freshness"]["p99_ms"]),
         value("capacity-large", a, lambda r: 100*pipe(r)["FetchSeconds"]/(r["WriteSeconds"]+r["DrainSeconds"]), 1)] for a in ARMS])
    body += "Fetch busy is the fraction of elapsed time inside epoch fetching, including inline routing for pipelined arms. It is not device utilization. All arms run one SQL payload reader; source writing and capture share the database.\n\n"
    body += "### Healthy freshness\n\n"
    body += table(["Fetch", "50/sec p99 (ms)", "1k/sec p99 (ms)", "3k/sec p99 (ms)", "3k/sec quiet p99 (ms)"], [
        [LABEL[a]]+[value(f"healthy-{rate}", a, lambda r: r["Freshness"]["p99_ms"]) for rate in [50, 1000, 3000]]+[value("healthy-3000", a, lambda r: r["Cohorts"]["quiet-roots"]["p99_ms"])] for a in ARMS])
    body += "### Backlog drain and effective fetch capacity\n\nThe consumer starts after all source transactions commit. Drain time includes consumer startup, initial durable capture, fetching, routing, and publication. Effective fetch MiB/sec uses payload bytes divided by total epoch fetch duration, including decoding, routing, and any backpressure. It excludes source writing and root publication after EOF; it is not a storage-device bandwidth measurement. Backlog freshness includes the deliberate consumer-off interval and is not a healthy-latency statistic.\n\n"
    for name in ["backlog-small", "backlog-large", "backlog-history"]:
        body += f"**{name}**\n\n"
        body += table(["Fetch", "Drain (sec)", "Fetch MiB/sec", "Peak payload MiB", "Staged before EOF (%)", "First row p99 (ms)"], [
            [LABEL[a], value(name, a, lambda r: r["DrainSeconds"], 3), value(name, a, lambda r: pipe(r)["PayloadMiBPerFetchSecond"], 1),
             value(name, a, lambda r: r["snapshot"]["PeakBufferedPayload"]/1048576, 2),
             value(name, a, lambda r: 100*pipe(r)["StagedBeforeEOF"]/r["operations"], 1),
             value(name, a, lambda r: pipe(r)["StatementFirstRow"]["p99_ms"])] for a in ARMS])
    body += "### Bursts and slow application\n\nQuiet roots are the lower-ranked 90% of roots (ranks 104–1,024), not every root except rank one. Bursts offer 6k small transactions or 2k large transactions every two seconds. Application stalls pause root 0 for 200 ms every 1,000th source operation, forced to that root; fetching itself is not deliberately stalled. Arrival starts at the intended arrival and includes source queueing.\n\n"
    for name in ["burst-small", "burst-large", "apply-stall-small", "apply-stall-large"]:
        body += f"**{name}**\n\n"
        body += table(["Fetch", "Quiet fresh p99 (ms)", "Quiet arrival p99 (ms)", "All fresh p99 (ms)", "Peak payload MiB"], [
            [LABEL[a], value(name, a, lambda r: r["Cohorts"]["quiet-roots"]["p99_ms"]), value(name, a, lambda r: r["CohortArrivals"]["quiet-roots"]["p99_ms"]),
             value(name, a, lambda r: r["Freshness"]["p99_ms"]), value(name, a, lambda r: r["snapshot"]["PeakBufferedPayload"]/1048576, 2)] for a in ARMS])
    body += "### Consumer cost\n\nCounts include snapshot capture/probe/manifest, fetch, and separate notification SQL; BEGIN/COMMIT protocol commands are excluded. Wire bytes include the consumer pool and notification bridge, excluding prewarmed pool connection establishment. Backlog startup additionally establishes the notification connections after the writers stop. Payload peaks exclude pgx transient buffers, staging identifiers, and graph state.\n\n"
    for name in ["capacity-small", "capacity-large", "backlog-small", "backlog-large"]:
        body += f"**{name}**\n\n"
        body += table(["Fetch", "Fetch statements", "All SQL/write", "Wire bytes/write", "Peak staged IDs", "Peak Go heap MiB"], [
            [LABEL[a], value(name, a, lambda r: r["snapshot"]["FetchQueries"], 0), value(name, a, lambda r: (r["Queries"]+r["snapshot"]["NotifySent"])/r["operations"], 3),
             value(name, a, lambda r: (r["snapshot"]["ReadWireBytes"]+r["snapshot"]["WriteWireBytes"])/r["operations"], 0), value(name, a, lambda r: r["snapshot"]["PeakStagedIDs"], 0),
             value(name, a, lambda r: r["snapshot"]["PeakHeapBytes"]/1048576, 1)] for a in ARMS])
    body += "### Retained history\n\nTwo million old entries are loaded before measurement. This bulk history has one old writer XID; measured source transactions each have their own XID.\n\n"
    body += table(["Fetch", "Freshness p99 (ms)", "Quiet p99 (ms)", "Capture max (ms)", "First row p99 (ms)"], [
        [LABEL[a], value("history-2m", a, lambda r: r["Freshness"]["p99_ms"]), value("history-2m", a, lambda r: r["Cohorts"]["quiet-roots"]["p99_ms"]),
         value("history-2m", a, lambda r: r["snapshot"]["CaptureSQL"]["max_ms"]), value("history-2m", a, lambda r: pipe(r)["StatementFirstRow"]["p99_ms"])] for a in ARMS])
    body += "### Application batching control\n\nThese additional samples change the common application batch from one row to at most 32 rows / 256 KiB. SQL page boundaries never flush batches. All four fetch arms share the same batch rule within each comparison.\n\n"
    body += table(["Fetch", "Large write capacity (tx/sec)", "Small backlog drain (sec)", "Small backlog fetch MiB/sec"], [
        [LABEL[a], value("batch32-capacity-large", a, lambda r: r["WriteTPS"], 0), value("batch32-backlog-small", a, lambda r: r["DrainSeconds"], 3),
         value("batch32-backlog-small", a, lambda r: pipe(r)["PayloadMiBPerFetchSecond"], 1)] for a in ARMS])
    file = HERE/"snapshot-pipeline.md"
    old = file.read_text()
    prefix = old.split("## Results", 1)[0]
    tail = "\n## Interpretation"+old.split("## Interpretation", 1)[1] if "## Interpretation" in old else ""
    file.write_text(prefix+body+tail)
    print(f"{len(samples)} samples; {len(groups)} configurations; {count:,} measured transactions")
    for name in ["capacity-small", "capacity-large", "healthy-1000", "backlog-small", "backlog-large", "backlog-history"]:
        for a in ARMS:
            if (name, a) in groups:
                print(name, a, "writes/s", value(name, a, lambda r: r["WriteTPS"], 0), "fresh", value(name, a, lambda r: r["Freshness"]["p99_ms"]), "drain", value(name, a, lambda r: r["DrainSeconds"], 3))

if __name__ == "__main__":
    main()
