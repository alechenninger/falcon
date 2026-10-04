#!/usr/bin/env python3
"""Isolated fetch-plan experiments, with mandatory container cleanup checks."""
import argparse, datetime, hashlib, json, os, pathlib, random, subprocess, time

HERE = pathlib.Path(__file__).resolve().parent
PODMAN = os.environ.get("PODMAN", "/opt/homebrew/bin/podman")
SOURCES = ["domain_capture_bench_test.go", "snapshot_fetch_bench_test.go", "snapshot_pipeline_bench_test.go",
           "snapshot_pipeline_protocol_test.go", "snapshot_fetch_optimization_bench_test.go",
           "snapshot_fetch_optimization_plan_test.go", "snapshot_fetch_optimization_protocol_test.go", "replication_bench_test.go"]
ARMS = [("control", "", False), ("guarded", "guarded", False), ("metadata", "metadata", False),
        ("adaptive", "adaptive", False)]

def inventory():
    return json.loads(subprocess.check_output([PODMAN, "ps", "-a", "--format", "json"]))

def matrix(phase):
    cases = []
    def add(name, n, repeats=3, arms=None, **kw):
        for rep in range(repeats):
            for label, plan, cover in (arms or ARMS):
                c = dict(name=name, mode="snapshot-routed", tenants=64, roots_per_tenant=16, rows=1,
                         payload=256, keys=4096, workers=4, skew="pareto-roots", snapshot_fetch="pipeline-stream",
                         snapshot_wake="notify", snapshot_probe="indexed", notify_batch_ms=2, fallback_ms=100,
                         apply_batch_rows=1, queue_bytes=32*1048576, fetch_plan=plan, position_cover=cover)
                c.update(kw)
                cases.append((label, c, n, rep))
    if phase == "smoke":
        add("smoke-small", 4000, repeats=1, rate=1000)
        add("smoke-large", 3000, repeats=1, backlog=True, rows=16, payload=1024)
    elif phase == "backlog":
        add("backlog-small", 100000, backlog=True)
        add("backlog-large", 20000, backlog=True, rows=16, payload=1024)
        add("backlog-history", 100000, backlog=True, history=2000000)
    elif phase == "capacity":
        add("capacity-small", 100000)
        add("capacity-large", 50000, rows=16, payload=1024)
    elif phase == "latency":
        add("healthy-1000", 20000, rate=1000)
        add("healthy-3000", 60000, rate=3000)
    elif phase == "history":
        add("history-auto", 20000, rate=1000, history=2000000)
        add("history-generic", 10000, rate=1000, history=2000000, plan_cache="force_generic_plan")
    elif phase == "index":
        pair = [("control", "", False), ("cover-control", "", True)]
        add("index-capacity-small", 100000, arms=pair)
        add("index-capacity-large", 50000, arms=pair, rows=16, payload=1024)
    else:
        raise ValueError(phase)
    random.Random(9621+sum(map(ord, phase))).shuffle(cases)
    return cases

def invoke(args, phase, command, extra_env=None, header=None, program=None):
    env = dict(os.environ)
    env.setdefault("DOCKER_HOST", "unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock")
    if env.get("FALCON_REPLICATION_BENCH_DATABASE_URL"):
        raise RuntimeError("Fresh benchmark containers are required")
    env.update(extra_env or {})
    before = inventory()
    if before:
        raise RuntimeError(f"Unexpected pre-run containers: {before}")
    meta = dict(header or {}, phase=phase, utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                container_inventory=before, binary=args.binary,
                binary_sha256=hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest(),
                implementation={n: hashlib.sha256((HERE.parent/n).read_bytes()).hexdigest() for n in SOURCES})
    with (HERE/f"fetch-plans-{phase}.txt").open("a", buffering=1) as raw:
        raw.write("RUN "+json.dumps(meta)+"\n")
        start = time.monotonic()
        try:
            proc = subprocess.run([program or args.binary]+command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, timeout=1200)
        except subprocess.TimeoutExpired:
            for item in inventory():
                if (item.get("Labels") or {}).get("falcon.benchmark") == "domain-capture":
                    subprocess.run([PODMAN, "rm", "-f", item["Id"]], check=True)
            raw.write("TIMEOUT: cleaned benchmark-owned containers\n")
            raise
        raw.write(proc.stdout)
        after = inventory()
        raw.write("END "+json.dumps(dict(exit=proc.returncode, elapsed=time.monotonic()-start,
                                        container_inventory=after, index=meta.get("index")))+"\n")
        if after:
            for item in after:
                if (item.get("Labels") or {}).get("falcon.benchmark") == "domain-capture":
                    subprocess.run([PODMAN, "rm", "-f", item["Id"]], check=True)
            raise RuntimeError(f"Post-run containers remained; cleaned owned instances: {after}")
        if proc.returncode:
            print(proc.stdout[-16000:], flush=True)
            raise RuntimeError(f"{phase} failed; see raw log")
    print(f"  completed in {time.monotonic()-start:.1f}s; containers=0", flush=True)
    return proc.stdout, meta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["diagnostics", "diagnostics-cover", "diagnostics-stats", "validate", "suite", "smoke", "backlog", "capacity", "latency", "history", "index", "all"])
    parser.add_argument("--binary", default="/tmp/falcon-snapshot-fetch-plans.test")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.phase == "suite":
        invoke(args, "suite", ["test", "-count=1", "./..."], program="go")
        return
    if args.phase in ["diagnostics", "diagnostics-cover", "diagnostics-stats", "validate"]:
        print(args.phase, flush=True)
        pattern = {"diagnostics": "^TestSnapshotFetchPlanDiagnostics$", "diagnostics-cover": "^TestSnapshotFetchPlanCoverVisibility$", "diagnostics-stats": "^TestSnapshotFetchPlanStatistics$", "validate": "^TestSnapshot(Pipeline|FetchPlanSparseAndEmpty)"}[args.phase]
        extra = None
        if args.phase == "diagnostics-stats":
            name = "snapshot_fetch_optimization_statistics_test.go"
            extra = {"supplementary_implementation": {name: hashlib.sha256((HERE.parent/name).read_bytes()).hexdigest()}}
        invoke(args, args.phase, ["-test.run", pattern, "-test.v", "-test.timeout", "18m"],
               {"FALCON_FETCH_PLAN_DIAGNOSTICS": "1"} if args.phase.startswith("diagnostics") else {}, extra)
        return
    phases = ["backlog", "capacity", "latency", "history", "index"] if args.phase == "all" else [args.phase]
    if args.phase == "all" and (args.start or args.limit is not None):
        parser.error("--start/--limit require one phase")
    for phase in phases:
        cases = matrix(phase)
        if args.limit is not None:
            cases = cases[:args.limit]
        for index, (arm, c, n, rep) in enumerate(cases):
            if index < args.start:
                continue
            print(f"{phase} {index+1}/{len(cases)} {c['name']} {arm} rep={rep+1}", flush=True)
            output, meta = invoke(args, phase, ["-test.run", "^$", "-test.bench", "^BenchmarkSnapshotFetchPlans$",
                                                "-test.benchtime", f"{n}x", "-test.count", "1", "-test.timeout", "10m", "-test.v"],
                                  {"FALCON_DOMAIN_CAPTURE_CONFIG": json.dumps(c)},
                                  dict(index=index, repeat=rep, arm=arm, operations=n, requested_config=c))
            rows = [json.loads(l.split("DOMAIN_SAMPLE ",1)[1]) for l in output.splitlines() if "DOMAIN_SAMPLE " in l]
            rows = [r for r in rows if r["operations"] == n]
            if len(rows) != 1:
                raise RuntimeError(f"Expected one measured sample, got {len(rows)}")
            r = rows[0]
            r.update({k:v for k,v in meta.items() if k != "operations"})
            with (HERE/f"fetch-plans-{phase}.jsonl").open("a", buffering=1) as f:
                f.write(json.dumps(r)+"\n")
            print(f"  {r['WriteTPS']:.0f} writes/s; fresh p99={r['Freshness']['p99_ms']:.2f}ms; "
                  f"drain={r['DrainSeconds']:.3f}s; first={r['snapshot']['pipeline']['EpochFirstRow']['p99_ms']:.2f}ms", flush=True)

if __name__ == "__main__":
    main()
