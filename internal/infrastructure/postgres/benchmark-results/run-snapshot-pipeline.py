#!/usr/bin/env python3
"""Run sequential, isolated snapshot pipeline experiments; never reuse a DB."""
import argparse, datetime, hashlib, json, os, pathlib, random, subprocess, time

HERE = pathlib.Path(__file__).resolve().parent
PODMAN = os.environ.get("PODMAN", "/opt/homebrew/bin/podman")
ARMS = [("buffered128", "pipeline-buffered", 128),
        ("paged128", "pipeline-paged", 128),
        ("paged2048", "pipeline-paged", 2048),
        ("stream", "pipeline-stream", 0)]
PHASES = ["backlog", "capacity", "latency", "burst", "apply-stall", "history", "batch32"]
SOURCES = ["domain_capture_bench_test.go", "snapshot_fetch_bench_test.go", "snapshot_pipeline_bench_test.go",
           "snapshot_pipeline_protocol_test.go", "snapshot_pipeline_plan_test.go", "replication_bench_test.go"]

def inventory():
    return json.loads(subprocess.check_output([PODMAN, "ps", "-a", "--format", "json"]))

def matrix(phase):
    cases = []
    def add(name, n, repeats=3, **kw):
        for rep in range(repeats):
            for label, fetch, page in ARMS:
                c = dict(name=name, mode="snapshot-routed", tenants=64, roots_per_tenant=16,
                         rows=1, payload=256, keys=4096, workers=4, skew="pareto-roots",
                         snapshot_fetch=fetch, snapshot_wake="notify", snapshot_probe="indexed",
                         notify_batch_ms=2, fallback_ms=100, fetch_page_rows=page,
                         apply_batch_rows=1, queue_bytes=32*1048576)
                c.update(kw)
                cases.append((label, c, n, rep))
    if phase == "smoke":
        add("smoke", 5000, repeats=1, rate=2000)
        add("smoke-backlog", 3000, repeats=1, backlog=True, rows=16, payload=1024)
    elif phase == "capacity":
        add("capacity-small", 100000)
        add("capacity-large", 50000, rows=16, payload=1024)
    elif phase == "latency":
        for rate in [50, 1000, 3000]:
            add(f"healthy-{rate}", rate*20, rate=rate)
    elif phase == "burst":
        add("burst-small", 60000, rate=3000, burst_size=6000)
        add("burst-large", 30000, rate=1000, burst_size=2000, rows=16, payload=1024)
    elif phase == "apply-stall":
        add("apply-stall-small", 60000, rate=3000, fault="delivery")
        add("apply-stall-large", 30000, rate=1500, fault="delivery", rows=16, payload=1024)
    elif phase == "history":
        add("history-2m", 20000, rate=1000, history=2000000)
    elif phase == "backlog":
        add("backlog-small", 100000, backlog=True)
        add("backlog-large", 20000, backlog=True, rows=16, payload=1024)
        add("backlog-history", 100000, backlog=True, history=2000000)
    elif phase == "batch32":
        add("batch32-capacity-large", 50000, rows=16, payload=1024, apply_batch_rows=32)
        add("batch32-backlog-small", 100000, backlog=True, apply_batch_rows=32)
    else:
        raise ValueError(phase)
    random.Random(8317+sum(map(ord, phase))).shuffle(cases)
    return cases

def run(phase, args):
    cases = matrix(phase)
    if args.limit is not None:
        cases = cases[:args.limit]
    env = dict(os.environ)
    env.setdefault("DOCKER_HOST", "unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock")
    if env.get("FALCON_REPLICATION_BENCH_DATABASE_URL"):
        raise RuntimeError("Fresh containers are required.")
    prefix = HERE / ("snapshot-pipeline-"+phase)
    hashes = {name: hashlib.sha256((HERE.parent/name).read_bytes()).hexdigest() for name in SOURCES}
    binary_hash = hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest()
    with open(str(prefix)+".txt", "a", buffering=1) as raw, open(str(prefix)+".jsonl", "a", buffering=1) as samples:
        for index, (label, config, n, rep) in enumerate(cases):
            if index < args.start:
                continue
            before = inventory()
            if before:
                raise RuntimeError(f"Unexpected pre-run containers: {before}")
            header = dict(index=index, repeat=rep, phase=phase, arm=label, requested_config=config,
                          operations=n, utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          container_inventory=before, binary=args.binary, binary_sha256=binary_hash, implementation=hashes)
            raw.write("RUN "+json.dumps(header)+"\n")
            print(f"{phase} {index+1}/{len(cases)} rep={rep+1} {config['name']} {label}", flush=True)
            env["FALCON_DOMAIN_CAPTURE_CONFIG"] = json.dumps(config)
            start = time.monotonic()
            try:
                proc = subprocess.run([args.binary, "-test.run", "^$", "-test.bench", "^BenchmarkSnapshotPipeline$",
                                       "-test.benchtime", f"{n}x", "-test.count", "1", "-test.timeout", "10m", "-test.v"],
                                      env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=620)
            except subprocess.TimeoutExpired:
                for item in inventory():
                    if (item.get("Labels") or {}).get("falcon.benchmark") == "domain-capture":
                        subprocess.run([PODMAN, "rm", "-f", item["Id"]], check=True)
                raw.write("TIMEOUT: removed benchmark-owned containers\n")
                raise
            raw.write(proc.stdout)
            after = inventory()
            raw.write("END "+json.dumps(dict(index=index, elapsed=time.monotonic()-start,
                                           exit=proc.returncode, container_inventory=after))+"\n")
            if after:
                for item in after:
                    if (item.get("Labels") or {}).get("falcon.benchmark") == "domain-capture":
                        subprocess.run([PODMAN, "rm", "-f", item["Id"]], check=True)
                raise RuntimeError(f"Unexpected post-run containers; removed benchmark-owned instances: {after}")
            if proc.returncode:
                print(proc.stdout, flush=True)
                raise RuntimeError(f"Sample {index} failed")
            found = []
            for line in proc.stdout.splitlines():
                if "DOMAIN_SAMPLE " in line:
                    r = json.loads(line.split("DOMAIN_SAMPLE ", 1)[1])
                    if r["operations"] == n:
                        r.update({k: v for k, v in header.items() if k != "operations"})
                        found.append(r)
            if len(found) != 1:
                raise RuntimeError(f"Expected one measured sample, got {len(found)}")
            r = found[0]
            samples.write(json.dumps(r)+"\n")
            p = r["snapshot"]["pipeline"]
            print(f"  {r['WriteTPS']:.0f} writes/s; fresh p99 {r['Freshness']['p99_ms']:.2f} ms; "
                  f"drain {r['DrainSeconds']:.3f}s; fetch {p['PayloadMiBPerFetchSecond']:.1f} MiB/s; "
                  f"peak payload {r['snapshot']['PeakBufferedPayload']/1048576:.1f} MiB; containers=0", flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["smoke", "all"]+PHASES)
    parser.add_argument("--binary", default="/tmp/falcon-snapshot-pipeline.test")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.phase == "all":
        if args.start or args.limit is not None:
            parser.error("--start and --limit require one phase")
        for phase in PHASES:
            run(phase, args)
    else:
        run(args.phase, args)

if __name__ == "__main__":
    main()
