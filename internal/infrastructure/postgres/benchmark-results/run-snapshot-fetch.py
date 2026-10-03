#!/usr/bin/env python3
"""Sequential fresh-container 2x2 snapshot fetch/wakeup experiments."""
import argparse, datetime, hashlib, json, os, pathlib, random, subprocess, time

HERE = pathlib.Path(__file__).resolve().parent
PODMAN = os.environ.get("PODMAN", "/opt/homebrew/bin/podman")
VARIANTS = [(f, w) for f in ["global", "fair"] for w in ["poll", "notify"]]
PHASES = ["idle", "capacity", "latency", "dominance", "history", "notify-control"]

def inventory():
    return json.loads(subprocess.check_output([PODMAN, "ps", "-a", "--format", "json"]))

def matrix(phase):
    cases = []
    def add(name, n, repeats=3, variants=VARIANTS, wal=False, **kw):
        for rep in range(repeats):
            for fetch, wake in variants:
                config = dict(name=name, mode="snapshot-routed", tenants=64, roots_per_tenant=16,
                              rows=1, payload=256, keys=4096, skew="pareto",
                              snapshot_fetch=fetch, snapshot_wake=wake, notify_batch_ms=2,
                              fallback_ms=100, fetch_batch_roots=64, fetch_page_rows=128)
                config.update(kw)
                cases.append((config, n, rep))
            if wal:
                config = dict(name=name, mode="wal", tenants=64, roots_per_tenant=16,
                              rows=1, payload=256, keys=4096, skew="pareto")
                config.update(kw)
                cases.append((config, n, rep))
    if phase == "smoke":
        add("smoke-fetch", 2000, 1, rate=1000, skew="hot-root", hot_share=.99, fault="fetch")
        add("smoke-delivery", 2000, 1, rate=1000, skew="hot-root", hot_share=.99, fault="delivery")
    elif phase == "idle":
        add("idle", 5)
    elif phase == "capacity":
        add("capacity-uniform", 100000, wal=True, skew="uniform")
        add("capacity-pareto", 100000, wal=True)
        add("capacity-large", 50000, wal=True, rows=16, payload=1024)
    elif phase == "latency":
        for rate in [50, 1000, 3000]:
            add(f"healthy-{rate}", rate*20, rate=rate, wal=rate==1000)
    elif phase == "dominance":
        add("hot80-small", 60000, rate=3000, skew="hot-root", hot_share=.8)
        add("hot99-small", 60000, rate=3000, skew="hot-root", hot_share=.99)
        add("hot99-large", 60000, rate=2000, skew="hot-root", hot_share=.99, rows=16, payload=1024)
        add("hot99-fetch-stall", 60000, rate=3000, skew="hot-root", hot_share=.99, fault="fetch")
    elif phase == "history":
        add("history-2m", 20000, rate=1000, history=2000000)
    elif phase == "notify-control":
        add("capacity-notify0", 100000, variants=[(f, "notify") for f in ["global", "fair"]], notify_batch_ms=0)
    elif phase == "probe-fix":
        # A separate follow-up preserves the original 162-sample matrix.
        for name, n, kw in [("history-2m", 20000, dict(rate=1000, history=2000000)),
                            ("healthy-1000", 20000, dict(rate=1000)),
                            ("capacity-pareto", 100000, {})]:
            add(name, n, variants=[(f, "notify") for f in ["global", "fair"]], snapshot_probe="indexed", **kw)
    else:
        raise ValueError(phase)
    random.Random(2901+sum(map(ord, phase))).shuffle(cases)
    return cases

def run(phase, args):
    cases = matrix(phase)
    if args.limit is not None:
        cases = cases[:args.limit]
    env = dict(os.environ)
    env.setdefault("DOCKER_HOST", "unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock")
    if env.get("FALCON_REPLICATION_BENCH_DATABASE_URL"):
        raise RuntimeError("Fresh containers are required for this matrix.")
    prefix = HERE / ("snapshot-fetch-"+phase)
    implementation = {name: hashlib.sha256((HERE.parent/name).read_bytes()).hexdigest()
                      for name in ["domain_capture_bench_test.go", "snapshot_fetch_bench_test.go", "snapshot_fetch_protocol_test.go"]}
    with open(str(prefix)+".txt", "a", buffering=1) as raw, open(str(prefix)+".jsonl", "a", buffering=1) as samples:
        for index, (config, n, rep) in enumerate(cases):
            if index < args.start:
                continue
            before = inventory()
            if before:
                raise RuntimeError(f"Nonempty pre-run inventory: {before}")
            header = dict(index=index, repeat=rep, phase=phase, config=config, operations=n,
                          utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), container_inventory=before,
                          binary=args.binary, implementation=implementation)
            raw.write("RUN "+json.dumps(header)+"\n")
            variant = config.get("snapshot_fetch", "wal")+"-"+config.get("snapshot_wake", "")
            print(f"{phase} {index+1}/{len(cases)} rep={rep+1} {config['name']} {variant}", flush=True)
            env["FALCON_DOMAIN_CAPTURE_CONFIG"] = json.dumps(config)
            bench = "BenchmarkSnapshotFetchIdle" if phase=="idle" else "BenchmarkDomainCapture"
            start = time.monotonic()
            try:
                process = subprocess.run([args.binary, "-test.run", "^$", "-test.bench", "^"+bench+"$",
                                          "-test.benchtime", f"{n}x", "-test.count", "1", "-test.timeout", "10m", "-test.v"],
                                         env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=620)
            except subprocess.TimeoutExpired:
                for c in inventory():
                    if (c.get("Labels") or {}).get("falcon.benchmark")=="domain-capture":
                        subprocess.run([PODMAN, "rm", "-f", c["Id"]], check=True)
                raw.write("TIMEOUT: removed benchmark-owned containers\n")
                raise
            raw.write(process.stdout)
            after = inventory()
            raw.write("END "+json.dumps(dict(index=index, elapsed=time.monotonic()-start,
                                           exit=process.returncode, container_inventory=after))+"\n")
            if after:
                for c in after:
                    if (c.get("Labels") or {}).get("falcon.benchmark")=="domain-capture":
                        subprocess.run([PODMAN, "rm", "-f", c["Id"]], check=True)
                raise RuntimeError(f"Container leaked after run; removed owned containers: {after}")
            if process.returncode:
                print(process.stdout, flush=True)
                raise RuntimeError(f"Sample {index} failed")
            found = []
            for line in process.stdout.splitlines():
                if "DOMAIN_SAMPLE " in line:
                    r = json.loads(line.split("DOMAIN_SAMPLE ", 1)[1])
                    if r["operations"] != n:
                        continue # Go calibration
                    r.update({k:v for k,v in header.items() if k!="config"})
                    r["requested_config"] = config
                    found.append(r)
            if len(found)!=1:
                raise RuntimeError(f"Expected one measured sample: {len(found)}")
            r=found[0];samples.write(json.dumps(r)+"\n")
            extra=r.get("snapshot") or {}
            if phase=="idle":
                print(f"  {r['Queries']/r['WriteSeconds']:.1f} idle queries/s; {r['Cuts']} cuts; containers=0", flush=True)
            else:
                cold=r.get("Cohorts",{}).get("other-tenants",{})
                print(f"  {r['WriteTPS']:.0f} writes/s; fresh p99 {r['Freshness']['p99_ms']:.2f} ms; "
                      f"other-tenant p99 {cold.get('p99_ms',0):.2f}; "
                      f"payload peak {extra.get('PeakBufferedPayload',0)/1048576:.2f} MiB; containers=0", flush=True)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("phase", choices=["smoke", "all", "probe-fix"]+PHASES)
    parser.add_argument("--binary", default="/tmp/falcon-snapshot-fetch.test")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int)
    args=parser.parse_args()
    if args.phase=="all":
        if args.start or args.limit is not None:
            parser.error("--start and --limit require one phase")
        for phase in PHASES:
            run(phase,args)
    else:
        run(args.phase,args)

if __name__=="__main__":
    main()
