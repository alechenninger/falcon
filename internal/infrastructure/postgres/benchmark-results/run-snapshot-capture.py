#!/usr/bin/env python3
"""Sequential single-request capture experiments with mandatory empty container inventories."""
import argparse, datetime, hashlib, json, os, pathlib, random, subprocess, time
HERE=pathlib.Path(__file__).resolve().parent
PODMAN=os.environ.get("PODMAN","/opt/homebrew/bin/podman")
SOURCES=["domain_capture_bench_test.go", "snapshot_fetch_bench_test.go", "snapshot_pipeline_bench_test.go",
 "snapshot_pipeline_protocol_test.go", "snapshot_fetch_optimization_bench_test.go", "replication_bench_test.go",
 "snapshot_partition_bench_test.go", "snapshot_capture_bench_test.go", "snapshot_capture_protocol_test.go",
 "snapshot_capture_plan_test.go", "snapshot_fetch_protocol_test.go"]
ARMS=[("multi",dict(snapshot_capture="multi")),("single",dict(snapshot_capture="single")),
 ("wal",dict(mode="wal",snapshot_fetch="",snapshot_wake="",snapshot_probe="",fetch_plan=""))]
JOURNALS=ARMS[:2]
PHASES=["capacity","latency","spacing","history","maintenance","faults"]
def inventory():
 return json.loads(subprocess.check_output([PODMAN,"ps","-a","--format","json"]))
def matrix(phase):
 cases=[]
 def add(name,n,repeats=3,arms=None,**kw):
  for rep in range(repeats):
   for label,settings in (arms or ARMS):
    c=dict(name=name,mode="snapshot-routed",tenants=64,roots_per_tenant=16,rows=1,payload=256,
     keys=4096,workers=4,skew="pareto-roots",snapshot_fetch="pipeline-stream",snapshot_wake="notify",
     snapshot_probe="indexed",notify_batch_ms=2,fallback_ms=100,apply_batch_rows=1,
     queue_bytes=32*1048576,fetch_plan="guarded")
    c.update(kw);c.update(settings);cases.append((label,c,n,rep))
 if phase=="smoke":
  add("smoke-small",3000,repeats=1,rate=1000)
  add("smoke-large",2000,repeats=1,rate=1000,rows=16,payload=1024)
  add("smoke-history",2000,repeats=1,arms=JOURNALS,history=20000,history_tx=128,rate=1000,plan_cache="force_generic_plan")
  add("smoke-maintenance",3000,repeats=1,arms=JOURNALS,partition_span=256,partition_ahead=4,partition_gc=True,rate=1000)
 elif phase=="capacity":
  add("capacity-small",100000)
  add("capacity-large",50000,rows=16,payload=1024)
 elif phase=="latency":
  for rate in [50,1000,3000]: add(f"healthy-{rate}",rate*20,rate=rate)
  add("healthy-large-1000",20000,rate=1000,rows=16,payload=1024)
 elif phase=="spacing":
  add("immediate-1000",20000,arms=JOURNALS,rate=1000,notify_batch_ms=0)
  add("immediate-capacity",100000,arms=JOURNALS,notify_batch_ms=0)
 elif phase=="history":
  add("history-auto",20000,arms=JOURNALS,rate=1000,history=2000000,history_tx=2048)
  add("history-generic",10000,arms=JOURNALS,rate=1000,history=2000000,history_tx=2048,plan_cache="force_generic_plan")
 elif phase=="maintenance":
  add("rotation-1000",40000,arms=JOURNALS,rate=1000,partition_span=8192,partition_ahead=4,partition_gc=True)
 elif phase=="faults":
  for fault in ["rollback","after"]: add(f"fault-{fault}",20000,arms=JOURNALS,rate=1000,fault=fault)
 else:raise ValueError(phase)
 random.Random(17529+sum(map(ord,phase))).shuffle(cases)
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
    with (HERE/f"snapshot-capture-{phase}.txt").open("a", buffering=1) as raw:
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
 ap=argparse.ArgumentParser();ap.add_argument("phase",choices=PHASES+["all","smoke","validate","diagnostics","suite"])
 ap.add_argument("--binary",default="/tmp/falcon-snapshot-capture.test");ap.add_argument("--start",type=int,default=0);ap.add_argument("--limit",type=int)
 args=ap.parse_args()
 if args.phase=="suite":
  invoke(args,"suite",["test","-count=1","./..."],program="go");return
 if args.phase in ["validate","diagnostics"]:
  pattern="^TestSnapshot(SingleCapture|FetchMissingWake|Pipeline|Partition)" if args.phase=="validate" else "^TestSnapshotCapturePlans$"
  invoke(args,args.phase,["-test.run",pattern,"-test.v","-test.timeout","18m"],
   {"FALCON_SNAPSHOT_CAPTURE_DIAGNOSTICS":"1"} if args.phase=="diagnostics" else {});return
 if args.phase=="all" and (args.start or args.limit is not None):ap.error("--start/--limit require one phase")
 for phase in (PHASES if args.phase=="all" else [args.phase]):
  cases=matrix(phase)
  if args.limit is not None:cases=cases[:args.limit]
  for index,(arm,c,n,rep) in enumerate(cases):
   if index<args.start:continue
   print(f"{phase} {index+1}/{len(cases)} {c['name']} {arm} rep={rep+1}",flush=True)
   output,meta=invoke(args,phase,["-test.run","^$","-test.bench","^BenchmarkSnapshotCapture$","-test.benchtime",f"{n}x","-test.count","1","-test.timeout","10m","-test.v"],
    {"FALCON_DOMAIN_CAPTURE_CONFIG":json.dumps(c)},dict(index=index,repeat=rep,arm=arm,operations=n,requested_config=c))
   rows=[json.loads(l.split("DOMAIN_SAMPLE ",1)[1]) for l in output.splitlines() if "DOMAIN_SAMPLE " in l]
   rows=[r for r in rows if r["operations"]==n]
   if len(rows)!=1:raise RuntimeError(f"Expected one measured sample, got {len(rows)}")
   r=rows[0];r.update({k:v for k,v in meta.items() if k!="operations"})
   with (HERE/f"snapshot-capture-{phase}.jsonl").open("a",buffering=1) as f:f.write(json.dumps(r)+"\n")
   part=r.get("partitions") or {}
   print(f"  {r['WriteTPS']:.0f} writes/s; fresh p99={r['Freshness']['p99_ms']:.2f}ms; drain={r['DrainSeconds']:.3f}s; leaves={part.get('FinalLeaves',0)}; dropped={part.get('Dropped',0)}",flush=True)
if __name__=="__main__":main()
