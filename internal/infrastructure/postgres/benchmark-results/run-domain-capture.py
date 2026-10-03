#!/usr/bin/env python3
"""Fresh-container, sequential, seeded and interleaved domain capture experiments."""
import argparse, datetime, json, os, pathlib, random, subprocess, sys, time

PODMAN = os.environ.get('PODMAN', '/opt/homebrew/bin/podman')
HERE = pathlib.Path(__file__).resolve().parent
MODES = ['snapshot-coupled', 'snapshot-roots', 'snapshot-routed', 'counter-global', 'counter-tenant', 'counter-root', 'wal']

def inventory():
    return json.loads(subprocess.check_output([PODMAN, 'ps', '-a', '--format', 'json']))

def matrix(phase):
    cases = []
    def add(name, n, repeats, modes=MODES, **kw):
        for rep in range(repeats):
            for mode in modes:
                cases.append((dict(name=name, mode=mode, tenants=64, roots_per_tenant=16,
                                   rows=1, payload=256, keys=4096, skew='uniform', **kw), n, rep))
    if phase == 'smoke':
        add('smoke', 2000, 1, rate=1000, fault='rollback')
    elif phase == 'capacity':
        # Minimal controls and cardinality sweep, then identical cardinality with
        # different tenant layout and demand distributions.
        for roots in [1, 64, 1024]:
            for rep in range(3):
                for mode in [m for m in MODES if m!='counter-tenant']:
                    cases.append((dict(name=f'one-tenant-{roots}',mode=mode,tenants=1,roots_per_tenant=roots),100000,rep))
        for skew in ['uniform','pareto','hot-root']:
            for rep in range(3):
                for mode in MODES:
                    cases.append((dict(name=f'multi-{skew}',mode=mode,tenants=64,roots_per_tenant=16,skew=skew),100000,rep))
        for rep in range(3):
            for mode in MODES:
                cases.append((dict(name='batch16-pareto',mode=mode,tenants=64,roots_per_tenant=16,skew='pareto',rows=16,payload=1024),50000,rep))
    elif phase == 'latency':
        for rate in [1000,3000]:
            for rep in range(3):
                for mode in MODES:
                    cases.append((dict(name=f'pareto-at-{rate}',mode=mode,tenants=64,roots_per_tenant=16,skew='pareto',rate=rate),rate*20,rep))
    elif phase == 'faults':
        for fault in ['before','after','rollback','delivery']:
            for rep in range(3):
                for mode in MODES:
                    cases.append((dict(name=f'pareto-{fault}',mode=mode,tenants=64,roots_per_tenant=16,skew='pareto',rate=1000,fault=fault),20000,rep))
    elif phase == 'repairs':
        for line in (HERE/'domain-capture-capacity.jsonl').read_text().splitlines():
            r=json.loads(line);c=r['config']
            if c['mode'].startswith('counter-') and not c.get('counter_fetch'):
                cases.append((c,r['operations'],r['repeat']))
    elif phase == 'controls':
        for rep in range(3):
            for mode in ['snapshot-routed','counter-global','counter-tenant','counter-root','wal']:
                cases.append((dict(name='hot-key',mode=mode,tenants=64,roots_per_tenant=16,skew='hot-root',keys=1),100000,rep))
            for mode in ['snapshot-roots','snapshot-routed']:
                cases.append((dict(name='one-worker-delivery',mode=mode,tenants=64,roots_per_tenant=16,skew='pareto',rate=1000,fault='delivery',workers=1),20000,rep))
    elif phase == 'history':
        for rep in range(3):
            for mode in MODES[:-1]:
                cases.append((dict(name='history-2m',mode=mode,tenants=64,roots_per_tenant=16,skew='pareto',history=2000000,rate=1000),20000,rep))
    else:
        raise ValueError(phase)
    random.Random(817 + sum(map(ord, phase))).shuffle(cases)
    return cases

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['smoke','capacity','latency','faults','history','repairs','controls'])
    parser.add_argument('--binary', default='/tmp/falcon-domain-bench.test')
    parser.add_argument('--limit',type=int)
    parser.add_argument('--start',type=int,default=0)
    args = parser.parse_args()
    cases=matrix(args.phase)
    if args.limit is not None: cases=cases[:args.limit]
    prefix=HERE / ('domain-capture-'+args.phase)
    raw=open(str(prefix)+'.txt','a',buffering=1)
    samples=open(str(prefix)+'.jsonl','a',buffering=1)
    env=dict(os.environ)
    env.setdefault('DOCKER_HOST','unix:///var/folders/0g/8qk101ks3xl4v_w5kqv47r_h0000gn/T/podman/podman-machine-default-api.sock')
    if env.get('FALCON_REPLICATION_BENCH_DATABASE_URL'):
        raise RuntimeError('The matrix runner requires fresh containers; run BenchmarkDomainCapture directly for an external database.')
    for index,(config,n,rep) in enumerate(cases):
        if index<args.start: continue
        before=inventory()
        if before: raise RuntimeError(f'Container inventory is not empty before sample {index}: {before}')
        env['FALCON_DOMAIN_CAPTURE_CONFIG']=json.dumps(config)
        header=dict(index=index,repeat=rep,phase=args.phase,config=config,operations=n,
                    utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),container_inventory=before)
        raw.write('RUN '+json.dumps(header)+'\n')
        print(f'{args.phase} {index+1}/{len(cases)} rep={rep+1} {config["name"]} {config["mode"]}',flush=True)
        start=time.monotonic()
        try:
            process=subprocess.run([args.binary,'-test.run','^$','-test.bench','^BenchmarkDomainCapture$',
                                '-test.benchtime',f'{n}x','-test.count','1','-test.timeout','10m','-test.v'],
                               env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=620)
        except subprocess.TimeoutExpired:
            for c in inventory():
                if (c.get('Labels') or {}).get('falcon.benchmark')=='domain-capture':
                    subprocess.run([PODMAN,'rm','-f',c['Id']],check=True)
            raw.write('TIMEOUT: removed benchmark-owned containers\n')
            raise
        raw.write(process.stdout)
        after=inventory()
        raw.write('END '+json.dumps(dict(index=index,elapsed=time.monotonic()-start,exit=process.returncode,container_inventory=after))+'\n')
        if after:
            # Only remove containers explicitly labelled for these benchmarks.
            for c in after:
                labels=c.get('Labels') or {}
                if labels.get('falcon.benchmark')=='domain-capture':
                    subprocess.run([PODMAN,'rm','-f',c['Id']],check=True)
            raise RuntimeError(f'Container leaked after sample {index}; cleaned owned containers: {after}')
        if process.returncode: print(process.stdout,flush=True);raise RuntimeError(f'Sample {index} failed')
        found=[]
        for line in process.stdout.splitlines():
            if 'DOMAIN_SAMPLE ' in line:
                result=json.loads(line.split('DOMAIN_SAMPLE ',1)[1])
                if result['operations']!=n: continue
                result.update({k:v for k,v in header.items() if k!='config'});result['requested_config']=config;found.append(result)
        if len(found)!=1: raise RuntimeError(f'Expected one measurement, got {len(found)}')
        samples.write(json.dumps(found[0])+'\n')
        r=found[0]
        print(f'  {r["WriteTPS"]:.0f} write/s; fresh p99 {r["Freshness"]["p99_ms"]:.2f} ms; '
              f'arrival p99 {r["Arrival"]["p99_ms"]:.2f} ms; drain {r["DrainSeconds"]:.3f}s; containers=0',flush=True)

if __name__=='__main__':main()
