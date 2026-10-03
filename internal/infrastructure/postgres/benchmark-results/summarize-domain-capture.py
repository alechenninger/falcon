#!/usr/bin/env python3
"""Normalize samples, excluding calibration and superseded counter fetch plans."""
import collections, csv, json, pathlib, statistics
HERE=pathlib.Path(__file__).resolve().parent
PHASES=['capacity','repairs','latency','faults','controls','history']
rows=[]
for phase in PHASES:
    path=HERE/f'domain-capture-{phase}.jsonl'
    if path.exists(): rows.extend(json.loads(line) for line in path.read_text().splitlines())
accepted=[];legacy=[]
for r in rows:
    if r['config']['mode'].startswith('counter-') and r['config'].get('counter_fetch')!='bounded':
        legacy.append(r);continue
    accepted.append(r)
columns=['phase','scenario','mode','repeat','tenants','roots_per_tenant','roots','rows','payload','keys','rate','skew','fault','history','workers','operations','write_tps','apply_tps','write_seconds','drain_seconds','queries','cuts','root_tasks','max_queued','apply_before_ack','root_top20_share','root_top1_share','wal_bytes','journal_bytes','source_bytes']
for metric in ['write','append_commit','freshness','arrival']:
    columns += [metric+'_'+key for key in ['count','p50_ms','p95_ms','p99_ms','p999_ms','max_ms']]
for cohort in ['affected-root','sibling-roots','other-tenants','injected']:
    for metric in ['freshness','write','arrival']:
        columns += [cohort+'_'+metric+'_'+key for key in ['count','p99_ms','p999_ms','max_ms']]
with (HERE/'domain-capture-samples.csv').open('w') as file:
    writer=csv.DictWriter(file,fieldnames=columns);writer.writeheader()
    for r in accepted:
        c=r['config'];out={key:c.get(key,'') for key in ['tenants','roots_per_tenant','rows','payload','keys','rate','skew','fault','history','workers']}
        out.update(phase=r['phase'],scenario=c['name'],mode=c['mode'],repeat=r['repeat']+1,roots=c['tenants']*c['roots_per_tenant'],operations=r['operations'])
        for key,source in {'write_tps':'WriteTPS','apply_tps':'ApplyTPS','write_seconds':'WriteSeconds','drain_seconds':'DrainSeconds','queries':'Queries','cuts':'Cuts','root_tasks':'RootTasks','max_queued':'MaxQueued','apply_before_ack':'ApplyBeforeAck','root_top20_share':'RootTop20Share','root_top1_share':'RootTop1Share','wal_bytes':'WALBytes','journal_bytes':'JournalBytes','source_bytes':'SourceBytes'}.items():out[key]=r[source]
        for metric,source in {'write':'Write','append_commit':'AppendCommit','freshness':'Freshness','arrival':'Arrival'}.items():
            for key,value in r[source].items():out[metric+'_'+key]=value if key=='count' or r[source]['count'] else ''
        for cohort in ['affected-root','sibling-roots','other-tenants','injected']:
            for metric,source in {'freshness':'Cohorts','write':'CohortWrites','arrival':'CohortArrivals'}.items():
                for key in ['count','p99_ms','p999_ms','max_ms']:
                    out[cohort+'_'+metric+'_'+key]=r.get(source,{}).get(cohort,{}).get(key,'')
        writer.writerow(out)
groups=collections.defaultdict(list)
for r in accepted:groups[(r['config']['name'],r['config']['mode'])].append(r)
with (HERE/'domain-capture-summary.csv').open('w') as file:
    names=['scenario','mode','samples','write_tps','write_tps_min','write_tps_max','fresh_p99_ms','fresh_p99_min','fresh_p99_max','arrival_p99_ms','write_p99_ms','drain_max_s','queries_per_op']
    writer=csv.DictWriter(file,fieldnames=names);writer.writeheader()
    for (name,mode),rs in sorted(groups.items()):
        median=lambda fn:statistics.median(fn(r) for r in rs)
        out=dict(scenario=name,mode=mode,samples=len(rs),write_tps=median(lambda r:r['WriteTPS']),write_tps_min=min(r['WriteTPS'] for r in rs),write_tps_max=max(r['WriteTPS'] for r in rs),fresh_p99_ms=median(lambda r:r['Freshness']['p99_ms']),fresh_p99_min=min(r['Freshness']['p99_ms'] for r in rs),fresh_p99_max=max(r['Freshness']['p99_ms'] for r in rs),arrival_p99_ms=median(lambda r:r['Arrival']['p99_ms']) if rs[0]['Arrival']['count'] else '',write_p99_ms=median(lambda r:r['Write']['p99_ms']),drain_max_s=max(r['DrainSeconds'] for r in rs),queries_per_op=median(lambda r:r['Queries']/r['operations']))
        writer.writerow(out)
print(f'{len(accepted)} accepted samples, {sum(r["operations"] for r in accepted):,} operations, {len(legacy)} superseded counter samples')
for (name,mode),rs in sorted(groups.items()):
    if name=='multi-pareto': print(name,mode,len(rs),round(statistics.median(r['WriteTPS'] for r in rs)),round(statistics.median(r['Freshness']['p99_ms'] for r in rs),2))
