"""Reproduce request accounting and diagnostic comparison for ADR 0022."""
import json
import math
from collections import Counter
from pathlib import Path

ROOT=Path(__file__).parent

def read(p):
    return json.loads(p.read_text(encoding='utf-8'))

def stats(values):
    values=sorted(values)
    return {'count':len(values),'p95_ms':values[math.ceil(len(values)*.95)-1],'max_ms':values[-1]} if values else {'count':0}

result={}
for version in ('baseline','candidate'):
    clients=[r for f in sorted(ROOT.glob(f'generator/service-{version}-hot-*/worker-*.json')) for r in read(f)['requests']]
    ids={r['request_id'] for r in clients}
    assert len(ids)==len(clients)==2000
    records=[json.loads(l) for l in (ROOT/f'backend/service-{version}-records.jsonl').read_text().splitlines()]
    records=[r for r in records if r.get('request_id') in ids]
    app={r['request_id']:r for r in records if r['message']=='request'}
    assert len(app)==2000
    for c in clients:
        assert str(app[c['request_id']]['status'])==c['status']
        assert app[c['request_id']].get('error_code')==c.get('code')
    admitted={r['request_id'] for r in clients if r['code']!='ADMISSION_FULL'}
    stages={}
    expected=('authentication','hold_handler') if version=='candidate' else ('authentication','service_dependency','hold_handler')
    probes=[r for r in records if r['message']=='dispatch_probe']
    assert {r['stage'] for r in probes}==set(expected)
    for stage in expected:
        rows=[r for r in probes if r['stage']==stage]
        assert Counter(r['request_id'] for r in rows)==Counter({i:1 for i in admitted})
        stages[stage]={phase:stats([r['timing_ms'][phase] for r in rows]) for phase in ('submit_to_entry','execution','finish_to_resume')}
    uniform=read(ROOT/f'generator/service-{version}-uniform/summary.json')
    workers=[read(f) for f in ROOT.glob(f'generator/service-{version}-uniform/worker-*.json')]
    statuses={kind:dict(sum((Counter(w['statuses'][kind]) for w in workers),Counter())) for kind in ('hold','read')}
    conflicts=[r for r in app.values() if r['status']==409]
    result[version]={'conflict_app_ms':stats([r['duration_ms'] for r in conflicts]),'conflict_dispatch_ms':stats([r['hold_phase_ms']['dispatch'] for r in conflicts]),'transport_error_types':dict(sum((Counter(w.get('transport_error_types',{})) for w in workers),Counter())),'statuses':dict(Counter(c['status'] for c in clients)),'admitted_requests':len(admitted),'stages':stages,'uniform':uniform,'uniform_statuses':statuses,'waves':[{'directory':f.parent.name,'failed_total_ms':read(f)['failed_total_ms'],'statuses':read(f)['statuses'],'measurement_valid':read(f)['measurement_valid']} for f in sorted(ROOT.glob(f'generator/service-{version}-hot-*/summary.json'))]}
(ROOT/'comparison.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:{'statuses':v['statuses'],'uniform_gate':v['uniform']['gate_pass']} for k,v in result.items()}))
