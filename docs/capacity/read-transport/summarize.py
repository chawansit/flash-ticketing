"""Summarize diagnostic load outcomes and namespace-wide TCP counter deltas."""
import json
from collections import Counter
from itertools import pairwise
from pathlib import Path

ROOT=Path(__file__).parent

def read(path):
    return json.loads(path.read_text(encoding='utf-8'))

def counters(path):
    result={}
    for source,text in read(path).items():
        if source.endswith('sockstat'):
            continue
        lines=text.splitlines()
        for i in range(0,len(lines)-1,2):
            names,values=lines[i].split(),lines[i+1].split()
            result.update({names[0]+k:int(v) for k,v in zip(names[1:],values[1:],strict=True)})
    return result

runs={}
for directory in ('transport-uniform','transport-uniform-repeat'):
    workers=[read(p) for p in (ROOT/'generator'/directory).glob('worker-*.json')]
    assert len(workers)==4 and all(w['transport_diagnostics_enabled'] for w in workers)
    counts={op:dict(sum((Counter(w['statuses'][op]) for w in workers),Counter())) for op in ('read','hold')}
    errors=dict(sum((Counter(w['transport_error_types']) for w in workers),Counter()))
    examples=[{'run_id':w['run_id'],**e} for w in workers for e in w['transport_failure_examples']]
    assert len(examples)<=sum(errors.values())
    assert sum(sum(v.values()) for v in counts.values())+sum(w['generator_drops'] for w in workers)==120000
    runs[directory]={'summary':read(ROOT/'generator'/directory/'summary.json'),'statuses':counts,'transport_errors':errors,'failure_examples':examples}
selected=('Tcp:EstabResets','Tcp:OutRsts','Tcp:RetransSegs','TcpExt:ListenDrops','TcpExt:TCPAbortOnClose','TcpExt:TCPTimeouts')
network={}
for host in ('backend','generator'):
    snapshots=[counters(ROOT/host/name) for name in ('transport-net-before.json','transport-net-after-0.json','transport-net-after-1.json')]
    network[host]=[{k:b[k]-a[k] for k in selected} for a,b in pairwise(snapshots)]
result={'runs':runs,'network_deltas':network,'note':'API-container and generator-host network namespace totals include non-measured traffic. Reset counters are not failed-request counts and do not establish a cause. Trace overhead can change timing. No retries or pooling-policy changes.'}
(ROOT/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:v['transport_errors'] for k,v in runs.items()}))
