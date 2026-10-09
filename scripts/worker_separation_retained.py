"""ADR0202 exact inactive historical inventory; no deletion or remote action."""
import hashlib
import inspect
import json
import re

from two_host_topology import NORMAL_COUNTS
from worker_separation_topology import WORKERS

MANAGED=set(NORMAL_COUNTS)|set(WORKERS)
HASH=re.compile(r'[0-9a-f]{64}$')


def row_digest(row):
    value=dict(row)
    if 'Mounts' in value:
        value['Mounts']=sorted(value['Mounts'],key=lambda m:json.dumps(m,sort_keys=True,separators=(',',':'),allow_nan=False))
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def eligible(row):
    labels=row['Config'].get('Labels') or {}
    state=row['State']
    return (state.get('Running') is False and state.get('Status')=='exited'
            and state.get('Paused',False) is False and state.get('Restarting',False) is False
            and (labels.get('com.docker.compose.project')=='flash-cloud-bench'
                 or (labels.get('com.docker.compose.project')=='flash-ticketing'
                     and labels.get('com.docker.compose.service') not in MANAGED)))


def validate_rows(rows):
    if (not isinstance(rows,list) or len(rows)>64
            or any(not isinstance(r,dict) or not HASH.fullmatch(r.get('Id','')) for r in rows)
            or len({r['Id'] for r in rows})!=len(rows)):
        raise ValueError('Complete bounded distinct container inventory required')


def validate_receipt(expected):
    if (not isinstance(expected,dict) or len(expected)>64
            or any(not isinstance(k,str) or not HASH.fullmatch(k)
                   or not isinstance(v,str) or not HASH.fullmatch(v) for k,v in expected.items())):
        raise ValueError('Exact bounded retained inspection receipt required')


def capture_retained(rows):
    validate_rows(rows)
    retained={r['Id']:row_digest(r) for r in rows if eligible(r)}
    selected=verify_retained(rows,retained)
    # Capture must not whitelist unowned or unknown inactive resources.
    if any((r['Config'].get('Labels') or {}).get('com.docker.compose.project')!='flash-ticketing'
           or (r['Config'].get('Labels') or {}).get('com.docker.compose.service') not in MANAGED
           for r in selected):
        raise ValueError('Unknown resource blocks original runtime capture')
    return selected,retained


def verify_retained(rows,expected):
    validate_rows(rows);validate_receipt(expected)
    actual={r['Id']:r for r in rows}
    if any(k not in actual or not eligible(actual[k]) or row_digest(actual[k])!=v for k,v in expected.items()):
        raise ValueError('Retained historical container missing, changed or activated')
    selected=[r for r in rows if r['Id'] not in expected]
    if any(eligible(r) for r in selected):
        raise ValueError('New historical container cannot be adopted')
    return selected


def program_prefix(expected):
    validate_receipt(expected)
    helpers='\n'.join(inspect.getsource(f) for f in (row_digest,eligible,validate_rows,validate_receipt,verify_retained))
    return ('import hashlib,json,re\nMANAGED='+repr(MANAGED)+"\nHASH=re.compile(r'[0-9a-f]{64}$')\n"
            +helpers+'\nretained_expected='+repr(expected)+'\n')
