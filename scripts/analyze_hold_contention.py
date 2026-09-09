"""Join retained hot-seat clients with server phases, preserving missing measurements."""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


def stats(values):
    values = sorted(values)
    return {'count': len(values), 'p50_ms': values[math.ceil(len(values)*.5)-1],
            'p95_ms': values[math.ceil(len(values)*.95)-1], 'max_ms': values[-1]} if values else {'count': 0}


def analyze(clients, logs):
    server = {}
    for row in logs:
        identifier = row['request_id']
        if identifier in server:
            raise ValueError('Duplicate server request ID')
        server[identifier] = row
    groups = defaultdict(list)
    seen = set()
    for client in clients:
        identifier = client['request_id']
        if identifier in seen:
            raise ValueError('Duplicate client request ID')
        seen.add(identifier)
        row = server[identifier]
        if str(row['status']) != client['status'] or row.get('error_code') != client.get('code'):
            raise ValueError('Client/server outcome mismatch')
        phases = row.get('hold_phase_ms', {})
        safe = {'request_id': identifier, 'status': client['status'], 'code': client.get('code'),
                'app_ms': row['duration_ms'], 'phases_ms': phases,
                'unattributed_app_ms': row['duration_ms']-sum(phases.values())}
        for field in ('total_ms', 'client_ms', 'lane_wait_ms', 'protocol_queue_ms', 'asgi_headers_ms'):
            safe[field] = client[field]
        groups[client.get('code') or 'SUCCESS'].append(safe)
    result = {}
    for code, rows in groups.items():
        names = sorted({k for r in rows for k in r['phases_ms']})
        result[code] = {
            'count': len(rows),
            'timing': {k: stats([r[k] for r in rows if r[k] is not None]) for k in
                       ('total_ms', 'client_ms', 'lane_wait_ms', 'protocol_queue_ms',
                        'asgi_headers_ms', 'app_ms', 'unattributed_app_ms')},
            'phases': {k: stats([r['phases_ms'][k] for r in rows if k in r['phases_ms']]) for k in names},
            'slowest_app_requests': sorted(rows, key=lambda r: r['app_ms'], reverse=True)[:5],
        }
    return {'matched_requests': len(seen), 'groups': result,
            'note': 'Missing phases are absent, not zero. dispatch combines dependency/body processing and scheduling before handler entry; authentication is not separately timed. database_body includes idempotency and seat checks. Residual is per-request app minus disjoint existing phases, not a direct scheduling metric. Percentiles are not additive.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clients', type=Path, nargs='+', required=True)
    parser.add_argument('--logs', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    clients = [r for path in args.clients for r in json.loads(path.read_text())['requests']]
    with args.logs.open(encoding='utf-8') as stream:
        logs = [json.loads(line) for line in stream]
    result = analyze(clients, logs)
    args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'matched_requests': result['matched_requests'], 'groups': {k:v['count'] for k,v in result['groups'].items()}}))
