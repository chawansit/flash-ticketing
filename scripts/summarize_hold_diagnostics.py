"""Summarize structured hold logs without retaining payloads or unbounded examples."""
import argparse
import heapq
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

PHASES = {'dispatch', 'rate_limit', 'redis_enter', 'db_enter', 'database_body', 'db_exit', 'redis_exit'}


def summarize(lines):
    codes, statuses, phases = Counter(), Counter(), defaultdict(list)
    rejected, slowest = [], []
    count = 0
    request_errors = Counter()
    read_errors = []
    for line in lines:
        try:
            record = json.loads(line.split('|', 1)[-1])
        except ValueError:
            continue
        if not isinstance(record, dict) or record.get('message') != 'request':
            continue
        if record.get('error_code'):
            request_errors[str(record['error_code'])] += 1
        if record.get('route') == '/v1/events/{event_id}/availability' and record.get('error_code') and len(read_errors) < 20:
            read_errors.append({k: record.get(k) for k in ('time','request_id','status','error_code','duration_ms')})
        if record.get('route') != '/v1/holds':
            continue
        count += 1
        status = str(record.get('status'))
        statuses[status] += 1
        code = record.get('error_code')
        if isinstance(code, str):
            codes[code] += 1
        timing = record.get('hold_phase_ms', {})
        if not isinstance(timing, dict):
            timing = {}
        timing = {k: v for k, v in timing.items() if k in PHASES and isinstance(v, (int, float))}
        if status == '201':
            for phase, duration in timing.items():
                phases[phase].append(duration)
        safe = {k: record.get(k) for k in (
            'time', 'request_id', 'status', 'duration_ms', 'error_code',
            'hold_arrival_occupancy', 'hold_limit')}
        safe['hold_phase_ms'] = timing
        if code and len(rejected) < 100:
            rejected.append(safe)
        duration = record.get('duration_ms')
        if status == '201' and isinstance(duration, (int, float)):
            heapq.heappush(slowest, (duration, count, safe))
            if len(slowest) > 20:
                heapq.heappop(slowest)
    summaries = {}
    for phase, values in phases.items():
        values.sort()
        summaries[phase] = {'count': len(values), 'mean_ms': sum(values)/len(values),
                            'p95_ms': values[math.ceil(len(values)*.95)-1],
                            'p99_ms': values[math.ceil(len(values)*.99)-1], 'max_ms': values[-1]}
    return {'request_error_codes': dict(request_errors), 'first_20_read_errors': read_errors,
            'hold_requests': count, 'statuses': dict(statuses), 'error_codes': dict(codes),
            'successful_hold_phases': summaries, 'first_100_errors': rejected,
            'slowest_20_successful_holds': [x[2] for x in sorted(slowest, reverse=True)],
            'note': 'Server log durations rounded to milliseconds. Phase percentiles are separate distributions; do not add them. db_exit includes commit/rollback and pool return.'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--logs', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    with a.logs.open(encoding='utf-8-sig') as stream:
        result = summarize(stream)
    a.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({k: result[k] for k in ['hold_requests','statuses','error_codes']}))
