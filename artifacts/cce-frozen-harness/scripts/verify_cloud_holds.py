"""Read-only, post-load verification of acknowledged holds and expiry."""

import argparse
import json
import os
import time
from pathlib import Path

import psycopg
import redis
from redis.exceptions import ResponseError

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--results', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
runs = {}
for path in a.results.rglob('*.json'):
    result = json.loads(path.read_text())
    if isinstance(result, dict) and 'run_id' in result:
        statuses = result['statuses']
        acknowledged_201 = sum(statuses.get(operation, {}).get('201', 0)
                               for operation in ('hold', 'hot_hold'))
        acknowledged_202 = sum(statuses.get(operation, {}).get('202', 0)
                               for operation in ('hold', 'hot_hold'))
        runs[result['run_id']] = {
            '201': acknowledged_201,
            '202': acknowledged_202,
            'total': acknowledged_201 + acknowledged_202,
            # Redis-first HTTP 201 can be a same-key replay after DURABLE.
            'expected_durable': (
                acknowledged_201 + acknowledged_202
                if result.get('reservation_mode') == 'redis-first'
                else acknowledged_202
            ),
        }
if not runs:
    p.error('No completed worker results found')
run_ids = sorted(runs)
if not run_ids[0] or len({len(run_id) for run_id in run_ids}) != 1:
    p.error('Load run identifiers must be nonempty and equal length')
run_width = len(run_ids[0])
prefixes = [run_id + '-' for run_id in run_ids]
checks = []
audit_started = time.perf_counter()
phase_seconds = {}
with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=False) as conn:
    conn.execute('SET TRANSACTION READ ONLY')
    conn.execute("SET LOCAL statement_timeout = '60s'")
    conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")

    phase_started = time.perf_counter()
    count_rows = conn.execute('''SELECT left(r.key,%s) AS run_id,
        count(*),count(DISTINCT h.id),count(DISTINCT o.id),
        count(*) FILTER (WHERE h.id IS NULL OR o.id IS NULL
            OR o.hold_id IS DISTINCT FROM h.id OR h.actor IS DISTINCT FROM r.actor
            OR o.actor IS DISTINCT FROM r.actor),
        count(*) FILTER (WHERE h.status='ACTIVE'),
        count(*) FILTER (WHERE h.status='ACTIVE' AND h.expires_at<clock_timestamp()),
        count(*) FILTER (WHERE o.status='PENDING')
        FROM idempotency_records r
        LEFT JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
        LEFT JOIN orders o ON o.id=(r.response->>'order_id')::uuid
        WHERE r.operation='hold' AND left(r.key,%s)=ANY(%s)
        GROUP BY 1''', (run_width, run_width + 1, prefixes)).fetchall()
    counted = {row[0]: row[1:] for row in count_rows}
    phase_seconds['idempotency_links'] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    durable_rows = conn.execute('''SELECT left(c.idempotency_key,%s) AS run_id,count(*)
        FROM reservation_commands c
        WHERE c.status='DURABLE' AND left(c.idempotency_key,%s)=ANY(%s)
        GROUP BY 1''', (run_width, run_width + 1, prefixes)).fetchall()
    durable = dict(durable_rows)
    phase_seconds['reservation_commands'] = time.perf_counter() - phase_started

    for run_id in run_ids:
        acknowledged = runs[run_id]
        row = (*counted.get(run_id, (0,) * 7), durable.get(run_id, 0))
        passed = (
            row[:3] == (acknowledged['total'],) * 3
            and all(n == 0 for n in row[3:7])
            and row[7] == acknowledged['expected_durable']
        )
        checks.append(dict(zip(
            ['run_id','acknowledged_201','acknowledged_202','acknowledged_total',
             'expected_durable_commands',
             'idempotency_records','distinct_holds','distinct_orders','broken_links',
             'active_holds','overdue_holds','pending_orders','durable_commands','pass'],
            [run_id, acknowledged['201'], acknowledged['202'], acknowledged['total'],
             acknowledged['expected_durable'], *row, passed], strict=True)))

    # This harness never explicitly releases holds: expiry is the only reuse boundary.
    phase_started = time.perf_counter()
    audited_holds, audited_intervals, overlaps = conn.execute('''WITH selected AS (
        SELECT DISTINCT h.id,h.event_id,h.expires_at,o.created_at,i.seat_id
        FROM idempotency_records r
        JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
        JOIN orders o ON o.id=(r.response->>'order_id')::uuid
        JOIN order_items i ON i.order_id=o.id
        WHERE r.operation='hold' AND left(r.key,%s)=ANY(%s)
    ), ordered AS (
        SELECT *,max(expires_at) OVER (
            PARTITION BY event_id,seat_id ORDER BY created_at,id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_expiry
        FROM selected
    ) SELECT count(DISTINCT id),count(*),count(*) FILTER (WHERE created_at<prior_expiry) FROM ordered''',
        (run_width + 1, prefixes)).fetchone()
    phase_seconds['overlap'] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    queues = conn.execute('''SELECT
        (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
        (SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),
        (SELECT count(*) FROM dead_letters)''').fetchone()
    phase_seconds['postgres_queues'] = time.perf_counter() - phase_started

redis_url = os.getenv('TEST_REDIS_URL') or os.getenv('REDIS_URL')
if not redis_url:
    p.error('TEST_REDIS_URL or REDIS_URL is required')
redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
stream_entries = 0
stream_pending = 0
stream_keys_scanned = 0
phase_started = time.perf_counter()
try:
    cursor = 0
    while True:
        cursor, streams = redis_client.scan(
            cursor=cursor, match='reservation-stream:*', count=1000
        )
        stream_keys_scanned += len(streams)
        for start in range(0, len(streams), 256):
            batch = streams[start:start + 256]
            pipeline = redis_client.pipeline(transaction=False)
            for stream in batch:
                pipeline.xlen(stream)
                pipeline.xpending(stream, 'reservation-writers')
            results = pipeline.execute(raise_on_error=False)
            for index in range(0, len(results), 2):
                length, group = results[index:index + 2]
                if isinstance(length, ResponseError):
                    raise length
                stream_entries += length
                if isinstance(group, ResponseError):
                    if 'NOGROUP' not in str(group):
                        raise group
                    if length:
                        stream_pending += length
                else:
                    stream_pending += group['pending']
        if cursor == 0:
            break
finally:
    redis_client.close()
phase_seconds['redis_streams'] = time.perf_counter() - phase_started
phase_seconds['total'] = time.perf_counter() - audit_started
queues_snapshot = dict(zip(
    ['unpublished_outbox','pending_refresh','dead_letters'], queues, strict=True))
queues_snapshot['reservation_stream_entries'] = stream_entries
queues_snapshot['reservation_stream_pending'] = stream_pending
result = {
    'runs': checks,
    'queues_snapshot': queues_snapshot,
    'audit_phase_seconds': phase_seconds,
    'reservation_stream_keys_scanned': stream_keys_scanned,
    'audited_holds': audited_holds,
    'audited_seat_intervals': audited_intervals,
    'overlapping_load_hold_intervals': overlaps,
    'durability_and_expiry_pass': (
        all(row['pass'] for row in checks)
        and overlaps == 0
        and audited_holds == sum(row['total'] for row in runs.values())
        and stream_entries == 0
        and stream_pending == 0
    ),
    'note': 'Run after expiry drain for workloads without explicit early release. HTTP 202 counts as provisional acknowledgement only when one matching reservation command is DURABLE. Intervals use order creation to hold expiry and cover only selected load run IDs. Queue counts are global context; this is not a payment test.',
}
a.output.write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result))
if not result['durability_and_expiry_pass']:
    raise SystemExit(1)
