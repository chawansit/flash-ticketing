"""Read-only, post-load verification of acknowledged holds and expiry."""
import argparse
import json
import os
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
            # In redis-first mode HTTP 201 is a same-key replay observed after
            # the original command became DURABLE. Direct PostgreSQL runs can
            # still return 201 without creating a reservation command.
            'expected_durable': (
                acknowledged_201 + acknowledged_202
                if result.get('reservation_mode') == 'redis-first'
                else acknowledged_202
            ),
        }
if not runs:
    p.error('No completed worker results found')
checks = []
with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=False) as conn:
    conn.execute('SET TRANSACTION READ ONLY')
    conn.execute("SET LOCAL statement_timeout = '60s'")
    conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
    for run_id, acknowledged in sorted(runs.items()):
        pattern = run_id + '-%'
        row = conn.execute('''SELECT count(*),count(DISTINCT h.id),count(DISTINCT o.id),
            count(*) FILTER (WHERE h.id IS NULL OR o.id IS NULL
                OR o.hold_id IS DISTINCT FROM h.id OR h.actor IS DISTINCT FROM r.actor
                OR o.actor IS DISTINCT FROM r.actor),
            count(*) FILTER (WHERE h.status='ACTIVE'),
            count(*) FILTER (WHERE h.status='ACTIVE' AND h.expires_at<clock_timestamp()),
            count(*) FILTER (WHERE o.status='PENDING'),
            (SELECT count(*) FROM reservation_commands c
             WHERE c.idempotency_key LIKE %s AND c.status='DURABLE')
            FROM idempotency_records r
            LEFT JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
            LEFT JOIN orders o ON o.id=(r.response->>'order_id')::uuid
            WHERE r.operation='hold' AND r.key LIKE %s''', (pattern, pattern)).fetchone()
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
    audited_holds, audited_intervals, overlaps = conn.execute("""WITH selected AS (
        SELECT DISTINCT h.id,h.event_id,h.expires_at,o.created_at,i.seat_id
        FROM idempotency_records r
        JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
        JOIN orders o ON o.id=(r.response->>'order_id')::uuid
        JOIN order_items i ON i.order_id=o.id
        WHERE r.operation='hold' AND r.key LIKE ANY(%s)
    ), ordered AS (
        SELECT *,max(expires_at) OVER (
            PARTITION BY event_id,seat_id ORDER BY created_at,id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_expiry
        FROM selected
    ) SELECT count(DISTINCT id),count(*),count(*) FILTER (WHERE created_at<prior_expiry) FROM ordered""",
        ([run_id+'-%' for run_id in runs],)).fetchone()
    queues = conn.execute('''SELECT
        (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
        (SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),
        (SELECT count(*) FROM dead_letters)''').fetchone()
redis_url = os.getenv('TEST_REDIS_URL') or os.getenv('REDIS_URL')
if not redis_url:
    p.error('TEST_REDIS_URL or REDIS_URL is required')
redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
stream_entries = 0
stream_pending = 0
try:
    for stream in redis_client.scan_iter(match='reservation-stream:*'):
        length = redis_client.xlen(stream)
        stream_entries += length
        try:
            stream_pending += redis_client.xpending(stream, 'reservation-writers')['pending']
        except ResponseError:
            if length:
                stream_pending += length
finally:
    redis_client.close()
queues_snapshot = dict(zip(
    ['unpublished_outbox','pending_refresh','dead_letters'], queues, strict=True))
queues_snapshot['reservation_stream_entries'] = stream_entries
queues_snapshot['reservation_stream_pending'] = stream_pending
result = {
    'runs': checks,
    'queues_snapshot': queues_snapshot,
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
