"""Read-only, post-load verification of acknowledged holds and expiry."""
import argparse
import json
import os
from pathlib import Path

import psycopg

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--results', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
runs = {}
for path in a.results.rglob('*.json'):
    result = json.loads(path.read_text())
    if isinstance(result, dict) and 'run_id' in result:
        runs[result['run_id']] = result['statuses'].get('hold', {}).get('201', 0)
if not runs:
    p.error('No completed worker results found')
checks = []
with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True) as conn:
    conn.execute('SET default_transaction_read_only = on')
    conn.execute("SET statement_timeout = '60s'")
    for run_id, acknowledged in sorted(runs.items()):
        row = conn.execute('''SELECT count(*),count(DISTINCT h.id),count(DISTINCT o.id),
            count(*) FILTER (WHERE h.id IS NULL OR o.id IS NULL
                OR o.hold_id IS DISTINCT FROM h.id OR h.actor IS DISTINCT FROM r.actor
                OR o.actor IS DISTINCT FROM r.actor),
            count(*) FILTER (WHERE h.status='ACTIVE'),
            count(*) FILTER (WHERE h.status='ACTIVE' AND h.expires_at<clock_timestamp()),
            count(*) FILTER (WHERE o.status='PENDING')
            FROM idempotency_records r
            LEFT JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
            LEFT JOIN orders o ON o.id=(r.response->>'order_id')::uuid
            WHERE r.operation='hold' AND r.key LIKE %s''', (run_id+'-%',)).fetchone()
        checks.append(dict(zip(
            ['run_id','acknowledged_201','idempotency_records','distinct_holds',
             'distinct_orders','broken_links','active_holds','overdue_holds','pending_orders','pass'],
            [run_id, acknowledged, *row,
             row[:3] == (acknowledged,)*3 and all(n == 0 for n in row[3:])], strict=True)))
    queues = conn.execute('''SELECT
        (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
        (SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),
        (SELECT count(*) FROM dead_letters)''').fetchone()
result = {
    'runs': checks,
    'queues_snapshot': dict(zip(['unpublished_outbox','pending_refresh','dead_letters'], queues, strict=True)),
    'durability_and_expiry_pass': all(row['pass'] for row in checks),
    'note': 'Run after expiry drain. Queue counts are global context; this is not a contention or payment test.',
}
a.output.write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result))
if not result['durability_and_expiry_pass']:
    raise SystemExit(1)
