"""Read-only immediate ownership evidence for one synchronized contention wave."""
import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--run-id', required=True)
p.add_argument('--event-id', required=True)
p.add_argument('--seat-id', required=True)
p.add_argument('--hold-id', required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=False) as conn:
    conn.execute('SET TRANSACTION READ ONLY')
    row = conn.execute('''SELECT count(*), count(DISTINCT h.id), count(DISTINCT o.id),
        count(*) FILTER (WHERE h.id=%s::uuid AND h.event_id=%s::uuid
          AND o.hold_id=h.id AND o.actor=h.actor AND r.actor=h.actor)
        FROM idempotency_records r
        LEFT JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
        LEFT JOIN orders o ON o.id=(r.response->>'order_id')::uuid
        WHERE r.operation='hold' AND r.key LIKE %s''',
        (a.hold_id,a.event_id,a.run_id+'-%')).fetchone()
    seat = conn.execute('SELECT hold_id FROM event_seats WHERE event_id=%s AND seat_id=%s',
                        (a.event_id,a.seat_id)).fetchone()
    active = conn.execute("""SELECT count(DISTINCT h.id),count(DISTINCT o.id)
        FROM order_items i JOIN orders o ON o.id=i.order_id JOIN holds h ON h.id=o.hold_id
        WHERE i.event_id=%s AND i.seat_id=%s AND h.status='ACTIVE'
          AND h.expires_at>clock_timestamp()""", (a.event_id,a.seat_id)).fetchone()
    # Only the wave prefix identifies its durable records; unrelated historical holds are excluded.
    result = {'utc': datetime.now(UTC).isoformat(), 'wave_run_id': a.run_id,
              'event_id': a.event_id, 'seat_id': a.seat_id, 'acknowledged_hold_id': a.hold_id,
              'record_counts': list(row), 'active_seat_hold_order_counts': list(active), 'current_seat_hold_id': str(seat[0]) if seat else None,
              'one_durable_owner_pass': row == (1,1,1,1) and active == (1,1) and seat is not None and str(seat[0]) == a.hold_id}
a.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result))
if not result['one_durable_owner_pass']:
    raise SystemExit(1)
