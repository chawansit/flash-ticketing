"""Read-only preflight for the explicitly named isolated cloud benchmark."""
import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--manifest', type=Path, required=True)
p.add_argument('--seconds', type=int, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
if a.output.exists() or a.seconds < 1:
    p.error('Positive duration and fresh evidence path required')

manifest = json.loads(a.manifest.read_text())
if manifest.get('environment') != 'development':
    p.error('Development fixture required')

deadline = datetime.now(UTC) + timedelta(seconds=a.seconds + 180)
row = [0, 0, None]
with psycopg.connect(os.getenv('STAGE_DATABASE_URL', 'postgresql://ticketing:ticketing@127.0.0.1:5432/ticketing')) as c:
    c.execute('SET TRANSACTION READ ONLY')
    c.execute("SET LOCAL statement_timeout = '20s'")
    with c.cursor() as cur:
        cur.execute(
            "SELECT count(*),count(*) FILTER (WHERE sale_starts<clock_timestamp() AND sale_ends>%s::timestamptz),min(sale_ends)::text "
            "FROM events WHERE id=ANY(%s::uuid[])",
            (deadline, manifest['show_ids']),
        )
        row = cur.fetchone()

result = {
    'utc': datetime.now(UTC).isoformat(),
    'seconds': a.seconds,
    'deadline_with_drain': deadline.isoformat(),
    'expected_shows': len(manifest['show_ids']),
    'found_shows': row[0],
    'open_through_deadline': row[1],
    'earliest_sale_end': row[2],
    'credentials_valid': datetime.fromisoformat(manifest['expires_at']) > deadline,
}
result['pass'] = result['found_shows'] == result['open_through_deadline'] == result['expected_shows'] and result['credentials_valid']

a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
if not result['pass']:
    raise SystemExit(1)
