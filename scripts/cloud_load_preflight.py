"""Read-only preflight for the explicitly named isolated cloud benchmark."""
import argparse
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cloud_failure_drill import COMPOSE

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--manifest', type=Path, required=True)
p.add_argument('--seconds', type=int, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
if a.output.exists() or a.seconds < 1:
    p.error('Positive duration and fresh evidence path required')
m = json.loads(a.manifest.read_text())
if m.get('environment') != 'development':
    p.error('Development fixture required')
deadline = datetime.now(UTC) + timedelta(seconds=a.seconds + 180)
code = "import os,sys,json,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as c:\n c.execute('SET TRANSACTION READ ONLY')\n c.execute(\"SET LOCAL statement_timeout='20s'\")\n ids=json.loads(sys.argv[1])\n row=c.execute('SELECT count(*),count(*) FILTER (WHERE sale_starts<clock_timestamp() AND sale_ends>%s::timestamptz),min(sale_ends)::text FROM events WHERE id=ANY(%s::uuid[])',(sys.argv[2],ids)).fetchone()\n print(json.dumps(row))"
row = json.loads(subprocess.check_output(COMPOSE + ['exec', '-T', 'api', 'python', '-c', code, json.dumps(m['show_ids']), deadline.isoformat()], stdin=subprocess.DEVNULL, timeout=40, text=True))
result = {'utc': datetime.now(UTC).isoformat(), 'seconds': a.seconds, 'deadline_with_drain': deadline.isoformat(), 'expected_shows': len(m['show_ids']), 'found_shows': row[0], 'open_through_deadline': row[1], 'earliest_sale_end': row[2], 'credentials_valid': datetime.fromisoformat(m['expires_at']) > deadline}
result['pass'] = row[0] == row[1] == len(m['show_ids']) and result['credentials_valid']
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
if not result['pass']:
    raise SystemExit(1)
