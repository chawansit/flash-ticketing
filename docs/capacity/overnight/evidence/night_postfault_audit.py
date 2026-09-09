import json,subprocess,datetime
from pathlib import Path
report=json.load(open('night-fault-results.json'))
orders=[c['after_recovery']['body']['order_id'] for c in report['cases'][:2]]+[report['cases'][2]['order_id']]
compose=['docker','compose','-p','flash-cloud-bench','-f','compose.yaml','-f','compose.private.yaml','-f','compose.reconciler.yaml','-f','compose.ingress.yaml']
code='import os,sys,json,psycopg\nwith psycopg.connect(os.environ["DATABASE_URL"]) as c:\n c.execute("SET TRANSACTION READ ONLY")\n c.execute("SET LOCAL statement_timeout = \'20s\'")\n print(json.dumps(c.execute(sys.argv[1],json.loads(sys.argv[2])).fetchall(),default=str))'
def db(q,p=[]):return json.loads(subprocess.check_output(compose+['exec','-T','api','python','-c',code,q,json.dumps(p)],text=True))
r={'utc':datetime.datetime.now(datetime.UTC).isoformat(),'orders':db('SELECT o.id,o.status,h.status,(SELECT count(*) FROM bookings b WHERE b.order_id=o.id),(SELECT count(*) FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE b.order_id=o.id) FROM orders o JOIN holds h ON h.id=o.hold_id WHERE o.id=ANY(%s::uuid[]) ORDER BY o.created_at',[orders]),'queues':db('SELECT (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),(SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),(SELECT count(*) FROM dead_letters)'),'duplicate_bookings':db('SELECT count(*) FROM (SELECT event_id,seat_id FROM bookings GROUP BY event_id,seat_id HAVING count(*)>1) x')}
Path('night-postfault-durable-state.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
