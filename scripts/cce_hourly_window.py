"""ADR0232 read-only durable ticket count within the actual one-hour window."""

import math
from uuid import UUID


def program(events, start):
    if (
        len(events) != 1008
        or len(set(events)) != 1008
        or any(str(UUID(value)) != value for value in events)
        or type(start) not in (int, float)
        or not math.isfinite(start)
        or start <= 0
    ):
        raise ValueError("Exact owned hourly events and finite common start required")
    return "events=" + repr(events) + "\nstart=" + repr(start) + "\n" + QUERY


QUERY = (
    r"""
import json,os,psycopg,time
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:
 with conn.transaction():
  conn.execute('SET TRANSACTION READ ONLY')
  conn.execute("SET LOCAL statement_timeout='60s'")
  row=conn.execute("""
    + '"""'
    + """
SELECT count(DISTINCT t.id), count(DISTINCT b.order_id), count(DISTINCT p.id),count(*),
       count(DISTINCT t.id) FILTER (WHERE t.issued_at >= to_timestamp(%s+2)
                                      AND t.issued_at < to_timestamp(%s+3598)),
       clock_timestamp(), min(t.issued_at), max(t.issued_at)
FROM tickets t JOIN bookings b ON b.id=t.booking_id
JOIN orders o ON o.id=b.order_id JOIN payment_attempts p ON p.order_id=o.id
WHERE b.event_id=ANY(%s::uuid[]) AND o.status='FULFILLED' AND p.status='SUCCEEDED'
  AND t.issued_at>=to_timestamp(%s) AND t.issued_at<to_timestamp(%s+3600)
"""
    + '"""'
    + """,(start,start,events,start,start)).fetchone()
result={'window_start_epoch':start,'window_end_epoch':start+3600,
 'unique_paid_issued_tickets':row[0],'unique_orders':row[1],'unique_payments':row[2],
 'joined_rows':row[3],'conservative_inner_window_tickets':row[4],
 'inner_window_guard_seconds':2,'database_now_epoch':row[5].timestamp(),
 'api_helper_now_epoch':time.time(),'first_issued_at':row[6].isoformat() if row[6] else None,
 'last_issued_at':row[7].isoformat() if row[7] else None}
result['pass']=(300000<=row[4]<=row[0]<=302400 and row[0]==row[1]==row[2]==row[3]
                and row[5].timestamp()>=start+3600 and abs(row[5].timestamp()-time.time())<=2)
print(json.dumps(result))
"""
)
