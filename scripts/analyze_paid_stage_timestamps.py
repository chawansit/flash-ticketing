"""Read-only aggregate timestamps for one isolated paid-stage fixture; no new load."""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import psycopg

# Timestamps share the database clock. They mark statement execution, not commit.
QUERY = """
WITH fixture AS MATERIALIZED (
  SELECT id,status,created_at,hold_id FROM orders WHERE event_id=ANY(%s::uuid[])
), payments AS MATERIALIZED (
  SELECT p.* FROM payment_attempts p JOIN fixture f ON f.id=p.order_id
), callbacks AS (
  SELECT p.order_id,min(c.created_at) AS callback_at
  FROM payments p JOIN payment_callbacks c ON c.payment_id=p.id GROUP BY p.order_id
), ticket_times AS (
  SELECT b.order_id,max(t.issued_at) AS ticket_at,count(*) AS ticket_count
  FROM bookings b JOIN tickets t ON t.booking_id=b.id JOIN fixture f ON f.id=b.order_id
  GROUP BY b.order_id
), event_times AS (
  SELECT e.aggregate_id,
    min(e.occurred_at) FILTER(WHERE e.event_type='OrderPaid') AS paid_at,
    max(e.published_at) FILTER(WHERE e.event_type='OrderPaid') AS published_at,
    count(*) FILTER(WHERE e.event_type='OrderPaid') AS paid_events
  FROM outbox_events e JOIN fixture f ON f.id=e.aggregate_id
  WHERE e.event_type='OrderPaid' GROUP BY e.aggregate_id
), stamps AS MATERIALIZED (
  SELECT f.id,f.status,f.created_at,p.due_at,p.status AS payment_status,c.callback_at,
         t.ticket_at,t.ticket_count,e.paid_at,e.published_at,e.paid_events
  FROM fixture f LEFT JOIN payments p ON p.order_id=f.id
  LEFT JOIN callbacks c ON c.order_id=f.id LEFT JOIN ticket_times t ON t.order_id=f.id
  LEFT JOIN event_times e ON e.aggregate_id=f.id
), durations AS (
  SELECT label,1000*extract(epoch FROM elapsed) AS milliseconds
  FROM stamps CROSS JOIN LATERAL (VALUES
    ('order_to_payment_due',due_at-created_at),
    ('payment_due_to_callback',callback_at-due_at),
    ('callback_to_paid_event',paid_at-callback_at),
    ('paid_event_to_publication_record',published_at-paid_at),
    ('publication_record_to_ticket',ticket_at-published_at),
    ('callback_to_ticket',ticket_at-callback_at),
    ('payment_due_to_ticket',ticket_at-due_at)
  ) AS phases(label,elapsed)
), metrics AS (
  SELECT label,jsonb_build_object('samples',count(milliseconds),
    'negative_samples',count(*) FILTER(WHERE milliseconds<0),
    'mean_ms',avg(milliseconds),'p50_ms',percentile_cont(0.5) WITHIN GROUP(ORDER BY milliseconds),
    'p95_ms',percentile_cont(0.95) WITHIN GROUP(ORDER BY milliseconds),
    'p99_ms',percentile_cont(0.99) WITHIN GROUP(ORDER BY milliseconds),'max_ms',max(milliseconds)) AS stats
  FROM durations GROUP BY label
)
SELECT jsonb_build_object(
  'observed_at_utc',clock_timestamp(),
  'latest_hold_deadline',(SELECT max(h.expires_at) FROM holds h JOIN fixture f ON f.hold_id=h.id),
  'hold_deadlines_elapsed',clock_timestamp()>=(SELECT max(h.expires_at) FROM holds h JOIN fixture f ON f.hold_id=h.id),
  'orders',(SELECT count(*) FROM stamps),
  'payment_attempts',(SELECT count(*) FROM stamps WHERE due_at IS NOT NULL),
  'succeeded_payments',(SELECT count(*) FROM stamps WHERE payment_status='SUCCEEDED'),
  'fulfilled_orders',(SELECT count(*) FROM stamps WHERE status='FULFILLED'),
  'orders_with_callback',(SELECT count(*) FROM stamps WHERE callback_at IS NOT NULL),
  'orders_with_tickets',(SELECT count(*) FROM stamps WHERE ticket_at IS NOT NULL),
  'orders_with_paid_event',(SELECT count(*) FROM stamps WHERE paid_at IS NOT NULL),
  'orders_with_publication',(SELECT count(*) FROM stamps WHERE published_at IS NOT NULL),
  'extra_paid_events',(SELECT coalesce(sum(greatest(paid_events-1,0)),0) FROM stamps),
  'timings',(SELECT jsonb_object_agg(label,stats) FROM metrics)) AS report
"""


def analyze(conn, show_ids, expected_orders, expected_paid):
    if (not 1 <= expected_orders <= 300000 or not 0 <= expected_paid <= expected_orders
            or not 1 <= len(show_ids) <= 1000):
        raise ValueError('Bounded isolated fixture and expected counts required')
    shows = [str(UUID(str(value))) for value in show_ids]
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout = '20s'")
        conn.execute("SET LOCAL lock_timeout = '1s'")
        row = conn.execute(QUERY, (shows,)).fetchone()
    result = row['report'] if isinstance(row, dict) else row[0]
    result['observed_at_utc'] = datetime.fromisoformat(result['observed_at_utc']).astimezone(UTC).isoformat()
    result['expected_orders'], result['expected_paid'] = expected_orders, expected_paid
    result['cohort_pass'] = (result['orders'] == expected_orders and result['extra_paid_events'] == 0
                            and all(result[name] == expected_paid for name in (
                                'payment_attempts', 'succeeded_payments', 'fulfilled_orders',
                                'orders_with_callback', 'orders_with_tickets',
                                'orders_with_paid_event', 'orders_with_publication')))
    result['notes'] = [
        'Database statement timestamps are proxies, not measured commit or exact queue service time.',
        'Publication is recorded after Kafka acknowledgments; a fast consumer can issue before that record.',
        'No per-order IDs, actors, credentials, individual payloads or client-clock subtraction are emitted.',
        'This cohort/timing report does not replace durability, uniqueness or queue audits.',
    ]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--expected-orders', required=True, type=int)
    parser.add_argument('--expected-paid', required=True, type=int)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Fresh output required')
    manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
    if manifest.get('environment') != 'development' or not manifest.get('show_ids'):
        parser.error('Isolated development fixture required')
    url = os.getenv('TEST_DATABASE_URL')
    if not url:
        raise RuntimeError('TEST_DATABASE_URL required')
    with psycopg.connect(url) as conn:
        result = analyze(conn, manifest['show_ids'], args.expected_orders, args.expected_paid)
    args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))
    if not result['cohort_pass']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
