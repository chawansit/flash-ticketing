"""Bounded managed-Redis primary-failover drill for Redis-first reservation intake.

Run against the isolated Redis-first cloud topology. The operator triggers the managed
primary/standby switchover externally (for example the DCS console); this script observes,
measures and verifies. See ADR 0060.

Schema note: terminal FAILED commands exist only in Redis; PostgreSQL reservation_commands
contains DURABLE receipts only. The audit therefore compares the polled per-key terminal
states with PostgreSQL DURABLE receipts by command count and linkage.
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import redis


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--origin', required=True, help='API base URL, for example https://host')
    p.add_argument('--token', required=True, help='Bearer token for the drill actor')
    p.add_argument('--event-id', required=True, help='Fixture event with free drill seats')
    p.add_argument('--commands', type=int, default=50,
                   help='Pre/post-failover hold commands per phase')
    p.add_argument('--max-outage-seconds', type=float, default=120.0)
    p.add_argument('--drain-seconds', type=float, default=180.0)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    if a.output.exists():
        p.error('Use a fresh result path')
    if not 1 <= a.commands <= 500:
        p.error('Bounded drill: 1-500 commands per phase')
    redis_url = os.getenv('TEST_REDIS_URL') or os.getenv('REDIS_URL')
    database_url = os.getenv('TEST_DATABASE_URL')
    if not redis_url or not database_url:
        p.error('TEST_REDIS_URL (or REDIS_URL) and TEST_DATABASE_URL are required')

    run_id = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    report = {'run_id': run_id, 'started_utc': datetime.now(UTC).isoformat(),
              'event_id': a.event_id, 'phases': {}, 'gates': {}, 'pass': False}

    def save():
        a.output.write_text(json.dumps(report, indent=2) + '\n')

    def request(method, path, body=None, key=None, timeout=10):
        headers = {'Authorization': 'Bearer ' + a.token,
                   'Content-Type': 'application/json'}
        if key:
            headers['Idempotency-Key'] = key
        req = urllib.request.Request(
            a.origin + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers, method=method)
        started = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status, payload = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, payload = exc.code, exc.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            return {'status': None, 'error': type(exc).__name__,
                    'milliseconds': (time.monotonic() - started) * 1000}
        try:
            parsed = json.loads(payload)
        except ValueError:
            parsed = None
        return {'status': status, 'body': parsed,
                'milliseconds': (time.monotonic() - started) * 1000}

    client = redis.Redis.from_url(redis_url, decode_responses=True,
                                  socket_timeout=10, socket_connect_timeout=10)

    def replication():
        info = client.info('replication')
        return {'role': info.get('role'),
                'connected_replicas': info.get('connected_slaves', 0),
                'master_replid': info.get('master_replid')}

    def wait_probe():
        probe_key = '{' + a.event_id + '}:failover-probe:' + run_id
        pipe = client.pipeline(transaction=False)
        pipe.set(probe_key, '1', px=30000)
        pipe.execute_command('WAIT', 1, 1000)
        results = pipe.execute()
        client.delete(probe_key)
        return results[1]

    def seat_for(index):
        # Drill seats are dedicated fixtures; never reuse load-test inventory.
        return 'DRILL%03d' % index

    def send_holds(prefix):
        sent = []
        for i in range(a.commands):
            key = '%s-%s-%03d' % (prefix, run_id, i)
            result = request('POST', '/v1/holds',
                             {'event_id': a.event_id, 'seat_ids': [seat_for(i)]}, key)
            sent.append({'key': key, 'seat': seat_for(i), 'response': result})
            save()
        return sent

    def poll_durable(sent, deadline_seconds):
        """Return {key: (terminal_state, command_id_or_None)} for every sent key."""
        deadline = time.monotonic() + deadline_seconds
        states = {}
        for item in sent:
            if item['response'].get('status') != 202:
                states[item['key']] = ('not_acknowledged', None,
                                       item['response'].get('status'))
                continue
            body = item['response']['body']
            command_id = body['command_id']
            status = None
            while time.monotonic() < deadline:
                polled = request('GET', '/v1/reservation-commands/%s/%s'
                                 % (a.event_id, command_id))
                if polled.get('body') and polled['body'].get('persistence_status') in (
                        'DURABLE', 'FAILED'):
                    status = polled['body']['persistence_status']
                    break
                time.sleep(1)
            states[item['key']] = (status or 'unresolved', command_id, 202)
        return states

    def db_audit(prefix):
        """DURABLE receipts and linkage for this drill phase's keys (read-only)."""
        pattern = prefix + '-' + run_id + '-%'
        with psycopg.connect(database_url, autocommit=False) as conn:
            conn.execute('SET TRANSACTION READ ONLY')
            conn.execute("SET LOCAL statement_timeout = '60s'")
            row = conn.execute(
                '''SELECT count(*), count(DISTINCT c.hold_id),
                    count(DISTINCT c.order_id),
                    count(*) FILTER (WHERE h.id IS NULL OR o.id IS NULL
                        OR o.hold_id IS DISTINCT FROM h.id)
                   FROM reservation_commands c
                   LEFT JOIN holds h ON h.id = c.hold_id
                   LEFT JOIN orders o ON o.id = c.order_id
                   WHERE c.idempotency_key LIKE %s''', (pattern,)).fetchone()
            overlap = conn.execute(
                '''WITH selected AS (
                     SELECT h.id, h.event_id, h.expires_at, o.created_at, i.seat_id
                     FROM reservation_commands c
                     JOIN holds h ON h.id = c.hold_id
                     JOIN orders o ON o.id = c.order_id
                     JOIN order_items i ON i.order_id = o.id
                     WHERE c.idempotency_key LIKE %s),
                   ordered AS (
                     SELECT *, max(expires_at) OVER (
                       PARTITION BY event_id, seat_id ORDER BY created_at, id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_expiry
                     FROM selected)
                   SELECT count(*) FILTER (WHERE created_at < prior_expiry) FROM ordered''',
                (pattern,)).fetchone()[0]
        return {'durable_commands': row[0], 'distinct_holds': row[1],
                'distinct_orders': row[2], 'broken_links': row[3],
                'overlapping_intervals': overlap}

    def stream_state():
        entries = pending = 0
        for stream in client.scan_iter(match='reservation-stream:*'):
            length = client.xlen(stream)
            entries += length
            try:
                pending += client.xpending(stream, 'reservation-writers')['pending']
            except redis.exceptions.ResponseError:
                pending += length
        return {'stream_entries': entries, 'stream_pending': pending}

    save()

    # Phase 1: preflight
    ready = request('GET', '/health/ready')
    repl = replication()
    config = {k: client.config_get(k).get(k) for k in
              ('maxmemory-policy', 'appendonly', 'appendfsync')}
    acks = wait_probe()
    report['phases']['preflight'] = {
        'readiness': ready, 'replication': repl, 'redis_config': config,
        'wait_probe_replica_acks': acks}
    save()
    assert ready['status'] == 200, ready
    assert repl['role'] == 'master' and repl['connected_replicas'] >= 1, repl
    assert config['maxmemory-policy'] == 'noeviction', config
    assert config['appendonly'] == 'yes', config
    assert acks >= 1, acks
    initial_replid = repl['master_replid']

    # Phase 2: pre-failover acknowledged command set
    pre_sent = send_holds('pre')
    pre_states = poll_durable(pre_sent, a.drain_seconds)
    ack_202 = {k: v for k, v in pre_states.items() if v[2] == 202}
    pre_durable = sum(1 for v in pre_states.values() if v[0] == 'DURABLE')
    pre_failed = sum(1 for v in pre_states.values() if v[0] == 'FAILED')
    pre_unresolved = {k: v for k, v in pre_states.items() if v[0] == 'unresolved'}
    report['phases']['pre_failover'] = {
        'acknowledged_202': len(ack_202), 'durable': pre_durable,
        'terminal_failed': pre_failed, 'unresolved': len(pre_unresolved),
        'states': {k: v[:2] for k, v in pre_states.items()}}
    save()
    assert ack_202, 'No acknowledged commands to protect'
    assert not pre_unresolved, 'Commands must reach a terminal state before failover'

    # Phase 3: failover window; operator triggers the managed switchover
    print('FAILOVER WINDOW: start the managed primary/standby switchover now.')
    print('Watching for promotion (master_replid change) and intake recovery...')
    outage = {'first_failure_utc': None, 'last_failure_utc': None,
              'unknown_outcomes': [], 'samples': 0}
    deadline = time.monotonic() + a.max_outage_seconds * 3
    promoted = False
    probe_key = 'failover-probe-' + run_id
    while time.monotonic() < deadline and not promoted:
        sample = request('POST', '/v1/holds',
                         {'event_id': a.event_id, 'seat_ids': [seat_for(a.commands)]},
                         probe_key, timeout=5)
        outage['samples'] += 1
        failed = sample['status'] is None or sample['status'] >= 500
        now = datetime.now(UTC).isoformat()
        if failed:
            outage['first_failure_utc'] = outage['first_failure_utc'] or now
            outage['last_failure_utc'] = now
            if sample.get('body') and sample['body'].get('code') == \
                    'RESERVATION_DURABILITY_UNKNOWN':
                outage['unknown_outcomes'].append(now)
        else:
            try:
                current = replication()
            except redis.exceptions.RedisError:
                current = None
            if outage['first_failure_utc'] and current and \
                    current['role'] == 'master' and \
                    current['master_replid'] != initial_replid:
                promoted = True
                report['phases']['post_promotion_replication'] = current
        time.sleep(1)
        if outage['samples'] % 10 == 0:
            save()
    report['phases']['failover_window'] = outage
    save()
    assert promoted, 'Failover did not complete within the observation window'

    # Phase 4: post-failover verification of the pre-failover command set
    repl_after = replication()
    acks_after = wait_probe()
    outage_seconds = (
        datetime.fromisoformat(outage['last_failure_utc']) -
        datetime.fromisoformat(outage['first_failure_utc'])).total_seconds()
    replay = request('POST', '/v1/holds',
                     {'event_id': a.event_id, 'seat_ids': [seat_for(a.commands)]},
                     probe_key)
    resolved = replay['status'] in (201, 202, 409)
    pre_audit = db_audit('pre')
    streams = stream_state()
    report['phases']['post_failover'] = {
        'replication': repl_after, 'wait_probe_replica_acks': acks_after,
        'outage_seconds': outage_seconds,
        'unknown_outcome_replay_status': replay['status'],
        'pre_failover_audit': pre_audit, 'streams': streams}
    save()

    # Phase 5: fresh post-failover intake must persist with replica acknowledgement
    post_sent = send_holds('post')
    post_states = poll_durable(post_sent, a.drain_seconds)
    post_audit = db_audit('post')
    report['phases']['post_failover_intake'] = {
        'states': {k: v[:2] for k, v in post_states.items()}, 'audit': post_audit}
    save()

    # Gates (ADR 0060)
    gates = {
        # Every pre-failover terminal-DURABLE command has exactly one PostgreSQL
        # receipt with intact linkage; terminal FAILED commands stay out of
        # PostgreSQL and are reported separately above.
        'zero_lost_acknowledged_commands':
            pre_audit['durable_commands'] == pre_durable
            and pre_audit['distinct_holds'] == pre_durable
            and pre_audit['broken_links'] == 0,
        'unknown_outcomes_resolved': resolved,
        'outage_within_budget': outage_seconds <= a.max_outage_seconds,
        'post_failover_replica_acknowledgement':
            repl_after['role'] == 'master'
            and repl_after['connected_replicas'] >= 1 and acks_after >= 1,
        'post_failover_intake_durable':
            post_audit['durable_commands'] >= 1 and post_audit['broken_links'] == 0,
        'zero_overlapping_ownership':
            pre_audit['overlapping_intervals'] == 0
            and post_audit['overlapping_intervals'] == 0,
        'streams_drained':
            streams['stream_entries'] == 0 and streams['stream_pending'] == 0,
    }
    report['gates'] = gates
    report['pass'] = all(gates.values())
    report['completed_utc'] = datetime.now(UTC).isoformat()
    save()
    client.close()
    print(json.dumps({'run_id': run_id, 'pass': report['pass'], 'gates': gates}))
    if not report['pass']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
