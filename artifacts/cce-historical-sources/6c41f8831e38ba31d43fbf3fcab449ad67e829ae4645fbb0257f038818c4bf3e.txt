import json
import runpy
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from redis.exceptions import ResponseError


class RedisClient:
    def __init__(self, streams=None):
        self.streams = streams or {}
        self.commands = []

    def scan(self, cursor=0, match=None, count=None):
        assert match == 'reservation-stream:*'
        return 0, list(self.streams)

    def pipeline(self, transaction=False):
        assert transaction is False
        self.commands = []
        return self

    def xlen(self, stream):
        self.commands.append(('length', stream))

    def xpending(self, stream, group):
        assert group == 'reservation-writers'
        self.commands.append(('pending', stream))

    def execute(self, raise_on_error=False):
        assert raise_on_error is False
        results = []
        for kind, stream in self.commands:
            length, pending = self.streams[stream]
            if kind == 'length':
                results.append(length)
            elif pending is None:
                results.append(ResponseError('NOGROUP No such key'))
            else:
                results.append({'pending': pending})
        return results

    def close(self):
        pass


def run_verifier(tmp_path, monkeypatch, results, counts, durable, audited, overlaps, streams=None):
    result_dir = tmp_path / 'runs'
    result_dir.mkdir()
    for name, result in results.items():
        (result_dir / (name + '.json')).write_text(json.dumps(result))

    statements = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, params=None):
            statements.append(query)
            if 'FROM reservation_commands c' in query:
                assert params[0] == 1
                assert params[1] == 2
                return SimpleNamespace(fetchall=lambda: durable)
            if 'AS run_id' in query:
                assert params[0] == 1
                assert params[1] == 2
                return SimpleNamespace(fetchall=lambda: counts)
            if 'WITH selected AS' in query:
                assert params[0] == 2
                assert sorted(params[1]) == sorted(run_id + '-' for run_id in results)
                return SimpleNamespace(fetchone=lambda: (audited, audited, overlaps))
            return SimpleNamespace(fetchone=lambda: (0, 0, 0))

    monkeypatch.setattr('psycopg.connect', lambda *args, **kwargs: Connection())
    monkeypatch.setenv('TEST_DATABASE_URL', 'unused')
    monkeypatch.setenv('TEST_REDIS_URL', 'redis://unused')
    monkeypatch.setattr('redis.Redis.from_url', lambda *args, **kwargs: RedisClient(streams))
    output = tmp_path / 'verified.json'
    monkeypatch.setattr(sys, 'argv', [
        'verify', '--results', str(result_dir), '--output', str(output),
    ])
    return output, statements


@pytest.mark.parametrize('overlaps,audited,passes', [
    (0, 10, True), (1, 10, False), (0, 9, False),
])
def test_nested_contention_and_mixed_acknowledgements(
    tmp_path, monkeypatch, overlaps, audited, passes,
):
    results = {
        'a': {'run_id': 'a', 'statuses': {'hold': {'201': 1, '409': 7, '503': 92}}},
        'b': {'run_id': 'b', 'statuses': {'hold': {'202': 8}, 'hot_hold': {'202': 1}}},
        'c': {'run_id': 'c', 'statuses': {'hold': {'500': 1}}},
    }
    output, statements = run_verifier(
        tmp_path, monkeypatch, results,
        [('a', 1, 1, 1, 0, 0, 0, 0), ('b', 9, 9, 9, 0, 0, 0, 0)],
        [('b', 9)], audited, overlaps,
    )
    with nullcontext() if passes else pytest.raises(SystemExit):
        runpy.run_path(str(Path(__file__).parents[2] / 'scripts/verify_cloud_holds.py'),
                       run_name='__main__')
    assert statements[:3] == [
        'SET TRANSACTION READ ONLY',
        "SET LOCAL statement_timeout = '60s'",
        'SET LOCAL max_parallel_workers_per_gather = 0',
    ]
    assert sum('FROM idempotency_records r' in query for query in statements) == 2
    assert sum('FROM reservation_commands c' in query for query in statements) == 1
    result = json.loads(output.read_text())
    assert result['durability_and_expiry_pass'] is passes
    assert result['overlapping_load_hold_intervals'] == overlaps
    assert [r['acknowledged_201'] for r in result['runs']] == [1, 0, 0]
    assert [r['acknowledged_202'] for r in result['runs']] == [0, 9, 0]
    assert [r['acknowledged_total'] for r in result['runs']] == [1, 9, 0]
    assert result['runs'][2]['idempotency_records'] == 0
    assert result['audit_phase_seconds']['total'] >= 0


def test_provisional_acknowledgement_requires_durable_command(tmp_path, monkeypatch):
    output, _ = run_verifier(
        tmp_path, monkeypatch,
        {'a': {'run_id': 'a', 'statuses': {'hold': {'202': 1}}}},
        [('a', 1, 1, 1, 0, 0, 0, 0)], [], 1, 0,
    )
    with pytest.raises(SystemExit):
        runpy.run_path(str(Path(__file__).parents[2] / 'scripts/verify_cloud_holds.py'),
                       run_name='__main__')
    assert not json.loads(output.read_text())['durability_and_expiry_pass']


def test_redis_first_durable_replay_counts_201_command(tmp_path, monkeypatch):
    output, _ = run_verifier(
        tmp_path, monkeypatch,
        {'a': {'run_id': 'a', 'reservation_mode': 'redis-first',
               'statuses': {'hold': {'201': 1, '202': 2}}}},
        [('a', 3, 3, 3, 0, 0, 0, 0)], [('a', 3)], 3, 0,
    )
    runpy.run_path(str(Path(__file__).parents[2] / 'scripts/verify_cloud_holds.py'),
                   run_name='__main__')
    result = json.loads(output.read_text())
    assert result['durability_and_expiry_pass']
    assert result['runs'][0]['expected_durable_commands'] == 3


def test_mixed_width_run_identifiers_fail_before_database_connection(tmp_path, monkeypatch):
    results = tmp_path / 'runs'
    results.mkdir()
    for run_id in ('a', 'long'):
        (results / (run_id + '.json')).write_text(json.dumps({
            'run_id': run_id, 'statuses': {'hold': {'202': 1}},
        }))
    monkeypatch.setattr(sys, 'argv', [
        'verify', '--results', str(results), '--output', str(tmp_path / 'out.json'),
    ])
    with pytest.raises(SystemExit):
        runpy.run_path(str(Path(__file__).parents[2] / 'scripts/verify_cloud_holds.py'),
                       run_name='__main__')


@pytest.mark.parametrize('streams,passes,entries,pending', [
    ({'reservation-stream:empty': (0, None)}, True, 0, 0),
    ({'reservation-stream:orphan': (2, None)}, False, 2, 2),
    ({'reservation-stream:pending': (0, 1)}, False, 0, 1),
])
def test_pipelined_stream_audit_preserves_queue_gate(
    tmp_path, monkeypatch, streams, passes, entries, pending,
):
    output, _ = run_verifier(
        tmp_path, monkeypatch,
        {'a': {'run_id': 'a', 'statuses': {'hold': {'202': 1}}}},
        [('a', 1, 1, 1, 0, 0, 0, 0)], [('a', 1)], 1, 0, streams,
    )
    with nullcontext() if passes else pytest.raises(SystemExit):
        runpy.run_path(str(Path(__file__).parents[2] / 'scripts/verify_cloud_holds.py'),
                       run_name='__main__')
    result = json.loads(output.read_text())
    assert result['durability_and_expiry_pass'] is passes
    assert result['reservation_stream_keys_scanned'] == 1
    assert result['queues_snapshot']['reservation_stream_entries'] == entries
    assert result['queues_snapshot']['reservation_stream_pending'] == pending
