"""Bounded shared-show assignment and rejection before HTTP/cloud dispatch."""
import asyncio
import runpy
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scripts import paid_ticket_load_generator as child
from scripts import paid_ticket_sharded_generator as shards
from scripts import prepare_capacity_fixture as fixtures
from scripts.paid_fixture_layout import fixture_layout, validate_child_allocation
from ticketing.config import Settings


def manifest(seats=18000):
    return {"schema_version": 1, "environment": "development", "fixture_layout": "single-concert",
            "origin": "http://127.0.0.1:8000", "expires_at": (datetime.now(UTC)+timedelta(hours=1)).isoformat(),
            "show_ids": ["one-show"], "viewer_tokens": ["viewer-a", "viewer-b", "viewer-c", "viewer-d"],
            "seat_offset": 0, "seats_per_show": seats}


@pytest.mark.parametrize("width", [1, 2, 150, 9000])
def test_single_show_ranges_account_for_every_distinct_seat(width):
    parent = manifest(width*2)
    parts = shards.split_manifest(parent, 2, width)
    pairs = []
    for part in parts:
        validate_child_allocation(part, width)
        pairs.extend((part["show_ids"][0], part["seat_offset"]+index) for index in range(width))
    assert len(pairs) == len(set(pairs)) == width*2
    assert {seat for _, seat in pairs} == set(range(width*2))
    assert set(parts[0]["viewer_tokens"]).isdisjoint(parts[1]["viewer_tokens"])
    assert parts[0]["viewer_tokens"]+parts[1]["viewer_tokens"] != []


@pytest.mark.parametrize("changes", [
    {"fixture_layout": "other"}, {"show_ids": ["one", "two"]}, {"seats_per_show": 18001},
    {"seats_per_show": 0}, {"seats_per_show": True}, {"seat_offset": 1},
    {"viewer_tokens": ["same", "same"]}, {"viewer_tokens": ["only"]},
    {"seat_allocation": {"shard": 0}}, {"seats_per_show": 17999},
])
def test_invalid_single_concert_parent_rejected(changes):
    fixture = {**manifest(), **changes}
    with pytest.raises(ValueError):
        shards.split_manifest(fixture, 2, 9000)


@pytest.mark.parametrize("changes", [
    {"seat_allocation": None}, {"seat_allocation": {"shard": 0}},
    {"seat_offset": 1}, {"seats_per_show": 1}, {"fixture_layout": "unknown"},
    {"seat_allocation": {"shard": True, "shards": 2, "first": 0, "stop": 1}},
])
def test_bad_child_range_rejected_before_http_client_creation(monkeypatch, tmp_path, changes):
    part = shards.split_manifest(manifest(2), 2, 1)[0]
    part.update(deepcopy(changes))
    def forbidden_client(*args, **kwargs):
        pytest.fail("Invalid range created an HTTP client")
    monkeypatch.setattr(child.httpx, "AsyncClient", forbidden_client)
    args = SimpleNamespace(origin=part["origin"], output=tmp_path/"unused.json", rate=1, seconds=1,
                           concurrency=1, http_max_connections=1, duplicates=1,
                           timeout_seconds=2, poll_seconds=.1, completion_deadline_seconds=3)
    with pytest.raises(ValueError):
        asyncio.run(child.scheduled_journeys(args, part))


def test_child_cannot_dispatch_beyond_its_range():
    part = shards.split_manifest(manifest(), 2, 9000)[0]
    with pytest.raises(ValueError, match="exceeds"):
        validate_child_allocation(part, 9001)


def test_fixture_limits_require_opt_in_and_leave_distributed_defaults(tmp_path):
    args = SimpleNamespace(output=tmp_path/"fixture.json", shows=1, seats=18000, sale_hours=1)
    with pytest.raises(ValueError):
        fixtures.validate(args, Settings(environment="development"))
    args.fixture_layout = "single-concert"
    fixtures.validate(args, Settings(environment="development"))
    args.shows = 2
    with pytest.raises(ValueError):
        fixtures.validate(args, Settings(environment="development"))
    assert fixture_layout("distributed", 60, 300) == "distributed"
    with pytest.raises(ValueError):
        fixture_layout("distributed", 60, 18000)


def test_distributed_shards_keep_disjoint_shows_and_legacy_offsets():
    fixture = {**manifest(300), "fixture_layout": "distributed",
               "show_ids": [str(i) for i in range(60)]}
    parts = shards.split_manifest(fixture, 2, 9000)
    assert set(parts[0]["show_ids"]).isdisjoint(parts[1]["show_ids"])
    assert all(part["seat_offset"] == 0 and "seat_allocation" not in part for part in parts)


def cloud_argv():
    return ["checkout", "--backend-host", "root@example.invalid", "--generator-host", "root@generator.invalid",
            "--backend-dir", "/isolated/backend", "--generator-dir", "/isolated/generator",
            "--origin", "http://192.0.2.1:8000", "--identity-file", "/absent-key",
            "--admission-candidate", "4", "--admission-rollback", "4",
            "--fixture-layout-candidate", "single-concert", "--shows", "1", "--viewers", "18000",
            "--paid-rate", "60", "--paid-seconds", "300", "--paid-concurrency", "500",
            "--paid-generator-shards", "2", "--paid-http-client-count", "8", "--paid-poll-seconds", "1",
            "--callback-duplicates", "1", "--simulator-dispatch-mode-candidate", "refill",
            "--simulator-concurrency-candidate", "8", "--consumer-candidate", "6", "--consumer-pool-per-instance", "8",
            "--api-pool-per-instance-candidate", "4", "--api-pool-waiters-candidate", "12",
            "--api-payment-pool-max-candidate", "2", "--api-pool-shared-waiting-candidate", "1",
            "--order-status-cache-ms-candidate", "0", "--paid-lifecycle-diagnostics"]


@pytest.mark.parametrize("option,value", [
    ("--paid-rate", "62"), ("--paid-seconds", "301"), ("--shows", "2"), ("--viewers", "17000"),
    ("--paid-concurrency", "600"), ("--paid-generator-shards", "1"), ("--paid-http-client-count", "4"),
    ("--paid-poll-seconds", ".2"), ("--callback-duplicates", "3"),
    ("--simulator-concurrency-candidate", "12"), ("--api-pool-per-instance-candidate", "3"),
])
def test_unapproved_single_concert_controls_rejected_before_transport(monkeypatch, option, value):
    import unattended_capacity_stage
    def forbidden_transport(*args, **kwargs):
        pytest.fail("Unsupported profile reached cloud transport")
    monkeypatch.setattr(unattended_capacity_stage, "Transport", forbidden_transport)
    argv = cloud_argv(); argv[argv.index(option)+1] = value
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as rejected:
        runpy.run_path("scripts/run_huawei_checkout_smoke.py", run_name="__main__")
    assert rejected.value.code == 2


def test_exact_single_concert_control_accepts_before_transport_without_cloud_calls(monkeypatch):
    import unattended_capacity_stage
    paths = []
    def stop_transport(_ssh, _scp, logs, _key):
        paths.append(logs)
        raise RuntimeError("stop before actual cloud calls")
    monkeypatch.setattr(unattended_capacity_stage, "Transport", stop_transport)
    monkeypatch.setattr(sys, "argv", cloud_argv())
    try:
        with pytest.raises(RuntimeError, match="before actual cloud"):
            runpy.run_path("scripts/run_huawei_checkout_smoke.py", run_name="__main__")
    finally:
        for logs in paths:
            assert logs.resolve().is_relative_to((Path.cwd()/"tmp").resolve())
            logs.rmdir()
            logs.parent.rmdir()


def test_second_child_launch_failure_reaps_first_and_removes_private_manifests(monkeypatch):
    private = []
    first = None
    class Process:
        returncode = None
        terminated = False
        def terminate(self):
            self.terminated = True
            self.returncode = -15
        async def wait(self):
            assert self.terminated
            return self.returncode
    async def launch(*argv, **kwargs):
        nonlocal first
        path = Path(argv[argv.index("--manifest")+1])
        private.append(path.parent)
        if first is not None:
            raise RuntimeError("second launch failed")
        first = Process()
        return first
    monkeypatch.setattr(shards.asyncio, "create_subprocess_exec", launch)
    args = SimpleNamespace(rate=2, seconds=1, concurrency=2, completion_deadline_seconds=3,
                           origin=manifest()["origin"], poll_seconds=.1, duplicates=1)
    with pytest.raises(RuntimeError, match="second launch"):
        asyncio.run(shards.run(args, manifest(2)))
    assert first.terminated and all(not path.exists() for path in private)

def test_synthetic_responder_rejects_cross_key_same_seat_assignment():
    from scripts.diagnose_paid_generator import Responder
    server = Responder(1, 0, 0)
    body = {"event_id": "one-show", "seat_ids": ["S0"]}
    code, first = server.route("POST", "/v1/holds", {"idempotency-key": "first"}, body)
    assert code == 202
    assert server.route("POST", "/v1/holds", {"idempotency-key": "first"}, body)[1] == first
    assert server.route("POST", "/v1/holds", {"idempotency-key": "second"}, body)[0] == 409
    assert server.stats()["unique_assigned_seats"] == server.stats()["synthetic_orders"] == 1
    assert server.stats()["duplicate_seat_attempts"] == 1
