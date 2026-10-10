import asyncio
import copy
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_hourly_paid_leaf as leaf
import cce_paid_stage as stage
import cce_recovery_hourly_profile as profile
import customer_recovery_bundle as bundle


def goal():
    return {"extension_decision": "ADR0256", "decision": "ADR0232", "profile": "cce_hourly_qualification",
            "comparison_arm": "control", "acquisition_budget": 20, "candidate_factor": profile.factor(),
            "baseline_sha256": profile.policy.digest(profile.hourly_baseline())}


def test_same_passed_runtime_and_full_hour_gates():
    result = profile.plan(goal())
    assert result["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 3600}
    assert (result["expected_terminal_tickets"], result["minimum_issued_inside_hour"], result["experiment_seconds_limit"]) == (302400, 300000, 5400)
    assert (result["replicas"], result["connections_per_api"], result["pooler_server_connections"], result["acquisition_budget"]) == (4, 4, 24, 20)
    assert result["common_environment_changes"]["ORDER_STATUS_READ_PIPELINE"] == "0"
    assert result["customer_recovery_max_attempts"] == 3


@pytest.mark.parametrize("field,value", [("profile", "cce_paid_comparison"), ("comparison_arm", "candidate"),
    ("acquisition_budget", 21), ("baseline_sha256", "0" * 64), ("extension_decision", "ADR0255")])
def test_hourly_binding_rejects_scope_drift(field, value):
    value_goal = goal()
    value_goal[field] = value
    with pytest.raises(ValueError, match="separately authorized"):
        profile.active(value_goal)


def test_prerequisite_rejects_failed_or_different_short_control(monkeypatch):
    data = copy.deepcopy(profile.hourly_baseline())
    data["control"]["financial"]["pass"] = False
    original = profile.sealed
    monkeypatch.setattr(profile, "sealed", lambda name, expected: data if name == profile.BASELINE else original(name, expected))
    with pytest.raises(ValueError, match="passed and restored"):
        profile.hourly_baseline()


def test_hourly_leaf_preserves_original_identity_and_forwards_recovery():
    manifest = {"show_ids": [str(i) for i in range(504)], "viewer_tokens": [str(i) for i in range(151200)],
                "seats_per_show": 300, "seat_offset": 0}
    calls = []
    async def journey(client, view, index, run, timeout, poll, duplicates, **kwargs):
        calls.append((view["viewer_tokens"][index], index, run, timeout, kwargs))
        return {"outcome": "fulfilled"}
    async def schedule(args, data, journey_fn):
        return await journey_fn(None, data, 12600, "same-run", 90, 1, 1, recovery_max_attempts=3)
    module = SimpleNamespace(journey=journey, scheduled_journeys=schedule)
    leaf.install(module)
    args = SimpleNamespace(rate=42, seconds=3600, concurrency=250, completion_deadline_seconds=3720,
                           http_max_connections=250, http_client_count=8, poll_seconds=1, duplicates=1)
    assert asyncio.run(module.scheduled_journeys(args, manifest))["outcome"] == "fulfilled"
    assert calls == [("12600", 0, "same-run-block-1", 90, {"recovery_max_attempts": 3})]


def test_hourly_remote_imports_only_transferred_recovery_and_adapter_files(tmp_path):
    candidate, _ = bundle.qualify(stage.generator_bundle())
    for name in bundle.OVERLAYS:
        (tmp_path / Path(name).name).write_bytes(candidate[name])
    for name in profile.qualify_hourly_adapters()["adapters"]:
        (tmp_path / Path(name).name).write_bytes((profile.policy.ROOT / name).read_bytes())
    clean = dict(os.environ)
    clean.pop("PYTHONPATH", None)
    for name in ("cce_hourly_paid_generator.py", "cce_hourly_paid_leaf.py"):
        result = subprocess.run([sys.executable, "-E", str(tmp_path / name), "--help"], cwd=tmp_path,
                                env=clean, capture_output=True, text=True, timeout=10, check=False)
        assert result.returncode == 0, result.stderr
        assert "--recovery-max-attempts" in result.stdout


import cce_hourly_paid_generator as parent
import paid_ticket_sharded_generator as frozen
from test_cce_hourly_generator import arguments, cohort  # noqa: F401


@pytest.mark.parametrize("attempts", [1, 3])
def test_hourly_parent_forwards_recovery_without_changing_schedule(cohort, monkeypatch, attempts):  # noqa: F811 - imported pytest fixture
    import json
    from datetime import UTC, datetime

    monkeypatch.setattr(parent, "time", lambda: 1000)
    args = arguments()
    args.recovery_max_attempts = attempts
    args.lifecycle_diagnostics = False
    commands, aggregates = [], []
    async def start(*command, **kwargs):
        commands.append(command)
        output = Path(command[command.index("--output") + 1])
        output.write_text(json.dumps({"started_at_utc": datetime.fromtimestamp(1030, UTC).isoformat()}))
        async def wait(): return 0
        return SimpleNamespace(returncode=0, wait=wait)
    def aggregate(*values, **kwargs):
        aggregates.append((values[1:], kwargs))
        return {"pass": True}
    monkeypatch.setattr(parent.asyncio, "create_subprocess_exec", start)
    monkeypatch.setattr(frozen, "aggregate", aggregate)
    assert asyncio.run(parent.run(args, cohort))["pass"]
    assert len(commands) == 2
    for command in commands:
        assert command[command.index("--rate") + 1] == "42"
        assert command[command.index("--seconds") + 1] == "3600"
        assert command[command.index("--concurrency") + 1] == "250"
        if attempts == 3:
            assert command[-2:] == ("--recovery-max-attempts", "3")
        else:
            assert "--recovery-max-attempts" not in command
    assert aggregates == [((84, 3600, 500, [0, 0]), {"recovery_max_attempts": 3} if attempts == 3 else {})]
