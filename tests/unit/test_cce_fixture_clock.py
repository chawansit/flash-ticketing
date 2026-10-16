import copy
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_fixture_clock as clock
import cce_transaction_profile as transaction
import work_envelope as policy
from cce_paid_inputs import Inputs
from fixture_identity_evidence import fixture_identity
from test_fixture_identity_evidence import NOW, fixture


def proof(value=None, **changes):
    value = fixture() if value is None else value
    stamp = NOW.timestamp()
    args = dict(nonce="owned", wall_start=stamp - 0.3, wall_end=stamp - 0.1, mono_start=100, mono_end=100.2)
    args.update(changes)
    return clock.evidence(value, {"nonce": "owned", "utc": NOW.isoformat()}, **args)


def test_small_positive_offset_preserves_strict_freshness_and_sale_window():
    value = fixture()
    before = copy.deepcopy(value)
    with pytest.raises(ValueError, match="Fresh open"):
        fixture_identity(value, 1, now=NOW - timedelta(seconds=0.1))
    record = {"fixture_clock_evidence": {value["fixture_id"]: proof(value)}}
    current = clock.reference_now(record, value, monotonic_now=101.2)
    assert current >= NOW + timedelta(seconds=1)
    assert fixture_identity(value, 1, now=current)["sale_ends"] == value["sale_ends"]
    assert value == before


@pytest.mark.parametrize(
    "changes",
    [
        {"wall_start": NOW.timestamp() - 3, "wall_end": NOW.timestamp() - 2.8},
        {"wall_start": NOW.timestamp() + 3, "wall_end": NOW.timestamp() + 3.2},
        {"mono_end": 106, "wall_start": NOW.timestamp() - 6, "wall_end": NOW.timestamp()},
        {"mono_end": 99},
        {"wall_end": NOW.timestamp() + 10},
        {"mono_end": float("nan")},
        {"nonce": "wrong"},
    ],
)
def test_unbounded_or_untrusted_clock_proof_rejected(changes):
    with pytest.raises(ValueError):
        proof(**changes)


def test_old_receipt_stays_old_and_closed_sale_stays_closed():
    value = fixture()
    value["created_at"] = (NOW - timedelta(seconds=121)).isoformat()
    r = {"fixture_clock_evidence": {value["fixture_id"]: proof(value)}}
    with pytest.raises(ValueError, match="Fresh open"):
        fixture_identity(value, 1, now=clock.reference_now(r, value, monotonic_now=100.3))
    value = fixture()
    value["sale_ends"] = (NOW - timedelta(seconds=1)).isoformat()
    r = {"fixture_clock_evidence": {value["fixture_id"]: proof(value)}}
    with pytest.raises(ValueError, match="Fresh open"):
        fixture_identity(value, 1, now=clock.reference_now(r, value, monotonic_now=100.3))


def test_fixture_process_mutation_or_monotonic_replay_rejected():
    value = fixture()
    p = proof(value)
    r = {"fixture_clock_evidence": {value["fixture_id"]: p}}
    for changed in ({**value, "sale_ends": NOW.isoformat()}, {**value, "fixture_id": "other"}):
        with pytest.raises(ValueError):
            clock.reference_now(r, changed, monotonic_now=101)
    p["controller_pid"] = os.getpid() + 1
    with pytest.raises(ValueError):
        clock.reference_now(r, value, monotonic_now=101)
    p["controller_pid"] = os.getpid()
    with pytest.raises(ValueError):
        clock.reference_now(r, value, monotonic_now=99)


def test_receipt_persisted_before_failed_validation(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(transaction, "active", lambda: None)
    output = tmp_path / "candidate"
    output.mkdir()
    stages = []
    obj = Inputs.__new__(Inputs)
    obj.stage = SimpleNamespace(output=output, record={})
    obj.checkpoint = lambda: stages.append(copy.deepcopy(obj.stage.record))
    value = fixture()
    value["shows"] = 999
    with pytest.raises(ValueError):
        obj.validate_fixture_receipt("safety", value, 1, "producer")
    assert json.loads((output / "fixture-raw-safety.private.json").read_text()) == value
    assert stages[0]["fixture_raw_receipts"]["safety"]["sha256"] == policy.digest(value)


def test_clock_admission_is_explicit_and_source_bound():
    assert not clock.selected(None) and not clock.selected({})
    goal = {
        "fixture_clock_decision": "ADR0334",
        "fixture_clock_contract_sha256": policy.digest(clock.contract()),
        "generator_process_decision": "ADR0333",
    }
    assert clock.selected(goal)
    with pytest.raises(ValueError):
        clock.selected({**goal, "fixture_clock_contract_sha256": "0" * 64})


def test_real_session_adapter_has_positive_inner_timeout_budget():
    import ast

    from qualify_two_host_deployment import Session

    session = Session.__new__(Session)

    def call(role, program, timeout):
        assert role == "primary"
        tree = ast.parse(program)
        subprocess_call = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "run"
        )
        inner_timeout = next(
            ast.literal_eval(k.value) for k in subprocess_call.keywords if k.arg == "timeout"
        )
        assert 0 < inner_timeout < timeout
        command = ast.literal_eval(subprocess_call.args[0])
        source = ast.parse(command[-1])
        dictionary = next(n for n in ast.walk(source) if isinstance(n, ast.Dict))
        nonce = ast.literal_eval(dictionary.values[0])
        return {"nonce": nonce, "utc": datetime.now(UTC).isoformat()}

    session.call = call
    stage = SimpleNamespace(session=session, cid="owned-audit", record={}, checkpoint=lambda: None)
    value = fixture()
    result = clock.sample_creator(stage, value)
    assert result.tzinfo is not None
    assert value["fixture_id"] in stage.record["fixture_clock_evidence"]
