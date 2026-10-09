"""ADR0234 exact one-factor environment, admission binding and unchanged pool budgets."""

import ast
import base64
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as cce
import cce_historical_sources as historical
import work_envelope as policy
from test_cce_api_adapter import RUN, service
from test_cce_api_adapter import lifecycle as adapter_area


@pytest.fixture
def owned(monkeypatch):
    return adapter_area.__wrapped__(monkeypatch)


def test_candidate_changes_only_finite_acquisition_count():
    baseline = cce.api_environment(service(), "10.1.137.69")
    candidate = cce.api_environment(service(), "10.1.137.69", acquisition_budget=20)
    assert {k for k in baseline if baseline[k] != candidate[k]} == {"DB_POOL_MAX_WAITING"}
    assert baseline["DB_POOL_MAX_WAITING"] == "12" and candidate["DB_POOL_MAX_WAITING"] == "20"
    assert candidate["DB_POOL_WAIT_MS"] == baseline["DB_POOL_WAIT_MS"] == "500"
    assert cce.contract()["api_settings"]["DB_POOL_MAX_WAITING"] == "12"


@pytest.mark.parametrize("value", [0, 1, 13, 19, 21, 1000, True, 20.0, "20", None])
def test_unbounded_or_undeclared_queue_config_rejected(value):
    with pytest.raises(ValueError):
        cce.api_environment(service(), "10.1.137.69", acquisition_budget=value)


def test_frozen_partition_keeps_four_connections_and_finite_role_caps():
    path = historical.ROOT / "tmp/adr0163-offline-images/source/src/ticketing/infrastructure/postgres.py"
    tree = ast.parse(historical.read_source_file(path).decode("utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "api_pool_budgets")
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<frozen-budget-function>", "exec"), namespace)  # noqa: S102 - compile exact retained pure function
    baseline = namespace["api_pool_budgets"](4, 12, 2, True)
    candidate = namespace["api_pool_budgets"](4, 20, 2, True)
    assert sum(v["maximum"] for v in candidate.values()) == 4
    for role in ("general", "payment"):
        assert baseline[role]["maximum"] == candidate[role]["maximum"] == 2
        assert baseline[role]["maximum_waiting"] == 10
        assert candidate[role]["maximum_waiting"] == 18


def test_candidate_manifest_changes_secret_and_environment_proofs_only():
    before = cce.objects(RUN, service(), "10.1.137.69", "user", "secret")
    after = cce.objects(RUN, service(), "10.1.137.69", "user", "secret", acquisition_budget=20)
    for name, value in before[1]["data"].items():
        if name == "DB_POOL_MAX_WAITING":
            assert base64.b64decode(after[1]["data"][name]) == b"20"
        else:
            assert after[1]["data"][name] == value
    assert before[0] == after[0] and before[2] == after[2]
    for baseline, candidate in zip(before[3:], after[3:], strict=True):
        a, b = copy.deepcopy(baseline), copy.deepcopy(candidate)
        assert a["spec"]["containers"][0]["resources"] == b["spec"]["containers"][0]["resources"]
        assert (
            a["metadata"]["annotations"]["codex-environment-sha256"]
            != b["metadata"]["annotations"]["codex-environment-sha256"]
        )
        a["metadata"]["annotations"].pop("codex-environment-sha256")
        b["metadata"]["annotations"].pop("codex-environment-sha256")
        a["spec"]["containers"][0].pop("command")
        b["spec"]["containers"][0].pop("command")
        assert a == b


def test_new_budget_must_match_declaration_binding_and_secret(owned, monkeypatch):
    deployment, _old, events, _, _ = owned
    values = cce.objects(RUN, service(), "10.1.137.69", "user", "secret", acquisition_budget=20)
    monkeypatch.setattr(cce, "admission_budget", lambda: 20)
    deployment.guard.binding.update(cce_manifest_sha256=policy.digest(values), cce_acquisition_budget=20)
    deployment.create(values)
    assert len(deployment.pod_uids) == 4
    assert any(method == "POST" for method, _, _ in events)


@pytest.mark.parametrize("fault", ["binding", "secret", "declaration"])
def test_wrong_candidate_budget_cannot_create_namespace(owned, monkeypatch, fault):
    deployment, _old, events, _, _ = owned
    values = cce.objects(RUN, service(), "10.1.137.69", "user", "secret", acquisition_budget=20)
    monkeypatch.setattr(cce, "admission_budget", lambda: 12 if fault == "declaration" else 20)
    if fault == "secret":
        values[1]["data"]["DB_POOL_MAX_WAITING"] = base64.b64encode(b"999999").decode()
    deployment.guard.binding.update(
        cce_manifest_sha256=policy.digest(values), cce_acquisition_budget=12 if fault == "binding" else 20
    )
    with pytest.raises(ValueError):
        deployment.create(values)
    assert not any(method == "POST" for method, _, _ in events)


def test_selector_rejects_mislabeled_factor(monkeypatch):
    envelope = copy.deepcopy(policy.envelope())
    goal = envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    goal.update(
        profile="cce_paid_comparison",
        extension_decision="ADR0234",
        candidate_factor={
            "name": "api_shared_acquisition_budget",
            "baseline": 12,
            "candidate": 20,
            "database_connections_unchanged": True,
        },
    )
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    assert cce.admission_budget() == 20
    goal["candidate_factor"]["candidate"] = 200
    with pytest.raises(ValueError):
        cce.admission_budget()
