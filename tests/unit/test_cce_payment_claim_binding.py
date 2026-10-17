"""Actual manifest construction with synthetic secrets; no cloud or allowance calls."""
import base64
import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as api
import cce_fulfillment_batch_identity as pins
import cce_payment_claim_binding as claim
import cce_worker_identity as roles
import run_cce_paid_comparison as runner
import work_envelope as policy
from cce_committed_status_contract import CommittedStatusContract
from test_cce_api_client_headroom import goal


def inputs():
    base = {"DATABASE_URL": "postgresql://local-preview:local-preview@pgbouncer:5432/ticketing",
            "REDIS_URL": "redis://10.1.245.61:6379/0", "JWT_SECRET": "synthetic-test-only",
            "WEBHOOK_SECRET": "synthetic-test-only", "DB_POOL_MAX": "2"}
    service = {"image": api.dependency.INDEX, "environment": base,
               "command": ["uvicorn", "ticketing.api:app", "--host", "0.0.0.0", "--port", "8000",
                           "--limit-concurrency", "256", "--timeout-keep-alive", "10"]}
    contract = CommittedStatusContract(arm="candidate", identity=pins)
    environments = {r: {**base, **contract.settings(r)} for r in roles.specification("ADR0324")["counts"]}
    return goal(), service, environments, "10.1.1.10"


@pytest.fixture(scope="module")
def proposal():
    return runner.prepare_payment_claim_proposal(*inputs())


def test_actual_generated_arms_bind_nineteen_pods_and_only_worker_source_delta(proposal):
    assert claim.validate_local(proposal)
    before, after = [proposal["arms"][arm] for arm in ("control", "candidate")]
    assert {k for k in before["runtime_sources"] if before["runtime_sources"][k] != after["runtime_sources"][k]} == {
        "src/ticketing/workers.py"}
    for row in (before, after):
        assert len([o for o in row["objects"] if o["kind"] == "Pod"]) == 19
    assert after["image"] == claim.IMAGE


def test_summary_is_sanitized_and_explicitly_not_admitted(proposal):
    summary = claim.public_summary(proposal)
    raw = json.dumps(summary)
    assert "synthetic-test-only" not in raw and "local-preview" not in raw
    assert "DATABASE_URL" not in raw and "Secret" not in raw and "objects" not in raw
    assert summary["deployment_admitted"] is False and summary["registry_verified"] is False
    assert summary["cloud_measured"] is False and summary["live_runner_registration_ready"] is False
    with pytest.raises(ValueError, match="LOCAL_ONLY"):
        claim.require_dispatch_admission(proposal)


@pytest.mark.parametrize("key,value", [("offered_rate", 180), ("api_general_slots", 4),
    ("physical_backends", 32), ("simulator_delivery_slots", 32), ("http_connections", 800),
    ("maximum_experiment_seconds", 5400)])
def test_proposal_budget_drift_is_rejected(proposal, key, value):
    changed = copy.deepcopy(proposal)
    changed["fixed"][key] = value
    with pytest.raises(ValueError):
        claim.validate_local(changed)


@pytest.mark.parametrize("change", ["image", "resources", "source", "role_count", "simulator", "freshness"])
def test_generated_manifest_drift_is_rejected_even_with_updated_digest(proposal, change):
    result = copy.deepcopy(proposal)
    row = result["arms"]["candidate"]
    pod = next(o for o in row["objects"] if o["kind"] == "Pod")
    container = pod["spec"]["containers"][0]
    if change == "image":
        container["image"] = "wrong"
    elif change == "resources":
        container["resources"]["limits"]["cpu"] = "2"
    elif change == "source":
        container["command"][-1] = "print('wrong')"
    elif change == "role_count":
        row["objects"].remove(pod)
    else:
        name = "simulator-env" if change == "simulator" else "api-env"
        secret = next(o for o in row["objects"] if o["kind"] == "Secret" and o["metadata"]["name"] == name)
        key, value = ("SIMULATOR_CONCURRENCY", "32") if change == "simulator" else ("ORDER_STATUS_CACHE_MS", "9999")
        secret["data"][key] = base64.b64encode(value.encode()).decode()
    row["manifest_sha256"] = policy.digest(row["objects"])
    with pytest.raises(ValueError):
        claim.validate_local(result)


@pytest.mark.parametrize("field,value", [("offered_journeys_per_second", 180), ("maximum_paid_stages", 2),
    ("pod_vcpu", 2), ("generator_concurrency", 1000)])
def test_actual_preparation_rejects_changed_goal(field, value):
    actual, service, environments, primary = inputs()
    actual[field] = value
    with pytest.raises(ValueError):
        runner.prepare_payment_claim_proposal(actual, service, environments, primary)


def test_candidate_checkout_source_drift_rejected(monkeypatch):
    original = Path.read_bytes
    def altered(path):
        raw = original(path)
        return raw + b"\n# drift\n" if path.as_posix().endswith("src/ticketing/workers.py") else raw
    monkeypatch.setattr(Path, "read_bytes", altered)
    with pytest.raises(ValueError, match="sole claim"):
        claim.images()
