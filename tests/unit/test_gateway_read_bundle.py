"""Sealed child integration preserves the workload and adopted runtime budgets."""
import asyncio
import copy
import sys
import types
from pathlib import Path
from time import perf_counter

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_gateway_read_profile as profile
import cce_paid_stage as stage
import customer_gateway_read_bundle as child
import customer_recovery_bundle as recovery
import customer_timeout_bundle as timeout
import work_envelope as policy


def parent():
    bundle, coordinator = recovery.qualify(stage.generator_bundle())
    return timeout.qualify(bundle, coordinator)


def test_delivered_child_executes_gateway_recovery_and_preserves_parent_and_writes():
    bundle, coordinator = parent()
    old = copy.deepcopy(bundle)
    changed, current_coordinator = child.qualify(bundle, coordinator)
    assert bundle == old and coordinator == current_coordinator
    assert len(changed) == 80
    assert {k for k in bundle if bundle[k] != changed[k]} == {child.CLIENT}
    module = types.ModuleType("adr0311_sealed_test")
    sys.modules[module.__name__] = module
    try:
        exec(compile(changed[child.CLIENT], child.CLIENT, "exec"), module.__dict__)  # noqa: S102 - verified sealed bytes
        async def scenario():
            calls = []
            def handler(request):
                calls.append((request.method, request.url.path, request.headers.get("Authorization")))
                return httpx.Response(502 if len(calls) == 1 else 200)
            async def no_sleep(_):
                pass
            async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
                instance = module.CustomerRecoveryClient(client, perf_counter() + 10, sleep=no_sleep)
                assert (await instance.get("/v1/orders/existing", headers={"Authorization": "same"})).status_code == 200
                assert calls == [("GET", "/v1/orders/existing", "same")] * 2
                assert instance.first_attempt_errors == {"order_status:http_502": 1}
                assert instance.retry_attempts == {"order_status": 1}
                assert module.CustomerRecoveryClient.post is module.GatewayReadRecoveryClient.__bases__[0].post
        asyncio.run(scenario())
    finally:
        sys.modules.pop(module.__name__, None)


@pytest.mark.parametrize("kind", ["parent", "coordinator", "optional", "manifest"])
def test_sealed_source_drift_is_rejected(monkeypatch, kind):
    bundle, coordinator = parent()
    if kind == "parent":
        bundle[child.CLIENT] += b"\n# drift"
    elif kind == "coordinator":
        coordinator += b"\n# drift"
    else:
        original = child.raw
        name = child.MANIFEST if kind == "manifest" else "scripts/customer_gateway_read_recovery.py"
        monkeypatch.setattr(child, "raw", lambda p: original(p) + (b"\n# drift" if p == name else b""))
    with pytest.raises(ValueError):
        child.qualify(bundle, coordinator)


def goal():
    return {"gateway_read_recovery_decision": "ADR0311",
            "gateway_read_recovery_contract_sha256": policy.digest(child.contract()),
            "timeout_retention_decision": "ADR0278", "profile": "cce_paid_comparison",
            "offered_journeys_per_second": 84, "offered_seconds": 300,
            "maximum_experiment_seconds": 3600, "payment_slot_arm": "candidate"}


def test_historical_scope_is_opt_out_and_partial_activation_is_rejected():
    assert not child.selected(None) and not child.selected({})
    for key, value in (("offered_journeys_per_second", 100), ("payment_slot_arm", "control"),
                       ("gateway_read_recovery_contract_sha256", "0" * 64)):
        value_goal = {**goal(), key: value}
        with pytest.raises(ValueError):
            child.selected(value_goal)


@pytest.fixture
def adoption(monkeypatch):
    import cce_customer_outcome_slo as outcome
    import cce_verification_contract as verification
    sources = {"scripts/unchanged.py": "same", **{k: "before" for k in profile.CHANGED_SOURCES}}
    binding = {"configuration_sha256": "same", "saved_api_service_sha256": "same",
               "cce_payment_pool_max": 3, "cce_acquisition_budget": 20,
               "cce_paid_entry_sources": sources, "cce_paid_core_sources": sources}
    current = {**binding, "cce_gateway_read_recovery_contract": child.contract(),
               "cce_customer_outcome_contract": outcome.contract(),
               "cce_verification_contract_sha256": policy.digest(verification.contract())}
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        current[key] = {**sources, **{k: "after" for k in profile.CHANGED_SOURCES},
                        **{k: "new" for k in profile.NEW_SOURCES}}
    monkeypatch.setattr(profile, "require_reference", lambda g: ("same", {}))
    monkeypatch.setattr(policy, "read", lambda p: {profile.reference.LEDGER: {"binding": binding}})
    return current


def test_adopted_runtime_preserves_payment_and_total_budget(adoption):
    profile.validate_binding(goal(), adoption)


@pytest.mark.parametrize("field", ["cce_payment_pool_max", "cce_acquisition_budget",
                                   "saved_api_service_sha256", "unknown_factor"])
def test_runtime_drift_cannot_be_adopted(adoption, field):
    adoption[field] = "changed"
    with pytest.raises(ValueError):
        profile.validate_binding(goal(), adoption)


def test_unreviewed_source_change_is_rejected(adoption):
    adoption["cce_paid_core_sources"]["scripts/unchanged.py"] = "changed"
    with pytest.raises(ValueError):
        profile.validate_binding(goal(), adoption)


def test_reference_requires_pinned_published_assessment_and_unique_journal(monkeypatch):
    published = {"pass": True, "original_result_sha256": profile.reference.RESULT,
                 "capacity_qualified": True, "descriptive_metadata": "retained"}
    monkeypatch.setattr(profile, "ASSESSMENT_SHA256", policy.digest(published))
    monkeypatch.setattr(profile.reference, "evaluate", lambda *a: {"pass": True,
                        "original_result_sha256": profile.reference.RESULT,
                        "capacity_qualified": True})
    monkeypatch.setattr(policy, "envelope", dict)
    entries = [{"ledger": profile.reference.LEDGER}]
    monkeypatch.setattr(policy, "journal", lambda e: {"experiments": entries})
    def read(path):
        if path == policy.STATE:
            return {profile.reference.LEDGER: {"binding": {}}}
        if path == policy.ROOT / profile.ASSESSMENT_PATH:
            return published
        return {"run": profile.reference.RUN}
    monkeypatch.setattr(policy, "read", read)
    g = {**goal(), "gateway_read_reference_sha256": policy.digest(published),
         "baseline_sha256": profile.reference.RESULT}
    assert profile.require_reference(g)[1] == {"run": profile.reference.RUN}
    published["capacity_qualified"] = False
    with pytest.raises(ValueError):
        profile.require_reference(g)
    published["capacity_qualified"] = True
    entries.append(entries[0])
    with pytest.raises(ValueError):
        profile.require_reference(g)
