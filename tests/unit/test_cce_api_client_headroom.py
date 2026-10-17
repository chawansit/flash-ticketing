"""ADR0338 explicit single-factor source/budget binding and environment protection."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import cce_api_client_headroom as client
import cce_fixture_clock as clock
import cce_generator_process_scaling as generator
import cce_primary_bootstrap as bootstrap
import cce_recovery_headroom as headroom
import cce_throughput_push as push
import work_envelope as policy
from test_cce_auth_dispatch import goal as auth_goal


def goal():
    value=auth_goal()
    for key in list(value):
        if key.startswith("auth_dispatch_"):
            del value[key]
    value.update(api_client_pool_decision="ADR0338",api_client_pool_receipt_sha256=client.file_sha(client.LOCAL),api_client_pool_contract_sha256=policy.digest(client.contract()))
    return value


def test_explicit_selection_and_api_identity():
    assert not client.selected(None) and not client.selected({})
    assert client.selected(goal()) and client.selected(headroom.runtime_view(goal()))
    assert client.image()["registry_manifest_digest"]=="sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2"
    with pytest.raises(ValueError):client.selected({"api_client_pool_decision":"ADR0338"})
    bad=goal();bad["auth_dispatch_decision"]="ADR0336"
    with pytest.raises(ValueError):client.selected(bad)


@pytest.mark.parametrize("field,value",[("offered_journeys_per_second",200),("generator_concurrency",500),("acquisition_budget",24),("maximum_paid_stages",2),("pod_vcpu",2),("maximum_experiment_seconds",5400)])
def test_budget_changes_rejected(field,value):
    bad=goal();bad[field]=value
    with pytest.raises(ValueError):client.selected(bad)


def test_settings_change_only_the_equal_client_partition():
    baseline={"DB_POOL_MAX":"4","API_PAYMENT_POOL_MAX":"2","OTHER":"unchanged"}
    assert client.api_settings(baseline,goal())=={**baseline,"DB_POOL_MAX":"6","API_PAYMENT_POOL_MAX":"3"}
    assert baseline["DB_POOL_MAX"]=="4"
    assert client.api_settings(baseline,None) is baseline
    with pytest.raises(ValueError):client.api_settings({**baseline,"DB_POOL_MAX":"8"},goal())
    conflicting=goal();conflicting["payment_delivery_decision"]="ADR0337"
    with pytest.raises(ValueError):client.selected(conflicting)


def binding_fixture(monkeypatch):
    import cce_paid_stage as paid
    import cce_verification_contract as verification
    import run_cce_paid_comparison as runner

    actual = goal()
    monkeypatch.setattr(client, "service_hashes", lambda goal: ("unchanged", "unchanged"))
    monkeypatch.setattr(
        policy,
        "envelope",
        lambda: {"spending": {"temporary_cce_pilot_exception": {"goal_bounded_authorization": actual}}},
    )
    binding = {
        "cce_api_client_pool_contract": client.contract(),
        "baseline_sha256": client.REFERENCE,
        "cce_paid_entry_sources": runner.identity(),
        "cce_paid_core_sources": paid.identity(),
        "cce_headroom_contract": headroom.contract(actual),
        "cce_throughput_push_contract": push.contract(actual),
        "cce_generator_process_contract": generator.contract(actual),
        "cce_fixture_clock_contract": clock.contract(),
        "cce_verification_contract_sha256": policy.digest(verification.contract()),
        "cce_primary_bootstrap_contract": bootstrap.contract(),
        "cce_simulator_concurrency":16,
        "cce_api_client_pool_max":6,
        "cce_payment_pool_max":3,
        "cce_acquisition_budget": 20,
        "saved_api_service_sha256": "unchanged",
        "cce_transaction_pair_sha256": "unchanged",
    }
    before = copy.deepcopy(binding)
    before.pop("cce_api_client_pool_contract")
    before.pop("cce_api_client_pool_max")
    before["cce_payment_pool_max"]=2
    before["baseline_sha256"] = "old-reference"
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        for name in client.ADDED:
            before[key].pop(name)
        for name in client.CHANGED & before[key].keys():
            before[key][name] = "old-source"
    monkeypatch.setattr(client, "reference", lambda: ("reference.json", {"pass": False}, before))
    return actual, binding


def test_bound_composition_accepts_only_delivery_change(monkeypatch):
    actual, binding = binding_fixture(monkeypatch)
    assert client.validate_binding(actual, binding)
    assert headroom.validate_binding(actual, binding)
    assert push.validate_binding(actual, binding)


@pytest.mark.parametrize("key", ["cce_paid_entry_sources", "cce_paid_core_sources"])
def test_unrelated_runtime_source_drift_is_rejected(monkeypatch, key):
    actual, binding = binding_fixture(monkeypatch)
    # The immutable customer recovery code is outside the reviewed image integration.
    protected = next(name for name in binding[key] if name.endswith("customer_gateway_read_bundle.py"))
    binding[key][protected] = "0"*64
    with pytest.raises(ValueError, match="runner source"):
        client.validate_binding(actual, binding)


@pytest.mark.parametrize(
    "field,value",
    [
        ("cce_acquisition_budget", 24),
        ("cce_simulator_concurrency",32),
        ("cce_api_client_pool_max",8),
        ("cce_payment_pool_max",4),
        ("saved_api_service_sha256", "changed"),
        ("cce_transaction_pair_sha256", "changed"),
        ("cce_fixture_clock_contract", {}),
        ("baseline_sha256", "changed"),
    ],
)
def test_binding_budget_worker_or_reference_drift_rejected(monkeypatch, field, value):
    actual, binding = binding_fixture(monkeypatch)
    binding[field] = value
    with pytest.raises(ValueError):
        client.validate_binding(actual, binding)
