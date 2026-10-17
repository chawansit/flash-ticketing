"""Offline ADR0336 composition; publication fixtures never authorize cloud work."""

import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_auth_dispatch_profile as auth
import cce_fixture_clock as clock
import cce_generator_process_scaling as generator
import cce_primary_bootstrap as bootstrap
import cce_recovery_headroom as headroom
import cce_throughput_push as push
import cce_transaction_profile as transaction
import work_envelope as policy
from test_cce_generator_process_scaling import goal as parent_goal


def goal():
    value = parent_goal()
    value.update(
        generator_process_decision="ADR0335",
        generator_concurrency=800,
        auth_dispatch_decision="ADR0336",
        auth_dispatch_receipt_sha256=auth.LOCAL_SHA256,
        primary_bootstrap_decision="ADR0330",
        fixture_clock_decision="ADR0334",
        fixture_clock_contract_sha256=policy.digest(clock.contract()),
    )
    value["generator_process_contract_sha256"] = policy.digest(generator.contract(value))
    value["throughput_push_contract_sha256"] = policy.digest(push.contract(value))
    value["headroom_contract_sha256"] = policy.digest(headroom.contract(value))
    value["auth_dispatch_contract_sha256"] = policy.digest(auth.contract())
    return value


@pytest.fixture
def publication_fixture(monkeypatch, tmp_path):
    value = policy.read(policy.ROOT / auth.LOCAL)
    value.update(
        registry_published=True,
        registry_pulled_runtime_verified=True,
        registry_manifest_digest=auth.IMAGE,
        registry_configuration_digest=auth.CONFIG,
        registry_image="swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + auth.IMAGE,
        platform={"os": "linux", "architecture": "amd64"},
    )
    path = tmp_path / "simulated-publication.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(auth, "PUBLICATION", str(path))
    return path, value


def test_unselected_and_partial_scopes():
    assert not auth.selected(None) and not auth.selected({})
    with pytest.raises(ValueError, match="Exact source-bound"):
        auth.selected({"auth_dispatch_decision": "ADR0336"})
    actual = goal()
    assert auth.selected(actual) and auth.selected(headroom.runtime_view(actual))
    assert headroom.selected(actual)


@pytest.mark.parametrize(
    "field,value",
    [
        ("generator_concurrency", 500),
        ("offered_journeys_per_second", 200),
        ("offered_seconds", 600),
        ("maximum_paid_stages", 2),
        ("maximum_experiment_seconds", 5400),
        ("pod_vcpu", 2),
        ("acquisition_budget", 24),
        ("maximum_pods", 20),
        ("payment_slot_arm", "candidate"),
        ("generator_process_decision", "ADR0333"),
        ("auth_dispatch_receipt_sha256", "0" * 64),
        ("auth_dispatch_contract_sha256", "0" * 64),
        ("primary_bootstrap_decision", "wrong"),
    ],
)
def test_budget_and_identity_drift_rejected(field, value):
    actual = goal()
    actual[field] = value
    with pytest.raises(ValueError, match="Exact source-bound"):
        auth.selected(actual)


def test_publication_fixture_preserves_all_worker_sources(publication_fixture):
    image = auth.image()
    assert image["source_identity_sha256"] == auth.IDENTITY
    before, after = image["parent_runtime_source_sha256"], image["runtime_sources_sha256"]
    assert {k for k in before if before[k] != after[k]} == {"src/ticketing/api.py"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("registry_published", False),
        ("registry_pulled_runtime_verified", False),
        ("registry_manifest_digest", "sha256:" + "0" * 64),
        ("registry_configuration_digest", "sha256:" + "0" * 64),
        ("source_identity_sha256", "0" * 64),
    ],
)
def test_incomplete_or_wrong_publication_rejected(publication_fixture, field, value):
    path, data = publication_fixture
    data[field] = value
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError):
        auth.image()


def test_worker_source_drift_rejected(publication_fixture):
    path, data = publication_fixture
    data["runtime_source_sha256"]["src/ticketing/workers.py"] = "0" * 64
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="Only the exact"):
        auth.image()


def test_native_api_image_overlay_does_not_change_worker_contract(monkeypatch, publication_fixture):
    import cce_native_workers as workers
    import run_cce_paid_comparison as runner
    from cce_fulfillment_batch_identity import IMAGE_ID

    actual = goal()
    monkeypatch.setattr(transaction, "active", lambda: headroom.runtime_view(actual))
    assert runner.native.api_image().endswith("@" + auth.IMAGE)
    assert runner.native.contract()["api_sources"] == auth.image()["runtime_sources_sha256"]
    contract = runner.worker_contract()
    assert contract.shared["registry_manifest_digest"] == IMAGE_ID
    assert workers.identity.specification("ADR0324")["manifest"] == IMAGE_ID
    assert contract.api_settings["FULFILLMENT_TRANSACTION_BATCH_SIZE"] == "1"


def test_plan_labels_failed_reference_honestly(monkeypatch, publication_fixture):
    actual = goal()
    monkeypatch.setattr(auth, "reference", lambda: ("reference.json", {"pass": False}, {}))
    plan = auth.extend_plan(actual, {"common": {"buyer_journeys_per_second": 160, "duration_seconds": 300}})
    assert plan["baseline_is_passing_control"] is False
    assert plan["worker_image_unchanged"] is True
    assert plan["single_changed_factor"] == "local_jwt_dependency_thread_handoff"


def binding_fixture(monkeypatch):
    import cce_paid_stage as paid
    import cce_verification_contract as verification
    import run_cce_paid_comparison as runner

    actual = goal()
    monkeypatch.setattr(
        policy,
        "envelope",
        lambda: {"spending": {"temporary_cce_pilot_exception": {"goal_bounded_authorization": actual}}},
    )
    binding = {
        "cce_auth_dispatch_contract": auth.contract(),
        "baseline_sha256": auth.REFERENCE,
        "cce_paid_entry_sources": runner.identity(),
        "cce_paid_core_sources": paid.identity(),
        "cce_headroom_contract": headroom.contract(actual),
        "cce_throughput_push_contract": push.contract(actual),
        "cce_generator_process_contract": generator.contract(actual),
        "cce_fixture_clock_contract": clock.contract(),
        "cce_verification_contract_sha256": policy.digest(verification.contract()),
        "cce_primary_bootstrap_contract": bootstrap.contract(),
        "cce_acquisition_budget": 20,
        "saved_api_service_sha256": "unchanged",
        "cce_transaction_pair_sha256": "unchanged",
    }
    before = copy.deepcopy(binding)
    before.pop("cce_auth_dispatch_contract")
    before["baseline_sha256"] = "old-reference"
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        for name in auth.ADDED:
            before[key].pop(name)
        for name in auth.CHANGED & before[key].keys():
            before[key][name] = "old-source"
    monkeypatch.setattr(auth, "reference", lambda: ("reference.json", {"pass": False}, before))
    return actual, binding


def test_bound_composition_accepts_only_api_image_change(monkeypatch, publication_fixture):
    actual, binding = binding_fixture(monkeypatch)
    assert auth.validate_binding(actual, binding)
    assert headroom.validate_binding(actual, binding)
    assert push.validate_binding(actual, binding)


@pytest.mark.parametrize("key", ["cce_paid_entry_sources", "cce_paid_core_sources"])
def test_unrelated_runtime_source_drift_is_rejected(monkeypatch, publication_fixture, key):
    actual, binding = binding_fixture(monkeypatch)
    # The immutable customer recovery code is outside the reviewed image integration.
    protected = next(name for name in binding[key] if name.endswith("customer_gateway_read_bundle.py"))
    binding[key][protected] = "0"*64
    with pytest.raises(ValueError, match="runner source"):
        auth.validate_binding(actual, binding)


@pytest.mark.parametrize(
    "field,value",
    [
        ("cce_acquisition_budget", 24),
        ("saved_api_service_sha256", "changed"),
        ("cce_transaction_pair_sha256", "changed"),
        ("cce_fixture_clock_contract", {}),
        ("baseline_sha256", "changed"),
    ],
)
def test_binding_budget_worker_or_reference_drift_rejected(monkeypatch, publication_fixture, field, value):
    actual, binding = binding_fixture(monkeypatch)
    binding[field] = value
    with pytest.raises(ValueError):
        auth.validate_binding(actual, binding)
