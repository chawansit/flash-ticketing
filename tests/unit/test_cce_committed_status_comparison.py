"""Same published image, explicit activation and fail-closed comparison admission."""
import copy

import cce_api_adapter as api
import cce_committed_status_identity as pins
import cce_committed_status_profile as profile
import cce_event_lane_identity as old_pins
import cce_transaction_profile as transaction
import cce_verification_contract as verification
import cce_worker_identity as workers
import cce_worker_rebalance_profile as placement
import pytest
import work_envelope as policy
from cce_committed_status_contract import CommittedStatusContract
from observe_slot_paid_pipeline import correction_inventory
from test_cce_event_lane_correction import inventory


def goal(arm="control"):
    return {"decision": "ADR0228", "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": placement.factor(),
            "offered_journeys_per_second": 84, "offered_seconds": 300, "maximum_paid_stages": 1,
            "committed_status_decision": "ADR0298", "committed_status_arm": arm,
            "committed_status_receipt_sha256": pins.RECEIPT_SHA256,
            "maximum_experiment_seconds": 3600, "generator_concurrency": 500,
            "pod_vcpu": 1, "pod_memory_gib": 2, "maximum_pods": 18,
            "worker_resources": {"cpu": "250m", "memory": "512Mi"},
            "ingress_route_decision": "ADR0291", "ingress_route_arm": "candidate",
            "ingress_route_contract_sha256": policy.digest(__import__("cce_nginx_ingress").contract()),
            "metric_admission_decision": "ADR0287",
            "metric_admission_contract_sha256": policy.digest(__import__("cce_metric_admission").contract()),
            "verification_decision": "ADR0285", "verification_contract_sha256": verification.digest(verification.contract())}


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_exact_same_image_keeps_api_flags_and_budgets_unchanged(monkeypatch, arm):
    value = goal(arm)
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    assert profile.selected(value)
    assert transaction.image_for(value)["local_image_id"] == pins.IMAGE_ID
    assert transaction.proof_digest(value) == pins.RECEIPT_SHA256
    actual = api.contract()
    assert actual["api_settings"]["ORDER_STATUS_COMMITTED_SNAPSHOT"] == "0"
    assert actual["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
    assert actual["api_settings"]["API_PAYMENT_POOL_MAX"] == "2"
    assert api.admission_budget() == 20
    contract = CommittedStatusContract(arm=arm)
    assert contract.shared["local_image_id"] == pins.IMAGE_ID
    assert all(contract.settings(r)["ORDER_STATUS_COMMITTED_SNAPSHOT"] == "0" for r in contract.roles)
    assert contract.api_settings["ORDER_STATUS_COMMITTED_SNAPSHOT"] == "0"  # Unmeasured ECS bootstrap.
    assert workers.specification("ADR0298")["source"] == pins.SOURCE
    assert workers.specification("ADR0271")["source"] == old_pins.SOURCE
    assert sum(workers.specification("ADR0298")["counts"].values()) == 14
    compile(contract.image_program(contract.roles), "<image-proof>", "exec")


@pytest.mark.parametrize("fault", ["receipt", "image", "source", "bytecode", "worker_flag", "bootstrap_flag", "marker", "seal"])
def test_reject_drift_before_legacy_observer_adapter(fault):
    data = inventory()
    contract = CommittedStatusContract(arm="candidate")
    data["status_refresh_contract"] = contract.inventory_marker()
    for api_row in data["apis"]:
        api_row["settings"] = copy.deepcopy(contract.api_settings)
    source = profile.image()["runtime_source_sha256"]
    for row in data["worker_sources"]:
        row["image_id"] = pins.IMAGE_ID
        row["settings"] = contract.settings(row["role"])
        row["source_identity"]["resolved_imports"] = {k: {"sha256": v, "code_matches_source": True} for k, v in source.items()}
    baseline = copy.deepcopy(data)
    legacy, digest = correction_inventory(data, old_pins.digest(data))
    assert data == baseline and digest == old_pins.digest(legacy)
    row = data["worker_sources"][0]
    if fault == "receipt": data["status_refresh_contract"]["shared_image_receipt_sha256"] = old_pins.RECEIPT_SHA256
    elif fault == "image": row["image_id"] = old_pins.IMAGE_ID
    elif fault == "source": next(iter(row["source_identity"]["resolved_imports"].values()))["sha256"] = "0" * 64
    elif fault == "bytecode": row["source_identity"]["import_code_matches_source"] = False
    elif fault == "worker_flag": row["settings"]["ORDER_STATUS_COMMITTED_SNAPSHOT"] = "1"
    elif fault == "bootstrap_flag": data["apis"][0]["settings"]["ORDER_STATUS_COMMITTED_SNAPSHOT"] = "1"
    elif fault == "marker": data["status_refresh_contract"]["committed_status_snapshot"] = "0"
    with pytest.raises(ValueError):
        correction_inventory(data, "0" * 64 if fault == "seal" else old_pins.digest(data))


def test_candidate_requires_fully_passed_restored_same_contract_control():
    with pytest.raises(ValueError, match="control ledger"):
        profile.require_control(goal("candidate"))


@pytest.mark.parametrize("key,value", [("offered_seconds", 3600), ("offered_journeys_per_second", 100),
    ("committed_status_receipt_sha256", "0" * 64), ("verification_contract_sha256", "0" * 64)])
def test_reject_unapproved_scope_or_changed_verification(key, value):
    changed = goal()
    changed[key] = value
    with pytest.raises(ValueError):
        profile.selected(changed)


@pytest.mark.parametrize("arm,flag", [("control", "0"), ("candidate", "1")])
def test_native_flag_only_enables_fulfillment_consumer(monkeypatch, arm, flag):
    from cce_native_workers import objects
    from test_cce_shared_worker_runtime import RUN, envs
    value = goal(arm)
    monkeypatch.setattr(transaction, "active", lambda: value)
    values = envs()
    values["projection-consumer"] = {**values["consumer"], "DB_POOL_MAX": "6"}
    for environment in values.values():
        environment["ORDER_STATUS_COMMITTED_SNAPSHOT"] = "0"
    manifests = objects(RUN, values, "10.1.137.69", "ADR0298")
    secrets = [row for row in manifests if row["kind"] == "Secret"]
    import base64
    assert len(secrets) == 7
    for row in secrets:
        actual = base64.b64decode(row["data"]["ORDER_STATUS_COMMITTED_SNAPSHOT"]).decode()
        assert actual == (flag if row["metadata"]["name"] == "consumer-env" else "0")
    pods = [row for row in manifests if row["kind"] == "Pod"]
    assert len(pods) == 14
    assert all(row["spec"]["containers"][0]["image"] == profile.image()["registry_image"] for row in pods)


def test_ingress_scope_does_not_authorize_an_unpassed_candidate(monkeypatch):
    import cce_nginx_ingress as ingress
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    ingress.authorize_sequence(envelope)
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal("candidate")
    with pytest.raises(ValueError, match="control ledger"):
        ingress.authorize_sequence(envelope)


def test_control_plan_changes_no_resource_or_latency_budget(monkeypatch):
    value = goal()
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    actual = transaction.plan()
    assert actual["kind"] == "committed_fulfillment_status_comparison"
    assert actual["single_changed_factor"] == "consumer_ORDER_STATUS_COMMITTED_SNAPSHOT"
    assert actual["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert actual["pooler_server_connections"] == 24 and actual["acquisition_budget"] == 20
    assert actual["consumer_local_pool_budget"] == 48 and actual["maximum_pods"] == 18
    assert actual["arm_manifest_digest"] == pins.IMAGE_ID
    assert actual["ingress_route_arm"] == "candidate"
    assert actual["committed_status_snapshot"] == "0"


def test_booleans_cannot_replace_integer_resource_bounds():
    value = goal()
    value["pod_vcpu"] = True
    with pytest.raises(ValueError, match="integer"):
        profile.selected(value)


def test_spending_guard_accepts_one_stage_and_rejects_second_stage():
    from cce_dependency_probe import authorized_today
    envelope = copy.deepcopy(policy.envelope())
    # Authorization is a test fixture, independent of the operator's live/spent goal.
    value = {**goal(), "unlimited_cumulative_time_explicitly_authorized": True,
             "spending_allowance": {"status": "APPROVED", "maximum_additional_spend": None,
                                    "no_spending_cap_explicitly_authorized": True}}
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    authorized_today(envelope)
    value["maximum_paid_stages"] = 2
    with pytest.raises(ValueError):
        authorized_today(envelope)
