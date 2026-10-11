"""Actual reuse flags remain verified before frozen-observer adaptation."""
import copy

import cce_event_lane_identity as pins
import cce_native_database_bridge as native
import cce_status_refresh_reuse as reuse
import cce_worker_rebalance_profile as profile
import pytest
from cce_event_lane_contract import EventLaneContract
from test_cce_event_lane_correction import inventory


def goal():
    return {"decision": "ADR0228", "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": profile.factor(),
            "timeout_retention_decision": "ADR0278", "native_database_bridge_decision": "ADR0279",
            "native_database_bridge_sha256": reuse.digest(native.contract()),
            "status_refresh_reuse_decision": "ADR0280", "status_refresh_reuse_sha256": reuse.digest(reuse.contract()),
            "offered_journeys_per_second": 84, "offered_seconds": 300}


def candidate_inventory():
    value = copy.deepcopy(inventory())
    service = EventLaneContract(status_refresh_reuse=True)
    value["status_refresh_contract"] = service.inventory_marker()
    for row in value["worker_sources"]:
        row["settings"] = service.settings(row["role"])
    return value


def test_selected_scope_is_explicit_and_fixed():
    assert reuse.selected({}) is False
    assert reuse.selected(goal())
    for key in ("status_refresh_reuse_decision", "status_refresh_reuse_sha256", "native_database_bridge_decision",
                "timeout_retention_decision", "offered_journeys_per_second", "offered_seconds"):
        with pytest.raises(ValueError):
            reuse.selected({**goal(), key: "wrong"})
    with pytest.raises(ValueError):
        reuse.selected({"status_refresh_reuse_sha256": "orphan"})


def test_same_image_budgets_and_raw_flags_verified_before_adapter():
    value = candidate_inventory()
    before = copy.deepcopy(value)
    assert pins.verify_inventory(value, pins.digest(value))
    EventLaneContract(status_refresh_reuse=True).verify_inventory(value)
    legacy, digest = pins.historical_view(value, pins.digest(value))
    assert value == before and legacy is not value and digest == pins.digest(legacy)
    assert all(row["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] == "0" for row in legacy["worker_sources"])
    assert "status_refresh_reuse_decision" not in legacy["status_refresh_contract"]
    assert all(row["image_id"] == pins.IMAGE_ID for row in value["worker_sources"])
    assert value["background"] == inventory()["background"]


@pytest.mark.parametrize("fault", ["missing_flag", "mixed_consumers", "projection", "api", "unknown_decision", "hash", "source", "pool"])
def test_actual_candidate_drift_is_never_hidden_by_adapter(fault):
    value = candidate_inventory()
    row = next(w for w in value["worker_sources"] if w["role"] == "consumer")
    if fault == "missing_flag": row["settings"].pop("ORDER_STATUS_EVENT_REFRESH_DEDUP")
    elif fault == "mixed_consumers": row["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "0"
    elif fault == "projection": next(w for w in value["worker_sources"] if w["role"] == "projection-consumer")["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    elif fault == "api": value["apis"][0]["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    elif fault == "unknown_decision": value["status_refresh_contract"]["status_refresh_reuse_decision"] = "wrong"
    elif fault == "hash": value["status_refresh_contract"]["status_refresh_reuse_sha256"] = "wrong"
    elif fault == "source": row["source_identity"]["import_code_matches_source"] = False
    elif fault == "pool": row["settings"]["DB_POOL_MAX"] = "8"
    with pytest.raises(ValueError):
        pins.historical_view(value, pins.digest(value))


def test_legacy_mode_stays_disabled_and_undeclared_flag_is_rejected():
    value = inventory()
    assert pins.verify_inventory(value, pins.digest(value))
    next(w for w in value["worker_sources"] if w["role"] == "consumer")["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    with pytest.raises(ValueError):
        pins.historical_view(value, pins.digest(value))


def test_plan_names_only_reuse_factor_and_retained_failed_baseline(monkeypatch):
    monkeypatch.setattr(profile.parent, "image", dict)
    monkeypatch.setattr(profile.parent, "plan", lambda _: {})
    value = profile.plan(goal())
    assert value["single_changed_factor"] == reuse.contract()["factor"]
    assert value["baseline_evidence"] == reuse.BASELINE and value["baseline_is_passing_control"] is False
    assert value["maximum_pods"] == 18 and value["total_requested_vcpu"] == 7.5
    assert value["native_database_bridge"] == native.contract()
