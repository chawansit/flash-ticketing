"""A consumed, failed, or changed short runtime cannot authorize hourly load."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_committed_hourly_profile as hourly
import cce_committed_status_identity as pins
import cce_customer_outcome_slo as outcome
import cce_inventory_renewal as renewal
import work_envelope as policy


@pytest.fixture
def receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    ledger = "bounded_cce_paid_comparison__" + "a" * 12
    relative = "tmp/adr0151-aaaaaaaaaaaa/cce-comparison.private.json"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")
    goal = {"committed_hourly_decision": "ADR0308", "hourly_qualification_decision": "ADR0281",
            "extension_decision": "ADR0271", "payment_slot_arm": "candidate",
            "qualification_short_ledger": ledger, "qualification_short_report_path": relative}
    binding = {"configuration_sha256": "configuration", "cce_payment_slot_decision": "ADR0306",
               "cce_payment_slot_arm": "candidate", "cce_payment_pool_max": 3,
               "cce_committed_status_receipt_sha256": pins.RECEIPT_SHA256,
               "cce_committed_status_snapshot": "1", "cce_acquisition_budget": 20,
               "cce_customer_outcome_contract": outcome.contract(),
               "cce_inventory_renewal_contract": renewal.contract()}
    report = {"pass": True, "restoration_complete": True, "integrity_verified": True,
              "transport_credentials_cleared": True, "capacity_stages_started": 1,
              "binding_sha256": policy.digest(binding),
              "native": {"cleanup_complete": True, "measurement_gates":
                         {"customer_load": True, "payment_durability": True, "full_queue_drain": True}},
              "paid_stage": {"customer": {"scheduled": 25200, "dispatched": 25200, "generator_drops": 0},
                             "short_issuance_window": {"unique_paid_issued_tickets": 25095},
                             "customer_outcome_gate": {"pass": True},
                             "financial": {"pass": True, "hold_deadlines_elapsed": True}}}
    state = {ledger: {"binding": binding, "paid_runs_started": 1, "active_run": None}}
    entry = {"ledger": ledger, "status": "PASSED_RESTORED"}
    def seal():
        digest = policy.digest(report)
        state[ledger]["cce_paid_result_sha256"] = digest
        entry["result_sha256"] = digest
    seal()
    monkeypatch.setattr(policy, "envelope", lambda: {"existing_resource_configuration_sha256": "configuration"})
    monkeypatch.setattr(policy, "journal", lambda _: {"experiments": [entry]})
    monkeypatch.setattr(policy, "read", lambda p: state if p == policy.STATE else report)
    return goal, report, state, entry, seal


def test_authenticated_passed_runtime_can_extend_duration(receipt):
    goal, report, *_ = receipt
    relative, value = hourly.require_short(goal)
    assert relative == goal["qualification_short_report_path"]
    assert value == report


@pytest.mark.parametrize("kind", ["tampered", "failed", "active", "second_dispatch", "not_restored",
                                "drops", "financial_loss", "undrained", "not_expired", "low_issuance",
                                "wrong_partition", "changed_budget", "changed_image"])
def test_unsafe_short_prerequisite_blocks_hour(receipt, kind):
    goal, report, state, entry, seal = receipt
    row = state[entry["ledger"]]
    if kind == "tampered":
        report["paid_stage"]["customer"]["dispatched"] -= 1
    elif kind == "failed":
        entry["status"] = "FAILED_RESTORED"
    elif kind == "active":
        row["active_run"] = "still-running"
    elif kind == "second_dispatch":
        row["paid_runs_started"] = 2
    elif kind == "not_restored":
        report["restoration_complete"] = False
    elif kind == "drops":
        report["paid_stage"]["customer"]["generator_drops"] = 1
    elif kind == "financial_loss":
        report["paid_stage"]["financial"]["pass"] = False
    elif kind == "undrained":
        report["native"]["measurement_gates"]["full_queue_drain"] = False
    elif kind == "not_expired":
        report["paid_stage"]["financial"]["hold_deadlines_elapsed"] = False
    elif kind == "low_issuance":
        report["paid_stage"]["short_issuance_window"]["unique_paid_issued_tickets"] = 24999
    elif kind == "wrong_partition":
        goal["payment_slot_arm"] = "control"
    elif kind == "changed_budget":
        row["binding"]["cce_acquisition_budget"] = 21
    else:
        row["binding"]["cce_committed_status_receipt_sha256"] = "wrong"
    if kind != "tampered":
        report["binding_sha256"] = policy.digest(row["binding"])
        seal()
    with pytest.raises(ValueError):
        hourly.require_short(goal)


def test_hourly_runtime_binding_rejects_changed_service(receipt):
    goal, _report, state, entry, _ = receipt
    binding = copy.deepcopy(state[entry["ledger"]]["binding"])
    assert hourly.validate_binding(goal, binding)["duration_only_extension"]
    binding["saved_api_service_sha256"] = "unreviewed"
    with pytest.raises(ValueError, match="drift"):
        hourly.validate_binding(goal, binding)


def test_short_plan_view_does_not_reenter_hourly_qualification(receipt):
    goal, *_ = receipt
    goal.update(profile="cce_hourly_qualification", offered_seconds=3600,
                maximum_experiment_seconds=5400, hourly_observer_bootstrap_decision="ADR0294",
                ingress_hourly_decision="ADR0293")
    view = hourly.short_view(goal)
    assert view["offered_seconds"] == 300 and view["maximum_experiment_seconds"] == 3600
    assert view["payment_slot_arm"] == "control"
    assert not hourly.selected(view)
    assert "hourly_observer_bootstrap_decision" not in view
    assert "ingress_hourly_decision" not in view


@pytest.mark.parametrize("relative", ["../outside/cce-comparison.private.json",
                                    "tmp\\\\escape\\\\cce-comparison.private.json",
                                    "tmp/other/report.json"])
def test_prerequisite_path_cannot_escape_owned_evidence(receipt, relative):
    goal, *_ = receipt
    goal["qualification_short_report_path"] = relative
    with pytest.raises(ValueError):
        hourly.require_short(goal)
