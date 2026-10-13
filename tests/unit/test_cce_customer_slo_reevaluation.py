"""A missing status label cannot hide another failed gate or rewrite original evidence."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_customer_slo_reevaluation as assessment
import work_envelope as policy
from test_customer_outcome_slo import customer, financial


@pytest.fixture
def proof(monkeypatch):
    c = customer(1, expected=302400)
    c["outcomes"] = {"fulfilled": 302399, "order_http_502": 1}
    for shard in c["shards"]:
        if "payment_http_503" in shard["outcomes"]:
            shard["outcomes"]["order_http_502"] = shard["outcomes"].pop("payment_http_503")
    old_contract = {"maximum_final_failure_percent": 0.01}
    sha = policy.digest(old_contract)
    monkeypatch.setattr(assessment, "OLD_CONTRACT", sha)
    binding = {"cce_customer_outcome_contract": old_contract}
    gates = {k: k != "customer_load" for k in assessment.GATES}
    report = {"run": assessment.RUN, "pass": False, "capacity_stages_started": 1,
              "binding_sha256": policy.digest(binding), "restoration_complete": True,
              "integrity_verified": True, "transport_credentials_cleared": True,
              "native": {"cleanup_complete": True, "cleanup_failures": [], "measurement_gates": gates,
                         "namespace_cleanup": {"namespace_removed": True},
                         "resource_cleanup": {"owned_resources_removed": True}},
              "paid_stage": {"private_cleanup_pass": True, "customer": c,
                             "customer_outcome_gate": {"contract_sha256": sha, "pass": False,
                                      "checks": {"known_customer_failures": False, "dispatch_latency": True}},
                             "financial": {**financial(paid=302400, orders=302400),
                                           "hold_deadlines_elapsed": True, "pass": True},
                             "hourly_issuance": {"pass": True, "conservative_inner_window_tickets": 302066}}}
    result = policy.digest(report)
    monkeypatch.setattr(assessment, "RESULT", result)
    entry = {"ledger": assessment.LEDGER, "status": "FAILED_RESTORED",
             "result_sha256": result, "binding_sha256": policy.digest(binding)}
    scope = {"binding": binding, "paid_runs_started": 1, "active_run": None,
             "cce_paid_result_sha256": result}
    monkeypatch.setattr(policy, "envelope", lambda: {"approved_customer_outcome_target":
                         {"maximum_final_customer_failure_percent": 0.01,
                          "maximum_final_customer_failure_fraction": 0.0001}})
    return report, entry, scope


def test_classifier_only_case_qualifies_without_rewriting_raw_counts(proof):
    before = copy.deepcopy(proof)
    result = assessment.evaluate(*proof)
    assert result["capacity_qualified"] and result["pass"]
    assert result["original_runner_status"] == "FAILED_RESTORED"
    assert result["original_measurement_gates"]["customer_load"] is False
    assert result["approved_measurement_gates"]["customer_load"] is True
    assert not result["additional_paid_load"]
    assert proof == before


@pytest.mark.parametrize("kind", ["raw_tamper", "latency", "drain", "financial",
                                  "not_restored", "wrong_status", "active"])
def test_any_other_failure_blocks_qualification(proof, monkeypatch, kind):
    report, entry, scope = copy.deepcopy(proof)
    if kind == "raw_tamper":
        report["paid_stage"]["customer"]["final_customer_failures"] = 0
    elif kind == "latency":
        report["paid_stage"]["customer_outcome_gate"]["checks"]["dispatch_latency"] = False
    elif kind == "drain":
        report["native"]["measurement_gates"]["full_queue_drain"] = False
    elif kind == "financial":
        report["paid_stage"]["financial"]["pass"] = False
    elif kind == "not_restored":
        report["restoration_complete"] = False
    elif kind == "wrong_status":
        entry["status"] = "RECOVERY_REQUIRED"
    else:
        scope["active_run"] = "another"
    if kind != "raw_tamper":
        sha = policy.digest(report)
        monkeypatch.setattr(assessment, "RESULT", sha)
        entry["result_sha256"] = scope["cce_paid_result_sha256"] = sha
    with pytest.raises(ValueError):
        assessment.evaluate(report, entry, scope)


@pytest.mark.parametrize("kind", ["latency", "financial"])
def test_actual_gate_rechecks_still_fail_even_if_old_metadata_claims_success(proof, monkeypatch, kind):
    report, entry, scope = copy.deepcopy(proof)
    if kind == "latency":
        report["paid_stage"]["customer"]["dispatch_lag_max_ms"] = 501
    else:
        report["paid_stage"]["financial"]["counts"]["tickets"] -= 1
    digest = policy.digest(report)
    monkeypatch.setattr(assessment, "RESULT", digest)
    entry["result_sha256"] = scope["cce_paid_result_sha256"] = digest
    result = assessment.evaluate(report, entry, scope)
    assert not result["capacity_qualified"] and not result["pass"]
