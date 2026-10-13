"""ADR0310 explicit classifier reevaluation; original runner output is immutable."""
import copy

import cce_customer_outcome_slo as outcome
import work_envelope as policy

RUN = "adr0151-b6117a6713d7"
LEDGER = "bounded_cce_hourly_qualification__93ca626ddd90"
RESULT = "4bb87df75913d721c1ce874c09db0db551640bb571a7640a7607105c11e270d1"
OLD_CONTRACT = "3f6cef57504aa710b3ac141fa05d98fc38eaec0a2590287989eb1b6b320ea977"
GATES = {"customer_load", "post_ttl_financial", "zero_double_booking", "payment_durability",
         "full_queue_drain", "native_observer", "native_cpu", "unchanged_background",
         "unchanged_native_pods", "live_admission", "hourly_issuance", "unchanged_ingress_route"}


def evaluate(report, entry, scope):
    approved = policy.envelope().get("approved_customer_outcome_target", {})
    paid = report.get("paid_stage", {})
    old = paid.get("customer_outcome_gate", {})
    gates = report.get("native", {}).get("measurement_gates", {})
    binding = scope.get("binding", {})
    financial = paid.get("financial", {})
    customer = paid.get("customer", {})
    issuance = paid.get("hourly_issuance", {})
    checks = old.get("checks", {})
    required = (
        report.get("run") == RUN, policy.digest(report) == RESULT,
        entry.get("ledger") == LEDGER, entry.get("status") == "FAILED_RESTORED",
        entry.get("result_sha256") == scope.get("cce_paid_result_sha256") == RESULT,
        entry.get("binding_sha256") == report.get("binding_sha256") == policy.digest(binding),
        scope.get("active_run") is None, scope.get("paid_runs_started") == 1,
        report.get("capacity_stages_started") == 1, report.get("pass") is False,
        all(report.get(k) is True for k in
            ("restoration_complete", "integrity_verified", "transport_credentials_cleared")),
        report.get("native", {}).get("cleanup_complete") is True,
        report.get("native", {}).get("cleanup_failures") == [],
        report.get("native", {}).get("namespace_cleanup", {}).get("namespace_removed") is True,
        report.get("native", {}).get("resource_cleanup", {}).get("owned_resources_removed") is True,
        paid.get("private_cleanup_pass") is True,
        old.get("contract_sha256") == OLD_CONTRACT,
        policy.digest(binding.get("cce_customer_outcome_contract")) == OLD_CONTRACT,
        approved.get("maximum_final_customer_failure_percent") == 0.01,
        approved.get("maximum_final_customer_failure_fraction") == 0.0001,
        old.get("pass") is False, checks.get("known_customer_failures") is False,
        bool(checks) and all(v is True for k, v in checks.items() if k != "known_customer_failures"),
        set(gates) == GATES, gates.get("customer_load") is False,
        all(v is True for k, v in gates.items() if k != "customer_load"),
        customer.get("outcomes") == {"fulfilled": 302399, "order_http_502": 1},
        customer.get("final_customer_failures") == 1, customer.get("generator_drops") == 0,
        issuance.get("pass") is True, issuance.get("conservative_inner_window_tickets", 0) >= 300000,
        financial.get("pass") is True, financial.get("hold_deadlines_elapsed") is True,
    )
    if not all(required):
        raise ValueError("Exact restored classifier-only failure required; other failures cannot qualify")
    customer_assessment = outcome.evaluate(customer, 302400)
    financial_assessment = outcome.financial(financial, customer, 302400)
    accepted = {**gates, "customer_load": customer_assessment["pass"]}
    passed = all(accepted.values()) and financial_assessment["pass"]
    return {"decision": "ADR0310", "run": RUN, "ledger": LEDGER,
            "status": "QUALIFIED_UNDER_APPROVED_SLO" if passed else "NOT_QUALIFIED",
            "pass": passed, "capacity_qualified": passed,
            "original_runner_status": "FAILED_RESTORED", "original_runner_pass": False,
            "original_result_sha256": RESULT, "original_binding_sha256": policy.digest(binding),
            "original_outcome_contract_sha256": OLD_CONTRACT,
            "reevaluated_outcome_contract": outcome.contract(),
            "customer_assessment": customer_assessment,
            "financial_policy_recheck_pass": financial_assessment["pass"],
            "original_measurement_gates": copy.deepcopy(gates),
            "approved_measurement_gates": accepted,
            "hourly_issuance": copy.deepcopy(issuance),
            "original_results_and_customer_counts_unchanged": True,
            "additional_paid_load": False,
            "qualification_basis": "Previously approved0.01% final-error budget; explicit missing gateway-read label correction. Every other original measurement, latency, financial, source/restoration gate retained."}
