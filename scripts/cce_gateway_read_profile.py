"""ADR0311 adopt the authenticated hourly runtime for client-only verification."""
import cce_customer_slo_reevaluation as reference
import work_envelope as policy

RESULT_PATH = "tmp/" + reference.RUN + "/cce-comparison.private.json"
ASSESSMENT_PATH = "docs/capacity/cce/approved-slo-hourly-qualification-2026-10-13.json"
ASSESSMENT_SHA256 = "ca365cfead5c2d60a6635515cabccc66853ab0200d8e4c583dacbb8c372c59a4"
CHANGED_SOURCES = {
    "scripts/cce_paid_stage.py", "scripts/cce_payment_slot_profile.py",
    "scripts/run_cce_paid_comparison.py", "scripts/cce_customer_outcome_slo.py",
    "artifacts/cce-transaction-runner/manifest.json",
}
HOURLY_ENTRY_SOURCES = {
    "scripts/cce_hourly_paid_generator.py", "scripts/cce_hourly_paid_leaf.py",
    "scripts/cce_hourly_window.py", "scripts/run_cce_hourly_qualification.py",
}
NEW_SOURCES = {
    "scripts/cce_gateway_read_profile.py", "scripts/customer_gateway_read_bundle.py",
    "scripts/customer_gateway_read_recovery.py",
    "scripts/cce_customer_slo_reevaluation.py",
    "artifacts/customer-gateway-read-recovery/manifest.json",
}


def require_reference(goal):
    from customer_gateway_read_bundle import selected
    if not selected(goal):
        raise ValueError("Explicit gateway-read recovery adoption required")
    report = policy.read(policy.ROOT / RESULT_PATH)
    state = policy.read(policy.STATE)[reference.LEDGER]
    entries = [v for v in policy.journal(policy.envelope())["experiments"]
               if v.get("ledger") == reference.LEDGER]
    if len(entries) != 1:
        raise ValueError("Exact restored hourly reference journal required")
    assessment = reference.evaluate(report, entries[0], state)
    published = policy.read(policy.ROOT / ASSESSMENT_PATH)
    if (policy.digest(published) != ASSESSMENT_SHA256
            or any(published.get(k) != v for k, v in assessment.items())
            or assessment.get("pass") is not True
            or goal.get("gateway_read_reference_sha256") != ASSESSMENT_SHA256
            or goal.get("baseline_sha256") != reference.RESULT):
        raise ValueError("Authenticated approved-SLO hourly reference required")
    return RESULT_PATH, report


def extend_plan(plan, goal):
    relative, report = require_reference(goal)
    from customer_gateway_read_bundle import contract
    return {**plan, "kind": "qualified_runtime_gateway_read_recovery_verification",
            "single_changed_factor": "authenticated_GET_502_504_recovery",
            "baseline_evidence": relative, "baseline_sha256": policy.digest(report),
            "baseline_is_passing_control": False, "matched_performance_comparison": False,
            "qualified_reference_decision": "ADR0310",
            "gateway_read_recovery_contract": contract(),
            "duration_reason": "One84/s300s verification of a sealed read-only client correction on the adopted1+3 runtime; historical hourly evidence remains unchanged."}


def validate_binding(goal, binding):
    require_reference(goal)
    original = policy.read(policy.STATE)[reference.LEDGER]["binding"]
    # All application, route, placement, resource, budget and payment semantics
    # are compared. Hourly-only adapters, diagnostics scope and new cohort
    # identity are the explicit duration differences.
    ignored = {
        "cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256",
        "baseline_sha256", "cce_verification_contract_sha256", "cce_customer_outcome_contract",
        "cce_hourly_qualification_decision", "cce_hourly_adapters_sha256",
        "cce_ingress_hourly_decision", "cce_ingress_hourly_prerequisite_sha256",
        "cce_hourly_observer_bootstrap_decision", "cce_hourly_observer_seconds",
        "cce_terminal_audit_admission_contract", "cce_committed_hourly_qualification",
        "cce_gateway_read_recovery_contract",
    }
    if any(original.get(k) != binding.get(k) for k in original.keys() | binding.keys()
           if k not in ignored):
        raise ValueError("Gateway-read adoption changed runtime, payment semantics or budgets")
    for name in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        before, after = original[name], binding[name]
        removed = HOURLY_ENTRY_SOURCES if name == "cce_paid_entry_sources" else set()
        if (set(after) != (set(before) - removed) | NEW_SOURCES
                or any(before[k] != after[k] for k in before
                       if k not in CHANGED_SOURCES | removed)):
            raise ValueError("Source drift outside the reviewed gateway-read integration")
    import cce_customer_outcome_slo as outcome
    import cce_verification_contract as verification
    from customer_gateway_read_bundle import contract
    if (binding["cce_customer_outcome_contract"] != outcome.contract()
            or binding["cce_verification_contract_sha256"] != policy.digest(verification.contract())
            or binding["cce_gateway_read_recovery_contract"] != contract()):
        raise ValueError("Exact current quality, verification and sealed child contracts required")
