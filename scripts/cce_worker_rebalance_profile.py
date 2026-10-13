"""ADR0271 fixed-budget event-lane worker placement correction."""
import cce_event_lane_identity as pins
import cce_event_lane_profile as parent
import cce_worker_identity as workers
import work_envelope as policy

BASELINE = "docs/capacity/cce/event-lane-paid-result-2026-10-10.json"


def factor():
    return {"name": "event_lane_worker_placement", "comparison_arm": "correction",
            "worker_counts": workers.specification("ADR0271")["counts"],
            "worker_resources": workers.RESOURCES, "image_receipt_sha256": pins.RECEIPT_SHA256,
            "database_connections_unchanged": True, "gateway_delay_unchanged": True}


def active(goal):
    if (goal.get("extension_decision") != "ADR0271" or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("comparison_arm") != "correction"
            or goal.get("acquisition_budget") != 20 or goal.get("candidate_factor") != factor()):
        raise ValueError("Exact event-lane worker placement correction required")
    from cce_native_database_bridge import selected
    selected(goal)
    from cce_status_refresh_reuse import selected as reuse_selected
    reuse_selected(goal)
    from cce_nginx_ingress import selected as ingress_selected
    ingress_selected(goal)
    parent.image()
    return goal


def plan(goal):
    active(goal)
    legacy = {**goal, "extension_decision": "ADR0266", "candidate_factor": parent.factor()}
    result = parent.plan(legacy)
    plan = {**result, "extension_decision": "ADR0271", "kind": "event_lane_worker_placement_correction",
            "single_changed_factor": "event_lane_worker_placement", "baseline_evidence": BASELINE,
            "baseline_sha256": policy.digest(policy.read(policy.ROOT / BASELINE)),
            "baseline_is_passing_control": False, "worker_counts": factor()["worker_counts"],
            "worker_resources": workers.RESOURCES, "maximum_pods": 18,
            "total_requested_vcpu": 7.5, "total_requested_memory_gib": 15,
            "diagnostic_decision": "ADR0272",
            "duration_reason": "One five-minute worker placement correction; actual paid cohort and both lanes must drain."}

    from customer_timeout_bundle import MANIFEST_SHA256, selected
    if selected(goal):
        plan.update(kind="event_lane_fixed_budget_baseline", timeout_retention_decision="ADR0278",
                    timeout_bundle_sha256=MANIFEST_SHA256,
                    duration_reason="One unchanged 84/s five-minute baseline with timeout evidence retention.")
    from cce_native_database_bridge import contract
    from cce_native_database_bridge import selected as native_selected
    if native_selected(goal):
        baseline = "docs/capacity/cce/timeout-baseline-result-2026-10-11.json"
        plan.update(kind="native_database_bridge_comparison", single_changed_factor="database_tcp_bridge_implementation",
                    native_database_bridge=contract(), baseline_evidence=baseline,
                    baseline_sha256=policy.digest(policy.read(policy.ROOT / baseline)),
                    duration_reason="One fixed-budget native database bridge comparison at unchanged 84/s for 300s.")
    from cce_status_refresh_reuse import BASELINE as reuse_baseline
    from cce_status_refresh_reuse import contract as reuse_contract
    from cce_status_refresh_reuse import selected as reuse_selected
    if reuse_selected(goal):
        plan.update(kind="committed_status_refresh_reuse_comparison", single_changed_factor="consumer_ORDER_STATUS_EVENT_REFRESH_DEDUP",
                    status_refresh_reuse=reuse_contract(), baseline_evidence=reuse_baseline,
                    baseline_sha256=policy.digest(policy.read(policy.ROOT / reuse_baseline)),
                    duration_reason="One existing-image status reuse comparison at unchanged 84/s for 300s.")
    if goal.get("observer_cost_decision") is not None:
        from observe_cce_paid_pipeline import cohort_sampling_contract
        expected = cohort_sampling_contract()
        if (goal["observer_cost_decision"] != "ADR0283"
                or goal.get("observer_cost_sha256") != policy.digest(expected)):
            raise ValueError("Exact observer correction binding required")
        baseline = "docs/capacity/cce/committed-status-reuse-result-2026-10-11.json"
        plan.update(kind="observer_cost_correction", single_changed_factor="observer_query_planning",
                    observer_cost=expected, baseline_evidence=baseline,
                    baseline_sha256=policy.digest(policy.read(policy.ROOT / baseline)),
                    baseline_is_passing_control=True,
                    duration_reason="One unchanged 84/s five-minute run after bounding observer query work.")
    elif "observer_cost_sha256" in goal:
        raise ValueError("Orphan observer correction binding")
    from cce_committed_status_profile import extend_plan as extend_committed
    from cce_metric_admission import extend_plan as extend_capture
    from cce_nginx_ingress import extend_plan as extend_ingress
    from cce_payment_pipeline_profile import extend_plan
    from cce_runtime_cohort_profile import extend_plan as extend_cohort
    return extend_cohort(extend_committed(extend_ingress(extend_capture(extend_plan(plan, goal), goal), goal), goal), goal)
