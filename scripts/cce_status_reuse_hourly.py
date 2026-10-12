"""ADR0281 separately bind the passing ADR0280 runtime to the proven hourly runner."""
import work_envelope as policy

BASELINE = "docs/capacity/cce/committed-status-reuse-result-2026-10-11.json"
BASELINE_SHA256 = "6e04b6008fcd08a8e0a3727dab2a6dd6b910c6d1d2fa4ff051911e835993b254"


def selected(goal):
    value = goal.get("hourly_qualification_decision") if goal else None
    if value is None:
        if goal and "hourly_adapters_sha256" in goal:
            raise ValueError("Orphan hourly adapter binding")
        return False
    if value != "ADR0281":
        raise ValueError("Unknown hourly qualification decision")
    return True


def adapters():
    from cce_recovery_hourly_profile import qualify_hourly_adapters
    from customer_timeout_bundle import MANIFEST_SHA256
    value = qualify_hourly_adapters()
    return {"decision": "ADR0281", "adapters": value["adapters"],
            "timeout_bundle_sha256": MANIFEST_SHA256, "recovery_max_attempts": 3,
            "seconds": 3600, "completion_deadline_seconds": 3720}


def baseline():
    path = policy.ROOT / BASELINE
    if path.is_symlink():
        raise ValueError("Hourly baseline must be retained regular evidence")
    value = policy.read(path)
    if policy.digest(value) != BASELINE_SHA256:
        raise ValueError("Exact passing ADR0280 hourly prerequisite required")
    if (value["status"] != "PASSED_RESTORED" or value["decision"] != "ADR0280"
            or value["customer_quality_pass"] is not True
            or not value["original_measurement_gates"]
            or any(v is not True for v in value["original_measurement_gates"].values())
            or value["issued_inside_offered_window"] < 25000
            or any(value[k] != 0 for k in ("generator_drops", "first_attempt_error_journeys", "final_customer_failures"))
            or any(value["correctness"][k] is not True for k in
                   ("financial_audit_pass", "zero_double_booking", "all_queues_zero", "runtime_restored"))
            or value["correctness"]["payment_loss"] != 0):
        raise ValueError("Fully passing short prerequisite required")
    return value


def short_view(goal):
    return {k: v for k, v in {**goal, "decision": "ADR0228", "profile": "cce_paid_comparison",
                              "offered_seconds": 300}.items()
            if k not in {"hourly_qualification_decision", "hourly_adapters_sha256"}}


def active(goal):
    if not selected(goal):
        raise ValueError("Explicit hourly status reuse required")
    from cce_nginx_ingress import HOURLY_BASELINE_SHA256, hourly_baseline
    ingress_hourly = goal.get("ingress_hourly_decision") is not None
    expected = {"decision": "ADR0232", "profile": "cce_hourly_qualification",
                "extension_decision": "ADR0271", "comparison_arm": "correction",
                "offered_journeys_per_second": 84, "offered_seconds": 3600,
                "generator_concurrency": 500, "acquisition_budget": 20,
                "maximum_experiment_seconds": 5400, "maximum_paid_stages": 1,
                "maximum_pods": 18, "pod_vcpu": 1, "pod_memory_gib": 2,
                "worker_resources": {"cpu": "250m", "memory": "512Mi"},
                "baseline_sha256": HOURLY_BASELINE_SHA256 if ingress_hourly else BASELINE_SHA256,
                "hourly_adapters_sha256": policy.digest(adapters()),
                "status_refresh_reuse_decision": "ADR0280"}
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact unchanged ADR0281 hourly runtime and workload required")
    if ingress_hourly:
        hourly_baseline(goal)
    else:
        baseline()
    from cce_worker_rebalance_profile import active as placement_active
    placement_active(short_view(goal))
    return goal


def plan(goal):
    active(goal)
    from cce_worker_rebalance_profile import plan as placement_plan
    value = placement_plan(short_view(goal))
    result = {**value, "decision": "ADR0232", "hourly_qualification_decision": "ADR0281",
            "common": {"buyer_journeys_per_second": 84, "duration_seconds": 3600},
            "kind": "committed_status_reuse_hourly_qualification",
            "single_changed_factor": "duration_and_distinct_cohort",
            "baseline_evidence": BASELINE, "baseline_sha256": BASELINE_SHA256,
            "baseline_is_passing_control": True, "matched_performance_comparison": False,
            "hourly_adapters_sha256": policy.digest(adapters()), "worker_deadline_seconds": 5400,
            "live_admission_maximum_seconds": 4300, "live_admission_maximum_samples": 440,
            "expected_terminal_tickets": 302400, "minimum_issued_inside_hour": 300000,
            "cohort_observer_interval_seconds": 10, "experiment_seconds_limit": 5400,
            "fixture_shows": 1008,
            "duration_reason": "Qualify the same passed runtime for one hour; mandatory audits and restoration within 90 minutes."}
    from cce_metric_admission import extend_plan
    from cce_nginx_ingress import extend_plan as extend_ingress
    return extend_ingress(extend_plan(result, goal), goal)
