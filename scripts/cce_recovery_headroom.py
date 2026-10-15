"""ADR0312 exact102/s headroom; application budgets and client remain unchanged."""
import hashlib
from uuid import UUID

import work_envelope as policy
from cce_throughput_push import bounds

RATE, SHOWS, EXPECTED, TARGET = 102, 102, 30600, 30000
BASELINE_LEDGER = "bounded_cce_paid_comparison__018812d8ffb1"
BASELINE_PATH = "tmp/adr0151-19ac8d32b4df/cce-comparison.private.json"
BASELINE_SHA256 = "d874af069489b42fb22a2c35f1c5222714e7d3fe1a7c92f7c06c39d58e886f41"
SOURCE = "scripts/cce_recovery_headroom.py"
FAILURE_RUN = "adr0151-211188fa6615"
FAILURE_LEDGER = "bounded_cce_ticket_target__37fc44dbf69e"
FAILURE_SHA256 = "463e92253a3613ea9bc5e591823596681b890611290d224bc8ad9a83618b1424"
SLOT_RUN = "adr0151-4a166cdbdda6"
SLOT_LEDGER = "bounded_cce_ticket_target__476c1bdb0c87"
SLOT_SHA256 = "d5a00a534026bce91b131a968f036e38c816e076f95e056e842a011127f4ae01"
SCALE_KEYS = {"reservation_writer_scale_decision", "reservation_writer_failure_sha256"}
CORRECTION_KEYS = {"headroom_slot_correction_decision", "headroom_slot_failure_sha256"}
BATCH_KEYS = {"writer_batch_decision", "writer_batch_failure_sha256", "writer_batch_size"}
PARTITION_KEYS = {"backend_partition_decision", "backend_partition_contract_sha256"}
WRITER_RUN = "adr0151-6fd6d0275799"
WRITER_LEDGER = "bounded_cce_ticket_target__cf9e12d21c2f"
WRITER_SHA256 = "250aa2541308119f8da10a7ba3ebdf8024d99fb8a1fffbb796e8eead55fbfa2a"
CHANGED_SOURCES = {
    "scripts/cce_inventory_renewal.py", "scripts/cce_paid_observers.py",
    "scripts/cce_writer_scale_verification.py", "scripts/cce_paid_lifecycle.py",
    "scripts/observe_cce_paid_pipeline.py",
    SOURCE,
    "scripts/cce_paid_profiles.py", "scripts/cce_paid_stage.py", "scripts/cce_paid_inputs.py",
    "scripts/cce_live_admission.py", "scripts/cce_native_workers.py",
    "scripts/cce_transaction_profile.py", "scripts/cce_api_adapter.py",
    "scripts/cce_payment_slot_profile.py", "scripts/run_cce_paid_comparison.py",
    "scripts/cce_dependency_probe.py", "scripts/work_envelope.py", "scripts/customer_gateway_read_bundle.py",
    "scripts/cce_worker_identity.py", "scripts/cce_worker_transition.py", "scripts/cce_bounded_financial_audit.py", "scripts/fixture_identity_evidence.py",
    "artifacts/cce-transaction-runner/manifest.json",
    "scripts/cce_explicit_start_profile.py", "scripts/cce_explicit_start_identity.py",
    "scripts/cce_fulfillment_batch_profile.py", "scripts/cce_fulfillment_batch_identity.py",
    "scripts/cce_committed_status_contract.py", "scripts/observe_slot_paid_pipeline.py", "scripts/cce_metric_admission.py",
    "scripts/observe_cce_workers.py",
}


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def contract(goal=None):
    rate, shows, expected, target = bounds(goal)
    return {"decision": "ADR0312", "offered_rate": rate, "offered_seconds": 300,
            "expected_journeys": expected, "fixture_shows": shows,
            "minimum_unique_inside_window": target, "concurrency": 500,
            "max_attempts": 3, "experiment_limit": 3600,
            "helper_sha256": sha((policy.ROOT / SOURCE).read_bytes()),
            "sealed_client_unchanged": True, "resources_and_budgets_unchanged": True,
            "slot_correction_decision": "ADR0314", "per_api_total": 4,
            "baseline_payment_pool_max": 3, "corrective_payment_pool_max": 2,
            "writer_scale_decision": "ADR0315", "native_writers": [3, 4],
            "maximum_owned_pods_after_scale": 19, "physical_server_connections": 24,
            "writer_batch_decision": "ADR0316", "baseline_batch_size": 4,
            "candidate_batch_size": 8}


def runtime_view(goal):
    return {**goal, "decision": "ADR0228", "extension_decision": "ADR0271",
            "profile": "cce_paid_comparison", "offered_journeys_per_second": 84,
            "maximum_pods": 18, "_headroom_runtime_view": True}


def selected(goal):
    if goal is None or not {"headroom_decision", "headroom_contract_sha256"}.intersection(goal):
        return False
    rate = bounds(goal)[0]
    if "_headroom_runtime_view" in goal:
        actual = {**goal, "decision": "ADR0277", "extension_decision": "ADR0277",
                  "profile": "cce_ticket_target", "offered_journeys_per_second": rate,
                  "maximum_pods": 19 if SCALE_KEYS.intersection(goal) else 18}
        actual.pop("_headroom_runtime_view")
        if goal.get("_headroom_runtime_view") is not True or goal != runtime_view(actual):
            raise ValueError("Exact declared application-only runtime view required")
        goal = actual
    correction = bool(CORRECTION_KEYS.intersection(goal))
    if correction and (goal.get("headroom_slot_correction_decision") != "ADR0314"
                       or goal.get("headroom_slot_failure_sha256") != FAILURE_SHA256):
        raise ValueError("Exact failed headroom slot-correction decision required")
    scale = bool(SCALE_KEYS.intersection(goal))
    if scale and (not correction or goal.get("reservation_writer_scale_decision") != "ADR0315"
                  or goal.get("reservation_writer_failure_sha256") != SLOT_SHA256):
        raise ValueError("Exact reconciled writer-scale reference required")
    batch = bool(BATCH_KEYS.intersection(goal))
    if batch and (not scale or goal.get("writer_batch_decision") != "ADR0316"
                  or goal.get("writer_batch_failure_sha256") != WRITER_SHA256
                  or type(goal.get("writer_batch_size")) is not int or goal["writer_batch_size"] != 8):
        raise ValueError("Exactly declared four-writer batch-eight correction required")
    partition = bool(PARTITION_KEYS.intersection(goal))
    if partition:
        from cce_backend_partition import contract as partition_contract
        if (not scale or batch or goal.get("backend_partition_decision") not in {"ADR0317","ADR0318"}
                or goal.get("backend_partition_contract_sha256") != policy.digest(partition_contract(goal.get("backend_partition_decision")))):
            raise ValueError("Exact four-writer batch-four backend isolation required")
    from cce_explicit_start_profile import selected as explicit_selected
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    from cce_measured_payment_context_profile import selected as context_selected
    from cce_notification_dedup_profile import selected as notification_selected
    from cce_writer_sale_context_profile import selected as writer_context_selected
    fulfillment = fulfillment_selected(goal)
    explicit = explicit_selected(goal)
    notification = notification_selected(goal)
    writer_context = writer_context_selected(goal)
    context = context_selected(goal) or writer_context or notification or explicit or fulfillment
    expected = {"headroom_decision": "ADR0312", "decision": "ADR0277",
                "extension_decision": "ADR0277", "profile": "cce_ticket_target",
                "offered_journeys_per_second": rate, "offered_seconds": 300,
                "maximum_experiment_seconds": 3600, "maximum_paid_stages": 1,
                "generator_concurrency": 500, "acquisition_budget": 20,
                "maximum_pods": 19 if scale else 18, "pod_vcpu": 1, "pod_memory_gib": 2,
                "payment_slot_arm": "control" if correction else "candidate", "gateway_read_recovery_decision": "ADR0311"}
    if (any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items())
            or goal.get("headroom_contract_sha256") != policy.digest(contract(goal))
            or goal.get("baseline_sha256") != (__import__("cce_fulfillment_batch_profile").BASELINE if fulfillment else __import__("cce_explicit_start_profile").BASELINE if explicit else __import__("cce_notification_dedup_profile").BASELINE if notification else WRITER_SHA256 if batch or partition or context else SLOT_SHA256 if scale else FAILURE_SHA256 if correction else BASELINE_SHA256)):
        raise ValueError("Exact declared fixed-budget recovery headroom authorization required")
    return True


def require_short(goal):
    if not selected(goal):
        raise ValueError("Explicit recovery headroom prerequisite required")
    from cce_throughput_push import require_reference
    from cce_throughput_push import selected as push_selected
    if push_selected(goal):
        require_reference(goal)
    report = policy.read(policy.ROOT / BASELINE_PATH)
    state = policy.read(policy.STATE)[BASELINE_LEDGER]
    binding = state["binding"]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == BASELINE_LEDGER]
    stage = report.get("paid_stage", {})
    customer = stage.get("customer", {})
    gates = report.get("native", {}).get("measurement_gates", {})
    if (policy.digest(report) != BASELINE_SHA256 or len(entries) != 1
            or entries[0].get("status") != "PASSED_RESTORED"
            or entries[0].get("result_sha256") != BASELINE_SHA256
            or state.get("cce_paid_result_sha256") != BASELINE_SHA256
            or report.get("binding_sha256") != policy.digest(binding)
            or state.get("active_run") or state.get("paid_runs_started") != 1
            or any(report.get(k) is not True for k in
                   ("pass", "restoration_complete", "integrity_verified", "transport_credentials_cleared"))
            or not gates or any(v is not True for v in gates.values())
            or customer.get("scheduled") != 25200 or customer.get("dispatched") != 25200
            or customer.get("fulfilled") != 25200 or customer.get("generator_drops") != 0
            or stage.get("customer_outcome_gate", {}).get("pass") is not True
            or stage.get("financial", {}).get("pass") is not True
            or stage.get("financial", {}).get("hold_deadlines_elapsed") is not True
            or stage.get("global_queues", {}).get("pass") is not True
            or stage.get("short_issuance_window", {}).get("unique_paid_issued_tickets", 0) < 25000):
        raise ValueError("Exact fully passed/restored84/s recovery prerequisite required")
    return BASELINE_PATH, report


def correction_selected(goal):
    return selected(goal) and bool(CORRECTION_KEYS.intersection(goal))


def writer_scale_selected(goal):
    return selected(goal) and bool(SCALE_KEYS.intersection(goal))


def writer_scale_contract():
    from cce_worker_identity import specification
    return {"decision": "ADR0315", "normal_ecs_workers": 14,
            "native_background_workers": 15, "native_counts": specification("ADR0315")["counts"],
            "normal_counts": specification("ADR0298")["counts"],
            "owned_pods": 19, "physical_connections": 24,
            "writer_client_pool_max_per_replica": 12, "api_total_connections": 16}


def require_slot_failure(goal):
    if not writer_scale_selected(goal):
        raise ValueError("Explicit writer scale required")
    report = policy.read(policy.ROOT / "tmp" / SLOT_RUN / "cce-comparison.private.json")
    state = policy.read(policy.STATE)[SLOT_LEDGER]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == SLOT_LEDGER]
    if len(entries) != 1:
        raise ValueError("Exact slot-correction journal required")
    entry = entries[0]
    path = policy.ROOT / "tmp" / SLOT_RUN / "headroom-recovery.private.json"
    recovery = entry.get("recovery", {})
    evidence = policy.read(path)
    if (policy.digest(report) != SLOT_SHA256 or entry.get("result_sha256") != SLOT_SHA256
            or state.get("cce_paid_result_sha256") != SLOT_SHA256
            or report.get("binding_sha256") != policy.digest(state["binding"])
            or entry.get("binding_sha256") != policy.digest(state["binding"])
            or entry.get("status") != "FAILED_RESTORED" or state.get("active_run")
            or report.get("pass") is not False or report.get("restoration_complete") is not True
            or recovery.get("decision") != "ADR0313" or recovery.get("capacity_qualified") is not False
            or recovery.get("sha256") != policy.digest(evidence)
            or recovery.get("evidence") != path.relative_to(policy.ROOT).as_posix()
            or evidence.get("original_result_sha256") != SLOT_SHA256
            or evidence.get("binding_sha256") != report.get("binding_sha256")
            or evidence.get("pass") is not True or evidence.get("capacity_qualified") is not False
            or evidence.get("fresh_financial", {}).get("pass") is not True):
        raise ValueError("Exact independently reconciled2+2failed workload required")
    return path.with_name("cce-comparison.private.json").relative_to(policy.ROOT).as_posix(), report


def require_failure(goal):
    if not correction_selected(goal):
        raise ValueError("Explicit slot correction required")
    report = policy.read(policy.ROOT / "tmp" / FAILURE_RUN / "cce-comparison.private.json")
    scope = policy.read(policy.STATE)[FAILURE_LEDGER]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == FAILURE_LEDGER]
    if len(entries) != 1:
        raise ValueError("Exact failed headroom journal required")
    entry = entries[0]
    recovery = entry.get("recovery", {})
    path = policy.ROOT / "tmp" / FAILURE_RUN / "headroom-recovery.private.json"
    evidence = policy.read(path)
    if (policy.digest(report) != FAILURE_SHA256 or entry.get("result_sha256") != FAILURE_SHA256
            or scope.get("cce_paid_result_sha256") != FAILURE_SHA256
            or entry.get("binding_sha256") != report.get("binding_sha256")
            or report.get("binding_sha256") != policy.digest(scope.get("binding", {}))
            or entry.get("status") != "FAILED_RESTORED"
            or entry.get("initial_status") != "RECOVERY_REQUIRED" or scope.get("active_run")
            or report.get("pass") is not False or report.get("restoration_complete") is not True
            or report.get("capacity_stages_started") != 1 or scope.get("paid_runs_started") != 1
            or recovery.get("decision") != "ADR0313" or recovery.get("capacity_qualified") is not False
            or recovery.get("evidence") != path.relative_to(policy.ROOT).as_posix()
            or recovery.get("sha256") != policy.digest(evidence)
            or evidence.get("original_result_sha256") != FAILURE_SHA256
            or evidence.get("decision") != "ADR0313"
            or evidence.get("binding_sha256") != report.get("binding_sha256")
            or evidence.get("pass") is not True or evidence.get("capacity_qualified") is not False
            or evidence.get("fresh_financial", {}).get("pass") is not True):
        raise ValueError("Independently reconciled failed headroom reference required")
    return path.with_name("cce-comparison.private.json").relative_to(policy.ROOT).as_posix(), report



def writer_batch_selected(goal):
    return selected(goal) and bool(BATCH_KEYS.intersection(goal))


def writer_batch_contract():
    return {"decision": "ADR0316", "baseline_result_sha256": WRITER_SHA256,
            "reservation_writers": 4, "baseline_batch_size": 4, "candidate_batch_size": 8,
            "changed_environment": "RESERVATION_WRITER_BATCH_SIZE",
            "physical_connections": 24, "additional_resources": 0,
            "commit_before_acknowledgment": True, "replay_and_ttl_unchanged": True}


def require_writer_failure(goal):
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    if fulfillment_selected(goal):
        from cce_fulfillment_batch_profile import require_parent as fulfillment_parent
        return fulfillment_parent()
    from cce_explicit_start_profile import selected as explicit_selected
    from cce_notification_dedup_profile import require_parent
    from cce_notification_dedup_profile import selected as notification_selected
    if explicit_selected(goal):
        from cce_explicit_start_profile import require_parent as explicit_parent
        return explicit_parent()
    if notification_selected(goal):
        return require_parent()

    if not selected(goal) or not (BATCH_KEYS|PARTITION_KEYS|{"payment_context_decision", "writer_sale_context_decision"}).intersection(goal):
        raise ValueError("Explicit four-writer-reference comparison required")
    if goal.get("backend_partition_decision") == "ADR0318":
        require_rejected_partition()
    report_path = policy.ROOT / "tmp" / WRITER_RUN / "cce-comparison.private.json"
    report = policy.read(report_path)
    scope = policy.read(policy.STATE)[WRITER_LEDGER]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == WRITER_LEDGER]
    if len(entries) != 1:
        raise ValueError("Exact four-writer failed-reference journal required")
    entry = entries[0]
    evidence_path = report_path.with_name("headroom-recovery.private.json")
    evidence = policy.read(evidence_path)
    receipt = entry.get("recovery", {})
    if (policy.digest(report) != WRITER_SHA256 or entry.get("result_sha256") != WRITER_SHA256
            or scope.get("cce_paid_result_sha256") != WRITER_SHA256
            or entry.get("binding_sha256") != report.get("binding_sha256")
            or report.get("binding_sha256") != policy.digest(scope["binding"])
            or entry.get("status") != "FAILED_RESTORED" or scope.get("active_run")
            or report.get("pass") is not False or report.get("restoration_complete") is not True
            or receipt.get("decision") != "ADR0313" or receipt.get("capacity_qualified") is not False
            or receipt.get("sha256") != policy.digest(evidence)
            or receipt.get("evidence") != evidence_path.relative_to(policy.ROOT).as_posix()
            or evidence.get("original_result_sha256") != WRITER_SHA256
            or evidence.get("binding_sha256") != report.get("binding_sha256")
            or evidence.get("pass") is not True or evidence.get("capacity_qualified") is not False
            or evidence.get("fresh_financial", {}).get("pass") is not True):
        raise ValueError("Exact independently reconciled four-writer failed reference required")
    return report_path.relative_to(policy.ROOT).as_posix(), report

def extend_plan(plan, goal):
    relative, report = require_short(goal)
    correction = correction_selected(goal)
    if correction:
        relative, report = require_failure(goal)
    scale = writer_scale_selected(goal)
    if scale:
        relative, report = require_slot_failure(goal)
    batch = writer_batch_selected(goal)
    if batch:
        relative, report = require_writer_failure(goal)
    from cce_backend_partition import contract as partition_contract
    from cce_backend_partition import selected as partition_selected
    partition = partition_selected(goal)
    if partition:
        relative, report = require_writer_failure(goal)
    from cce_explicit_start_profile import selected as explicit_selected
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    from cce_measured_payment_context_profile import selected as context_selected
    from cce_notification_dedup_profile import selected as notification_selected
    from cce_writer_sale_context_profile import selected as writer_context_selected
    fulfillment = fulfillment_selected(goal)
    explicit = explicit_selected(goal)
    notification = notification_selected(goal)
    writer_context = writer_context_selected(goal)
    context = context_selected(goal) or writer_context or notification or explicit or fulfillment
    if context:
        relative, report = require_writer_failure(goal)
    from cce_throughput_push import require_reference
    from cce_throughput_push import selected as push_selected
    push = push_selected(goal)
    if push:
        relative, report = require_reference(goal)
    rate, shows, expected, target = bounds(goal)
    payment = 2 if correction else 3
    scale_fields = ({"worker_counts": writer_scale_contract()["native_counts"],
                     "maximum_pods": 19, "total_requested_vcpu": 7.75,
                     "total_requested_memory_gib": 15.5,
                     "writer_scale_contract": writer_scale_contract()} if scale else {})
    batch_fields = ({"writer_batch_contract": writer_batch_contract(),
                     "reservation_writer_batch_size": 8} if batch else {})
    partition_fields = ({"backend_partition_contract":partition_contract(goal["backend_partition_decision"]),
                         "backend_partition_decision":goal["backend_partition_decision"]} if partition else {})
    return {**plan, **scale_fields, **batch_fields, **partition_fields, "decision": "ADR0277", "extension_decision": "ADR0277",
            "kind": "fulfillment_transaction_batch_headroom" if fulfillment else "explicit_driver_start_headroom" if explicit else "ticket_notification_dedup_headroom" if notification else "post_lock_writer_sale_context_headroom" if writer_context else "post_lock_payment_context_headroom" if context else "foreground_backend_isolation_headroom" if partition else "writer_batch_headroom" if batch else "writer_concurrency_headroom" if scale else "corrective_general_slot_headroom" if correction else "bounded_recovery_runtime_headroom",
            "single_changed_factor": "offered_workload" if push else "bounded_fulfillment_transaction_grouping" if fulfillment else "one_explicit_driver_transaction_start" if explicit else "bounded_ticket_notification_refresh_suppression" if notification else "post_lock_writer_sale_context" if writer_context else "post_lock_payment_context" if context else "foreground_background_backend_partition" if partition else "RESERVATION_WRITER_BATCH_SIZE" if batch else "reservation_writer_replicas" if scale else "API_PAYMENT_POOL_MAX" if correction else "offered_workload",
            "common": {"buyer_journeys_per_second": rate, "duration_seconds": 300},
            "expected_journeys": expected, "target_unique_tickets": target,
            "fixture_shows": shows, "generator_concurrency": 500,
            "payment_connections_per_api": payment, "general_connections_per_api": 4 - payment,
            "connections_per_api": 4, "baseline_evidence": relative,
            "baseline_sha256": policy.digest(report), "baseline_is_passing_control": push or not correction,
            "slot_correction_decision": "ADR0314" if correction else None,
            "matched_performance_comparison": False, "headroom_contract": contract(goal),
            "duration_reason": f"One{rate}offered/s300s test; require{target}unique paid-issued tickets in-window; unchanged app/resources/budgets and bounded recovery."}


def plan(goal):
    from cce_worker_rebalance_profile import plan as placement_plan
    if not selected(goal):
        raise ValueError("Explicit headroom profile required")
    view = goal if goal.get("_headroom_runtime_view") else runtime_view(goal)
    return placement_plan(view)


def validate_binding(goal, binding):
    require_short(goal)
    old = policy.read(policy.STATE)[BASELINE_LEDGER]["binding"]
    correction = correction_selected(goal)
    scale = writer_scale_selected(goal)
    if correction:
        require_failure(goal)
    if scale:
        require_slot_failure(goal)
        old = policy.read(policy.STATE)[SLOT_LEDGER]["binding"]
    batch = writer_batch_selected(goal)
    if batch:
        require_writer_failure(goal)
        old = policy.read(policy.STATE)[WRITER_LEDGER]["binding"]
    from cce_backend_partition import contract as partition_contract
    from cce_backend_partition import selected as partition_selected
    partition = partition_selected(goal)
    if partition:
        require_writer_failure(goal)
        old = policy.read(policy.STATE)[WRITER_LEDGER]["binding"]
    from cce_explicit_start_profile import selected as explicit_selected
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    from cce_measured_payment_context_profile import contract as context_contract
    from cce_measured_payment_context_profile import selected as context_selected
    from cce_notification_dedup_profile import selected as notification_selected
    from cce_writer_sale_context_profile import selected as writer_context_selected
    fulfillment = fulfillment_selected(goal)
    explicit = explicit_selected(goal)
    notification = notification_selected(goal)
    writer_context = writer_context_selected(goal)
    context = context_selected(goal) or writer_context or notification or explicit or fulfillment
    if context:
        require_writer_failure(goal)
        old = policy.read(policy.STATE)[WRITER_LEDGER]["binding"]
        if fulfillment:
            old = policy.read(policy.STATE)["bounded_cce_ticket_target__e2efadbfaac3"]["binding"]
        elif explicit:
            old = policy.read(policy.STATE)["bounded_cce_ticket_target__88dc5ceb63ad"]["binding"]
        elif notification:
            old = policy.read(policy.STATE)["bounded_cce_ticket_target__384290e57344"]["binding"]
    ignored = {"cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256",
               "baseline_sha256", "cce_verification_contract_sha256", "cce_headroom_contract",
               "cce_gateway_read_recovery_contract", "cce_inventory_renewal_contract",
               "cce_metric_admission_contract_sha256", "cce_throughput_push_contract"}
    if context:
        if fulfillment:
            import cce_fulfillment_batch_identity as context_pins
            from cce_fulfillment_batch_profile import contract as context_contract
            context_binding_key = "cce_fulfillment_batch_contract"
            ignored.add("cce_explicit_start_contract")
        elif explicit:
            import cce_explicit_start_identity as context_pins
            from cce_explicit_start_profile import contract as context_contract
            context_binding_key = "cce_explicit_start_contract"
            ignored.add("cce_notification_dedup_contract")
        elif notification:
            import cce_notification_dedup_identity as context_pins
            from cce_notification_dedup_profile import contract as context_contract
            context_binding_key = "cce_notification_dedup_contract"
            ignored.add("cce_writer_sale_context_contract")
        elif writer_context:
            import cce_writer_sale_context_identity as context_pins
            from cce_writer_sale_context_profile import contract as context_contract
            context_binding_key = "cce_writer_sale_context_contract"
        else:
            import cce_measured_payment_context_identity as context_pins
            context_binding_key = "cce_measured_payment_context_contract"
        import run_cce_paid_comparison as core
        saved = policy.read(policy.ROOT / goal["payment_slot_snapshot_path"])["saved"]
        semantics = core.api_semantics(core.service_for(saved))
        original_semantics = dict(semantics)
        if fulfillment:
            import copy
            original_semantics = copy.deepcopy(semantics)
            if original_semantics["environment"].pop("FULFILLMENT_TRANSACTION_BATCH_SIZE", None) != "1":
                raise ValueError("Fulfillment grouping must stay at one on APIs")
            if original_semantics["environment"].get("DB_EXPLICIT_TRANSACTION_START") != "1":
                raise ValueError("Reference driver mode must stay enabled")
            ignored.add("saved_api_service_sha256")
        elif explicit:
            import copy
            original_semantics = copy.deepcopy(semantics)
            if original_semantics["environment"].pop("DB_EXPLICIT_TRANSACTION_START", "0") != "1":
                raise ValueError("Exactly the selected driver mode must be enabled on APIs")
            ignored.add("saved_api_service_sha256")
        elif notification:
            import copy
            original_semantics = copy.deepcopy(semantics)
            if original_semantics["environment"].pop("ORDER_STATUS_NOTIFICATION_DEDUP", "0") != "0":
                raise ValueError("Notification correction must remain disabled on APIs")
            ignored.add("saved_api_service_sha256")
        expected_image = "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + context_pins.IMAGE_ID
        if (core.native.api_image() != expected_image
                or binding.get(context_binding_key) != context_contract()
                or binding.get("cce_transaction_pair_sha256") != context_pins.RECEIPT_SHA256
                or binding.get("saved_api_service_sha256") != policy.digest(semantics)
                or policy.digest(original_semantics) != old["saved_api_service_sha256"]):
            raise ValueError("Only the reviewed shared image and declared single method may change")
        ignored |= {context_binding_key, "cce_transaction_pair_sha256"}
    if partition:
        ignored.add("cce_backend_partition_contract")
        if binding.get("cce_backend_partition_contract") != partition_contract(goal["backend_partition_decision"]):
            raise ValueError("Exact reserved backend partition required")
        import copy
        from urllib.parse import urlsplit, urlunsplit

        import run_cce_paid_comparison as core
        saved = policy.read(policy.ROOT / goal["payment_slot_snapshot_path"])["saved"]
        semantics = core.api_semantics(core.service_for(saved))
        db = urlsplit(semantics["environment"]["DATABASE_URL"])
        prior = copy.deepcopy(semantics)
        prior["environment"]["DATABASE_URL"] = urlunsplit(db._replace(path="/ticketing"))
        if (db.path != "/ticketing_foreground" or db.username != "ticketing"
                or binding.get("saved_api_service_sha256") != policy.digest(semantics)
                or policy.digest(prior) != old["saved_api_service_sha256"]):
            raise ValueError("Only declared database alias may change API semantics")
        ignored.add("saved_api_service_sha256")
    if batch:
        ignored.add("cce_writer_batch_contract")
        if binding.get("cce_writer_batch_contract") != writer_batch_contract():
            raise ValueError("Exactly bound batch-eight correction required")
    if scale:
        ignored |= {"cce_worker_placement_factor", "cce_writer_scale_contract"}
        from cce_worker_rebalance_profile import factor
        expected_factor = {**factor(), "worker_counts": writer_scale_contract()["native_counts"]}
        if (binding.get("cce_worker_placement_factor") != expected_factor
                or binding.get("cce_writer_scale_contract") != writer_scale_contract()):
            raise ValueError("Exact native-only fourth writer and unchanged physical budget required")
    if correction and not scale:
        ignored |= {"cce_payment_slot_arm", "cce_payment_pool_max",
                    "cce_transaction_arm", "saved_api_service_sha256"}
        import copy

        import run_cce_paid_comparison as core
        saved = policy.read(policy.ROOT / goal["payment_slot_snapshot_path"])["saved"]
        semantics = core.api_semantics(core.service_for(saved))
        if (binding.get("cce_payment_pool_max") != 2
                or binding.get("cce_payment_slot_arm") != "control"
                or binding.get("cce_transaction_arm") != "control"
                or semantics["environment"].get("API_PAYMENT_POOL_MAX") != "2"
                or binding["saved_api_service_sha256"] != policy.digest(semantics)):
            raise ValueError("Exactly2+2API partition required")
        prior = copy.deepcopy(semantics)
        prior["environment"]["API_PAYMENT_POOL_MAX"] = "3"
        if policy.digest(prior) != old["saved_api_service_sha256"]:
            raise ValueError("API service changed outside the declared payment partition")
    if any(old.get(k) != binding.get(k) for k in old.keys() | binding.keys() if k not in ignored):
        raise ValueError("Headroom changed application, recovery, route, resources or database budgets")
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        before, after = old[key], binding[key]
        added = {SOURCE, "scripts/cce_writer_scale_verification.py", "scripts/cce_backend_partition.py", "scripts/cce_backend_partition_runtime.py"}
        added |= {"scripts/cce_measured_payment_context_profile.py", "scripts/cce_measured_payment_context_identity.py",
                  "docs/capacity/cce/measured-payment-context-image-2026-10-14.json"}
        added |= {"scripts/cce_writer_sale_context_profile.py", "scripts/cce_writer_sale_context_identity.py",
                  "docs/capacity/cce/writer-sale-context-image-2026-10-14.json"}
        added |= {"scripts/cce_notification_dedup_profile.py", "scripts/cce_notification_dedup_identity.py",
                  "docs/capacity/cce/ticket-notification-dedup-image-2026-10-14.json"}
        if fulfillment:
            added |= {"scripts/cce_throughput_push.py"}
            added |= {"scripts/cce_fulfillment_batch_profile.py", "scripts/cce_fulfillment_batch_identity.py",
                      "docs/capacity/cce/fulfillment-transaction-batching-local-2026-10-14.json"}
        if explicit:
            added |= {"scripts/cce_explicit_start_profile.py", "scripts/cce_explicit_start_identity.py",
                      "docs/capacity/cce/explicit-transaction-start-image-2026-10-14.json"}
        if (set(after) != set(before) | added
                or any(before[k] != after[k] for k in before if k not in CHANGED_SOURCES)):
            raise ValueError("Headroom source changed outside the declared rate integration")
    from cce_metric_admission import contract as metric_contract
    if binding.get("cce_metric_admission_contract_sha256") != policy.digest(metric_contract()):
        raise ValueError("Exact unchanged-sampling admission diagnostic contract required")
    from cce_inventory_renewal import contract as renewal_contract
    if binding.get("cce_inventory_renewal_contract") != renewal_contract():
        raise ValueError("Exact live renewal and declared worker-count policy required")
    from cce_verification_contract import contract as verification_contract
    from customer_gateway_read_bundle import contract as client_contract
    actual_client, old_client = binding.get("cce_gateway_read_recovery_contract", {}), old["cce_gateway_read_recovery_contract"]
    if (actual_client != client_contract()
            or {k:v for k,v in actual_client.items() if k != "helper_sha256"}
               != {k:v for k,v in old_client.items() if k != "helper_sha256"}):
        raise ValueError("Sealed client or recovery semantics changed")
    if (binding.get("cce_headroom_contract") != contract(goal)
            or binding.get("cce_verification_contract_sha256") != policy.digest(verification_contract())):
        raise ValueError("Exact sealed headroom and financial verification contracts required")


def qualify_workload(parent, coordinator, goal=None):
    from customer_gateway_read_bundle import CLIENT, receipt
    if len(parent) != 80 or sha(parent[CLIENT]) != receipt()["child_client_sha256"]:
        raise ValueError("Unchanged sealed gateway-read client required")
    name = "scripts/paid_ticket_sharded_generator.py"
    from customer_timeout_bundle import MANIFEST
    data = policy.read(policy.ROOT / MANIFEST)
    if (sha(parent[name]) != data["files"]["artifacts/customer-timeout/paid_ticket_sharded_generator.py"]
            or sha(coordinator) != data["files"]["artifacts/customer-timeout/coordinator.py"]):
        raise ValueError("Exact timeout-retaining sharded parent and coordinator required")
    old = b"if not 2 <= args.rate <= 100 or not 1 <= args.seconds <= 300:"
    new = f"if not 2 <= args.rate <= {bounds(goal)[0]} or not 1 <= args.seconds <= 300:".encode()
    if parent[name].count(old) != 1:
        raise ValueError("Exact prior sharded ceiling required")
    source = parent[name].replace(old, new)
    before, after = sha(parent[name]).encode(), sha(source).encode()
    if coordinator.count(before) != 1:
        raise ValueError("Exact coordinator-to-sharded-source binding required")
    return {**parent, name: source}, coordinator.replace(before, after)


def issuance_program(events, start, goal=None):
    import math
    _, shows, expected, target = bounds(goal)
    if (len(events) != shows or len(set(events)) != shows
            or any(str(UUID(v)) != v for v in events)
            or type(start) not in (int, float) or not math.isfinite(start) or start <= 0):
        raise ValueError("Exact declared cohort and finite offered start required")
    from cce_hourly_window import QUERY
    query = QUERY.replace("3598", "298").replace("3600", "300").replace("300000", str(target)).replace("302400", str(expected))
    return "events=" + repr(events) + "\nstart=" + repr(start) + "\n" + query


def require_rejected_partition():
    from cce_backend_partition import PRIOR_PARTITION_RESULT
    from cce_headroom_recovery import PARTITION_LEDGER, PARTITION_RUN, validate
    root = policy.ROOT / "tmp" / PARTITION_RUN
    report = policy.read(root / "cce-comparison.private.json")
    evidence = policy.read(root / "headroom-recovery.private.json")
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == PARTITION_LEDGER]
    scope = policy.read(policy.STATE)[PARTITION_LEDGER]
    if len(entries) != 1:
        raise ValueError("Exact reconciled partition ledger required")
    entry = entries[0]
    receipt = entry.get("recovery", {})
    if (policy.digest(report) != PRIOR_PARTITION_RESULT or entry.get("status") != "FAILED_RESTORED"
            or scope.get("active_run") or receipt.get("sha256") != policy.digest(evidence)
            or receipt.get("evidence") != (root / "headroom-recovery.private.json").relative_to(policy.ROOT).as_posix()
            or receipt.get("decision") != "ADR0313" or receipt.get("capacity_qualified") is not False):
        raise ValueError("Exact independently reconciled rejected allocation required")
    pending = {**entry, "status":"RECOVERY_REQUIRED"}
    retained_scope = {**scope, "active_run":PARTITION_RUN}
    fixture = policy.read(root / "candidate/fixture-identity.json")["fixture_identity"]
    validate(report,pending,retained_scope,evidence,fixture)
    return True
