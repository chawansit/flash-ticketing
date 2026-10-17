"""ADR0337 one explicit delivery-concurrency correction; unchanged application image."""
import cce_auth_dispatch_profile as previous
import work_envelope as policy
from cce_auth_dispatch_profile import file_sha, reference
from cce_fulfillment_batch_profile import image

SOURCE = "scripts/cce_payment_delivery_headroom.py"
LOCAL = "docs/capacity/cce/payment-delivery-headroom-local-2026-10-17.json"
REFERENCE = previous.REFERENCE
KEYS = {"payment_delivery_decision", "payment_delivery_receipt_sha256", "payment_delivery_contract_sha256"}
CHANGED = previous.CHANGED | {SOURCE, "scripts/cce_native_workers.py"}
ADDED = previous.ADDED | {SOURCE, LOCAL}


def contract():
    return {"decision":"ADR0337", "baseline_delivery_slots":16, "candidate_delivery_slots":32,
            "simulator_database_pool_max":10, "physical_backends":24, "api_pools":[2,2],
            "shared_admission":20, "api_pods":4, "worker_pods":15,
            "offered_rate":160, "seconds":300, "active_journeys":800,
            "http_connections":500, "http_clients":16, "generator_processes":4,
            "maximum_paid_stages":1, "maximum_experiment_seconds":3600,
            "image_manifest":image()["registry_manifest_digest"],
            "local_receipt_sha256":file_sha(LOCAL), "reference_result_sha256":REFERENCE,
            "gateway_delay_unchanged":True, "application_unchanged":True,
            "sources":{p:file_sha(p) for p in sorted(CHANGED)}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if {"auth_dispatch_decision", "auth_dispatch_receipt_sha256", "auth_dispatch_contract_sha256"}.intersection(goal):
        raise ValueError("Payment delivery comparison must use unchanged ADR0324 APIs")
    view = goal.get("_headroom_runtime_view") is True
    expected = {
        "payment_delivery_decision": "ADR0337",
        "payment_delivery_receipt_sha256": file_sha(LOCAL),
        "payment_delivery_contract_sha256": policy.digest(contract()),
        "generator_process_decision": "ADR0335",
        "generator_concurrency": 800,
        "throughput_push_decision": "ADR0329",
        "fulfillment_batch_decision": "ADR0324",
        "offered_journeys_per_second": 84 if view else 160,
        "offered_seconds": 300,
        "profile": "cce_paid_comparison" if view else "cce_ticket_target",
        "extension_decision": "ADR0271" if view else "ADR0277",
        "maximum_paid_stages": 1,
        "maximum_experiment_seconds": 3600,
        "maximum_pods": 18 if view else 19,
        "acquisition_budget": 20,
        "pod_vcpu": 1,
        "pod_memory_gib": 2,
        "comparison_arm": "correction",
        "committed_status_arm": "candidate",
        "payment_slot_arm": "control",
        "primary_bootstrap_decision": "ADR0330",
        "fixture_clock_decision": "ADR0334",
    }
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact source-bound payment delivery comparison required")
    return True


def extend_plan(goal, plan):
    if not selected(goal):
        return plan
    path, report, _ = reference()
    return {**plan, "kind":"payment_delivery_headroom_comparison",
            "single_changed_factor":"simulator_delivery_concurrency",
            "baseline_evidence":path, "baseline_sha256":policy.digest(report),
            "baseline_is_passing_control":False, "matched_performance_comparison":True,
            "simulator_concurrency":32, "payment_delivery_contract":contract(),
            "duration_reason":"One160/s300s configuration-only delivery-headroom comparison; unchanged SQL/image/gateway budgets."}


def environment(role, values, goal):
    if not selected(goal):
        return values
    if role != "simulator":
        return values
    if any(values.get(k) != v for k,v in {"SIMULATOR_CONCURRENCY":"16", "DB_POOL_MAX":"10", "SIMULATOR_DISPATCH_MODE":"refill"}.items()):
        raise ValueError("Exact sixteen-slot simulator baseline required")
    return {**values, "SIMULATOR_CONCURRENCY":"32"}


def validate_binding(goal, binding):
    if not selected(goal):
        raise ValueError("Explicit payment delivery overlay required")
    image()
    _, _, old = reference()
    if binding.get("cce_payment_delivery_contract") != contract() or binding.get("baseline_sha256") != REFERENCE:
        raise ValueError("Payment delivery image/reference must be reserved before deployment")
    if binding.get("cce_simulator_concurrency") != 32:
        raise ValueError("Exactly32 delivery slots must be reserved")
    varied = {
        "cce_simulator_concurrency",
        "cce_payment_delivery_contract",
        "baseline_sha256",
        "cce_paid_entry_sources",
        "cce_paid_core_sources",
        "cce_manifest_sha256",
        "cce_headroom_contract",
        "cce_throughput_push_contract",
        "cce_generator_process_contract",
        "cce_verification_contract_sha256",
        "cce_primary_bootstrap_contract",
    }
    if any(old.get(k) != binding.get(k) for k in old.keys() | binding.keys() if k not in varied):
        raise ValueError("Payment delivery comparison changed resources, settings, worker image or budgets")
    import cce_paid_stage as paid
    import run_cce_paid_comparison as core

    for key, expected in (
        ("cce_paid_entry_sources", core.identity()),
        ("cce_paid_core_sources", paid.identity()),
    ):
        before, after = old[key], binding.get(key, {})
        if (
            after != expected
            or set(after) != set(before) | ADDED
            or any(before[k] != after[k] for k in before if k not in CHANGED)
        ):
            raise ValueError("Payment delivery runner source changed outside the reviewed image integration")
    import cce_generator_process_scaling as generator
    import cce_primary_bootstrap as bootstrap
    import cce_recovery_headroom as headroom
    import cce_throughput_push as push
    import cce_verification_contract as verification

    if not headroom.selected(goal):
        raise ValueError("Full fixed-budget parent profile required")
    generator.validate_binding(goal, binding)
    expected = {
        "cce_headroom_contract": headroom.contract(goal),
        "cce_throughput_push_contract": push.contract(goal),
        "cce_generator_process_contract": generator.contract(goal),
        "cce_verification_contract_sha256": policy.digest(verification.contract()),
        "cce_primary_bootstrap_contract": bootstrap.contract(),
    }
    if any(binding.get(k) != v for k, v in expected.items()):
        raise ValueError("Fresh source-bound payment delivery comparison contracts required")
    before, after = old["cce_primary_bootstrap_contract"], binding["cce_primary_bootstrap_contract"]
    if (
        {k: v for k, v in before.items() if k != "sources"}
        != {k: v for k, v in after.items() if k != "sources"}
        or set(before["sources"]) != set(after["sources"])
        or any(before["sources"][k] != after["sources"][k] for k in before["sources"] if k not in CHANGED)
    ):
        raise ValueError("ECS bootstrap contract must remain unchanged")
    return True
