"""ADR0338 one explicit API-client-budget correction; unchanged application image."""
import cce_payment_delivery_headroom as previous
import work_envelope as policy
from cce_auth_dispatch_profile import file_sha, reference
from cce_fulfillment_batch_profile import image

SOURCE = "scripts/cce_api_client_headroom.py"
LOCAL = "docs/capacity/cce/api-client-pool-headroom-local-2026-10-17.json"
REFERENCE = previous.REFERENCE
KEYS = {"api_client_pool_decision", "api_client_pool_receipt_sha256", "api_client_pool_contract_sha256"}
CHANGED = previous.CHANGED | {SOURCE, "scripts/cce_api_adapter.py"}
ADDED = previous.ADDED | {SOURCE, LOCAL}


def contract(goal=None):
    return {"decision":"ADR0338", "baseline_api_client_slots":[2,2], "candidate_api_client_slots":[3,3],
            "aggregate_api_client_slots":24, "physical_backends":24, "shared_admission":20,
            "native_role_acquisition_limit":17, "acquisition_wait_ms":500,
            "api_pods":4, "worker_pods":15, "simulator_delivery_slots":16,"simulator_database_pool_max":10,
            "offered_rate":180 if goal is not None and goal.get("throughput_push_decision") == "ADR0339" else 160, "seconds":300, "active_journeys":800,
            "http_connections":500, "http_clients":16, "generator_processes":4,
            "maximum_paid_stages":1, "maximum_experiment_seconds":3600,
            "image_manifest":image()["registry_manifest_digest"],
            "local_receipt_sha256":file_sha(LOCAL), "reference_result_sha256":"5ec10ec764bc349911b28e2c5d20883eb7c2f1df58a02bc55e792b181ccaf05b" if goal is not None and goal.get("throughput_push_decision") == "ADR0339" else REFERENCE,
            "gateway_delay_unchanged":True, "application_unchanged":True,
            "sources":{p:file_sha(p) for p in sorted(CHANGED)}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if {"auth_dispatch_decision", "auth_dispatch_receipt_sha256", "auth_dispatch_contract_sha256", "payment_delivery_decision", "payment_delivery_receipt_sha256", "payment_delivery_contract_sha256"}.intersection(goal):
        raise ValueError("API client comparison must use unchanged image and sixteen delivery slots")
    view = goal.get("_headroom_runtime_view") is True
    expected = {
        "api_client_pool_decision": "ADR0338",
        "api_client_pool_receipt_sha256": file_sha(LOCAL),
        "api_client_pool_contract_sha256": policy.digest(contract(goal)),
        "generator_process_decision": "ADR0335",
        "generator_concurrency": 800,
        "throughput_push_decision": "ADR0339" if goal.get("throughput_push_decision") == "ADR0339" else "ADR0329",
        "fulfillment_batch_decision": "ADR0324",
        "offered_journeys_per_second": 84 if view else contract(goal)["offered_rate"],
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
    if goal.get("api_client_pool_contract_sha256") != policy.digest(contract(goal)):
        raise ValueError("Exact source-bound API client pool contract required")
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact source-bound API client pool comparison required")
    return True


def extend_plan(goal, plan):
    if not selected(goal):
        return plan
    if goal.get("throughput_push_decision") == "ADR0339":
        from cce_throughput_push import require_reference
        path, report = require_reference(goal)
    else:
        path, report, _ = reference()
    return {**plan, "kind":"api_client_pool_headroom_comparison",
            "single_changed_factor":"offered_workload_160_to_180" if goal.get("throughput_push_decision") == "ADR0339" else "equal_API_client_pools_2plus2_to_3plus3",
            "baseline_evidence":path, "baseline_sha256":policy.digest(report),
            "baseline_is_passing_control":goal.get("throughput_push_decision") == "ADR0339", "matched_performance_comparison":True,
            "connections_per_api":6, "payment_connections_per_api":3, "general_connections_per_api":3, "api_client_pool_contract":contract(goal),
            "duration_reason":f"One{contract(goal)['offered_rate']}offered/s300s comparison; fixed24 physical backends, unchanged image/gateway/workers."}


def api_settings(values, goal):
    if not selected(goal):
        return values
    if values.get("DB_POOL_MAX") != "4" or values.get("API_PAYMENT_POOL_MAX") != "2":
        raise ValueError("Exact2+2 API client baseline required")
    return {**values, "DB_POOL_MAX":"6", "API_PAYMENT_POOL_MAX":"3"}


def service_hashes(goal):
    import copy

    import run_cce_paid_comparison as core

    snapshot = policy.read(policy.ROOT / goal["payment_slot_snapshot_path"])["saved"]
    current = core.api_semantics(core.service_for(snapshot))
    if current["environment"].get("DB_POOL_MAX") != "6" or current["environment"].get("API_PAYMENT_POOL_MAX") != "3":
        raise ValueError("Generated API service must have exactly3+3 clients")
    prior = copy.deepcopy(current)
    prior["environment"].update(DB_POOL_MAX="4", API_PAYMENT_POOL_MAX="2")
    return policy.digest(current), policy.digest(prior)


def validate_binding(goal, binding):
    if not selected(goal):
        raise ValueError("Explicit API client pool overlay required")
    image()
    if goal.get("throughput_push_decision") == "ADR0339":
        return validate_rate_binding(goal, binding)
    _, _, old = reference()
    if binding.get("cce_api_client_pool_contract") != contract(goal) or binding.get("baseline_sha256") != REFERENCE:
        raise ValueError("API client pool image/reference must be reserved before deployment")
    if binding.get("cce_api_client_pool_max") != 6 or binding.get("cce_payment_pool_max") != 3:
        raise ValueError("Exactly3+3 API client slots must be reserved")
    varied = {
        "saved_api_service_sha256",
        "cce_api_client_pool_max",
        "cce_payment_pool_max",
        "cce_api_client_pool_contract",
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
    drift = sorted(k for k in old.keys() | binding.keys() if k not in varied and old.get(k) != binding.get(k))
    if drift:
        raise ValueError("API client pool comparison changed resources, settings, worker image or budgets: " + ",".join(drift))
    after_service, before_service = service_hashes(goal)
    if binding.get("saved_api_service_sha256") != after_service or old.get("saved_api_service_sha256") != before_service:
        raise ValueError("API service differs outside exact4/2-to6/3 client settings")
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
            raise ValueError("API client pool runner source changed outside the reviewed image integration")
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
        raise ValueError("Fresh source-bound API client pool comparison contracts required")
    before, after = old["cce_primary_bootstrap_contract"], binding["cce_primary_bootstrap_contract"]
    if (
        {k: v for k, v in before.items() if k != "sources"}
        != {k: v for k, v in after.items() if k != "sources"}
        or set(before["sources"]) != set(after["sources"])
        or any(before["sources"][k] != after["sources"][k] for k in before["sources"] if k not in CHANGED)
    ):
        raise ValueError("ECS bootstrap contract must remain unchanged")
    return True


def validate_rate_binding(goal, binding):
    """ADR0339 changes only workload and reviewed orchestration source bindings."""
    import cce_generator_process_scaling as generator
    import cce_paid_stage as paid
    import cce_primary_bootstrap as bootstrap
    import cce_recovery_headroom as headroom
    import cce_throughput_push as push
    import cce_verification_contract as verification
    import run_cce_paid_comparison as core

    _, report = push.require_reference(goal)
    old = policy.read(policy.STATE)[push.reference(goal)[1]]["binding"]
    varied = {
        "baseline_sha256", "cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256",
        "cce_headroom_contract", "cce_throughput_push_contract", "cce_generator_process_contract",
        "cce_api_client_pool_contract", "cce_primary_bootstrap_contract", "cce_verification_contract_sha256",
    }
    drift = sorted(k for k in old.keys() | binding.keys() if k not in varied and old.get(k) != binding.get(k))
    if drift:
        raise ValueError("180/s changed image, settings, resources or database budgets: " + ",".join(drift))
    expected = {
        "baseline_sha256": policy.digest(report),
        "cce_api_client_pool_contract": contract(goal),
        "cce_headroom_contract": headroom.contract(goal),
        "cce_throughput_push_contract": push.contract(goal),
        "cce_generator_process_contract": generator.contract(goal),
        "cce_primary_bootstrap_contract": bootstrap.contract(),
        "cce_verification_contract_sha256": policy.digest(verification.contract()),
        "cce_api_client_pool_max": 6, "cce_payment_pool_max": 3,
    }
    if any(binding.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact source-bound180/s budgets and passing baseline required")
    allowed = {"scripts/" + n for n in push.SOURCES} | {SOURCE, generator.SOURCE, "artifacts/cce-transaction-runner/manifest.json"}
    for key, current in (("cce_paid_entry_sources", core.identity()), ("cce_paid_core_sources", paid.identity())):
        before, after = old[key], binding.get(key, {})
        if (after != current or set(after) != set(before)
                or any(before[k] != after[k] for k in before if k not in allowed)):
            raise ValueError("180/s runner source changed outside reviewed workload integration: " + ",".join(sorted(
                set(before) ^ set(after) | {k for k in before.keys() & after.keys() if before[k] != after[k] and k not in allowed})))
    before, after = old["cce_primary_bootstrap_contract"], binding["cce_primary_bootstrap_contract"]
    if ({k:v for k,v in before.items() if k != "sources"} != {k:v for k,v in after.items() if k != "sources"}
            or set(before["sources"]) != set(after["sources"])
            or any(before["sources"][k] != after["sources"][k] for k in before["sources"] if k not in allowed)):
        raise ValueError("180/s bootstrap semantics changed")
    return True
