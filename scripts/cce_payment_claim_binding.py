"""ADR0342 local proposal through existing builders; never a paid admission."""
import base64
import copy
import hashlib
import json

import cce_api_adapter as api
import cce_api_client_headroom as clients
import cce_fulfillment_batch_profile as parent
import cce_native_workers as workers
import cce_recovery_headroom as headroom
import cce_worker_identity as roles
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_160

LOCAL = "docs/capacity/cce/payment-claim-cutoff-reproducible-local-2026-10-17.json"
SOURCE = "046b8b0fe4ea571822fc2f3e17af49dac61bf216acc6d185e7c6efabf25364b5"
IMAGE = "sha256:730bff63781ccffa1aa8ab081e5ab73621d01ea86269d4e6aa74846857e57ff4"
FIXED = {"offered_rate": 160, "offered_seconds": 300, "maximum_experiment_seconds": 3600,
         "proposed_paid_arms": 2, "api_pods": 4, "worker_pods": 15, "physical_backends": 24,
         "api_general_slots": 3, "api_payment_slots": 3, "shared_admission": 20,
         "aggregate_api_client_slots": 24, "physical_backend_reserve": 0, "acquisition_wait_ms": 500,
         "simulator_delivery_slots": 16, "simulator_database_pool_max": 10,
         "generator_processes": 4, "active_journeys": 800, "http_connections": 500, "http_clients": 16}
SOURCES = ("scripts/cce_payment_claim_binding.py", "scripts/run_cce_paid_comparison.py",
           "scripts/cce_api_adapter.py", "scripts/cce_native_workers.py",
           "scripts/prepare_payment_claim_cutoff_image.py")


def source_binding():
    return {name: hashlib.sha256((policy.ROOT / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for name in SOURCES}


def images():
    """Validate local candidate against the full measured parent; no invented registry digest."""
    control = parent.image()
    value = policy.read(policy.ROOT / LOCAL)
    module = "src/ticketing/workers.py"
    raw = (policy.ROOT / module).read_bytes().replace(b"\r\n", b"\n")
    old, new = b"p.due_at<=clock_timestamp()", b"p.due_at<=statement_timestamp()"
    if raw.count(new) != 1 or hashlib.sha256(raw.replace(new, old)).hexdigest() != control["runtime_source_sha256"][module]:
        raise ValueError("Exact sole claim predicate delta required")
    sources = {**control["runtime_source_sha256"], module: hashlib.sha256(raw).hexdigest()}
    identity = hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest()
    recipe = policy.ROOT / "scripts/prepare_payment_claim_cutoff_image.py"
    if (value.get("decision") != "ADR0341" or value.get("source_identity_sha256") != SOURCE
            or identity != SOURCE or value.get("local_image_id") != IMAGE
            or value.get("parent_registry_image") != control["registry_image"]
            or value.get("preparation_recipe_sha256") != hashlib.sha256(recipe.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            or value.get("built") is not True or value.get("registry_published") is not False
            or value.get("validation") != {"modules_verified": 22, "imported_worker_matches": True, "lease_clock_preserved": True}
            or value.get("network_disabled_build_and_verification") is not True):
        raise ValueError("Exact reproducible locally qualified image required")
    return {"control": {**control, "runtime_sources_sha256": control["runtime_source_sha256"]},
            "candidate": {"registry_image": IMAGE, "local_image_id": IMAGE,
                          "runtime_source_sha256": sources, "runtime_sources_sha256": sources,
                          "source_identity_sha256": SOURCE, "local_only": True}}


def prepare(goal, service, environments, primary_ip):
    """Private result contains Secret objects. Retain only public_summary(result)."""
    if not clients.selected(goal) or goal.get("_headroom_runtime_view") or goal.get("throughput_push_decision") != "ADR0329":
        raise ValueError("Exact qualified 160/s API pool configuration required")
    pinned = images()
    runtime = headroom.runtime_view(goal)
    arms = {}
    for arm, image in pinned.items():
        objects = api.objects("adr0151-034200000000", service, primary_ip, "local-preview", "local-preview",
                              acquisition_budget=20, profile=RATE_PUSH_160, goal=runtime, image_receipt=image)
        objects += workers.objects("adr0151-034200000000", environments, primary_ip, "ADR0324",
                                   profile=RATE_PUSH_160, goal=runtime, image_receipt=image)
        arms[arm] = {"image": image["registry_image"], "runtime_sources": image["runtime_source_sha256"],
                     "objects": objects, "manifest_sha256": policy.digest(objects)}
    result = {"decision": "ADR0342", "local_only": True, "deployment_admitted": False,
              "fixed": copy.deepcopy(FIXED), "preparation_sources": source_binding(),
              "stop_after_failed_control": True, "arms": arms}
    validate_local(result)
    return result


def validate_local(result):
    if (result.get("decision") != "ADR0342" or result.get("local_only") is not True
            or result.get("deployment_admitted") is not False or result.get("fixed") != FIXED
            or any(type(result["fixed"][k]) is not type(v) for k, v in FIXED.items())
            or result.get("preparation_sources") != source_binding()
            or result.get("stop_after_failed_control") is not True
            or set(result.get("arms", {})) != {"control", "candidate"}):
        raise ValueError("Exact local-only fixed-budget proposal required")
    pinned = images()
    canonical = {}
    for arm, row in result["arms"].items():
        image = pinned[arm]
        if (row["image"] != image["registry_image"] or row["runtime_sources"] != image["runtime_source_sha256"]
                or row["manifest_sha256"] != policy.digest(row["objects"])):
            raise ValueError("Bound image/source/manifest identity differs")
        pods = [o for o in row["objects"] if o["kind"] == "Pod"]
        expected = {"api": 4, **roles.specification("ADR0324")["counts"]}
        counts = {}
        for pod in pods:
            container = pod["spec"]["containers"][0]
            role = pod["metadata"].get("labels", {}).get("codex-worker-role", "api")
            counts[role] = counts.get(role, 0) + 1
            resources = ({k: {"cpu": "1", "memory": "2Gi"} for k in ("requests", "limits")}
                         if role == "api" else roles.RESOURCES)
            if container["image"] != row["image"] or container["resources"] != resources:
                raise ValueError("Role image or resources differ")
            if repr(row["runtime_sources"]) not in container["command"][-1]:
                raise ValueError("Startup source verification missing")
        if counts != expected:
            raise ValueError("Exactly four API and fifteen background pods required")
        secrets = {o["metadata"]["name"]: o for o in row["objects"] if o["kind"] == "Secret"}
        decoded = {name: {k: base64.b64decode(v).decode() for k, v in secret["data"].items()}
                   for name, secret in secrets.items()}
        for name, settings in {
            "api-env": {"DB_POOL_MAX": "6", "API_PAYMENT_POOL_MAX": "3", "DB_POOL_MAX_WAITING": "20",
                        "DB_POOL_WAIT_MS": "500", "API_POOL_SHARED_WAITING": "1"},
            "simulator-env": {"DB_POOL_MAX": "10", "SIMULATOR_CONCURRENCY": "16"},
            "consumer-env": {"FULFILLMENT_TRANSACTION_BATCH_SIZE": "4", "ORDER_STATUS_COMMITTED_SNAPSHOT": "1"},
        }.items():
            if any(decoded[name].get(k) != v for k, v in settings.items()):
                raise ValueError("Generated admission, simulator or fulfillment settings drifted")
        normalized = copy.deepcopy(row["objects"])
        for obj in normalized:
            if obj["kind"] == "Pod":
                container = obj["spec"]["containers"][0]
                container["image"] = pinned["control"]["registry_image"]
                container["command"][-1] = container["command"][-1].replace(
                    repr(row["runtime_sources"]), repr(pinned["control"]["runtime_source_sha256"]))
        canonical[arm] = normalized
    if canonical["control"] != canonical["candidate"]:
        raise ValueError("Arms differ outside exact image and startup source map")
    return True


def public_summary(result):
    validate_local(result)
    return {k: result[k] for k in ("decision", "local_only", "deployment_admitted", "fixed", "preparation_sources", "stop_after_failed_control")} | {
        "arms": {arm: {k: row[k] for k in ("image", "runtime_sources", "manifest_sha256")}
                 for arm, row in result["arms"].items()},
        "local_runner_binding_ready": True, "registry_verified": False, "live_runner_registration_ready": False,
        "cloud_measured": False, "live_snapshot_reconciled": False}


def require_dispatch_admission(result):
    validate_local(result)
    raise ValueError("LOCAL_ONLY: registry identity, live registration and fresh paid authorization required")
