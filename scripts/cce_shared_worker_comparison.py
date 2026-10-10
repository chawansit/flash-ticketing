"""ADR0259 fixed worker-placement contract; no deployment or paid dispatch."""
import copy
import hashlib
import ipaddress
import json
import re
from pathlib import Path

from cce_shared_application import ROLES, render
from cce_shared_worker_image_identity import IMAGE, RECEIPT_RELATIVE, RECEIPT_SHA256, SOURCE
from customer_recovery_bundle import MANIFEST_SHA256

ROOT = Path(__file__).resolve().parents[1]
RECEIPT = ROOT / RECEIPT_RELATIVE
COUNTS = {role: count for role, (count, _) in ROLES.items() if count}
GATES = ("customer_load", "post_ttl_financial", "zero_double_booking", "payment_durability",
         "full_queue_drain", "native_observer", "native_cpu", "unchanged_background",
         "unchanged_native_pods")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def selected_receipt():
    if RECEIPT.is_symlink() or hashlib.sha256(RECEIPT.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != RECEIPT_SHA256:
        raise ValueError("Shared-image receipt cannot be a symlink")
    value = json.loads(RECEIPT.read_text())
    if (value.get("registry_image") != IMAGE or value.get("source_identity_sha256") != SOURCE
            or value.get("registry_pulled_runtime_verified") is not True):
        raise ValueError("Exact ADR0261 selected and verified shared image required")
    render(value)  # Validate the complete source map and manifest digest.
    return value


def plan():
    selected_receipt()
    return {
        "decision": "ADR0259", "image_correction_decision": "ADR0261", "single_changed_factor": "worker_placement",
        "arms": {"control": "cce_api_ecs_workers", "candidate": "cce_api_cce_workers"},
        "image": IMAGE, "source_identity_sha256": SOURCE,
        "replicas": dict(COUNTS), "confirmation_replicas": 0,
        "api_resources": {"cpu": "1", "memory": "2Gi"},
        "worker_resources": {"cpu": "250m", "memory": "512Mi"},
        "maximum_pods": 17, "requested_vcpu": 7.25, "requested_memory_gib": 14.5,
        "pgbouncer_server_connections": 24, "connections_per_api": 4,
        "payment_connections_per_api": 2, "api_acquisition_budget": 20,
        "simulator_http_concurrency": 12, "simulator_database_pool": 10,
        "api_keepalive_seconds": 10, "offered_journeys_per_second": 84,
        "offered_seconds": 300, "scheduled_journeys": 25200,
        "generator_concurrency": 500, "generator_shards": 2, "http_clients_per_shard": 8,
        "poll_seconds": 1, "recovery_max_attempts": 3, "recovery_bundle_sha256": MANIFEST_SHA256,
        "maximum_paid_stages": 2, "maximum_experiment_seconds": 3600,
        "mandatory_cleanup_may_exceed_deadline": True,
        "candidate_requires_fresh_passing_control": True,
        "keep_current_profile_gates": True, "gateway_delay_changes": False,
        "inactive_preview_sha256": digest(inactive_candidate("ticketing-placement-preview", "10.1.137.69")),
        "cloud_load_started": False, "capacity_improvement_measured": False,
    }


def arm_binding(arm, role_environment_sha256):
    if arm not in ("control", "candidate"):
        raise ValueError("Explicit comparison arm required")
    if (set(role_environment_sha256) != set(COUNTS)
            or any(not isinstance(v, str) or re.fullmatch(r"[0-9a-f]{64}", v) is None
                   for v in role_environment_sha256.values())):
        raise ValueError("Complete sealed per-role environment hashes required")
    common = plan()
    return {"decision": "ADR0259", "arm": arm, "plan_sha256": digest(common),
            "image": IMAGE, "role_environment_sha256": dict(role_environment_sha256),
            "placement": common["arms"][arm]}


def match_bindings(control, candidate):
    for arm, binding in (("control", control), ("candidate", candidate)):
        if binding != arm_binding(arm, binding.get("role_environment_sha256", {})):
            raise ValueError("Exact current worker-placement binding required")
    if control["role_environment_sha256"] != candidate["role_environment_sha256"]:
        raise ValueError("Per-role behavior changed between placements")
    return True


def inactive_candidate(namespace, primary_private_ip):
    """Produce an inactive preview, correcting benchmark settings and Kafka DNS only."""
    address = ipaddress.ip_address(primary_private_ip)
    if address.version != 4 or not any(
            address in ipaddress.ip_network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):
        raise ValueError("Private primary IPv4 required")
    value = copy.deepcopy(render(selected_receipt(), namespace=namespace))
    for item in value["items"]:
        if item["kind"] != "Deployment":
            continue
        pod = item["spec"]["template"]["spec"]
        container = pod["containers"][0]
        role = container["name"]
        if role == "api":
            container["command"][-1] = "10"
        else:
            pod["hostAliases"] = [{"ip": str(address), "hostnames": ["kafka"]}]
            # Keep callbacks on the same existing private load balancer in both arms.
            for env in container["env"]:
                if env["name"] == "API_URL":
                    env["value"] = "http://" + str(address) + ":8000"
        for env in container["env"]:
            if env["name"] == "ORDER_STATUS_EVENT_REFRESH":
                env["value"] = "1" if role == "consumer" else "0"
        item["metadata"]["annotations"]["ticketing/comparison-decision"] = "ADR0259"
    return value


def require_passed_control(report, binding):
    """Use actual short-run evidence; an old hourly count never grants candidate dispatch."""
    if binding != arm_binding("control", binding.get("role_environment_sha256", {})):
        raise ValueError("Current sealed control binding required")
    native = report.get("native", {})
    gates = native.get("measurement_gates", {})
    if (report.get("comparison_binding") != binding or report.get("pass") is not True
            or report.get("capacity_stages_started") != 1
            or report.get("restoration_complete") is not True
            or report.get("integrity_verified") is not True
            or native.get("cleanup_complete") is not True
            or report.get("transport_credentials_cleared") is not True
            or set(gates) != set(GATES) or any(gates[k] is not True for k in GATES)):
        raise ValueError("Fresh fully passing and restored five-minute control required")
    return {"control_result_sha256": digest(report), "control_binding_sha256": digest(binding)}
