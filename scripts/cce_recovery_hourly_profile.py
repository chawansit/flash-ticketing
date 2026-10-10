"""ADR0256 qualify the sealed, passing recovery control for one hour."""
import hashlib

import work_envelope as policy
from cce_customer_recovery_profile import PROOF_SHA256, receipt
from customer_recovery_bundle import MANIFEST_SHA256 as RECOVERY_SHA256

BASELINE = "docs/capacity/cce/customer-recovery-comparison-2026-10-10.json"
BASELINE_SHA256 = "4af065617e31ab7081f6fe97f34171447ef3ccab5e5bdc5e729db3d7af140ff0"
MANIFEST = "artifacts/hourly-customer-recovery/manifest.json"
MANIFEST_SHA256 = "30ce66a7434748d27070bd8191560aa03bb618b87d896937d0d276398dd89b72"
ADAPTERS = {"scripts/cce_hourly_paid_generator.py", "scripts/cce_hourly_paid_leaf.py"}


def sealed(name, expected):
    path = policy.ROOT / name
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != expected:
        raise ValueError("Hourly recovery input drift: " + name)
    return policy.read(path)


def qualify_hourly_adapters():
    value = sealed(MANIFEST, MANIFEST_SHA256)
    if (set(value) != {"decision", "recovery_bundle_sha256", "recovery_max_attempts", "duration_seconds",
                       "completion_deadline_seconds", "adapters"}
            or value["decision"] != "ADR0256" or value["recovery_bundle_sha256"] != RECOVERY_SHA256
            or value["recovery_max_attempts"] != 3 or value["duration_seconds"] != 3600
            or value["completion_deadline_seconds"] != 3720 or set(value["adapters"]) != ADAPTERS):
        raise ValueError("Exact hourly recovery adapter receipt required")
    for name, expected in value["adapters"].items():
        path = policy.ROOT / name
        if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != expected:
            raise ValueError("Hourly recovery adapter drift: " + name)
    return value


def hourly_baseline():
    data = sealed(BASELINE, BASELINE_SHA256)
    config, control = data["configuration"], data["control"]
    expected = {"api_replicas": 4, "api_cpu_per_pod": 1, "api_memory_gib_per_pod": 1,
                "connections_per_api": 4, "payment_connections_per_api": 2, "general_connections_per_api": 2,
                "pooler_connections": 24, "shared_acquisition_budget": 20, "simulator_concurrency": 12,
                "simulator_database_pool_max": 10, "recovery_max_attempts": 3,
                "same_image_both_arms": True, "gateway_delay_and_quality_gates_changed": False,
                "api_image_manifest_digest": receipt()["images"]["control"]["registry_manifest_digest"]}
    customer, financial = control["customer"], control["financial"]
    if (any(config.get(k) != v for k, v in expected.items())
            or control.get("pass") is not True or control.get("status") != "PASSED_RESTORED"
            or control.get("offered_journeys_per_second") != 84 or control.get("offered_seconds") != 300
            or any(customer.get(k) != 25200 for k in ("scheduled", "dispatched", "fulfilled", "distinct_tickets"))
            or any(customer.get(k) != 0 for k in ("generator_drops", "final_customer_failures", "first_attempt_error_journeys", "retry_attempts"))
            or financial.get("pass") is not True or financial.get("hold_deadlines_elapsed") is not True
            or any(financial.get(k) != 25200 for k in ("succeeded_payments", "bookings", "tickets"))
            or financial.get("duplicate_booked_seats") != 0 or financial.get("multi_booking_orders") != 0
            or control["global_queues"].get("pass") is not True
            or not control["measurement_gates"] or any(v is not True for v in control["measurement_gates"].values())
            or any(control["restoration"].get(k) is not True for k in ("restoration_complete", "integrity_verified",
                "namespace_removed", "owned_resources_removed", "transport_credentials_cleared"))
            or control["restoration"].get("cleanup_failures") != []):
        raise ValueError("Exact passed and restored recovery control required")
    return data


def factor():
    return {"name": "customer_recovery_hourly", "comparison_arm": "control", "order_status_read_pipeline": "0",
            "image_pair_receipt_sha256": PROOF_SHA256, "recovery_bundle_sha256": RECOVERY_SHA256,
            "hourly_adapter_receipt_sha256": MANIFEST_SHA256, "recovery_max_attempts": 3,
            "database_connections_unchanged": True}


def active(goal):
    if (goal.get("extension_decision") != "ADR0256" or goal.get("decision") != "ADR0232"
            or goal.get("profile") != "cce_hourly_qualification" or goal.get("comparison_arm") != "control"
            or goal.get("acquisition_budget") != 20 or goal.get("candidate_factor") != factor()
            or goal.get("baseline_sha256") != policy.digest(hourly_baseline())):
        raise ValueError("Exact separately authorized hourly recovery qualification required")
    qualify_hourly_adapters()
    return goal


def plan(goal):
    active(goal)
    from cce_simulator_dispatch_profile import receipt as simulator_receipt
    from run_cce_hourly_qualification import historical_plan
    data = historical_plan()
    image = receipt()["images"]["control"]
    data.update(extension_decision="ADR0256", qualification_decision="ADR0256", comparison_arm="control",
                kind="fixed_recovery_hourly_qualification", baseline_evidence=BASELINE,
                published_baseline_evidence=BASELINE, baseline_section="control",
                baseline_sha256=policy.digest(hourly_baseline()), baseline_is_passing_control=True,
                image_pair_receipt_sha256=PROOF_SHA256, arm_manifest_digest=image["registry_manifest_digest"],
                arm_configuration_digest=image["registry_configuration_digest"], arm_api_source_sha256=image["runtime_sources_sha256"],
                common_environment_changes={"DB_FAILURE_DIAGNOSTICS": "1", "ORDER_STATUS_READ_PIPELINE": "0"},
                customer_recovery_max_attempts=3, recovery_bundle_sha256=RECOVERY_SHA256,
                hourly_adapter_receipt_sha256=MANIFEST_SHA256, connections_per_api=4,
                payment_connections_per_api=2, general_connections_per_api=2,
                simulator_image_id=simulator_receipt()["local_image_id"], simulator_concurrency=12,
                simulator_database_pool_max=10,
                duration_reason="Qualify the passing recovery control for one hour with unchanged machines, budgets and gates; mandatory audits and restoration follow.")
    return data
