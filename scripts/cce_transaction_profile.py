"""ADR0242 explicit short API source overlay; unchanged worker/restore contracts."""
import hashlib
import re
from pathlib import Path

import work_envelope as policy

PROOF = "docs/capacity/cce/slot-comparison-images-2026-10-09.json"
PROOF_SHA256 = "255b4d2de1cb356b9b48b0ac7025b0cff5194a961d4c94f179c5ec87f189d0c1"
BASELINE = "docs/capacity/flash-sale-opening/cce-hourly-qualification-2026-10-09.json"


def image_pair():
    from prepare_slot_comparison_sources import pairs
    path = policy.ROOT / PROOF
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != PROOF_SHA256:
        raise ValueError("Exact reviewed registry image pair receipt required")
    proof = policy.read(path)
    _, expected = pairs()
    if (proof["registry_published"] is not True or proof["cloud_load_started"] is not False
            or proof["manifest_media_type"] != "application/vnd.docker.distribution.manifest.v2+json"
            or proof["common_environment_changes"] != {"DB_FAILURE_DIAGNOSTICS": "1"}
            or proof["platform"] != {"os": "linux", "architecture": "amd64"}
            or set(proof["images"]) != {"control", "candidate"}):
        raise ValueError("Complete matched schema2 API receipt required")
    for arm, item in proof["images"].items():
        digest, config = item["registry_manifest_digest"], item["registry_configuration_digest"]
        if (not all(re.fullmatch(r"sha256:[0-9a-f]{64}", v) for v in (digest, config))
                or digest == config
                or item["registry_image"] != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + digest
                or item["runtime_sources_sha256"] != expected["runtime_sources"][arm]
                or len(item["runtime_sources_sha256"]) != 22
                or item["immutable_registry_verification_passed"] is not True
                or item["runtime_user"] != "ticketing"
                or item["registry_pulled_runtime_validation"] != {
                    "pass": True, "copied_installed_imported_bytecode_record_modules": 22,
                    "diagnostics_enabled": True, "payment_confirmation_async": False,
                    "general_database_connections": 2, "payment_database_connections": 2,
                    "customer_dispatches": 0, "cloud_receipt": False}):
            raise ValueError("Exact qualified arm image/source/configuration required")
    return proof


def active(envelope=None):
    envelope = policy.envelope() if envelope is None else envelope
    goal = envelope.get("spending", {}).get("temporary_cce_pilot_exception", {}).get("goal_bounded_authorization", {})
    if goal.get("extension_decision") not in {"ADR0242", "ADR0245"}:
        return None
    arm = goal.get("comparison_arm")
    expected = {"name": "redundant_explicit_begin", "comparison_arm": arm,
                "image_pair_receipt_sha256": PROOF_SHA256, "database_connections_unchanged": True}
    if goal.get("extension_decision") == "ADR0245":
        expected = {"name": "api_payment_pool_partition", "comparison_arm": arm,
                    "baseline_payment_connections": 2, "candidate_payment_connections": 3,
                    "connections_per_api": 4, "image_pair_receipt_sha256": PROOF_SHA256,
                    "database_connections_unchanged": True}
    if (arm not in {"control", "candidate"} or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("acquisition_budget") != 20
            or goal.get("candidate_factor") != expected):
        raise ValueError("Exact short transaction comparison discriminator required")
    image_pair()
    return goal


def image_for(goal):
    """ADR0245 holds the accepted control binary fixed across both partitions."""
    arm = "control" if goal["extension_decision"] == "ADR0245" else goal["comparison_arm"]
    return image_pair()["images"][arm]


def payment_connections(goal):
    return 3 if goal["extension_decision"] == "ADR0245" and goal["comparison_arm"] == "candidate" else 2


def control_receipt(goal):
    ledger, relative = goal.get("control_ledger", ""), goal.get("control_report_path", "")
    if not re.fullmatch(r"bounded_cce_paid_comparison__[0-9a-f]{12}", ledger):
        raise ValueError("Fresh passing control ledger required")
    parts = Path(relative)
    if parts.is_absolute() or ".." in parts.parts or "\\" in relative:
        raise ValueError("Canonical retained control report required")
    path = policy.ROOT / parts
    if (path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve())
            or path.name != "cce-comparison.private.json"):
        raise ValueError("Owned retained control report required")
    report = policy.read(path)
    journal = policy.journal(policy.envelope())
    entry = next((v for v in journal["experiments"] if v["ledger"] == ledger), {})
    scope = policy.read(policy.STATE).get(ledger, {})
    binding = scope.get("binding", {})
    import run_cce_paid_comparison as core
    if (entry.get("status") != "PASSED_RESTORED" or entry.get("profile") != "cce_paid_comparison"
            or entry.get("result_sha256") != policy.digest(report)
            or scope.get("cce_paid_result_sha256") != policy.digest(report)
            or binding.get("cce_transaction_arm") != "control"
            or (goal["extension_decision"] == "ADR0245" and (
                binding.get("cce_partition_decision") != "ADR0245"
                or binding.get("cce_payment_pool_max") != 2))
            or (goal["extension_decision"] == "ADR0242" and binding.get("cce_partition_decision") is not None)
            or binding.get("cce_transaction_pair_sha256") != PROOF_SHA256
            or binding.get("cce_paid_entry_sources") != core.identity()
            or binding.get("cce_paid_core_sources") != core.paid.identity()
            or binding.get("configuration_sha256") != policy.envelope()["existing_resource_configuration_sha256"]
            or binding.get("cce_acquisition_budget") != 20
            or scope.get("active_run") or report.get("pass") is not True
            or report.get("binding_sha256") != policy.digest(binding)
            or scope.get("paid_runs_started") != 1
            or report.get("capacity_stages_started") != 1
            or report.get("restoration_complete") is not True or report.get("integrity_verified") is not True
            or report.get("native", {}).get("cleanup_complete") is not True
            or report.get("transport_credentials_cleared") is not True
            or not report.get("native", {}).get("measurement_gates")
            or any(v is not True for v in report["native"]["measurement_gates"].values())):
        raise ValueError("Fully passed and restored same-pair control required")
    return relative, report


def plan():
    goal = active()
    if goal is None:
        raise ValueError("Explicit ADR0242 short comparison required")
    from cce_api_adapter import legacy_contract
    if goal["comparison_arm"] == "candidate":
        baseline, evidence = control_receipt(goal)
    else:
        baseline, evidence = BASELINE, policy.read(policy.ROOT / BASELINE)
    image = image_for(goal)
    partition = goal["extension_decision"] == "ADR0245"
    return {"decision": "ADR0228", "arms": ["candidate"],
            "extension_decision": goal["extension_decision"], "comparison_arm": goal["comparison_arm"],
            "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
            "kind": "matched_payment_partition" if partition else "matched_transaction_begin", "diagnostic_connection_decision": "ADR0180",
            "single_changed_factor": "api_payment_pool_partition" if partition else "redundant_explicit_begin", "baseline_evidence": baseline,
            "baseline_sha256": policy.digest(evidence), "image_pair_receipt_sha256": PROOF_SHA256,
            "arm_manifest_digest": image["registry_manifest_digest"],
            "arm_configuration_digest": image["registry_configuration_digest"],
            "arm_api_source_sha256": image["runtime_sources_sha256"],
            "common_environment_changes": {"DB_FAILURE_DIAGNOSTICS": "1"},
            "connections_per_api": 4, "payment_connections_per_api": payment_connections(goal),
            "general_connections_per_api": 4 - payment_connections(goal),
            "pod_resources": legacy_contract()["resources"], "replicas": 4,
            "pooler_server_connections": 24, "acquisition_budget": 20, "bridge_cpu_limit": 1,
            "qualification_runs_authorized": 1, "paid_runs_authorized": 1, "safety_tickets_authorized": 2,
            "duration_reason": "Match both five-minute arms; mandatory TTL, durability, drain and restoration are unchanged."}
