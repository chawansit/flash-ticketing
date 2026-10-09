"""ADR0251 simulator-only correction on the existing bounded CCE runner."""
import copy
import hashlib
import re

import generator_completion_probe_contract as baseline
import work_envelope as policy
from cce_api_adapter import legacy_contract

PROOF = "docs/capacity/cce/simulator-dispatch-image-2026-10-09.json"
PROOF_SHA256 = "aede238a4a8f56a565fac1003633d6df8947f42d9a9aa8704240ac5f1857bc4b"
BASELINE = "docs/capacity/cce/payment-context-control-2026-10-09.json"
FACTOR = "callback_dispatch_concurrency"
HOURLY_BASELINE = 'docs/capacity/cce/simulator-dispatch-comparison-2026-10-10.json'
HOURLY_BASELINE_SHA256 = '47bc9c0038fb80df9b6b0eba7b7f004786f15ea156b7efafad3ff6c1e296ff48'


def receipt():
    from status_refresh_contract import REVISION, digest
    path = policy.ROOT / PROOF
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != PROOF_SHA256:
        raise ValueError("Exact reviewed simulator image receipt required")
    data = policy.read(path)
    parent = data["parent_runtime_source_sha256"]
    candidate = data["runtime_source_sha256"]
    config = "src/ticketing/config.py"
    if (data["decision"] != "ADR0251" or data["pass"] is not True
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", data["local_image_id"])
            or data["parent_image_id"] != "sha256:271e1a8f91505d06e208c680ee479f38b5acc9641f6089aef61e3d070257a159"
            or set(parent) != set(candidate) or len(candidate) != 21
            or [name for name in parent if parent[name] != candidate[name]] != [config]
            or candidate[config] != "20d35f8e00aed40c60dcfa5482090e0923dd34db35b9b3e6df0b8297b1c9966c"
            or data["source_manifest_sha256"] != digest({"base_revision": REVISION, "runtime_source_sha256": candidate})
            or data["copied_installed_imported_bytecode_modules"] != 21
            or data["inherited_runtime_configuration_verified"] is not True
            or data["parent_layers_verified"] is not True
            or data["runtime_settings"] != {"simulator_concurrency": 12, "database_pool_max": 10, "pass": True}
            or data["registry_published"] is not False or data["cloud_load_started"] is not False
            or data["capacity_improvement_measured"] is not False):
        raise ValueError("Qualified simulator-only runtime required")
    return data


def active(goal):
    from cce_transaction_profile import PROOF_SHA256 as api_proof
    expected = {"name": FACTOR, "comparison_arm": "correction", "baseline": 8, "candidate": 12,
                "simulator_database_pool_max": 10, "image_pair_receipt_sha256": api_proof,
                "simulator_image_receipt_sha256": PROOF_SHA256, "database_connections_unchanged": True}
    decisions = {"cce_paid_comparison": "ADR0228", "cce_hourly_qualification": "ADR0232"}
    if (goal.get("comparison_arm") != "correction" or goal.get("profile") not in decisions
            or goal.get("decision") != decisions.get(goal.get("profile")) or goal.get("acquisition_budget") != 20
            or goal.get("candidate_factor") != expected):
        raise ValueError("Exact simulator correction discriminator required")
    receipt()
    return goal


def plan(goal):
    from cce_transaction_profile import image_pair
    active(goal)
    image = image_pair()["images"]["control"]
    evidence = policy.read(policy.ROOT / BASELINE)
    return {"decision": "ADR0228", "arms": ["candidate"], "extension_decision": "ADR0251",
            "comparison_arm": "correction", "kind": "simulator_dispatch_correction",
            "single_changed_factor": FACTOR,
            "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
            "diagnostic_connection_decision": "ADR0180", "baseline_evidence": BASELINE,
            "baseline_sha256": policy.digest(evidence), "baseline_is_passing_control": False,
            "image_pair_receipt_sha256": PROOF_SHA256,
            "arm_manifest_digest": image["registry_manifest_digest"],
            "arm_configuration_digest": image["registry_configuration_digest"],
            "arm_api_source_sha256": image["runtime_sources_sha256"],
            "simulator_image_id": receipt()["local_image_id"], "simulator_concurrency": 12,
            "simulator_database_pool_max": 10,
            "common_environment_changes": {"DB_FAILURE_DIAGNOSTICS": "1"},
            "connections_per_api": 4, "payment_connections_per_api": 2, "general_connections_per_api": 2,
            "pod_resources": legacy_contract()["resources"], "replicas": 4,
            "pooler_server_connections": 24, "acquisition_budget": 20, "bridge_cpu_limit": 1,
            "qualification_runs_authorized": 1, "paid_runs_authorized": 1, "safety_tickets_authorized": 2,
            "duration_reason": "One fresh corrected qualification; the failed reference remains failed. Mandatory checks and restoration are unchanged."}


class SimulatorDispatchContract(baseline.GeneratorCompletionProbeContract):
    def __init__(self, artifact, arm, sources):
        if arm != "candidate":
            raise ValueError("Fresh simulator correction only; no replay of an old control")
        super().__init__(artifact, arm, sources)
        self.simulator_receipt = receipt()
        from prepare_simulator_dispatch_sources import source_plan
        _, expected = source_plan()
        if (self.simulator_receipt["runtime_source_sha256"] != expected["runtime_source_sha256"]
                or self.simulator_receipt["parent_runtime_source_sha256"] != expected["parent_runtime_source_sha256"]):
            raise ValueError("Complete prepared simulator source plan differs")
        self.background = copy.deepcopy(self.background)
        self.background["simulator"]["concurrency"] = 12
        self.images["simulator"] = self.simulator_receipt["local_image_id"]

    def settings(self, role):
        values = super().settings(role)
        if role == "simulator":
            values.update(SIMULATOR_CONCURRENCY="12", SIMULATOR_DISPATCH_MODE="refill", DB_POOL_MAX="10")
        return values

    def source_map(self, role):
        if role == "simulator":
            return copy.deepcopy(self.simulator_receipt["runtime_source_sha256"])
        return super().source_map(role)

    def image_manifest(self, role):
        if role == "simulator":
            return self.simulator_receipt["source_manifest_sha256"]
        return super().image_manifest(role)

    def staging_contracts(self):
        return (self,)

    def inventory_marker(self):
        return {**super().inventory_marker(), "correction_decision": "ADR0251", "correction_factor": FACTOR,
                "simulator_concurrency": 12, "simulator_database_pool_max": 10,
                "simulator_image_id": self.images["simulator"], "simulator_receipt_sha256": PROOF_SHA256}


def worker_contract(goal=None):
    if goal is not None:
        active(goal)
    data = baseline.plan()
    return SimulatorDispatchContract(data["artifact_receipt"], "candidate", legacy_contract()["api_sources"])


def hourly_baseline():
    """Require the exact reconciled short evidence; never substitute an old image's pass."""
    path = policy.ROOT / HOURLY_BASELINE
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != HOURLY_BASELINE_SHA256:
        raise ValueError("Exact passing simulator correction evidence required")
    data = policy.read(path)
    config = data["configuration"]
    if (data.get("pass") is not True or data.get("status") != "PASSED_RESTORED"
            or data.get("original_report_sha256") != "f65809964486de89d7d3a24b01311c62c730f0b167804c53f8c697dbb48fd860"
            or data.get("offered_journeys_per_second") != 84 or data.get("offered_seconds") != 300
            or config.get("simulator_concurrency") != 12 or config.get("simulator_database_pool_max") != 10
            or config.get("simulator_image_id") != receipt()["local_image_id"]
            or config.get("api_shared_acquisition_budget") != 20 or config.get("api_replicas") != 4
            or config.get("connections_per_api") != 4 or config.get("payment_connections_per_api") != 2
            or config.get("general_connections_per_api") != 2 or config.get("pooler_connections") != 24
            or data.get("customer", {}).get("fulfilled") != 25200
            or data.get("customer", {}).get("generator_drops") != 0
            or data.get("financial", {}).get("pass") is not True
            or data.get("financial", {}).get("hold_deadlines_elapsed") is not True
            or data.get("global_queues", {}).get("pass") is not True
            or not data.get("measurement_gates") or any(v is not True for v in data["measurement_gates"].values())
            or any(data.get("restoration", {}).get(k) is not True for k in (
                "restoration_complete", "integrity_verified", "namespace_removed", "helpers_removed", "transport_credentials_cleared", "generator_idle"))
            or data["restoration"].get("cleanup_failures") != []):
        raise ValueError("Fully passing and restored simulator correction required")
    return data


def hourly_plan(goal):
    if active(goal).get("profile") != "cce_hourly_qualification":
        raise ValueError("Separate hourly simulator goal required")
    evidence = hourly_baseline()
    if goal.get("baseline_sha256") != policy.digest(evidence):
        raise ValueError("Hourly prerequisite binding differs")
    data = plan(goal)
    if data["arm_manifest_digest"] != evidence["configuration"]["api_manifest_digest"]:
        raise ValueError("Hourly API must match the passing simulator correction")
    data.update(decision="ADR0232", qualification_decision="ADR0252",
                common={"buyer_journeys_per_second": 84, "duration_seconds": 3600},
                kind="fixed_simulator_correction_hourly_qualification", baseline_evidence=HOURLY_BASELINE,
                baseline_sha256=policy.digest(evidence), baseline_is_passing_control=True,
                published_baseline_evidence=HOURLY_BASELINE,
                expected_terminal_tickets=302400, minimum_issued_inside_hour=300000,
                cohort_observer_interval_seconds=10, experiment_seconds_limit=5400,
                duration_reason="Qualify the same passing runtime for one continuous hour; mandatory durability, drain and restoration follow the offered window.")
    return data
