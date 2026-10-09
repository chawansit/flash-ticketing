"""ADR0193: exact arm-specific sources/images, unchanged synchronous budgets."""
import copy
import json

import prepare_atomic_payment_claim as export
import slow_database_contract as frozen
from partial_timeout_contract import AdmissionReclaimContract
from qualify_two_host_deployment import ROOT
from status_refresh_contract import StatusRefreshContract

PLAN = ROOT / "docs/capacity/flash-sale-opening/atomic-payment-claim-comparison-plan-2026-10-07.json"
FACTOR = "payment_dispatch_claim_two_statements_to_one"
GLOBAL_RECEIPT_AUDIT = frozen.GLOBAL_RECEIPT_AUDIT
receipt_drained = frozen.receipt_drained


def plan():
    data = json.loads(PLAN.read_text())
    baseline = frozen.plan()
    keys = {"decision", "factor", "common", "artifact_receipt", "expected_runtime_source_sha256",
            "isolated_source_directory", "candidate_source_directory", "arms", "allowance",
            "arm_settings", "scope", "status"}
    if (set(data) != keys or data["decision"] != "ADR0193" or data["factor"] != FACTOR
            or data["common"] != baseline["common"] or data["arms"] != ["control", "candidate"]
            or data["allowance"] != {"qualification_runs_authorized":1,"paid_runs_authorized":2,
                                     "safety_tickets_authorized":4}
            or set(data["artifact_receipt"]) != {"control", "candidate"}
            or set(data["expected_runtime_source_sha256"]) != {"control", "candidate"}
            or data["artifact_receipt"]["control"] != baseline["artifact_receipt"]
            or data["expected_runtime_source_sha256"]["control"] != baseline["expected_runtime_source_sha256"]
            or data["artifact_receipt"]["candidate"]["parent_images"] != baseline["artifact_receipt"]["parent_images"]
            or data["isolated_source_directory"] != baseline["isolated_source_directory"]
            or data["arm_settings"] != {arm:baseline["arm_settings"]["control"] for arm in data["arms"]}):
        raise ValueError("Exact fixed-budget atomic claim pair required")
    expected = data["expected_runtime_source_sha256"]
    if (set(expected["candidate"]) != set(expected["control"])
            or {p for p in expected["candidate"] if expected["candidate"][p] != expected["control"][p]} != {export.WORKERS}):
        raise ValueError("Only the qualified worker claim source may change")
    return data


def source_contract():
    data = plan()
    sources = {"control":frozen.source_contract(),
               "candidate":export.verify(ROOT / data["candidate_source_directory"])}
    if sources != data["expected_runtime_source_sha256"]:
        raise ValueError("Arm-specific immutable source proof differs")
    return sources


class AtomicPaymentClaimContract(AdmissionReclaimContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if (arm not in {"control", "candidate"} or artifact != data["artifact_receipt"]
                or sources != data["expected_runtime_source_sha256"]):
            raise ValueError("Exact bound pair and selected arm required")
        self.bound_artifact, self.bound_sources = copy.deepcopy(artifact), copy.deepcopy(sources)
        StatusRefreshContract.__init__(self, artifact[arm], arm, sources[arm])
        if (self.images["confirmation"] != self.images["simulator"]
                or self.parents["confirmation"] != self.parents["simulator"]):
            raise ValueError("Idle confirmation must retain simulator image and parent")
        self.api_settings.update(self.settings("api"))
        self.original_parents = {r:p for r,p in self.parents.items() if r != "confirmation"}

    @property
    def flag(self):
        return "0"  # Reclamation and async intake stay off in both arms.

    def inventory_marker(self):
        return {"decision":"ADR0193", "arm":self.arm, "factor":FACTOR,
                "claim_statements":1 if self.arm == "candidate" else 2,
                "worker_source_sha256":self.sources[export.WORKERS],
                "partial_timeout_reclaim":"0", "async_intake":"0",
                "cache_age_ms":1000, "poll_ms":500, "api_image_id":self.images["api"]}

    def staging_contracts(self):
        return tuple(type(self)(self.bound_artifact, arm, self.bound_sources)
                     for arm in ("control", "candidate"))


AsyncConfirmationContract = AtomicPaymentClaimContract
