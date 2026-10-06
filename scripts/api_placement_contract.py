"""ADR0174 placement-only policy; both arms retain frozen admission control settings."""
import copy
import json

import partial_timeout_contract as frozen
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "api_placement_2_2_to_1_3"
PLAN = ROOT / "docs/capacity/flash-sale-opening/api-placement-rebalance-plan-2026-10-06.json"
GLOBAL_RECEIPT_AUDIT = frozen.GLOBAL_RECEIPT_AUDIT
receipt_drained = frozen.receipt_drained
PLACEMENTS = {"control": {"primary": 2, "secondary": 2}, "candidate": {"primary": 1, "secondary": 3}}


def plan():
    data = json.loads(PLAN.read_text())
    original = json.loads(frozen.PLAN.read_text())
    control = frozen.AdmissionReclaimContract(original["artifact_receipt"], "control", original["expected_runtime_source_sha256"])
    expected = {"decision": "ADR0174", "factor": FACTOR, "common": original["common"],
                "artifact_receipt": original["artifact_receipt"],
                "expected_runtime_source_sha256": original["expected_runtime_source_sha256"],
                "isolated_source_directory": original["isolated_source_directory"],
                "arms": ["control", "candidate"], "placements": PLACEMENTS,
                "allowance": {"qualification_runs_authorized": 1, "paid_runs_authorized": 2, "safety_tickets_authorized": 4},
                "arm_settings": {arm: {r: control.settings(r) for r in control.roles} for arm in PLACEMENTS}}
    if set(data) != {*expected, "scope", "status"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact fixed-budget placement-only plan required")
    return data


def source_contract():
    plan()
    return frozen.source_contract()


class ApiPlacementContract(frozen.AdmissionReclaimContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm not in PLACEMENTS or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact placement images/source/arm required")
        super().__init__(artifact, arm, sources)

    @property
    def flag(self):
        return "0"

    @property
    def measured_api_counts(self):
        return copy.deepcopy(PLACEMENTS[self.arm])

    @property
    def cpu_placement(self):
        return "two-plus-two" if self.arm == "control" else "one-plus-three"

    def inventory_marker(self):
        return {"decision": "ADR0174", "arm": self.arm, "factor": FACTOR,
                "api_counts": self.measured_api_counts, "partial_timeout_reclaim": "0", "async_intake": "0",
                "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": self.images["api"]}


AsyncConfirmationContract = ApiPlacementContract
