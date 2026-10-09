"""ADR0219 exact candidate-only 84/s probe of the measured shared 1+3 configuration."""
import copy
import json

import shared_callback_placement_contract as parent
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "paid_offered_rate_60_to_84_shared_1_3"
PLAN = ROOT / "docs/capacity/flash-sale-opening/shared-callback-rate-probe-plan-2026-10-08.json"
ROUTES = {"candidate": "http://load-balancer:8000"}
PLACEMENTS = {"candidate": {"primary": 1, "secondary": 3}}
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def expected_plan():
    expected = copy.deepcopy(parent.expected_plan())
    expected.update(decision="ADR0219", factor=FACTOR, arms=["candidate"],
                    placements=PLACEMENTS, callback_routes=ROUTES,
                    allowance={"qualification_runs_authorized": 1, "paid_runs_authorized": 1,
                               "safety_tickets_authorized": 2})
    expected["common"].update(primary_apis=1, secondary_apis=3, buyer_journeys_per_second=84)
    expected["arm_settings"] = {"candidate": expected["arm_settings"]["candidate"]}
    expected["baseline_evidence"] = "docs/capacity/flash-sale-opening/shared-callback-placement-comparison-2026-10-08.json"
    baseline = json.loads((ROOT / expected["baseline_evidence"]).read_text())
    if baseline.get("pass") is not True or baseline.get("experiment_status") != "PASSED_RESTORED":
        raise ValueError("Passing restored measured placement baseline required")
    expected["baseline_sha256"] = digest(baseline)
    return expected


def plan():
    expected = expected_plan()
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact registered shared-callback 84/s probe required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class SharedCallbackRateProbeContract(parent.SharedCallbackPlacementContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm != "candidate" or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact candidate-only 84/s images, sources and placement required")
        super().__init__(artifact, arm, sources)

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0219", "factor": FACTOR}


AsyncConfirmationContract = SharedCallbackRateProbeContract
