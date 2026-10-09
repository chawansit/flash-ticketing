"""ADR0217 placement-only comparison with shared callbacks in both arms."""
import copy
import json

import api_placement_contract as parent
import callback_routing_contract as routing
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "api_placement_2_2_to_1_3_shared_callbacks"
PLAN = ROOT / "docs/capacity/flash-sale-opening/shared-callback-placement-plan-2026-10-08.json"
ROUTES = {arm: "http://load-balancer:8000" for arm in ("control", "candidate")}
PLACEMENTS = parent.PLACEMENTS
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def expected_plan():
    expected = routing.expected_plan()
    expected.update(decision="ADR0217", factor=FACTOR, placements=copy.deepcopy(PLACEMENTS), callback_routes=ROUTES)
    for arm, url in ROUTES.items():
        expected["arm_settings"][arm]["simulator"]["API_URL"] = url
    return expected


def plan():
    expected = expected_plan()
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact shared-routing placement-only plan required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class SharedCallbackPlacementContract(parent.ApiPlacementContract):
    diagnostic_context = None

    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm not in ROUTES or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact shared-routing placement artifact, sources and arm required")
        super().__init__(artifact, arm, sources)

    def settings(self, role):
        result = super().settings(role)
        if role == "simulator":
            result["API_URL"] = ROUTES[self.arm]
        return result

    def inventory_marker(self):
        return {"decision": "ADR0217", "arm": self.arm, "factor": FACTOR,
                "api_counts": self.measured_api_counts, "callback_url": ROUTES[self.arm],
                "partial_timeout_reclaim": "0", "async_intake": "0",
                "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": self.images["api"]}


AsyncConfirmationContract = SharedCallbackPlacementContract
