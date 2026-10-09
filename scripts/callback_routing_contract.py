"""ADR0216: routing-only comparison on fixed 2+2 APIs and frozen binaries."""
import copy
import json

import api_placement_contract as parent
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "simulator_callback_destination_local_to_shared"
PLAN = ROOT / "docs/capacity/flash-sale-opening/callback-routing-plan-2026-10-08.json"
ROUTES = {"control": "http://api:8000", "candidate": "http://load-balancer:8000"}
PLACEMENTS = {arm: {"primary": 2, "secondary": 2} for arm in ROUTES}
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def expected_plan():
    original = parent.plan()
    expected = {k: copy.deepcopy(v) for k, v in original.items() if k not in {"status", "scope"}}
    expected.update(decision="ADR0216", factor=FACTOR, placements=PLACEMENTS,
                    diagnostic_connection_decision="ADR0180", callback_routes=ROUTES)
    for arm, url in ROUTES.items():
        expected["arm_settings"][arm]["simulator"]["API_URL"] = url
    return expected


def plan():
    expected = expected_plan()
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact fixed-budget callback routing plan required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class CallbackRoutingContract(parent.ApiPlacementContract):
    diagnostic_context = None

    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm not in ROUTES or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact callback routing artifact, sources and arm required")
        super().__init__(artifact, arm, sources)

    @property
    def measured_api_counts(self):
        return copy.deepcopy(PLACEMENTS[self.arm])

    @property
    def cpu_placement(self):
        return "two-plus-two"

    def settings(self, role):
        result = super().settings(role)
        if role == "simulator":
            result["API_URL"] = ROUTES[self.arm]
        return result

    def inventory_marker(self):
        return {"decision": "ADR0216", "arm": self.arm, "factor": FACTOR,
                "api_counts": self.measured_api_counts, "callback_url": ROUTES[self.arm],
                "partial_timeout_reclaim": "0", "async_intake": "0",
                "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": self.images["api"]}


AsyncConfirmationContract = CallbackRoutingContract
