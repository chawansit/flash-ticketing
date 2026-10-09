"""ADR0181 full-visibility diagnostic placement; historical marker stays ADR0174."""
import json

import api_placement_contract as parent
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = parent.FACTOR
PLAN = ROOT / "docs/capacity/flash-sale-opening/diagnostic-placement-plan-2026-10-06.json"
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def plan():
    original = parent.plan()
    expected = {k: v for k, v in original.items() if k not in {"status", "scope"}}
    expected.update(decision="ADR0181", diagnostic_connection_decision="ADR0180")
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact full-visibility diagnostic placement plan required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class DiagnosticPlacementContract(parent.ApiPlacementContract):
    diagnostic_context = None

    def __init__(self, artifact, arm, sources):
        data = plan()
        if artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact diagnostic placement artifact and sources required")
        super().__init__(artifact, arm, sources)


AsyncConfirmationContract = DiagnosticPlacementContract
