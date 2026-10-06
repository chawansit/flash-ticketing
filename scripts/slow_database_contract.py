"""ADR0171 control-only diagnostic policy reusing exact ADR0163 images."""
import json

import partial_timeout_contract as frozen
from qualify_two_host_deployment import ROOT

FACTOR = "slow_database_phase_capture_control"
PLAN = ROOT / "docs/capacity/flash-sale-opening/slow-database-diagnostics-plan-2026-10-06.json"
GLOBAL_RECEIPT_AUDIT = frozen.GLOBAL_RECEIPT_AUDIT
receipt_drained = frozen.receipt_drained


def plan():
    data = json.loads(PLAN.read_text())
    original = json.loads(frozen.PLAN.read_text())
    if (data.get("decision") != "ADR0171" or data.get("factor") != FACTOR or data.get("arms") != ["control"]
            or data.get("common") != original["common"]
            or data.get("allowance") != {"qualification_runs_authorized":1,"paid_runs_authorized":1,"safety_tickets_authorized":2}
            or any(data.get(k) != original[k] for k in ("artifact_receipt","expected_runtime_source_sha256","isolated_source_directory"))):
        raise ValueError("Exact unchanged control diagnostic plan required")
    return data


def source_contract():
    plan()
    return frozen.source_contract()


class SlowDatabaseContract(frozen.AdmissionReclaimContract):
    def __init__(self,artifact,arm,sources):
        data=plan()
        if arm != "control" or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Only unchanged frozen control is permitted")
        super().__init__(artifact,arm,sources)


AsyncConfirmationContract = SlowDatabaseContract
