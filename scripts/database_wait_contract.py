"""ADR0173 control-only diagnostic policy reusing exact ADR0163 images."""
import json

import partial_timeout_contract as frozen
from qualify_two_host_deployment import ROOT

FACTOR = "database_wait_and_wal_capture_control"
PLAN = ROOT / "docs/capacity/flash-sale-opening/database-wait-diagnostics-plan-2026-10-06.json"
GLOBAL_RECEIPT_AUDIT = frozen.GLOBAL_RECEIPT_AUDIT
receipt_drained = frozen.receipt_drained


def plan():
    data = json.loads(PLAN.read_text())
    original = json.loads(frozen.PLAN.read_text())
    control = frozen.AdmissionReclaimContract(original["artifact_receipt"], "control", original["expected_runtime_source_sha256"])
    allowed = {"decision", "factor", "common", "artifact_receipt", "expected_runtime_source_sha256",
               "isolated_source_directory", "arms", "allowance", "arm_settings", "scope", "status"}
    if (set(data) != allowed or data.get("arm_settings") != {"control": {r: control.settings(r) for r in control.roles}}
            or data.get("decision") != "ADR0173" or data.get("factor") != FACTOR or data.get("arms") != ["control"]
            or data.get("common") != original["common"]
            or data.get("allowance") != {"qualification_runs_authorized":1,"paid_runs_authorized":1,"safety_tickets_authorized":2}
            or any(data.get(k) != original[k] for k in ("artifact_receipt","expected_runtime_source_sha256","isolated_source_directory"))):
        raise ValueError("Exact unchanged control diagnostic plan required")
    return data


def source_contract():
    plan()
    return frozen.source_contract()


class DatabaseWaitContract(frozen.AdmissionReclaimContract):
    def __init__(self,artifact,arm,sources):
        data=plan()
        if arm != "control" or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Only unchanged frozen control is permitted")
        super().__init__(artifact,arm,sources)


AsyncConfirmationContract = DatabaseWaitContract
