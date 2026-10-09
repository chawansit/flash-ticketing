"""ADR0177 placement pair with explicit ADR0176 role-scoped diagnostics."""
import json

import api_placement_contract as parent
import application_database_wait_evidence as scoped
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = parent.FACTOR
PLAN = ROOT / "docs/capacity/flash-sale-opening/application-role-rebalance-plan-2026-10-06.json"
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def plan():
    original = parent.plan()
    expected = {k: v for k, v in original.items() if k not in {"status", "scope"}}
    expected.update(decision="ADR0177", diagnostic_scope=scoped.SCOPE, diagnostic_decision="ADR0176")
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact role-scoped fixed-budget placement plan required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class ApplicationRoleRebalanceContract(parent.ApiPlacementContract):
    diagnostic_scope = scoped.SCOPE

    def __init__(self, artifact, arm, sources):
        data = plan()
        if artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact role-scoped images and sources required")
        super().__init__(artifact, arm, sources)

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0177",
                "diagnostic_scope": self.diagnostic_scope, "diagnostic_decision": "ADR0176"}

    def bind_database_roles(self, rows, observer_url):
        return scoped.binding_record(scoped.prove_role_coverage(rows, scoped.EXPECTED_REPLICAS, observer_url))

    def verify_inventory(self, data):
        super().verify_inventory(data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Exact scoped inventory marker required")
        scoped.binding_from_record(data.get("application_database_role_binding"))

    def verify_pipeline_startup(self, row):
        super().verify_pipeline_startup(row)
        scoped.startup(row)

    def validate_inventory_receipt(self, record, inventory, *, final, now=None):
        result = super().validate_inventory_receipt(record, inventory, final=final, now=now)
        binding = scoped.binding_from_record(inventory.get("application_database_role_binding"))
        scoped.startup(record.get("application_database_wait_startup", {}), binding)
        return result


AsyncConfirmationContract = ApplicationRoleRebalanceContract
