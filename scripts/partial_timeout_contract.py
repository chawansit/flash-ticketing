"""ADR0164 exact admission profile and full receipt/drain recovery policy."""

import json

import prepare_partial_timeout_reclamation as export
from async_confirmation_contract import GLOBAL_RECEIPT_AUDIT, receipt_drained
from async_confirmation_contract import AsyncConfirmationContract as ReceiptContract
from observe_two_host_pipeline import confirmation_startup
from partial_timeout_profile import COMMON, FACTOR, PartialTimeoutProfile
from partial_timeout_profile import source_contract as verify_source
from qualify_two_host_deployment import INSPECT, ROOT
from status_refresh_contract import StatusRefreshContract
from two_host_topology import grouped

PLAN = ROOT / "docs/capacity/flash-sale-opening/partial-timeout-runner-plan-2026-10-06.json"


def source_contract():
    plan = json.loads(PLAN.read_text())
    if (plan.get("decision") != "ADR0165" or plan.get("factor") != FACTOR
            or plan.get("common") != COMMON or plan.get("expected_runtime_source_sha256") != export.manifest()["runtime_source_sha256"]):
        raise ValueError("Exact bounded admission plan required")
    return verify_source(ROOT / plan["isolated_source_directory"])


class AdmissionReclaimContract(PartialTimeoutProfile):
    candidate_counts = ReceiptContract.candidate_counts
    changed_services = ReceiptContract.changed_services
    global_audit = GLOBAL_RECEIPT_AUDIT
    inventory_background = ReceiptContract.inventory_background
    before_restore = ReceiptContract.before_restore
    after_restore = ReceiptContract.after_restore

    def __init__(self, artifact, arm, sources):
        plan = json.loads(PLAN.read_text())
        if artifact != plan.get("artifact_receipt") or sources != plan.get("expected_runtime_source_sha256"):
            raise ValueError("Exact staged admission images/source required")
        super().__init__(artifact, arm, sources)
        self.original_parents = {r: p for r, p in self.parents.items() if r != "confirmation"}

    def verify_pipeline_startup(self, row):
        confirmation_startup(row)

    def pre_mutation(self, session, saved):
        StatusRefreshContract.pre_mutation(self, session, saved)
        cid = grouped(session.call("primary", INSPECT))["api"][0]["Id"]
        from prepare_async_confirmation import manifest
        expected = manifest()["migration_sha256"]["migrations/009_payment_confirmation_receipts.sql"]
        program = ("import json,os,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n"
                   " conn.execute('SET TRANSACTION READ ONLY');conn.execute(\"SET LOCAL statement_timeout='10s'\")\n"
                   " row=conn.execute('SELECT checksum FROM schema_migrations WHERE name=%s',('009_payment_confirmation_receipts.sql',)).fetchone()\n"
                   " assert row and row[0]==" + repr(expected) + "\nprint(json.dumps({'receipt_schema_unchanged':True}))")
        session.state.update(session.api(cid, program, 45))
        queues = session.api(cid, self.global_audit, 45)
        if not receipt_drained(queues) or queues.get("kafka_members") != 1:
            raise ValueError("All receipt/global queues must start drained")
        session.checkpoint()

    def stage_receipt_audit(self, session, cid, events, expected):
        # API intake is synchronous in BOTH arms: zero receipt admissions expected.
        return ReceiptContract.stage_receipt_audit(self, session, cid, events, 0)


# The reused private runner's profile seam has this historical name.
AsyncConfirmationContract = AdmissionReclaimContract
