"""ADR0224 index-only correction, reusing exactly the ADR0222 application images."""
import copy
import hashlib
import json

import interleaved_refresh_probe_contract as parent
from qualify_two_host_deployment import INSPECT, ROOT
from status_refresh_contract import digest
from two_host_topology import grouped

FACTOR = "orders_event_id_index_at_84_shared_1_3"
PLAN = ROOT / "docs/capacity/flash-sale-opening/orders-event-index-probe-plan-2026-10-08.json"
MIGRATION = "migrations/010_orders_event_index.sql"
ROUTES, PLACEMENTS = parent.ROUTES, parent.PLACEMENTS
GLOBAL_RECEIPT_AUDIT, receipt_drained = parent.GLOBAL_RECEIPT_AUDIT, parent.receipt_drained
INDEX = {"schema": "public", "table": "orders", "name": "orders_event_id", "method": "btree",
         "columns": ["event_id"], "unique": False, "include": [], "predicate": None,
         "migration": MIGRATION,
         "concurrent_statement": "CREATE INDEX CONCURRENTLY orders_event_id\nON public.orders(event_id)",
         "statement_timeout_seconds": 120, "lock_timeout_seconds": 2,
         "persistence": "intentional_schema_correction"}


def expected_plan():
    expected = copy.deepcopy(parent.plan())
    for key in ("status", "scope"):
        expected.pop(key)
    baseline = "docs/capacity/flash-sale-opening/paid-observer-timing-probe-2026-10-08.json"
    recovery = "docs/capacity/flash-sale-opening/interleaved-refresh-timing-recovery-2026-10-08.json"
    failed, recovered = (json.loads((ROOT / p).read_text()) for p in (baseline, recovery))
    if (failed.get("pass") is not False or failed.get("stage", {}).get("dispatched") != 25168
            or failed["stage"].get("generator_drops") != 32
            or recovered.get("dispatched_paid_tickets") != 25168
            or recovered.get("original_experiment_pass") is not False
            or not recovered.get("gates") or not all(recovered["gates"].values())):
        raise ValueError("Retained exact timed reference and independent recovery required")
    expected.update(decision="ADR0224", factor=FACTOR, baseline_evidence=baseline,
                    baseline_sha256=digest(failed), baseline_recovery_evidence=recovery,
                    baseline_recovery_sha256=digest(recovered),
                    database_index={**INDEX, "migration_sha256": hashlib.sha256((ROOT / MIGRATION).read_bytes()).hexdigest()})
    return expected


def plan():
    data, expected = json.loads(PLAN.read_text()), expected_plan()
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact index-only application artifacts, baseline and migration required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


def index_program(*, verify_only):
    source = (ROOT / "scripts/apply_orders_event_index.py").read_text()
    program = ("import json,os,psycopg\nnamespace={'__name__':'owned_orders_event_index'}\nexec("
               + repr(source) + ",namespace)\nassert namespace['INDEX_SQL']=="
               + repr(plan()["database_index"]["concurrent_statement"]) + "\nwith psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:\n"
               + " result=namespace['apply'](conn,verify_only=" + repr(verify_only) + ")\n"
               + "print(json.dumps(result))\n")
    compile(program, "owned_orders_event_index", "exec")
    return program


class OrdersEventIndexProbeContract(parent.InterleavedRefreshProbeContract):
    def __init__(self, artifact, arm, sources):
        if artifact != plan()["artifact_receipt"]:
            raise ValueError("Index correction must retain every application image")
        super().__init__(artifact, arm, sources)
        self.index_before = self.index_after = None

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0224", "factor": FACTOR}

    def pre_mutation(self, session, saved):
        super().pre_mutation(session, saved)
        rows = session.call("primary", INSPECT, 45)
        cid = grouped(rows)["api"][0]["Id"]
        queue = session.api(cid, self.global_audit, 45)
        if queue.get("pass") is not True or queue.get("kafka_members") != 1:
            raise ValueError("Index deployment requires complete original queue drain")
        session.state["orders_event_index_intent"] = plan()["database_index"]
        session.checkpoint()  # Before an ambiguous DDL outcome.
        session.phase("orders-event-index-concurrent-preflight")
        self.index_before = session.api(cid, index_program(verify_only=False), 160)
        session.state["orders_event_index_before"] = self.index_before
        session.checkpoint()
        if self.index_before.get("pass") is not True:
            raise ValueError("Exact valid orders event index required before safety or paid dispatch")

    def after_restore(self, session, live_path):
        super().after_restore(session, live_path)
        rows = session.call("primary", INSPECT, 45)
        cid = grouped(rows)["api"][0]["Id"]
        self.index_after = session.api(cid, index_program(verify_only=True), 30)
        session.state["orders_event_index_after"] = self.index_after
        session.checkpoint()
        if not self.index_verified():
            raise ValueError("Owned valid index must remain identical after application restoration")

    def index_verified(self):
        for proof in (self.index_before, self.index_after):
            if not isinstance(proof, dict) or proof.get("pass") is not True:
                return False
            index = proof.get("index")
            if (not isinstance(index, dict) or index.get("valid") is not True
                    or index.get("ready") is not True or index.get("unique") is not False
                    or index.get("method") != "btree" or index.get("key") != "event_id"
                    or index.get("predicate") is not None or index.get("columns") != 1
                    or index.get("expected_table") is not True or index.get("pass") is not True
                    or type(index.get("oid")) is not int or index["oid"] <= 0):
                return False
        return (self.index_before["index"] == self.index_after["index"]
                and self.index_after.get("verification_only") is True)



AsyncConfirmationContract = OrdersEventIndexProbeContract
