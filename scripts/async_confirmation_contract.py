"""ADR0161 exact receipt factor and global financial queue policy."""

import copy
import json
import time
from typing import ClassVar

import prepare_async_confirmation as export
from observe_two_host_pipeline import confirmation_startup, verify_confirmation_factor_evidence
from qualify_two_host_deployment import GLOBAL_AUDIT, INSPECT
from status_refresh_contract import ROOT, StatusRefreshContract
from two_host_topology import CANDIDATE_COUNTS, CHANGED, environment, grouped

PLAN = ROOT / "docs/capacity/flash-sale-opening/async-confirmation-comparison-plan-2026-10-05.json"
FACTOR = "api_PAYMENT_CONFIRMATION_ASYNC"

RECEIPT_AUDIT = r"""
with psycopg.connect(os.environ['DATABASE_URL']) as conn:
 conn.execute('SET TRANSACTION READ ONLY');conn.execute("SET LOCAL statement_timeout='10s'")
 rows=conn.execute("SELECT status,count(*) FROM payment_webhook_receipts GROUP BY status").fetchall()
 counters=conn.execute("SELECT c.provider,c.outstanding,(SELECT count(*) FROM payment_webhook_receipts r WHERE r.provider=c.provider AND r.status<>'COMPLETED') FROM payment_receipt_capacity c").fetchall()
 states=dict(rows)
 result['pending_confirmation_receipts']=sum(n for state,n in rows if state!='COMPLETED')
 result['review_confirmation_receipts']=states.get('REVIEW',0)
 result['confirmation_capacity_outstanding']=sum(n for _,n,_ in counters)
 result['confirmation_capacity_mismatches']=sum(n!=expected for _,n,expected in counters)
result['pass']=result['pass'] and all(result[k]==0 for k in ('pending_confirmation_receipts','review_confirmation_receipts','confirmation_capacity_outstanding','confirmation_capacity_mismatches'))
"""
GLOBAL_RECEIPT_AUDIT = GLOBAL_AUDIT.replace("print(json.dumps(result))", RECEIPT_AUDIT + "\nprint(json.dumps(result))")


def receipt_drained(queue):
    return queue.get("pass") is True and all(type(queue.get(k)) is int and queue[k] == 0
        for k in ("pending_confirmation_receipts", "review_confirmation_receipts",
                  "confirmation_capacity_outstanding", "confirmation_capacity_mismatches"))


def source_contract():
    data = json.loads(PLAN.read_text())
    manifest = export.manifest()
    expected_common = {"primary_apis": 2, "secondary_apis": 2, "cache_age_ms": 1000,
                       "consumer_event_refresh": "1", "consumer_dedup": "0", "poll_ms": 500,
                       "simulator_pool": 10, "confirmation_pool": 2,
                       "buyer_journeys_per_second": 60, "duration_seconds": 300}
    if (data.get("decision") != "ADR0161" or data.get("factor") != FACTOR
            or data.get("common") != expected_common
            or data.get("expected_runtime_source_sha256") != manifest["runtime_source_sha256"]):
        raise ValueError("Exact async confirmation plan required")
    source = (ROOT / data["isolated_source_directory"]).resolve()
    if not source.is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Owned isolated source required")
    receipt = json.loads((source.parent / "async-source-receipt.json").read_text())
    export.base.verify_tree(source, receipt["source_file_sha256"])
    if receipt.get("financial_transaction_body_ast_unchanged") is not True or receipt.get("callback_reserve_wiring_excluded") is not True:
        raise ValueError("Protected financial/excluded admission proof required")
    return manifest["runtime_source_sha256"]


class AsyncConfirmationContract(StatusRefreshContract):
    background: ClassVar[dict] = {**StatusRefreshContract.background,
                  "simulator": {**StatusRefreshContract.background["simulator"], "pool_per_replica": 10},
                  "confirmation": {"replicas": 1, "pool_per_replica": 2, "concurrency": 2}}
    roles = ("api", *background)
    candidate_counts: ClassVar[dict] = {**CANDIDATE_COUNTS, "confirmation": 1}
    changed_services = (*CHANGED, "confirmation")
    global_audit = GLOBAL_RECEIPT_AUDIT

    def __init__(self, artifact, arm, sources):
        data = json.loads(PLAN.read_text())
        if artifact != data.get("artifact_receipt") or sources != data.get("expected_runtime_source_sha256"):
            raise ValueError("Exact prepared receipt images and sources required")
        super().__init__(artifact, arm, sources)
        if self.images["confirmation"] != self.images["simulator"] or self.parents["confirmation"] != self.parents["simulator"]:
            raise ValueError("Confirmation must inherit pinned simulator image")
        self.original_parents = {r: p for r, p in self.parents.items() if r != "confirmation"}
        self.api_settings.update(PAYMENT_CONFIRMATION_ASYNC=self.flag, ORDER_STATUS_POLL_MS="500")

    def settings(self, role):
        return {"ORDER_STATUS_CACHE_MS": "1000" if role in {"api", "consumer"} else "0",
                "ORDER_STATUS_EVENT_REFRESH": "1" if role == "consumer" else "0",
                "ORDER_STATUS_EVENT_REFRESH_DEDUP": "0", "ORDER_STATUS_POLL_MS": "500",
                "PAYMENT_CALLBACK_PROVIDER": "simulator", "CONFIRMATION_MAX_PENDING": "10000",
                "CONFIRMATION_LEASE_SECONDS": "30", "CONFIRMATION_MAX_ATTEMPTS": "8",
                "CONFIRMATION_RETRY_MS": "100",
                "PAYMENT_CONFIRMATION_ASYNC": "1" if role == "confirmation" else self.flag if role == "api" else "0",
                **({"DB_POOL_MAX": "10"} if role == "simulator" else {}),
                **({"DB_POOL_MAX": "2", "CONFIRMATION_CONCURRENCY": "2", "SIMULATOR_CONCURRENCY": "1"} if role == "confirmation" else {})}

    def primary_model(self, model):
        result = copy.deepcopy(model)
        confirmation = copy.deepcopy(result["services"]["simulator"])
        confirmation["command"] = ["python", "-m", "ticketing.workers", "confirmation"]
        confirmation.pop("profiles", None)
        confirmation.pop("ports", None)
        result["services"]["confirmation"] = confirmation
        return super().primary_model(result)

    def inventory_background(self, groups, background):
        for role in ("simulator", "confirmation"):
            env = environment(groups[role][0])
            background[role]["pool_per_replica"] = int(env["DB_POOL_MAX"])
        background["confirmation"]["concurrency"] = int(environment(groups["confirmation"][0])["CONFIRMATION_CONCURRENCY"])
        return background

    def inventory_marker(self):
        return {"decision": "ADR0161", "cache_age_ms": 1000, "arm": self.arm,
                "api_image_id": self.images["api"], "async_confirmation": self.flag, "poll_ms": 500}

    def verify_worker_settings(self, role, environment):
        if any(environment.get(k) != v for k, v in self.settings(role).items()):
            raise ValueError("Every confirmation setting must be explicit")

    def verify_inventory(self, data):
        super().verify_inventory(data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Recorded async factor differs")
        verify_confirmation_factor_evidence(data)

    def verify_pipeline_startup(self, row):
        confirmation_startup(row)

    def pre_mutation(self, session, saved):
        super().pre_mutation(session, saved)
        rows = session.call("primary", INSPECT)
        cid = grouped(rows)["api"][0]["Id"]
        migration = "migrations/009_payment_confirmation_receipts.sql"
        text = (ROOT / json.loads(PLAN.read_text())["isolated_source_directory"] / migration).read_text()
        expected = export.manifest()["migration_sha256"][migration]
        program = ("import hashlib,json,os,psycopg\ntext=" + repr(text)
                   + "\nassert hashlib.sha256(text.encode()).hexdigest()==" + repr(expected)
                   + "\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n"
                     " conn.execute(\"SET LOCAL lock_timeout='3s'\");conn.execute(\"SET LOCAL statement_timeout='10s'\")\n"
                     " conn.execute(\"SELECT pg_advisory_xact_lock(739112)\")\n"
                     " old=conn.execute('SELECT checksum FROM schema_migrations WHERE name=%s',('009_payment_confirmation_receipts.sql',)).fetchone()\n"
                     " if old and old[0]!=hashlib.sha256(text.encode()).hexdigest():raise ValueError('Existing migration checksum differs')\n"
                     " conn.execute(text)\n"
                     " if not old:conn.execute('INSERT INTO schema_migrations VALUES (%s,%s)',('009_payment_confirmation_receipts.sql',hashlib.sha256(text.encode()).hexdigest()))\n"
                     "print(json.dumps({'receipt_schema_installed':True}))")
        session.state.update(session.api(cid, program, 45))
        queue = session.api(cid, self.global_audit, 45)
        if queue.get("pass") is not True or queue.get("kafka_members") != 1:
            raise ValueError("Receipt migration preflight must be globally drained")
        session.checkpoint()

    def stage_receipt_audit(self, session, cid, events, expected):
        # Match the simulator callback ID to its durable payment attempt and scoped order.
        program = r"""import json,os,psycopg
with psycopg.connect(os.environ['DATABASE_URL']) as conn:
 conn.execute('SET TRANSACTION READ ONLY');conn.execute("SET LOCAL statement_timeout='10s'")
 row=conn.execute("SELECT count(*),count(*) FILTER (WHERE r.status='COMPLETED') FROM payment_webhook_receipts r JOIN payment_attempts p ON p.id=r.callback_id JOIN orders o ON o.id=p.order_id WHERE r.provider='simulator' AND o.event_id=ANY(%s::uuid[])",(EVENTS,)).fetchone()
result={'receipts':row[0],'completed':row[1],'expected':EXPECTED,'pass':row[0]==EXPECTED and row[1]==EXPECTED}
print(json.dumps(result))
""".replace("EVENTS", repr(events)).replace("EXPECTED", str(expected if self.arm == "candidate" else 0))
        result = session.api(cid, program, 45)
        if result.get("pass") is not True:
            raise ValueError("Scoped durable receipt completion failed")
        return result

    def before_restore(self, session, cid, live_path):
        deadline = time.monotonic() + 60
        while True:
            queue = session.api(cid, self.global_audit, 45)
            if receipt_drained(queue):
                break
            if time.monotonic() >= deadline:
                raise ValueError("Keep confirmation worker/ownership until acknowledged receipts drain")
            time.sleep(2)
        session.state["confirmation_queue_before_worker_removal"] = queue
        session.checkpoint()

    def after_restore(self, session, live_path):
        # Both hosts' intake is now synchronous/off, while the receipt worker is retained.
        rows = session.call("primary", INSPECT)
        cid = grouped(rows)["api"][0]["Id"]
        self.before_restore(session, cid, live_path)
        program = ("import json,subprocess;subprocess.run(['docker','compose','-f'," + repr(live_path)
                   + ",'stop','--timeout','30','confirmation'],check=True,capture_output=True,timeout=45);"
                     "subprocess.run(['docker','compose','-f'," + repr(live_path)
                   + ",'rm','-f','confirmation'],check=True,capture_output=True,timeout=30);"
                     "print(json.dumps({'confirmation_worker_removed_after_drain':True}))")
        session.state.update(session.call("primary", program, 90))
        session.checkpoint()
