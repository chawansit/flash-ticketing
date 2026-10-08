"""Bounded ADR0147 matched paid stages using frozen workload and owned observers."""

import argparse
import json
import subprocess
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from collect_two_host_inventory import observe
from fixture_identity_evidence import retain_fixture_identity
from kafka_lag_observe import source_sha256
from observe_paid_pipeline import kafka_startup_view, pipeline_startup_view
from observe_two_host_cpu import compare_windows, validate_spec
from observe_two_host_cpu import summarize as summarize_cpu
from observe_two_host_pipeline import summarize_distribution
from prepare_two_host_scaling import REVISION
from qualify_two_host_deployment import GLOBAL_AUDIT, ROOT, run, transport_failure
from summarize_paid_kafka_lag import summarize as summarize_kafka
from summarize_paid_pipeline import summarize as summarize_pipeline

PLAN = ROOT / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json"
CORE = ("checkout_journey_probe.py", "paid_ticket_load_generator.py", "paid_ticket_sharded_generator.py")
ADAPTERS = (
    "observe_two_host_pipeline.py",
    "database_wait_evidence.py",
    "prepare_two_host_scaling.py",
    "observe_two_host_cpu.py",
    "run_synchronized_paid_generator.py",
)


def frozen_bundle():
    hashes = json.loads(PLAN.read_text())["harness_source_sha256"]
    bundle = {}
    for path, expected in hashes.items():
        blob = subprocess.check_output(["git", "show", REVISION + ":" + path], cwd=ROOT, timeout=10)
        if source_sha256(blob) != expected:
            raise ValueError("Frozen Git harness differs from recorded manifest")
        bundle[path] = blob
    if len(bundle) != 78:
        raise ValueError("Frozen78-file contract differs")
    return bundle


def fetch(session, role, remote):
    transport = session.clients[role].open_sftp()
    try:
        with transport.open(remote, "rb") as f:
            return f.read()
    finally:
        transport.close()


def copy_out(session, cid, source, target):
    session.call(
        "primary",
        "import json,os,subprocess;subprocess.run("
        + repr(["docker", "cp", cid + ":" + source, target])
        + ",check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15);os.chmod("
        + repr(target)
        + ",0o600);print(json.dumps({'copied':True}))",
        45,
    )
    return fetch(session, "primary", target)


def api_upload(session, cid, owner, directory, name, content):
    source = owner + "/" + name
    target = directory + "/" + name
    session.put("primary", source, content, True)
    session.call(
        "primary",
        "import json,subprocess;subprocess.run("
        + repr(["docker", "cp", source, cid + ":" + target])
        + ",check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);subprocess.run("
        + repr(["docker", "exec", "--user", "0", cid, "chown", "10001:10001", target])
        + ",check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);print(json.dumps({'uploaded':True}))",
    )
    return target


def job_program(args, directory, *, database=False):
    name = Path(args[1]).stem
    status_path = directory + "/job-" + name + "-exit.json"
    supervisor = (
        "import json,os,subprocess;from pathlib import Path;"
        + "r=subprocess.run(" + repr(args) + ",stdin=subprocess.DEVNULL,timeout=600);"
        + "p=Path(" + repr(status_path) + ");p.write_text(json.dumps({'returncode':r.returncode}));os.chmod(p,0o600)"
    )
    command = [args[0], "-c", supervisor]
    return (
        r"""import hashlib,json,os,subprocess,time
from pathlib import Path
directory=Path(__OWNED_DIRECTORY__)
env=dict(os.environ)
if __DATABASE_FLAG__:env['TEST_DATABASE_URL']=env['DATABASE_URL']
with (directory/'job-__JOB_NAME__.log').open('xb') as log:
 os.chmod(directory/'job-__JOB_NAME__.log',0o600)
 p=subprocess.Popen(__COMMAND__,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
proc=Path('/proc/'+str(p.pid))
fields=(proc/'stat').read_text().rsplit(')',1)[1].split()
expected=b'\0'.join(a.encode() for a in __COMMAND__)+b'\0'
job={'pid':p.pid,'start_ticks':int(fields[19]),'identity_path':str(directory),'name':__JOB_LITERAL__,'status_path':__STATUS_PATH__,'command_sha256':hashlib.sha256(expected).hexdigest()}
metadata=directory/'job-__JOB_NAME__-identity.json'
with metadata.open('x') as file:os.chmod(metadata,0o600);file.write(json.dumps(job))
end=time.monotonic()+2
actual=(proc/'cmdline').read_bytes()
while actual!=expected and time.monotonic()<end:
 time.sleep(.02)
 if int((proc/'stat').read_text().rsplit(')',1)[1].split()[19])!=job['start_ticks']:raise ValueError('Launch PID reused')
 actual=(proc/'cmdline').read_bytes()
if actual!=expected:raise ValueError('Launch command identity differs: '+json.dumps({'expected_sha256':job['command_sha256'],'actual_sha256':hashlib.sha256(actual).hexdigest(),'expected_bytes':len(expected),'actual_bytes':len(actual)}))
print(json.dumps(job))
""".replace("__OWNED_DIRECTORY__", repr(directory))
        .replace("__DATABASE_FLAG__", repr(database))
        .replace("__JOB_LITERAL__", repr(name))
        .replace("__JOB_NAME__", name)
        .replace("__STATUS_PATH__", repr(status_path))
        .replace("__COMMAND__", repr(command))
    )


def process_program(job, *, stop=False):
    return r"""import hashlib,json,os,signal,time
from pathlib import Path
job=JOB
p=Path('/proc')/str(job['pid'])
def running(terminating=False):
 try:
  fields=(p/'stat').read_text().rsplit(')',1)[1].split()
  if int(fields[19])!=job['start_ticks']:raise ValueError('Owned PID reused')
  if fields[0]=='Z':return False
  command=(p/'cmdline').read_bytes()
  if not command and terminating:return True
  if job['identity_path'].encode() not in command:raise ValueError('Owned command differs')
  if job.get('command_sha256') and hashlib.sha256(command).hexdigest()!=job['command_sha256']:raise ValueError('Owned command hash differs')
  if os.getpgid(job['pid'])!=job['pid']:raise ValueError('Owned process group differs')
  return True
 except (FileNotFoundError,ProcessLookupError):return False
if STOP and running():
 os.killpg(job['pid'],signal.SIGTERM)
 end=time.monotonic()+10
 while running(terminating=True) and time.monotonic()<end:time.sleep(.1)
 if running(terminating=True):
  os.killpg(job['pid'],signal.SIGKILL)
  end=time.monotonic()+3
  while running(terminating=True) and time.monotonic()<end:time.sleep(.1)
print(json.dumps({'running':running(terminating=STOP)}))
""".replace("JOB", repr(job)).replace("STOP", repr(stop))


def pipeline_pass(summary):
    return (
        summary["api"]["observed_replicas"] == 4
        and summary["consumer"]["max_replicas"] == 6
        and summary["api_metrics_errors"] == 0
        and summary["database_errors"] == 0
        and summary["resources"]["pass"] is True
        and summary["role_database"]["writer"]["observed_replicas"] == 3
        and all(not v["counter_reset_detected"] for v in summary["role_database"].values())
        and not summary["writer_phases"]["counter_reset_detected"]
    )


def kafka_pass(summary):
    return (
        summary["samples"] >= 5
        and summary["sample_errors"] == 0
        and summary["members_min"] == summary["members_max"] == 6
        and summary["ownership_observed"]
        and summary["max_partitions_per_member"] == 1
        and summary["last_member_partition_groups"] == [[i] for i in range(6)]
    )


def retain_observer_summaries(local, record, inventory=None):
    """Retain diagnostic summaries without clearing any original failure/pass field."""
    rows_by_name = {}
    for name, summarize in (("pipeline", summarize_pipeline), ("kafka", summarize_kafka)):
        path = local / (name + ".jsonl")
        if not path.exists():
            record.setdefault("summary_collection_errors", {})[name] = "FileNotFoundError"
            continue
        try:
            rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            if not rows:
                raise ValueError("Empty observer trace")
            rows_by_name[name] = rows
            record[name + "_summary"] = summarize(rows)
        except Exception as exc:  # noqa: BLE001 - preserve stage failure and other evidence
            record.setdefault("summary_collection_errors", {})[name] = type(exc).__name__
    if inventory and "pipeline" in rows_by_name and record.get("offered_start_utc") and record.get("offered_end_utc"):
        try:
            record["distribution"] = summarize_distribution(
                rows_by_name["pipeline"], inventory, offered_start_utc=record["offered_start_utc"],
                offered_end_utc=record["offered_end_utc"],
            )
        except Exception as exc:  # noqa: BLE001 - no inferred offered-window coverage
            record.setdefault("summary_collection_errors", {})["distribution"] = type(exc).__name__


def adapter_identity():
    paths = [*ADAPTERS, "fixture_identity_evidence.py", "run_two_host_paid_comparison.py", "qualify_two_host_deployment.py", "collect_two_host_inventory.py", "runtime_source_identity.py", "probe_two_host_safety.py"]
    return {"scripts/" + name: source_sha256((ROOT / "scripts" / name).read_bytes()) for name in paths}


def financial_audit_program(event_ids, expected_tickets=18000):
    if type(expected_tickets) is not int or expected_tickets not in (18000, 25200):
        raise ValueError("Unsupported bounded financial expectation")
    return r"""import json,os,time,sys,psycopg
sys.path.insert(0,'/app/scripts')
from audit_checkout_smoke import audit
events=EVENTS
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:
 conn.execute("SET statement_timeout='10s'")
 delay=float(conn.execute('SELECT greatest(coalesce(extract(epoch from max(expires_at)-clock_timestamp()),0),0) FROM holds WHERE event_id=ANY(%s::uuid[])',(events,)).fetchone()[0])
if not 0<=delay<=150:raise ValueError('TTL wait exceeds bound')
time.sleep(delay+.25)
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:
 conn.execute("SET statement_timeout='10s'")
 result=audit(conn,events,__EXPECTED__,__EXPECTED__,1)
 result['hold_deadlines_elapsed']=bool(conn.execute('SELECT coalesce(max(expires_at)<clock_timestamp(),true) FROM holds WHERE event_id=ANY(%s::uuid[])',(events,)).fetchone()[0])
print(json.dumps(result))
""".replace("EVENTS", repr(event_ids)).replace("__EXPECTED__", str(expected_tickets))


def drain(session, cid, audit_program=GLOBAL_AUDIT):
    deadline = time.monotonic() + 60
    while True:
        queue = session.api(cid, audit_program, 45)
        if queue["pass"] and queue["kafka_members"] == 6:
            return queue
        if time.monotonic() >= deadline:
            raise ValueError("Post-stage global drain failed")
        time.sleep(2)


def collect_profile_failure_evidence(session, inventory, local, profile_ledger):
    """Collect declared phase evidence, including the placement-only profile."""
    if profile_ledger in {"bounded_slow_database_diagnostics", "bounded_database_wait_diagnostics", "bounded_api_placement_rebalance", "bounded_application_role_rebalance", "bounded_diagnostic_placement", "bounded_atomic_payment_claim", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
        from slow_database_evidence import collect

        capture = collect(session, inventory, local)
        return {"slow_database_capture": capture, "admission_failure_capture": {
            "decision": "ADR0166", "complete": capture["complete"],
            "record_count": capture["failure_count"], "counter_coverage": capture["counter_coverage"]}}
    from admission_failure_evidence import collect

    return {"admission_failure_capture": collect(session, inventory, local)}


def cpu_spec(arm, role, rows, inventory, *, fixed_two_host=False, placement=None):
    if type(fixed_two_host) is not bool:
        raise ValueError("Explicit CPU placement policy required")
    spec = {
        "schema": 1,
        "arm": arm,
        "host_role": role,
        "instance_uuid_sha256": inventory["hosts"][role]["machine_id_sha256"],
        "containers": [
            {"id": r["Id"], "role": r["Config"]["Labels"]["com.docker.compose.service"]}
            for r in rows
        ],
    }
    if fixed_two_host:
        spec["placement"] = placement or "two-plus-two"
    elif placement is not None:
        raise ValueError("Explicit fixed placement required")
    validate_spec(spec)  # Exact emitted spec must pass before upload or owned job launch.
    return spec


class Stages:
    def __init__(self, execute, bundle, *, rate=60, ledger_key="bounded_control", stage_limit=2, contract=None):
        from work_envelope import base_ledger

        profile_ledger = base_ledger(ledger_key)
        if (type(rate) is not int or type(stage_limit) is not int
                or (rate, profile_ledger, stage_limit) not in ((60, "bounded_control", 2), (84, "bounded_rate_probe", 1), (60, "bounded_status_refresh", 1), (60, "bounded_status_refresh_dedup", 1), (60, "bounded_async_confirmation", 1), (60, "bounded_partial_timeout_reclamation", 1), (60, "bounded_partial_timeout_reclamation_v2", 1), (60, "bounded_payment_stall_diagnostics", 1), (60, "bounded_slow_database_diagnostics", 1), (60, "bounded_database_wait_diagnostics", 1), (60, "bounded_api_placement_rebalance", 1), (60, "bounded_application_role_rebalance", 1), (60, "bounded_diagnostic_placement", 1), (60, "bounded_atomic_payment_claim", 1), (60, "bounded_callback_routing", 1), (60, "bounded_shared_callback_placement", 1), (84, "bounded_shared_callback_rate_probe", 1), (84, "bounded_interleaved_refresh_probe", 1), (84, "bounded_orders_event_index_probe", 1), (84, "bounded_writer_write_pipeline_probe", 1), (84, "bounded_generator_completion_probe", 1))):
            raise ValueError("Unsupported bounded stage contract")
        if profile_ledger in {"bounded_status_refresh", "bounded_status_refresh_dedup", "bounded_async_confirmation", "bounded_partial_timeout_reclamation", "bounded_partial_timeout_reclamation_v2", "bounded_payment_stall_diagnostics", "bounded_slow_database_diagnostics", "bounded_database_wait_diagnostics"} and contract is None:
            raise ValueError("Isolated refresh contract required")
        if profile_ledger == "bounded_atomic_payment_claim" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0193"):
            raise ValueError("Exact source-pinned atomic claim contract required")
        if profile_ledger == "bounded_async_confirmation" and (
                not hasattr(contract, "inventory_marker") or contract.inventory_marker().get("decision") != "ADR0161"):
            raise ValueError("Exact durable confirmation contract required")
        if profile_ledger in {"bounded_partial_timeout_reclamation", "bounded_partial_timeout_reclamation_v2", "bounded_payment_stall_diagnostics", "bounded_slow_database_diagnostics", "bounded_database_wait_diagnostics"} and (
                not hasattr(contract, "inventory_marker") or contract.inventory_marker().get("decision") != "ADR0163"):
            raise ValueError("Exact partial timeout reclamation contract required")
        if profile_ledger in {"bounded_payment_stall_diagnostics", "bounded_slow_database_diagnostics", "bounded_database_wait_diagnostics"} and getattr(contract, "arm", None) != "control":
            raise ValueError("Diagnostic scope must use unchanged control")
        if profile_ledger in {"bounded_api_placement_rebalance", "bounded_diagnostic_placement"} and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0174"):
            raise ValueError("Exact fixed-budget placement contract required")
        if profile_ledger == "bounded_callback_routing" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0216"
                or contract.measured_api_counts != {"primary": 2, "secondary": 2}):
            raise ValueError("Exact fixed-placement callback routing contract required")
        if profile_ledger == "bounded_shared_callback_placement" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0217"
                or contract.measured_api_counts != ({"primary": 2, "secondary": 2} if contract.arm == "control"
                                                   else {"primary": 1, "secondary": 3})
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact shared-callback placement contract required")
        if profile_ledger == "bounded_shared_callback_rate_probe" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0219"
                or contract.arm != "candidate" or contract.measured_api_counts != {"primary": 1, "secondary": 3}
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact registered candidate-only 84/s shared-callback contract required")
        if profile_ledger == "bounded_interleaved_refresh_probe" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0222"
                or contract.arm != "candidate" or contract.measured_api_counts != {"primary": 1, "secondary": 3}
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact consumer-only 84/s correction contract required")
        if profile_ledger == "bounded_orders_event_index_probe" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0224"
                or contract.arm != "candidate" or contract.measured_api_counts != {"primary": 1, "secondary": 3}
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact index-only 84/s correction contract required")
        if profile_ledger == "bounded_generator_completion_probe" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0226"
                or contract.settings("reservation-writer").get("RESERVATION_WRITE_PIPELINE") != "1"
                or contract.arm != "candidate" or contract.measured_api_counts != {"primary": 1, "secondary": 3}
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact generator-only 84/s correction contract required")
        if profile_ledger == "bounded_writer_write_pipeline_probe" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0225"
                or contract.settings("reservation-writer").get("RESERVATION_WRITE_PIPELINE") != "1"
                or contract.arm != "candidate" or contract.measured_api_counts != {"primary": 1, "secondary": 3}
                or contract.settings("simulator").get("API_URL") != "http://load-balancer:8000"):
            raise ValueError("Exact writer-only 84/s correction contract required")
        if profile_ledger == "bounded_application_role_rebalance" and (
                contract is None or contract.inventory_marker().get("decision") != "ADR0177"
                or getattr(contract, "diagnostic_scope", None) != "application_role"):
            raise ValueError("Exact application-role placement contract required")
        if profile_ledger in {"bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'} and (
                contract is None or not hasattr(contract, "diagnostic_context")):
            raise ValueError("Exact protected diagnostic placement contract required")
        self.contract = contract
        self.execute, self.bundle, self.results = execute, bundle, {}
        self.rate, self.ledger_key, self.stage_limit = rate, ledger_key, stage_limit
        self.expected_tickets = rate * 300

    def __call__(self, session, arm, routes, saved, owner, output):
        local = output / arm
        local.mkdir(mode=0o700)
        directory = "/tmp/" + output.name + "-" + arm
        remote = owner + "/" + arm
        gen = session.config["generator"]["repo"] + "/tmp/" + output.name + "-" + arm
        cid = next(r["container_id"] for r in routes if r["host_role"] == "primary")
        jobs = []
        event_ids = []
        record = {"arm": arm, "pre_dispatch_qualified": False, "customers_dispatched": False, "pass": False}
        self.results[arm] = record
        try:
            for role, path in [("primary", remote), ("generator", gen)]:
                session.call(
                    role,
                    "import json;from pathlib import Path;Path("
                    + repr(path)
                    + ").mkdir(mode=0o700);print(json.dumps({'fresh':True}))",
                )
            session.api(
                cid,
                "import json;from pathlib import Path;Path("
                + repr(directory)
                + ").mkdir(mode=0o700);print(json.dumps({'fresh':True}))",
                45,
            )
            baseline_env = saved["model"]["services"]["api"]["environment"]
            # Allow bounded worker-group startup without dispatching any buyer.
            deadline = time.monotonic() + 45
            while True:
                try:
                    inventory, view, primary, secondary = observe(
                        session, arm, routes, baseline_env,
                        contract=self.contract,
                        expected_worker_images={role: saved["model"]["services"][role]["image"]
                                               for role in (self.contract.background if self.contract is not None else ("consumer", "reservation-writer", "maintenance", "publisher", "reconciler", "simulator"))},
                    )
                    break
                except ValueError as exc:
                    if (
                        str(exc) != "Six Kafka members required before dispatch"
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(2)
            (local / "inventory.private.json").write_text(json.dumps(inventory, indent=2))
            if self.contract is not None:
                record["inventory_qualification"] = self.contract.qualify_inventory(inventory)
                (local / "inventory-qualification.private.json").write_text(
                    json.dumps(record["inventory_qualification"], indent=2) + "\n"
                )
            record["inventory_contract"] = view
            record["pinned_harness_files"] = len(self.bundle)
            # Verify every frozen helper used inside the API image, including observer/audit.
            names = (
                "observe_paid_pipeline.py",
                "kafka_lag_observe.py",
                "prepare_capacity_fixture.py",
                "audit_checkout_smoke.py",
                "capacity_queue_state.py",
            )
            expected = {name: source_sha256(self.bundle["scripts/" + name]) for name in names}
            proof = session.api(
                cid,
                "import hashlib,json;from pathlib import Path;expected="
                + repr(expected)
                + ";actual={k:hashlib.sha256((Path('/app/scripts')/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected};assert actual==expected;print(json.dumps({'frozen_helpers_verified':True}))",
                45,
            )
            record.update(proof)
            from work_envelope import base_ledger
            if base_ledger(self.ledger_key) in {"bounded_database_wait_diagnostics", "bounded_api_placement_rebalance"}:
                from database_wait_evidence import preflight_program
                record["database_wait_preflight"] = session.api(cid, preflight_program(), 45)
            if base_ledger(self.ledger_key) in {"bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
                from diagnostic_runner_connection import prepare as prepare_diagnostic_connection
                from diagnostic_runner_connection import qualify_bound_inventory
                record["diagnostic_cleanup_required"] = True
                record.update(prepare_diagnostic_connection(session, cid, remote, directory, inventory, api_upload, self.contract.diagnostic_context))
                qualify_bound_inventory(self.contract, inventory, record, local)
            if base_ledger(self.ledger_key) == "bounded_application_role_rebalance":
                from application_role_runner_diagnostics import prepare as prepare_scoped_diagnostics
                record.update(prepare_scoped_diagnostics(session, cid, remote, directory, inventory, api_upload))
            fixture_program = (
                "import json,os,subprocess;from pathlib import Path;p=Path("
                + repr(directory)
                + ");env=dict(os.environ,TEST_DATABASE_URL=os.environ['DATABASE_URL'],TEST_REDIS_URL=os.environ['REDIS_URL']);r=subprocess.run(['python','/app/scripts/prepare_capacity_fixture.py','--output',str(p/'fixture.json'),'--shows',"
                + repr(str(self.rate) if self.execute else "1")
                + ",'--seats','300','--sale-hours','1'],env=env,capture_output=True,text=True,timeout=90);r.check_returncode();print((p/'fixture.json').read_text())"
            )
            fixture = session.api(cid, fixture_program, 115)
            event_ids = fixture["show_ids"]
            retain_fixture_identity(local, record, fixture, self.rate if self.execute else 1,
                                    producer_sha256=source_sha256(self.bundle["scripts/prepare_capacity_fixture.py"]))
            origin = "http://" + session.config["primary"]["private_ipv4"] + ":8000"
            # Mint only expiring viewer tokens on the API. Never send the signing key.
            mint = (
                r"""import json,os,jwt,time
from pathlib import Path
from uuid import uuid4
p=Path(DIRECTORY)
fixture=json.loads((p/'fixture.json').read_text())
identity=str(uuid4());expiry=time.time()+3600
manifest={'schema_version':1,'environment':'development','id':identity,'origin':ORIGIN,'expires_at':__import__('datetime').datetime.fromtimestamp(expiry,__import__('datetime').UTC).isoformat(),'show_ids':fixture['show_ids'],'viewer_tokens':[jwt.encode({'sub':'load-'+identity+'-'+str(i),'aud':'ticketing','iss':'ticketing','exp':expiry},os.environ['JWT_SECRET'],algorithm='HS256') for i in range(VIEWERS)],'seat_offset':0,'seats_per_show':300,'fixture_layout':'distributed'}
(p/'manifest.private.json').write_text(json.dumps(manifest));os.chmod(p/'manifest.private.json',0o600)
print(json.dumps({'viewers':len(manifest['viewer_tokens']),'shows':len(manifest['show_ids'])}))
""".replace("DIRECTORY", repr(directory))
                .replace("ORIGIN", repr(origin))
                .replace("VIEWERS", str(self.expected_tickets) if self.execute else "2")
            )
            session.api(cid, mint, 45)
            manifest_raw = copy_out(
                session, cid, directory + "/manifest.private.json", remote + "/manifest.private.json"
            )
            session.put("generator", gen + "/manifest.private.json", manifest_raw.decode(), True)
            for name in CORE:
                session.put("generator", gen + "/" + name, self.bundle["scripts/" + name].decode(), True)
            session.put(
                "generator",
                gen + "/run_synchronized_paid_generator.py",
                (ROOT / "scripts/run_synchronized_paid_generator.py").read_text(),
                True,
            )
            # Read-back verifies the transferred bytes, including scheduling adapter.
            expected = {name: source_sha256(self.bundle["scripts/" + name]) for name in CORE}
            expected["run_synchronized_paid_generator.py"] = source_sha256(
                (ROOT / "scripts/run_synchronized_paid_generator.py").read_bytes()
            )
            session.call(
                "generator",
                "import json,hashlib;from pathlib import Path;p=Path("
                + repr(gen)
                + ");expected="
                + repr(expected)
                + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'transferred_generator_identity':True}))",
            )
            record["transferred_generator_identity"] = True
            record["transferred_generator_source_sha256"] = expected
            if base_ledger(self.ledger_key) not in {"bounded_application_role_rebalance", "bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
                for name in ("observe_two_host_pipeline.py", "prepare_two_host_scaling.py", "database_wait_evidence.py"):
                    api_upload(session, cid, remote, directory, name, (ROOT / "scripts" / name).read_text())
            api_upload(session, cid, remote, directory, "inventory.private.json", json.dumps(inventory))
            for name, arguments in [
                (
                    "pipeline",
                    [
                        "python",
                        directory + "/observe_two_host_pipeline.py",
                        "--inventory",
                        directory + "/inventory.private.json",
                        "--frozen-observer",
                        "/app/scripts/observe_paid_pipeline.py",
                        "--manifest",
                        directory + "/manifest.private.json",
                        "--output",
                        directory + "/pipeline.jsonl",
                        "--seconds",
                        "480",
                    ],
                ),
                (
                    "kafka",
                    [
                        "python",
                        "/app/scripts/kafka_lag_observe.py",
                        "--backend",
                        "python",
                        "--output",
                        directory + "/kafka.jsonl",
                        "--seconds",
                        "480",
                    ],
                ),
            ]:
                if name == "pipeline" and self.contract is not None:
                    from status_refresh_contract import digest

                    arguments += ["--approved-inventory-sha256", digest(inventory)]
                from work_envelope import base_ledger
                if name == "pipeline" and base_ledger(self.ledger_key) in {"bounded_database_wait_diagnostics", "bounded_api_placement_rebalance", "bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
                    arguments += ["--database-wait-diagnostics"]
                    if base_ledger(self.ledger_key) in {"bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
                        arguments += ["--diagnostic-connection-bundle", directory + "/diagnostic.private.json"]
                if name == "pipeline" and base_ledger(self.ledger_key) == "bounded_application_role_rebalance":
                    arguments += ["--application-database-wait-diagnostics"]
                job = self.launch(session, "container", cid, arguments, directory, jobs, record, database=True)
                record[name + "_job"] = job
            # First samples must pass before either CPUs or paid traffic are scheduled.
            deadline = time.monotonic() + 30
            while True:
                status = session.api(
                    cid,
                    "import json;from pathlib import Path;p=Path("
                    + repr(directory)
                    + ");print(json.dumps({n:json.loads((p/(n+'.jsonl')).read_text().splitlines()[0]) if (p/(n+'.jsonl')).exists() and (p/(n+'.jsonl')).stat().st_size else None for n in ('pipeline','kafka')}))",
                    45,
                )
                if status["pipeline"] and status["kafka"]:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Observer startup bound exceeded")
                time.sleep(1)
            if base_ledger(self.ledger_key) in {"bounded_database_wait_diagnostics", "bounded_api_placement_rebalance", "bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'} and status["pipeline"].get("database_wait_diagnostics", {}).get("complete") is not True:
                raise ValueError("Database diagnostic startup gate failed before buyer dispatch")
            if base_ledger(self.ledger_key) == "bounded_application_role_rebalance":
                from application_role_runner_diagnostics import startup as scoped_startup
                record["application_database_wait_startup"] = scoped_startup(status["pipeline"], inventory)
            record["pipeline_startup"] = pipeline_startup_view(status["pipeline"], 6)
            if self.contract is not None and hasattr(self.contract, "verify_pipeline_startup"):
                self.contract.verify_pipeline_startup(status["pipeline"])
            record["kafka_startup"] = kafka_startup_view(status["kafka"], 6)
            if not record["pipeline_startup"]["pass"] or not record["kafka_startup"]["pass"]:
                raise ValueError("Observer startup gate failed")
            record["pre_dispatch_qualified"] = True
            if self.contract is not None:
                record["pre_dispatch_qualified_at_utc"] = datetime.now(UTC).isoformat()
            seconds = 300 if self.execute else 5
            start = time.time() + 30
            start_utc = datetime.fromtimestamp(start, UTC).isoformat()
            if self.contract is not None:
                record["scheduled_offered_start_utc"] = start_utc
            for role, rows in [("primary", primary), ("secondary", secondary)]:
                location = (
                    remote
                    if role == "primary"
                    else session.config["secondary"]["prepared_directory"] + "/" + output.name + "-" + arm
                )
                if role == "secondary":
                    session.call(
                        role,
                        "import json;from pathlib import Path;Path("
                        + repr(location)
                        + ").mkdir(mode=0o700);print(json.dumps({'fresh':True}))",
                    )
                spec = cpu_spec(
                    arm, role, rows, inventory, fixed_two_host=self.contract is not None,
                    placement=getattr(self.contract, "cpu_placement", None),
                )
                session.put(role, location + "/cpu-spec.json", json.dumps(spec), True)
                session.put(
                    role,
                    location + "/observe_two_host_cpu.py",
                    (ROOT / "scripts/observe_two_host_cpu.py").read_text(),
                    True,
                )
                args = [
                    "python3",
                    location + "/observe_two_host_cpu.py",
                    "--spec",
                    location + "/cpu-spec.json",
                    "--output",
                    location + "/cpu.json",
                    "--start-at",
                    start_utc,
                    "--seconds",
                    str(seconds),
                ]
                job = self.launch(session, role, cid, args, location, jobs, record)
                record[role + "_cpu_path"] = location + "/cpu.json"
            if self.execute:
                if self.contract is not None:
                    record["dispatch_requested_at_utc"] = datetime.now(UTC).isoformat()
                    self.contract.validate_inventory_receipt(record, inventory, final=False)
                    admission = {k: record[k] for k in (
                        "arm", "inventory_qualification", "inventory_contract",
                        "pre_dispatch_qualified", "pre_dispatch_qualified_at_utc",
                        "dispatch_requested_at_utc", "scheduled_offered_start_utc",
                    )}
                    (local / "dispatch-admission.private.json").write_text(
                        json.dumps(admission, indent=2) + "\n"
                    )
                    session.state.setdefault("dispatch_admissions", {})[arm] = admission
                    session.checkpoint()  # Persist before reserving or launching any paid customer.
                self.claim_paid_stage(session, arm)
                args = [
                    "/root/http-load-venv/bin/python",
                    gen + "/run_synchronized_paid_generator.py",
                    "--frozen-generator",
                    gen + "/paid_ticket_sharded_generator.py",
                    "--start-at-epoch",
                    str(start),
                    "--manifest",
                    gen + "/manifest.private.json",
                    "--origin",
                    origin,
                    "--output",
                    gen + "/customer.json",
                    "--rate",
                    str(self.rate),
                    "--seconds",
                    "300",
                    "--completion-deadline-seconds",
                    "420",
                    "--concurrency",
                    "500",
                    "--http-client-count",
                    "8",
                    "--poll-seconds",
                    "1",
                    "--duplicates",
                    "1",
                    "--lifecycle-diagnostics",
                ]
                record["customers_dispatched"] = True  # Conservatively includes ambiguous dispatch.
                session.checkpoint()
                job = self.launch(session, "generator", cid, args, gen, jobs, record)
                self.wait(session, job, "generator", 480, allowed_returncodes={0, 1})
                record["customer"] = json.loads(fetch(session, "generator", gen + "/customer.json"))
                # Core's HTTP/customer failure remains failed. Financial audit is independent.
            for role, cpu_job in jobs:
                if role in {"primary", "secondary"}:
                    self.wait(session, cpu_job, role, seconds + 65)
            cpus = {
                role: summarize_cpu(json.loads(fetch(session, role, record[role + "_cpu_path"])))
                for role in ("primary", "secondary")
            }
            record["cpu"] = cpus
            record["scheduled_cpu_window"] = compare_windows(
                cpus["primary"], cpus["secondary"], offered_start_utc=start_utc,
                offered_end_utc=datetime.fromtimestamp(start + seconds, UTC).isoformat(),
            )
            if self.execute:
                shards = record["customer"]["shards"]
                starts = [datetime.fromisoformat(row["started_at_utc"]) for row in shards]
                if len(starts) != 2 or abs((max(starts) - min(starts)).total_seconds()) > 1:
                    raise ValueError("Generator shards did not share offered window")
                offered_start = min(starts).isoformat()
                offered_end = datetime.fromtimestamp(
                    min(t.timestamp() for t in starts) + 300, UTC
                ).isoformat()
                record["offered_start_utc"] = offered_start
                record["offered_end_utc"] = offered_end
                record["cpu_window"] = compare_windows(
                    cpus["primary"],
                    cpus["secondary"],
                    offered_start_utc=offered_start,
                    offered_end_utc=offered_end,
                )
                record["financial"] = session.api(cid, financial_audit_program(event_ids, self.expected_tickets), 175)
            if self.execute and self.contract is not None and hasattr(self.contract, "stage_receipt_audit"):
                record["confirmation_receipts"] = self.contract.stage_receipt_audit(session, cid, event_ids, self.expected_tickets)
            record["global_queues"] = drain(session, cid, getattr(self.contract, "global_audit", GLOBAL_AUDIT))
            # Collect available traces before any owned stop can lose the SSH transport.
            for name in ("pipeline", "kafka"):
                raw = copy_out(session, cid, directory + "/" + name + ".jsonl", remote + "/" + name + ".jsonl")
                (local / (name + ".jsonl")).write_bytes(raw)
            retain_observer_summaries(local, record, inventory)
            # Final stopped traces are collected again on the normal success path.
            for role, job in jobs:
                stopped = self.stop(session, role, cid, job)
                if stopped["running"]:
                    raise ValueError("Owned observer/job did not stop")
            jobs = []
            for name in ("pipeline", "kafka"):
                raw = copy_out(
                    session, cid, directory + "/" + name + ".jsonl", remote + "/" + name + ".jsonl"
                )
                (local / (name + ".jsonl")).write_bytes(raw)
            pipeline_rows = [
                json.loads(line) for line in (local / "pipeline.jsonl").read_text().splitlines() if line
            ]
            kafka_rows = [
                json.loads(line) for line in (local / "kafka.jsonl").read_text().splitlines() if line
            ]
            record["pipeline_summary"] = summarize_pipeline(pipeline_rows)
            record["kafka_summary"] = summarize_kafka(kafka_rows)
            if self.execute:
                record["distribution"] = summarize_distribution(
                    pipeline_rows,
                    inventory,
                    offered_start_utc=record["offered_start_utc"],
                    offered_end_utc=record["offered_end_utc"],
                )
                if (
                    not record["financial"]["pass"]
                    or not record["financial"]["hold_deadlines_elapsed"]
                    or not pipeline_pass(record["pipeline_summary"])
                    or not kafka_pass(record["kafka_summary"])
                ):
                    raise ValueError("Financial or observer gate failed")
            elif not pipeline_pass(record["pipeline_summary"]) or not kafka_pass(record["kafka_summary"]):
                raise ValueError("Dry final observer summary failed")
            record["pass"] = True
        except BaseException as exc:
            record["failure_type"] = type(exc).__name__
            (local / "failure.private.log").write_text(traceback.format_exc())
            raise
        finally:
            if hasattr(session, "begin_cleanup"):
                session.begin_cleanup()
            cleanup_errors = []
            for name in ("pipeline", "kafka"):
                if not (local / (name + ".jsonl")).exists():
                    try:
                        raw = copy_out(session, cid, directory + "/" + name + ".jsonl", remote + "/failure-" + name + ".jsonl")
                        (local / (name + ".jsonl")).write_bytes(raw)
                    except Exception as exc:  # noqa: BLE001 - retain partial failure evidence
                        record.setdefault("trace_collection_errors", []).append(type(exc).__name__)
            retain_observer_summaries(local, record, inventory if "inventory" in locals() else None)
            from work_envelope import base_ledger
            if base_ledger(self.ledger_key) in {"bounded_database_wait_diagnostics", "bounded_api_placement_rebalance", "bounded_diagnostic_placement", "bounded_callback_routing", "bounded_shared_callback_placement", "bounded_shared_callback_rate_probe", "bounded_interleaved_refresh_probe", "bounded_orders_event_index_probe", 'bounded_writer_write_pipeline_probe', 'bounded_generator_completion_probe'}:
                from database_wait_evidence import summarize as summarize_database_wait
                try:
                    record["database_wait_capture"] = summarize_database_wait(local / "pipeline.jsonl")
                except Exception as exc:  # noqa: BLE001 - preserve cleanup and fail diagnostic gate
                    record["database_wait_capture"] = {"complete": False, "error_type": type(exc).__name__}
            if base_ledger(self.ledger_key) == "bounded_application_role_rebalance":
                from application_role_runner_diagnostics import summarize as summarize_scoped_diagnostics
                try:
                    record["application_database_wait_capture"] = summarize_scoped_diagnostics(local / "pipeline.jsonl", inventory)
                except Exception as exc:  # noqa: BLE001 - continue mandatory financial cleanup.
                    record["application_database_wait_capture"] = {
                        "application_database_wait_evidence_complete": False, "full_database_visibility_complete": False,
                        "error_type": type(exc).__name__}
            if (self.contract is not None and self.contract.inventory_marker().get("decision") in {"ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225', 'ADR0226'}
                    and "inventory" in locals()):
                try:
                    from work_envelope import base_ledger

                    record.update(collect_profile_failure_evidence(session, inventory, local, base_ledger(self.ledger_key)))
                    if record["admission_failure_capture"]["complete"] is not True:
                        record["pass"] = False
                except Exception as exc:  # noqa: BLE001 - retain failed diagnostics; always continue financial cleanup.
                    record["admission_failure_capture"] = {"complete": False, "failure_type": type(exc).__name__}
                    record["pass"] = False
            for role, job in jobs:
                try:
                    log = "job-" + job["name"] + ".log"
                    if role == "container":
                        raw = copy_out(session, cid, directory + "/" + log, remote + "/" + log)
                    else:
                        raw = fetch(session, role, job["identity_path"] + "/" + log)
                    (local / (role + "-" + log)).write_bytes(raw[-16000:])
                except Exception as exc:  # noqa: BLE001 - retain evidence failure, continue cleanup
                    record.setdefault("log_collection_errors", []).append(type(exc).__name__)
            for role, job in reversed(jobs):
                try:
                    self.stop(session, role, cid, job)
                except Exception as exc:  # noqa: BLE001 - preserve mandatory remaining cleanup
                    cleanup_errors.append(type(exc).__name__)
            if record.get("diagnostic_cleanup_required"):
                try:
                    from diagnostic_runner_connection import cleanup as cleanup_diagnostic_connection
                    record.update(cleanup_diagnostic_connection(session, cid, remote, directory))
                except BaseException as exc:  # noqa: BLE001 - preserve other mandatory recovery
                    cleanup_errors.append(type(exc).__name__)
            if record["customers_dispatched"]:
                # Audits remain mandatory even if CPU/transport/observer collection failed.
                for name, operation in (
                    ("financial", lambda: session.api(cid, financial_audit_program(event_ids, self.expected_tickets), 175)),
                    ("global_queues", lambda: drain(session, cid, getattr(self.contract, "global_audit", GLOBAL_AUDIT))),
                ):
                    if name not in record:
                        try:
                            record[name] = operation()
                        except Exception as exc:  # noqa: BLE001 - continue mandatory other audit/restoration
                            record[name + "_failure_type"] = type(exc).__name__
                            cleanup_errors.append(type(exc).__name__)
            if event_ids:
                try:
                    record["retired_shows"] = session.api(
                        cid,
                        "import json,os,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n rows=conn.execute('UPDATE events SET sale_ends=clock_timestamp() WHERE id=ANY(%s::uuid[]) RETURNING id',("
                        + repr(event_ids)
                        + ",)).fetchall()\nprint(json.dumps({'count':len(rows)}))",
                        45,
                    )["count"]
                except Exception as exc:  # noqa: BLE001 - preserve mandatory remaining cleanup
                    cleanup_errors.append(type(exc).__name__)
            for role, path, names in [
                ("primary", remote, ["manifest.private.json"]),
                ("generator", gen, ["manifest.private.json"]),
            ]:
                try:
                    session.call(
                        role,
                        "import json;from pathlib import Path;p=Path("
                        + repr(path)
                        + ");names="
                        + repr(names)
                        + "\nfor name in names:(p/name).unlink(missing_ok=True)"
                        + "\nfor child in p.glob('paid-shards-*'):"
                        + "\n if child.is_symlink() or child.resolve().parent!=p.resolve() or not child.is_dir():raise ValueError('Unsafe shard directory')"
                        + "\n for file in child.iterdir():"
                        + "\n  if file.is_symlink() or file.name not in {'manifest-0.json','manifest-1.json','result-0.json','result-1.json'} or not file.is_file():raise ValueError('Unexpected shard artifact')"
                        + "\n  file.unlink()"
                        + "\n child.rmdir()"
                        + "\nprint(json.dumps({'private_manifests_removed':True}))",
                    )
                except Exception as exc:  # noqa: BLE001 - preserve mandatory remaining cleanup
                    cleanup_errors.append(type(exc).__name__)
            record["private_cleanup_pass"] = not cleanup_errors
            record["cleanup_errors"] = cleanup_errors
            (local / "stage.private.json").write_text(json.dumps(record, indent=2) + "\n")
            session.state.setdefault("stages", {})[arm] = {
                "pass": record["pass"],
                "pre_dispatch_qualified": record["pre_dispatch_qualified"],
                "private_cleanup_pass": record["private_cleanup_pass"],
                "customers_dispatched": record["customers_dispatched"],
                "failure_type": record.get("failure_type"),
            }
            session.checkpoint()
            if cleanup_errors:
                raise RuntimeError("Owned stage cleanup failed")

    def claim_paid_stage(self, session, arm):
        if session.state["capacity_stages_started"] >= self.stage_limit:
            raise ValueError("Paid stage limit exceeded")
        session.state["capacity_stages_started"] += 1
        state_path = ROOT / "docs/capacity/CURRENT_STATE.json"
        journal = json.loads(state_path.read_text())
        journal[self.ledger_key]["paid_runs_started"] = session.state["capacity_stages_started"]
        journal[self.ledger_key]["attempted_paid_arms"] = [*session.state.get("attempted_paid_arms", []), arm]
        state_path.write_text(json.dumps(journal, indent=2) + "\n")
        session.state.setdefault("attempted_paid_arms", []).append(arm)
        session.checkpoint()
        if session.state["capacity_stages_started"] > self.stage_limit:
            raise ValueError("Paid stage limit exceeded")

    def launch(self, session, role, cid, arguments, directory, jobs, record, *, database=False):
        try:
            code = job_program(arguments, directory, database=database)
            job = session.api(cid, code, 45) if role == "container" else session.call(role, code, 45)
            jobs.append((role, job))
            return job
        except Exception:
            try:
                path = directory + "/job-" + Path(arguments[1]).stem + "-identity.json"
                if role == "container":
                    recovered = session.api(cid, "from pathlib import Path;print(Path(" + repr(path) + ").read_text())", 45)
                else:
                    recovered = json.loads(fetch(session, role, path))
                jobs.append((role, recovered))
                record.setdefault("recovered_launch_roles", []).append(role)
            except Exception as recovery_error:  # noqa: BLE001 - unknown launch remains failed
                record.setdefault("launch_identity_recovery_failures", []).append(type(recovery_error).__name__)
            raise

    def stop(self, session, role, cid, job):
        code = process_program(job, stop=True)
        try:
            return session.api(cid, code, 45) if role == "container" else session.call(role, code, 45)
        except Exception as exc:
            if not transport_failure(exc):
                raise
            # Only this exact identity-guarded cleanup program may be repeated.
            session.reconnect("primary" if role == "container" else role)
            return session.api(cid, code, 45) if role == "container" else session.call(role, code, 45)

    def wait(self, session, job, role, timeout, *, allowed_returncodes=None):
        deadline = time.monotonic() + timeout
        last = 0
        while True:
            if not session.call(role, process_program(job), 45)["running"]:
                receipt = json.loads(fetch(session, role, job["status_path"]))
                if receipt.get("returncode") not in (allowed_returncodes or {0}):
                    raise ValueError("Owned job failed with nonzero exit")
                return
            if time.monotonic() >= deadline:
                raise TimeoutError("Bounded paid completion exceeded")
            if time.monotonic() - last >= 30:
                print(
                    json.dumps(
                        {
                            "phase": "bounded-paid-completion",
                            "seconds_remaining": round(deadline - time.monotonic()),
                        }
                    ),
                    flush=True,
                )
                last = time.monotonic()
            time.sleep(5)


def gates_for_stage(record, inventory, restoration):
    """Fail closed; every gate derives from executed stage/restoration evidence."""
    source = (
        all(a.get("source_hashes_match") is True for a in inventory.get("apis", []))
        and len(inventory.get("apis", [])) == 4
    )
    observed = record.get("inventory_contract", {}).get("inventory_contract_pass") is True
    restored = (
        restoration.get("restore_pass") is True
        and restoration.get("primary_runtime_semantics_restored") is True
    )
    customer = record.get("customer", {})
    financial = record.get("financial", {})
    queue = record.get("global_queues", {})
    cpu = record.get("cpu_window", {})
    distribution = record.get("distribution", {})
    pool_settings = observed and all(
        a.get("settings", {}).get("API_PAYMENT_POOL_MAX") == "2" for a in inventory.get("apis", [])
    )
    harness = (
        record.get("frozen_helpers_verified") is True
        and record.get("transferred_generator_identity") is True
        and record.get("pinned_harness_files") == 78
    )
    paid = {
        "customer_load": customer.get("pass") is True,
        "post_ttl_financial": financial.get("pass") is True,
        "zero_double_booking": financial.get("pass") is True and financial.get("duplicate_booked_seats") == 0,
        "full_keyspace_queue_drain": queue.get("pass") is True,
        "kafka_drain": queue.get("pass") is True and queue.get("kafka_total_lag") == 0,
        "pipeline_observer": bool(record.get("pipeline_summary"))
        and pipeline_pass(record["pipeline_summary"]),
        "kafka_observer": bool(record.get("kafka_summary")) and kafka_pass(record["kafka_summary"]),
        "cpu_sampler": cpu.get("both_hosts_cpu_observed") is True,
        "source_and_restored_configuration_pass": restored and source,
        "generator_idle_pass": restoration.get("generator_idle_after") is True,
        "private_cleanup_pass": record.get("private_cleanup_pass") is True
        and restoration.get("credential_snapshots_removed") is True,
        "hold_deadlines_elapsed": financial.get("hold_deadlines_elapsed") is True,
        "pipeline_observer_source": record.get("frozen_helpers_verified") is True,
        "cache_disabled": observed
        and all(a.get("settings", {}).get("ORDER_STATUS_CACHE_MS") == "0" for a in inventory.get("apis", [])),
        "candidate_source_identity": observed and source,
        "only_expected_runtime_modules_changed": observed and source,
        "payment_partition_active": pool_settings,
        "fixed_connection_and_waiter_budget": observed,
        "payment_partition_restored": restored,
        "financial_logic_unchanged": source and harness,
        "baseline_runtime_deployed": observed and source,
        "pinned_harness_identity": harness,
    }
    safety = restoration.get("safety", {})
    additional = {
        "distinct_api_and_generator_machines": observed,
        "fixed_aggregate_budgets": observed,
        "all_four_replicas_observed": distribution.get("all_four_replicas_observed") is True,
        "both_hosts_cpu_observed": cpu.get("both_hosts_cpu_observed") is True,
        "same_offered_measurement_window": cpu.get("same_offered_measurement_window") is True,
        "per_replica_traffic_distribution": distribution.get("per_replica_traffic_distribution") is True,
        "cross_host_one_seat_one_owner": safety.get("one_durable_owner_before_payment") is True
        and safety.get("accepted_holds") == 1,
        "cross_host_idempotent_replay": safety.get("cross_host_hold_replay") is True
        and safety.get("cross_host_payment_replay") is True,
        "secondary_resources_removed": restoration.get("secondary_resources_removed") is True,
        "primary_lb_and_four_apis_restored": restored,
    }
    from prepare_two_host_scaling import evaluate_gates

    return {"paid": paid, "additional": additional, **evaluate_gates(paid, additional)}


def validate_config(config):
    from pathlib import PurePosixPath

    from prepare_two_host_scaling import private_ipv4

    for role, key in (("primary", "repo"), ("secondary", "prepared_directory"), ("generator", "repo")):
        if role not in config or key not in config[role]:
            raise ValueError("Missing required workspace path for " + role)
        path = PurePosixPath(config[role][key])
        if not path.is_absolute() or len(path.parts) < 3 or ".." in path.parts:
            raise ValueError("Owned absolute workspace required")
        private_ipv4(config[role]["private_ipv4"])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--ssh-runtime", type=Path, required=True)
    p.add_argument("--execute", action="store_true")
    args = p.parse_args()
    state = json.loads((ROOT / "docs/capacity/CURRENT_STATE.json").read_text())
    if state["cloud_load_requires_resume"] or (args.execute and state["bounded_control"]["paid_runs_started"] != 0):
        raise ValueError("Paused or already consumed comparison")
    identity = adapter_identity()
    if args.execute:
        qualification_path = state["bounded_control"].get("dry_qualification_report")
        if not qualification_path:
            raise ValueError("Passing dry qualification required")
        qualification = json.loads((ROOT / qualification_path).read_text())
        if qualification.get("pass") is not True or qualification.get("adapter_identity") != identity:
            raise ValueError("Dry qualification must pass for exact current adapters")
    config = json.loads(args.config.read_text())
    validate_config(config)
    bundle = frozen_bundle()
    output = ROOT / "tmp" / ("adr0147-topology-" + uuid4().hex[:12])
    output.mkdir(mode=0o700)
    print(json.dumps({"phase": "integration-output-created", "output": str(output)}), flush=True)
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    stages = Stages(args.execute, bundle)
    result = run(config, output, stage_hook=stages)
    dry_pass = (
        result["pass"]
        and set(stages.results) == {"control", "candidate"}
        and all(
            r["pass"] and r["private_cleanup_pass"] and r["pre_dispatch_qualified"]
            for r in stages.results.values()
        )
    )
    gates = {}
    if args.execute:
        for arm, record in stages.results.items():
            inventory = json.loads((output / arm / "inventory.private.json").read_text())
            gates[arm] = gates_for_stage(record, inventory, result)
    passed = dry_pass if not args.execute else (
        dry_pass and set(gates) == {"control", "candidate"}
        and all(g["all_required_gates_pass"] for g in gates.values())
    )
    report = {
        "kind": "two_host_paid_comparison" if args.execute else "two_host_paid_runner_dry_qualification",
        "pass": bool(passed),
        "capacity_stages_started": result["capacity_stages_started"],
        "output": str(output),
        "stage_names": list(stages.results),
        "restore_pass": result["restore_pass"],
        "adapter_identity": identity,
        "gates": gates,
    }
    report_path = output / "integration-summary.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    state = json.loads((ROOT / "docs/capacity/CURRENT_STATE.json").read_text())
    state["current_run"] = None
    state["bounded_control"]["active_run"] = None
    if args.execute:
        state["bounded_control"]["paid_runs_started"] = result["capacity_stages_started"]
    state["bounded_control"]["last_runner_report"] = report_path.relative_to(ROOT).as_posix()
    if not args.execute and passed:
        state["bounded_control"]["dry_qualification_report"] = report_path.relative_to(ROOT).as_posix()
        state["status"] = "adr0147_dry_qualified_paid_comparison_ready"
    elif args.execute:
        state["status"] = "adr0147_matched_paid_comparison_finished" if passed else "adr0147_matched_paid_comparison_failed_gates"
    else:
        state["status"] = "adr0147_dry_qualification_failed"
    (ROOT / "docs/capacity/CURRENT_STATE.json").write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(report), flush=True)
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
