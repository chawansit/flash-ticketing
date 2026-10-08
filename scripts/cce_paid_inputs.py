"""ADR0228 concrete fixed CCE fixture and four-pod safety caller."""

import base64
import json
from uuid import uuid4

import work_envelope as policy
from cce_paid_lifecycle import require_safety
from cce_paid_stage import sha
from fixture_identity_evidence import fixture_identity
from run_two_host_paid_comparison import copy_out, drain

HELPERS = (
    "cce_paid_safety.py",
    "cce_api_adapter.py",
    "cce_dependency_probe.py",
    "work_envelope.py",
    "prepare_two_host_scaling.py",
    "probe_two_host_safety.py",
)


def helper_upload_program(directory, name, raw):
    if name not in HELPERS:
        raise ValueError("Unknown native safety helper")
    return (
        "import base64,json,os;from pathlib import Path;p=Path("
        + repr(directory)
        + ")/"
        + repr(name)
        + ";raw=base64.b64decode("
        + repr(base64.b64encode(raw).decode())
        + ")\nwith p.open('xb') as f:\n os.chmod(p,0o600);f.write(raw)\n"
        + "print(json.dumps({'uploaded':True,'runtime_uid_owns_file':p.stat().st_uid==os.getuid()}))"
    )


def fixture_program(directory, shows, producer_sha):
    if shows not in (1, 84):
        raise ValueError("Only the fixed safety or paid fixture is supported")
    return (
        """import hashlib,json,os,subprocess
from pathlib import Path
p=Path(DIRECTORY);p.mkdir(mode=0o700)
f=Path('/app/scripts/prepare_capacity_fixture.py')
assert hashlib.sha256(f.read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest()==PRODUCER
(p/'creation-intent.json').write_text(json.dumps({'shows':SHOWS,'producer':PRODUCER}))
env=dict(os.environ,TEST_DATABASE_URL=os.environ['DATABASE_URL'],TEST_REDIS_URL=os.environ['REDIS_URL'])
subprocess.run(['python',str(f),'--output',str(p/'fixture.json'),'--shows',str(SHOWS),'--seats','300','--sale-hours','1'],env=env,cwd='/app',check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=90)
print((p/'fixture.json').read_text())
""".replace("DIRECTORY", repr(directory))
        .replace("SHOWS", str(shows))
        .replace("PRODUCER", repr(producer_sha))
    )


def safety_program(directory, event, receipts, actors):
    return (
        """import asyncio,json,os,sys,time,jwt
from pathlib import Path
sys.path.insert(0,DIRECTORY);sys.path.insert(1,'/app/scripts')
from cce_paid_safety import probe
from probe_two_host_safety import probe_actors
p=Path(DIRECTORY)
tokens=[jwt.encode({'sub':a,'aud':'ticketing','iss':'ticketing','exp':time.time()+3600},os.environ['JWT_SECRET'],algorithm='HS256') for a in probe_actors(ACTORS)]
def evidence(phase,values):
 with (p/'safety.jsonl').open('a') as f:f.write(json.dumps({'phase':phase,**values})+'\\n')
result=asyncio.run(probe(EVENT,RECEIPTS,tokens,ACTORS,evidence=evidence))
(p/'safety.json').write_text(json.dumps(result));print(json.dumps(result))
""".replace("DIRECTORY", repr(directory))
        .replace("EVENT", repr(event))
        .replace("RECEIPTS", repr(receipts))
        .replace("ACTORS", repr(actors))
    )


def safety_audit_program(events):
    return """import os,json,time,sys,psycopg
sys.path.insert(0,'/app/scripts')
from audit_checkout_smoke import audit
events=EVENTS
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:
 conn.execute("SET statement_timeout='10s'")
 delay=float(conn.execute("SELECT greatest(coalesce(extract(epoch from max(expires_at)-clock_timestamp()),0),0) FROM holds WHERE event_id=ANY(%s::uuid[])",(events,)).fetchone()[0])
if not 0<=delay<=150:raise ValueError('TTL wait exceeds bound')
time.sleep(delay+0.25)
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:
 conn.execute("SET statement_timeout='10s'")
 elapsed=conn.execute('SELECT coalesce(max(expires_at)<clock_timestamp(),true) FROM holds WHERE event_id=ANY(%s::uuid[])',(events,)).fetchone()[0]
 result=audit(conn,events,1,1,3);result['hold_deadlines_elapsed']=bool(elapsed)
print(json.dumps(result))
""".replace("EVENTS", repr(events))


def safety_result(probe, financial, queues):
    gates = {
        "all_four_pods_exercised": probe.get("requests_per_pod") == [25] * 4
        and probe.get("wave_requests") == 100,
        "exactly_one_hold": probe.get("accepted_holds") == 1,
        "one_durable_owner": probe.get("one_durable_owner_before_payment") is True,
        "cross_pod_hold_replay": probe.get("all_pod_hold_replay") is True,
        "cross_pod_payment_replay": probe.get("all_pod_payment_replay") is True,
        "other_actor_denied": probe.get("other_actor_denied_on_all_pods") is True,
        "customer_ticket_confirmed": probe.get("customer_ticket_confirmed") is True,
        "post_ttl_durable": financial.get("pass") is True and financial.get("hold_deadlines_elapsed") is True,
        "duplicate_callbacks_durable": financial.get("pass") is True
        and financial.get("callback_delivery_attempts") == financial.get("callback_delivery_target") == 3,
        "zero_double_booking": financial.get("pass") is True
        and financial.get("duplicate_booked_seats") == financial.get("multi_booking_orders") == 0,
        "full_queue_drain": queues.get("pass") is True and queues.get("kafka_members") == 6,
    }
    result = {"gates": gates, "probe": probe, "post_ttl_financial": financial, "global_queues": queues}
    require_safety(result)
    return result


class Inputs:
    def __init__(self, stage, owner, global_audit):
        self.stage, self.session, self.owner = stage, stage.session, owner
        self.global_audit = global_audit
        self.directory = "/tmp/" + stage.run + "/cce-inputs"
        self.record = {"fixtures": {}, "creation_attempted": []}
        self.ready = False

    def checkpoint(self):
        policy.write(self.stage.output / "native-inputs.private.json", self.record)
        self.stage.checkpoint()

    def prepare(self):
        if self.ready:
            return
        self.stage.check(900)
        self.session.api(
            self.stage.cid,
            "from pathlib import Path;import json;p=Path("
            + repr(self.directory)
            + ");p.mkdir(mode=0o700,parents=True);print(json.dumps({'fresh':True}))",
            45,
        )
        expected = {}
        for name in HELPERS:
            raw = (policy.ROOT / "scripts" / name).read_bytes()
            uploaded = self.session.api(self.stage.cid, helper_upload_program(self.directory, name, raw), 45)
            if uploaded != {"uploaded": True, "runtime_uid_owns_file": True}:
                raise ValueError("Native safety helper ownership unverified")
            expected[name] = sha(raw)
        proof = self.session.api(
            self.stage.cid,
            "import json,hashlib;from pathlib import Path;p=Path("
            + repr(self.directory)
            + ");expected="
            + repr(expected)
            + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'helpers_verified':True}))",
            45,
        )
        if proof.get("helpers_verified") is not True:
            raise ValueError("Native safety helper read-back failed")
        self.record["helpers_sha256"] = expected
        self.ready = True
        self.checkpoint()

    def fixture(self, name, shows):
        self.prepare()
        if name in self.record["creation_attempted"]:
            raise ValueError("No ambiguous fixture creation replay")
        self.record["creation_attempted"].append(name)
        self.checkpoint()
        directory = self.directory + "/" + name
        producer = sha(self.stage.bundle["scripts/prepare_capacity_fixture.py"])
        try:
            fixture = self.session.api(self.stage.cid, fixture_program(directory, shows, producer), 115)
        except (Exception, KeyboardInterrupt):
            # Recover the exact producer receipt after a lost acknowledgement. A
            # failure before the receipt is written remains unknown; never recreate.
            try:
                fixture = self.session.api(
                    self.stage.cid,
                    "from pathlib import Path;print(Path("
                    + repr(directory + "/fixture.json")
                    + ").read_text())",
                    45,
                )
                self.record["fixtures"][name] = fixture_identity(fixture, shows, producer_sha256=producer)
                self.checkpoint()
            except Exception:  # noqa: BLE001 - Unknown fixture ownership must block progression.
                self.record["unknown_fixture_identity"] = True
                self.stage.record["recovery_identity_unknown"] = True
                self.checkpoint()
            raise
        self.record["fixtures"][name] = fixture_identity(fixture, shows, producer_sha256=producer)
        self.checkpoint()
        if name == "paid":
            self.stage.events = list(fixture["show_ids"])
            self.stage.record["fixture_creation_attempted"] = True
            self.stage.checkpoint()
        return fixture

    def safety(self, receipts):
        fixture = self.fixture("safety", 1)
        event = fixture["show_ids"][0]
        actors = "adr0147-" + uuid4().hex
        self.record["safety_dispatch_attempted"] = True
        self.checkpoint()
        probe = self.session.api(self.stage.cid, safety_program(self.directory, event, receipts, actors), 140)
        self.record["probe"] = probe
        self.checkpoint()
        financial = self.session.api(self.stage.cid, safety_audit_program([event]), 175)
        self.record["post_ttl_financial"] = financial
        self.checkpoint()
        queues = drain(self.session, self.stage.cid, self.global_audit)
        self.record["global_queues"] = queues
        self.checkpoint()
        result = safety_result(probe, financial, queues)
        self.record["safety"] = result
        self.checkpoint()
        return result

    def paid(self):
        fixture = self.fixture("paid", 84)
        program = """import json,os,jwt,time
from pathlib import Path
from uuid import uuid4
from datetime import UTC,datetime
p=Path(DIRECTORY);fixture=json.loads((p/'fixture.json').read_text());identity=str(uuid4());expiry=time.time()+3600
manifest={'schema_version':1,'environment':'development','id':identity,'origin':'http://10.1.137.69:8000','expires_at':datetime.fromtimestamp(expiry,UTC).isoformat(),'show_ids':fixture['show_ids'],'viewer_tokens':[jwt.encode({'sub':'load-'+identity+'-'+str(i),'aud':'ticketing','iss':'ticketing','exp':expiry},os.environ['JWT_SECRET'],algorithm='HS256') for i in range(25200)],'seat_offset':0,'seats_per_show':300,'fixture_layout':'distributed'}
(p/'manifest.private.json').write_text(json.dumps(manifest));os.chmod(p/'manifest.private.json',0o600);print(json.dumps({'viewers':len(manifest['viewer_tokens'])}))
""".replace("DIRECTORY", repr(self.directory + "/paid"))
        result = self.session.api(self.stage.cid, program, 45)
        if result.get("viewers") != 25200:
            raise ValueError("Incomplete private paid cohort")
        raw = copy_out(
            self.session,
            self.stage.cid,
            self.directory + "/paid/manifest.private.json",
            self.owner + "/cce-paid-manifest.private.json",
        )
        return fixture, json.loads(raw)

    def cleanup(self):
        self.session.begin_cleanup()
        errors = []
        for name, fixture in self.record["fixtures"].items():
            try:
                events = fixture["show_ids"]
                if name == "safety" and self.record.get("safety_dispatch_attempted"):
                    audit = self.session.api(self.stage.cid, safety_audit_program(events), 175)
                    self.record["cleanup_safety_audit"] = audit
                    if audit.get("pass") is not True or audit.get("hold_deadlines_elapsed") is not True:
                        raise ValueError("Safety financial recovery incomplete")
                result = self.session.api(
                    self.stage.cid,
                    "import os,json,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n rows=conn.execute('UPDATE events SET sale_ends=clock_timestamp() WHERE id=ANY(%s::uuid[]) RETURNING id',("
                    + repr(events)
                    + ",)).fetchall()\nprint(json.dumps({'retired_shows':len(rows)}))",
                    45,
                )
                if result.get("retired_shows") != len(events):
                    raise ValueError("Owned fixture retirement incomplete")
                self.record[name + "_retired"] = True
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve independent retirement/audit failures.
                errors.append({"operation": name, "type": type(error).__name__})
        if self.stage.record.get("generator_idle_after_stop") is True and not self.stage.record.get(
            "job_cleanup_errors"
        ):
            try:
                self.session.call(
                    "primary",
                    "from pathlib import Path;import json; p=Path("
                    + repr(self.owner)
                    + ");f=p/'cce-paid-manifest.private.json'\nif p.is_symlink() or not p.is_dir() or f.is_symlink():raise ValueError('Unknown fixture secret path')\nif f.exists():\n if not f.is_file() or f.stat().st_nlink!=1:raise ValueError('Unknown fixture secret ownership')\n f.unlink()\nprint(json.dumps({'native_manifest_removed':not f.exists()}))",
                    45,
                )
                self.record["native_host_manifest_removed"] = True
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve secret cleanup failure independently.
                errors.append({"operation": "host_manifest", "type": type(error).__name__})
        if self.record.get("unknown_fixture_identity"):
            errors.append({"operation": "fixture", "type": "UnknownFixtureIdentity"})
        self.record["cleanup_errors"] = errors
        self.checkpoint()
        if errors:
            raise ValueError("Native fixture recovery incomplete")
        return {"fixtures_retired": True}
