"""Bounded two-host topology qualification with mandatory snapshot restoration."""

import argparse
import getpass
import json
import re
import sys
import time
from pathlib import Path

from prepare_two_host_scaling import API_SETTINGS, nginx_config, private_ipv4, secondary_compose
from two_host_topology import (
    CANDIDATE_COUNTS,
    CHANGED,
    deployment_model,
    environment,
    literal_model,
    restored,
    snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e"
INSPECT = "import json,subprocess;ids=subprocess.check_output(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=flash-ticketing'],text=True).split();print(json.dumps(json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []))"

GENERATOR_IDLE = r"""import json
from pathlib import Path
active=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:args=(p/'cmdline').read_bytes().split(b'\0')
 except OSError:continue
 if any(arg==b'ticketing.api:app' or Path(arg.decode(errors='replace')).name in {'paid_ticket_sharded_generator.py','paid_ticket_load_generator.py','run_synchronized_paid_generator.py'} for arg in args):active.append(p.name)
print(json.dumps({'generator_idle':not active}))
"""


def transport_failure(exc):
    """Narrow transport failures only; never retry authorization/remote ownership errors."""
    import sys

    if isinstance(exc, (EOFError, ConnectionError, TimeoutError)):
        return True
    paramiko = sys.modules.get("paramiko")
    if paramiko is None:
        return False
    rejected = (paramiko.AuthenticationException, paramiko.BadHostKeyException, paramiko.ChannelException)
    return isinstance(exc, paramiko.SSHException) and not isinstance(exc, rejected)


class Session:
    def __init__(self, config, output, password):
        if type(config.get("secondary_ssh_private_fallback", False)) is not bool:
            raise ValueError("Private SSH fallback must be explicitly boolean")
        self.config, self.output, self.clients = config, output, {}
        self._password, self._closed, self._via_primary = password, False, set()
        self.state = {
            "run_id": output.name,
            "capacity_stages_started": 0,
            "safety_probe_attempted": False,
            "pass": False,
            "restore_pass": False,
            "phases": [],
            "ssh_reconnect_attempts": {},
            "ssh_routes": {},
        }
        try:
            for role in ("primary", "secondary", "generator"):
                self.clients[role] = self._open(role)
        except BaseException:
            self.close()
            raise

    def _open(self, role):
        import paramiko

        item = self.config[role]

        def client():
            result = paramiko.SSHClient()
            try:
                result.load_host_keys(self.config["known_hosts"])
                result.set_missing_host_key_policy(paramiko.RejectPolicy())
                return result
            except BaseException:
                result.close()
                raise

        options = {"username": "root", "password": self._password, "look_for_keys": False,
                   "allow_agent": False, "timeout": 12, "auth_timeout": 15, "banner_timeout": 15}
        opened, channel = client(), None
        try:
            try:
                opened.connect(item["host"], **options)
                self.state["ssh_routes"][role] = "direct"
            except (TimeoutError, OSError):
                opened.close()
                if role != "secondary" or not self.config.get("secondary_ssh_private_fallback", False):
                    raise
                address = private_ipv4(item["private_ipv4"])
                parent = self.clients["primary"].get_transport()
                if parent is None or not parent.is_active():
                    raise RuntimeError("Pinned primary hop is unavailable")
                channel = parent.open_channel("direct-tcpip", (address, 22), ("127.0.0.1", 0), timeout=12)
                opened = client()
                # The public identity still selects the same pinned secondary host key.
                opened.connect(item["host"], sock=channel, **options)
                self._via_primary.add(role)
                self.state["ssh_routes"][role] = "pinned_private_hop"
            opened.get_transport().set_keepalive(30)
            return opened
        except BaseException:
            opened.close()
            if channel is not None:
                channel.close()
            raise

    def reconnect(self, role):
        if self._closed or role not in {"primary", "secondary", "generator"}:
            raise ValueError("Closed or unknown SSH role")
        attempts = self.state["ssh_reconnect_attempts"]
        if attempts.get(role, 0) >= 2:
            raise RuntimeError("SSH reconnect budget exhausted")
        attempts[role] = attempts.get(role, 0) + 1
        self.checkpoint()
        if role == "primary":
            for child in self._via_primary:
                self.clients[child].close()
            self._via_primary.clear()
        self._via_primary.discard(role)
        self.clients[role].close()
        self.clients[role] = self._open(role)
        self.checkpoint()

    def close(self):
        try:
            for c in self.clients.values():
                try:
                    c.close()
                except Exception as exc:  # noqa: BLE001 - close remaining transports and clear credentials
                    self.state.setdefault("ssh_close_errors", []).append(type(exc).__name__)
        finally:
            self.clients.clear()
            self._password = None
            self._closed = True

    def checkpoint(self):
        (self.output / "state.json").write_text(json.dumps(self.state, indent=2) + "\n")

    def phase(self, name):
        self.state["phases"].append(name)
        self.checkpoint()
        print(json.dumps({"phase": name}), flush=True)

    def call(self, role, code, timeout=180):
        compile(code, "owned_remote_action", "exec")
        i, o, e = self.clients[role].exec_command("python3 -", timeout=timeout)
        i.write(code)
        i.flush()
        i.channel.shutdown_write()
        payload, err = o.read().decode(), e.read().decode()
        status = o.channel.recv_exit_status()
        if status:
            name = re.sub(r"[^a-zA-Z0-9_-]", "_", self.state["phases"][-1])
            (self.output / (role + "-" + name + ".private-error.log")).write_text(err)
            raise RuntimeError("Remote step failed; private error retained")
        return json.loads(payload)

    def put(self, role, path, content, fresh=False):
        s = self.clients[role].open_sftp()
        try:
            with s.open(path, "wx" if fresh else "w") as f:
                s.chmod(path, 0o600)
                f.write(content.encode())
        finally:
            s.close()

    def up(self, role, path, counts, services):
        args = [
            "docker",
            "compose",
            "-f",
            path,
            "up",
            "-d",
            "--no-deps",
            "--no-build",
            "--pull",
            "never",
            "--force-recreate",
        ]
        for service in services:
            args += ["--scale", f"{service}={counts.get(service, 0)}"]
        args += list(services)
        return self.call(
            role,
            "import json,subprocess;subprocess.run("
            + repr(args)
            + ",check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=150);print(json.dumps({'applied':True}))",
        )

    def wait(self, role, count):
        project = "flash-ticketing" if role == "primary" else "flash-ticketing-api-secondary"
        code = r"""import json,subprocess,time
project=PROJECT
deadline=time.monotonic()+120
while time.monotonic()<deadline:
 ids=subprocess.check_output(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project='+project,'--filter','label=com.docker.compose.service=api'],text=True).split()
 rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 if len(rows)==COUNT and all(r['State'].get('Health',{}).get('Status')=='healthy' for r in rows):
  print(json.dumps(rows));break
 time.sleep(2)
else:raise TimeoutError('API readiness bound expired')
"""
        return self.call(role, code.replace("PROJECT", repr(project)).replace("COUNT", str(count)))

    def api(self, cid, program, timeout=180):
        command = ["docker", "exec", cid, "python", "-c", program]
        return self.call(
            "primary",
            "import subprocess,sys;r=subprocess.run(" + repr(command)
            + ",text=True,capture_output=True,timeout=" + str(timeout - 15)
            + ");sys.stderr.write(r.stderr);print(r.stdout);sys.exit(r.returncode)",
            timeout,
        )


def cleanup_program(owner, secondary_owner):
    """Exact owned-file cleanup; caller must first prove restored runtime and drain."""
    return r"""import json,re
from pathlib import Path
owner=Path(OWNER)
if not owner.is_absolute() or not re.fullmatch(r'(?:adr0147-topology|adr0148-rate)-[0-9a-f]{12}',owner.name) or owner.is_symlink():
 raise ValueError('Owned protocol directory required')
names=NAMES
for name in names:
 p=owner/name
 if p.is_symlink() or p.resolve().parent!=owner.resolve():raise ValueError('Unsafe owned file')
for name in names:
 (owner/name).unlink(missing_ok=True)
print(json.dumps({'removed_files':names}))
""".replace("OWNER", repr(secondary_owner or owner)).replace(
        "NAMES",
        repr(
            ["compose.json"]
            if secondary_owner
            else ["backup.private.json", "restore.compose.json", "live.compose.json"]
        ),
    )


def probe_program(cid, command, container_dir, owner):
    """Preserve failure evidence on the host before the API can be removed."""
    return (
        r"""import json,os,subprocess
from pathlib import Path
owner=OWNER
result=Path(owner+'/probe-result.private.json')
error=Path(owner+'/probe-stderr.private.log')
args=['docker','exec',CID,*COMMAND]
returncode=None
failure=None
try:
 completed=subprocess.run(args,text=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=110)
 returncode=completed.returncode
 error.write_text(completed.stderr)
except subprocess.TimeoutExpired as exc:
 failure='TimeoutExpired'
 error.write_text((exc.stderr or b'').decode() if isinstance(exc.stderr,bytes) else exc.stderr or '')
finally:
 if error.exists():os.chmod(error,0o600)
 copied=subprocess.run(['docker','cp',CID+':'+DIRECTORY+'/probe.json',str(result)],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
 if result.exists():os.chmod(result,0o600)
safety=json.loads(result.read_text()) if copied.returncode==0 and result.exists() else None
print(json.dumps({'returncode':returncode,'failure_type':failure,'evidence_copied':copied.returncode==0,'safety':safety}))
""".replace("OWNER", repr(owner))
        .replace("CID", repr(cid))
        .replace("COMMAND", repr(command))
        .replace("DIRECTORY", repr(container_dir))
    )


def endpoints(rows, role, address):
    result = []
    for row in rows:
        ports = row["NetworkSettings"]["Ports"].get("8000/tcp") or []
        if len(ports) != 1 or ports[0]["HostIp"] != address or not 8101 <= int(ports[0]["HostPort"]) <= 8104:
            raise ValueError("Private API publication differs")
        if row["Image"] != IMAGE or any(environment(row).get(k) != v for k, v in API_SETTINGS.items()):
            raise ValueError("Frozen image or API budgets differ")
        result.append(
            {
                "host_role": role,
                "private_ipv4": address,
                "port": int(ports[0]["HostPort"]),
                "container_id": row["Id"],
            }
        )
    return result


GLOBAL_AUDIT = (
    r"""import json,os,psycopg,sys
from redis import Redis
sys.path.insert(0,'/app/scripts')
from capacity_queue_state import reservation_queue_state
from kafka_lag_observe import python_observer
with psycopg.connect(os.environ['DATABASE_URL']) as conn:
 conn.execute('SET TRANSACTION READ ONLY');conn.execute("SET LOCAL statement_timeout='10s'")
 row=conn.execute("""
    + '"""'
    + """SELECT (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
 (SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),
 (SELECT count(*) FROM dead_letters),
 (SELECT count(*) FROM payment_attempts WHERE deliveries<target_deliveries),
 (SELECT count(*) FROM refund_requests WHERE status='PENDING')"""
    + '"""'
    + """).fetchone()
redis=Redis.from_url(os.environ['REDIS_URL'],decode_responses=True)
try:entries,pending=reservation_queue_state(redis)
finally:redis.close()
observer=python_observer(os.environ.get('KAFKA_BOOTSTRAP','kafka:9092'),'ticketing.events')
try:lag=observer.sample('ticketing-fulfillment-v1')
finally:observer.close()
result=dict(zip(['unpublished_outbox','pending_refresh','dead_letters','pending_callback_deliveries','pending_refunds'],row))
result.update(reservation_stream_entries=entries,reservation_stream_pending=pending,kafka_total_lag=lag['total_lag'],kafka_members=lag['members'])
result['pass']=all(value==0 for key,value in result.items() if key!='kafka_members')
print(json.dumps(result))
"""
)


def run(config, output, *, stage_hook=None):
    if not sys.stdin.isatty():
        raise ValueError("Protected password terminal required")
    session = Session(config, output, getpass.getpass("ECS password: "))
    saved = None
    mutated = False
    secondary_started = False
    fixture = None
    cid = None
    primary = config["primary"]
    owner = primary["repo"] + "/tmp/" + output.name
    restore_path, live_path, routes_path = (
        owner + "/restore.compose.json",
        owner + "/live.compose.json",
        owner + "/nginx.conf",
    )
    secondary_owner = config["secondary"]["prepared_directory"] + "/" + output.name
    secondary_path = secondary_owner + "/compose.json"
    try:
        session.phase("snapshot-and-preflight")
        cfg = session.call(
            "primary",
            "import subprocess;print(subprocess.check_output(['docker','compose','--env-file','.env.rds','-f','compose.yaml','-f','compose.rds.yaml','-f','compose.horizontal.yaml','-f','compose.keepalive10.yaml','config','--format','json'],cwd="
            + repr(primary["repo"])
            + ",text=True,stderr=subprocess.PIPE))",
        )
        generator_status = session.call("generator", GENERATOR_IDLE)
        if generator_status.get("generator_idle") is not True:
            raise ValueError("Generator must remain idle and API-free")
        session.state["generator_idle"] = True
        rows = session.call("primary", INSPECT)
        saved = snapshot(cfg, rows, image_id=IMAGE)
        cid = next(r["Id"] for r in rows if r["Config"]["Labels"]["com.docker.compose.service"] == "api")
        expected = json.loads(
            (
                ROOT / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json"
            ).read_text()
        )
        hashes = {
            k: expected["harness_source_sha256"][k]
            for k in (
                "scripts/capacity_queue_state.py",
                "scripts/kafka_lag_observe.py",
                "scripts/audit_checkout_smoke.py",
                "scripts/prepare_capacity_fixture.py",
            )
        }
        check = (
            "import hashlib,json;from pathlib import Path;expected="
            + repr(hashes)
            + ";actual={k:hashlib.sha256((Path('/app')/k).read_bytes().replace("
            + repr(bytes([13, 10]))
            + ","
            + repr(bytes([10]))
            + ")).hexdigest() for k in expected};assert actual==expected;print(json.dumps({'frozen_audit_harness_match':True}))"
        )
        session.state.update(session.api(cid, check, 45))
        before = session.api(cid, GLOBAL_AUDIT, 60)
        if before["pass"] is not True or before["kafka_members"] != 1:
            raise ValueError("Initial global queues/Kafka not drained")
        session.state["queue_before"] = before
        secondary = session.call(
            "secondary",
            "import json,subprocess;print(json.dumps(subprocess.check_output(['docker','ps','-aq'],text=True).split()))",
        )
        if secondary:
            raise ValueError("Secondary already has resources")
        for role, directory in (("primary", owner), ("secondary", secondary_owner)):
            session.call(
                role,
                "import json;from pathlib import Path;p=Path("
                + repr(directory)
                + ");p.mkdir(mode=0o700);print(json.dumps({'fresh_owned_directory':True}))",
            )
        session.put("primary", owner + "/backup.private.json", json.dumps(saved), True)
        session.put("primary", restore_path, json.dumps(literal_model(saved["model"])), True)
        model = deployment_model(saved, primary_ip=primary["private_ipv4"], nginx_path=routes_path)
        original_route = next(
            m["Source"]
            for r in rows
            if r["Config"]["Labels"]["com.docker.compose.service"] == "load-balancer"
            for m in r["Mounts"]
            if m["Destination"] == "/etc/nginx/nginx.conf"
        )
        route = session.call(
            "primary",
            "import json;from pathlib import Path;print(json.dumps(Path("
            + repr(original_route)
            + ").read_text()))",
        )
        session.put("primary", routes_path, route, True)
        session.put("primary", live_path, json.dumps(literal_model(model)), True)
        # Validate literals before the first cloud mutation, retaining actual runtime env.
        check = (
            "import json,subprocess;model=json.loads(subprocess.check_output(['docker','compose','-f',"
            + repr(live_path)
            + ",'config','--format','json'],text=True,stderr=subprocess.PIPE));expected="
            + repr(model["services"]["api"]["environment"])
            + ";assert model['services']['api']['environment']==expected;print(json.dumps({'rendered_api_environment_exact':True}))"
        )
        session.call("primary", check)
        session.phase("four-primary-control-topology")
        mutated = True
        session.up("primary", live_path, CANDIDATE_COUNTS, ["pgbouncer"])
        session.up(
            "primary",
            live_path,
            CANDIDATE_COUNTS,
            [r for r in CHANGED if r not in {"api", "pgbouncer", "load-balancer"}],
        )
        session.up("primary", live_path, CANDIDATE_COUNTS, ["api"])
        primary_rows = session.wait("primary", 4)
        controls = endpoints(primary_rows, "primary", primary["private_ipv4"])
        session.put("primary", routes_path, nginx_config(controls))
        session.up("primary", live_path, CANDIDATE_COUNTS, ["load-balancer"])
        session.state["four_primary_control_ready"] = True
        if stage_hook is not None:
            session.phase("control-stage")
            stage_hook(session, "control", controls, saved, owner, output)
        session.phase("two-plus-two-topology")
        session.up("primary", live_path, {**CANDIDATE_COUNTS, "api": 2}, ["api"])
        primary_rows = session.wait("primary", 2)
        secondary_model = json.loads(secondary_compose())
        secondary_model["services"]["api"]["image"] = IMAGE
        secondary_model["services"]["api"]["env_file"][0]["path"] = (
            config["secondary"]["prepared_directory"] + "/api.env"
        )
        secondary_model["services"]["api"]["ports"] = [
            {
                "target": 8000,
                "published": "8101-8104",
                "host_ip": config["secondary"]["private_ipv4"],
                "protocol": "tcp",
            }
        ]
        session.put("secondary", secondary_path, json.dumps(secondary_model), True)
        secondary_started = True
        session.up("secondary", secondary_path, {"api": 2}, ["api"])
        secondary_rows = session.wait("secondary", 2)
        routes = endpoints(primary_rows, "primary", primary["private_ipv4"]) + endpoints(
            secondary_rows, "secondary", config["secondary"]["private_ipv4"]
        )
        session.put("primary", routes_path, nginx_config(routes))
        session.up("primary", live_path, CANDIDATE_COUNTS, ["load-balancer"])
        session.phase("cross-host-private-readiness")
        urls = [f"http://{r['private_ipv4']}:{r['port']}/health/ready" for r in routes]
        session.call(
            "primary",
            "import json,urllib.request;urls="
            + repr(urls)
            + ";assert all(json.loads(urllib.request.urlopen(u,timeout=3).read())['status']=='ready' for u in urls);print(json.dumps({'all_four_private_endpoints_ready':True}))",
        )
        session.state["all_four_private_endpoints_ready"] = True
        cid = primary_rows[0]["Id"]
        session.phase("fresh-isolated-safety-fixture")
        container_dir = "/tmp/" + output.name
        program = (
            "import os,json,subprocess;from pathlib import Path;p=Path("
            + repr(container_dir)
            + ");p.mkdir(mode=0o700);env=dict(os.environ,TEST_DATABASE_URL=os.environ['DATABASE_URL'],TEST_REDIS_URL=os.environ['REDIS_URL']);subprocess.run(['python','scripts/prepare_capacity_fixture.py','--output',str(p/'fixture.json'),'--shows','1','--seats','300','--sale-hours','1'],env=env,cwd='/app',check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=90);print((p/'fixture.json').read_text())"
        )
        fixture = session.api(cid, program, 120)
        event = fixture["show_ids"][0]
        session.state["fixture_event_id"] = event
        session.put(
            "primary", owner + "/probe.py", (ROOT / "scripts/probe_two_host_safety.py").read_text(), True
        )
        session.call(
            "primary",
            "import json,subprocess;subprocess.run(['docker','cp',"
            + repr(owner + "/probe.py")
            + ","
            + repr(cid + ":" + container_dir + "/probe.py")
            + "],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);print(json.dumps({'probe_uploaded':True}))",
        )
        probe_path = container_dir + "/probe.py"
        session.state["probe_file_before_ownership"] = session.api(
            cid,
            "import os,json;from pathlib import Path;p=Path("
            + repr(probe_path)
            + ");s=p.stat();print(json.dumps({'uid':s.st_uid,'mode':s.st_mode & 0o777,'readable_by_api_user':os.access(p,os.R_OK),'api_uid':os.getuid()}))",
            45,
        )
        session.call(
            "primary",
            "import json,subprocess;subprocess.run(['docker','exec','--user','0',"
            + repr(cid)
            + ",'chown','10001:10001',"
            + repr(probe_path)
            + "],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);print(json.dumps({'ownership_fixed':True}))",
        )
        session.api(
            cid,
            "import os,json;assert os.getuid()==10001 and os.access("
            + repr(probe_path)
            + ",os.R_OK);print(json.dumps({'probe_readable':True}))",
            45,
        )
        selected = [next(r for r in routes if r["host_role"] == role) for role in ("primary", "secondary")]
        command = [
            "python",
            container_dir + "/probe.py",
            "--event-id",
            event,
            "--output",
            container_dir + "/probe.json",
        ]
        for route in selected:
            command += ["--origin", f"http://{route['private_ipv4']}:{route['port']}"]
        session.phase("100-way-cross-host-safety-probe")
        session.state["safety_probe_attempted"] = True
        session.checkpoint()
        payload = session.call("primary", probe_program(cid, command, container_dir, owner), 140)
        session.state["safety"] = payload.get("safety")
        session.state["probe_execution"] = {k: v for k, v in payload.items() if k != "safety"}
        session.checkpoint()
        if payload.get("returncode") != 0 or not payload.get("safety", {}).get("pass"):
            raise ValueError("Safety probe failed; host-owned evidence retained")
        session.phase("post-ttl-financial-and-global-drain")
        program = r"""import os,json,time,sys,psycopg
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
 result=audit(conn,events,1,1,3);result['hold_deadlines_elapsed']=bool(elapsed);assert result['pass'] and elapsed;print(json.dumps(result))
""".replace("EVENTS", repr([event]))
        session.state["post_ttl_financial"] = session.api(cid, program, 175)
        drain = None
        drain_deadline = time.monotonic() + 60
        while time.monotonic() < drain_deadline:
            try:
                drain = session.api(cid, GLOBAL_AUDIT, 45)
                if drain["pass"] and drain["kafka_members"] == 6:
                    break
            except RuntimeError:
                pass
            time.sleep(2)
        if not drain or drain["pass"] is not True or drain["kafka_members"] != 6:
            raise ValueError("Global queues/Kafka did not drain")
        session.state["queue_after"] = drain
        session.state["qualification_checks_pass"] = True
        if stage_hook is not None:
            session.phase("candidate-stage")
            stage_hook(session, "candidate", routes, saved, owner, output)
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001 - restoration must run
        session.state["failure_type"] = type(exc).__name__
        session.state["failure_phase"] = session.state["phases"][-1]
    finally:
        session.phase("restore-original-topology")
        if fixture and cid:
            try:
                program = (
                    "import os,json,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n rows=conn.execute('UPDATE events SET sale_ends=clock_timestamp() WHERE id=ANY(%s::uuid[]) RETURNING id',("
                    + repr(fixture["show_ids"])
                    + ",)).fetchall()\nprint(json.dumps({'retired_shows':len(rows)}))"
                )
                session.state["retirement"] = session.api(cid, program, 45)
            except Exception as exc:  # noqa: BLE001 - preserve failure and continue teardown
                session.state["retirement_error_type"] = type(exc).__name__
        try:
            if secondary_started:
                session.call(
                    "secondary",
                    "import json,subprocess;subprocess.run(['docker','compose','-f',"
                    + repr(secondary_path)
                    + ",'down','--timeout','20'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=60);assert not subprocess.check_output(['docker','ps','-aq','--filter','label=com.docker.compose.project=flash-ticketing-api-secondary'],text=True).split();print(json.dumps({'secondary_resources_removed':True}))",
                    90,
                )
            session.state["secondary_resources_removed"] = True
        except Exception as exc:  # noqa: BLE001 - preserve failure and continue teardown
            session.state["secondary_cleanup_error_type"] = type(exc).__name__
        try:
            if saved and mutated:
                session.up("primary", restore_path, saved["counts"], ["pgbouncer"])
                session.up("primary", restore_path, saved["counts"], [r for r in CHANGED if r != "pgbouncer"])
                session.wait("primary", 4)
                final_rows = session.call("primary", INSPECT)
                session.state.update(restored(saved, final_rows))
                restored_cid = next(
                    r["Id"]
                    for r in final_rows
                    if r["Config"]["Labels"]["com.docker.compose.service"] == "api"
                )
                restore_audit_deadline = time.monotonic() + 60
                final_queue = None
                while time.monotonic() < restore_audit_deadline:
                    try:
                        final_queue = session.api(restored_cid, GLOBAL_AUDIT, 45)
                        if final_queue["pass"] and final_queue["kafka_members"] == 1:
                            break
                    except RuntimeError:
                        pass
                    time.sleep(2)
                if not final_queue or not final_queue["pass"] or final_queue["kafka_members"] != 1:
                    raise ValueError("Restored queues/Kafka not drained")
                session.state["restored_global_queues"] = final_queue
                if stage_hook is not None:
                    session.state["generator_idle_after"] = session.call("generator", GENERATOR_IDLE)[
                        "generator_idle"
                    ]
                    if session.state["generator_idle_after"] is not True:
                        raise ValueError("Generator still active after restoration")
                session.state["restore_pass"] = bool(session.state.get("secondary_resources_removed"))
                if session.state["restore_pass"]:
                    session.call("primary", cleanup_program(owner, None))
                    if secondary_started:
                        session.call("secondary", cleanup_program(owner, secondary_owner))
                    session.state["credential_snapshots_removed"] = True
            elif not mutated:
                session.state["restore_pass"] = True
        except Exception as exc:  # noqa: BLE001 - preserve failure and continue teardown
            session.state["restoration_error_type"] = type(exc).__name__
        session.state["pass"] = bool(
            session.state.get("qualification_checks_pass")
            and session.state["restore_pass"]
            and not session.state.get("retirement_error_type")
        )
        session.checkpoint()
        session.close()
    return session.state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ssh-runtime", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to((ROOT / "tmp").resolve()) or output.exists():
        parser.error("Fresh owned repository tmp directory required")
    config = json.loads(args.config.read_text())
    for role in ("primary", "secondary", "generator"):
        private_ipv4(config[role]["private_ipv4"])
    if config["primary"]["private_ipv4"] == config["secondary"]["private_ipv4"]:
        parser.error("Distinct API hosts required")
    if not re.fullmatch(r"adr0147-topology-[0-9a-f]{12}", output.name):
        parser.error("Owned protocol run identity required")
    output.mkdir(mode=0o700)
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    result = run(config, output)
    print(
        json.dumps(
            {
                "pass": result["pass"],
                "restore_pass": result["restore_pass"],
                "failure_phase": result.get("failure_phase"),
                "failure_type": result.get("failure_type"),
                "capacity_stages_started": result["capacity_stages_started"],
            }
        ),
        flush=True,
    )
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
