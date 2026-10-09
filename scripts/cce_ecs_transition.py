"""ADR0228 owned ECS API stop, native CCE routing, and candidate restoration.

Component only: caller must qualify safety/observers, own the CCE and audit
resources, and stop/audit customer traffic before retiring the topology.
"""

import json
import math
import re

import cce_api_adapter as cce
import cce_paid_stage as paid
import work_envelope as policy
from prepare_two_host_scaling import nginx_config
from qualify_two_host_deployment import INSPECT

SECONDARY_INSPECT = INSPECT.replace("project=flash-ticketing'", "project=flash-ticketing-api-secondary'")


def native_route(receipts):
    """Reuse every baseline Nginx setting; replace only upstream endpoints."""
    addresses = cce.endpoints(receipts)
    text = nginx_config([{"private_ipv4": a, "port": 8101 + i} for i, a in enumerate(addresses)])
    for i, address in enumerate(addresses):
        old = f"server {address}:{8101 + i};"
        if text.count(old) != 1:
            raise ValueError("Baseline route template changed")
        text = text.replace(old, f"server {address}:8000;")
    return text


def receipt_gate(receipts, run):
    cce.endpoints(receipts)
    data = cce.contract()
    for row in receipts:
        proof = row.get("startup_proof", {})
        if (
            row.get("image_id", "").split("@")[-1] != cce.dependency.MANIFEST
            or row.get("resources") != data["resources"]
            or proof.get("run") != run
            or proof.get("pod_uid") != row["pod_uid"]
            or proof.get("sources") != data["api_sources"]
            or not re.fullmatch(r"[0-9a-f]{64}", proof.get("environment_sha256", ""))
            or proof.get("command_sha256")
            != policy.digest(
                [
                    "uvicorn",
                    "ticketing.api:app",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8000",
                    "--limit-concurrency",
                    "256",
                    "--timeout-keep-alive",
                    "10",
                ]
            )
            or not row.get("started_at")
            or type(row.get("process_start_time_seconds")) not in (int, float)
            or not math.isfinite(row["process_start_time_seconds"])
            or row["process_start_time_seconds"] <= 0
        ):
            raise ValueError("Verified native API admission required")
    return True


def execution_identity(row):
    result = {key: row[key] for key in ("Id", "Image", "Config", "HostConfig", "Mounts")}
    result["Mounts"] = sorted(result["Mounts"], key=lambda value: json.dumps(value, sort_keys=True))
    return result


def change_program(rows, verb):
    if verb not in {"stop", "start"} or not rows:
        raise ValueError("Explicit owned API state transition required")
    expected = {row["Id"]: execution_identity(row) for row in rows}
    return "expected=" + repr(expected) + "\nverb=" + repr(verb) + "\n" + CHANGE


CHANGE = r"""
import json,subprocess
ids=list(expected)
rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True))
if len(rows)!=len(ids) or {r['Id'] for r in rows}!=set(ids):raise ValueError('Missing owned API')
for row in rows:
 if {k:row[k] for k in expected[row['Id']]}!=expected[row['Id']]:raise ValueError('Owned API identity changed')
 if row['Config']['Labels'].get('com.docker.compose.service')!='api':raise ValueError('Only owned APIs can transition')
args=['docker',verb]+(['--time','20'] if verb=='stop' else [])+ids
subprocess.run(args,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=45)
final=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True))
if len(final)!=len(ids) or {r['Id'] for r in final}!=set(ids):raise ValueError('Owned API disappeared')
for row in final:
 if {k:row[k] for k in expected[row['Id']]}!=expected[row['Id']]:raise ValueError('Owned API changed during transition')
 if row['State']['Running'] is not (verb=='start'):raise ValueError('API transition incomplete')
ports={r['Id']:r['NetworkSettings']['Ports'].get('8000/tcp') for r in final} if verb=='start' else {}
print(json.dumps({'owned_api_transition_verified':True,'container_ids':ids,'running':verb=='start','published_ports':ports}))
"""


class Transition:
    def __init__(self, session, guard, run, routes, owner, persist):
        if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run):
            raise ValueError("Canonical experiment identity required")
        if owner != session.config["primary"]["repo"] + "/tmp/" + run:
            raise ValueError("Canonical existing owned route directory required")
        if (
            len(routes) != 4
            or len({r["container_id"] for r in routes}) != 4
            or sum(r["host_role"] == "primary" for r in routes) != 1
            or sum(r["host_role"] == "secondary" for r in routes) != 3
            or any(not re.fullmatch(r"[0-9a-f]{64}", r["container_id"]) for r in routes)
            or any(r["private_ipv4"] != session.config[r["host_role"]]["private_ipv4"] for r in routes)
        ):
            raise ValueError("Exact retained one-plus-three ECS API identities required")
        self.session, self.guard, self.run, self.routes = session, guard, run, routes
        self.route_path = owner + "/nginx.conf"
        self.original_route = nginx_config(routes)
        self.persist = persist
        self.rows = {}
        self.lb = None
        self.record = {
            "api_stop_attempted": False,
            "route_write_attempted": False,
            "candidate_restored": False,
        }

    def check(self):
        if (
            self.session.action_guard is not self.guard
            or self.guard.key.split("__")[0] != "bounded_cce_paid_comparison"
            or cce.PROFILE not in policy.PROFILES
            or cce.PROFILE not in policy.envelope()["qualified_profiles"]
            or self.guard.binding.get("configuration_sha256") != policy.digest(self.session.config)
            or self.guard.binding.get("cce_transition_source_sha256")
            != paid.sha((policy.ROOT / "scripts/cce_ecs_transition.py").read_bytes())
        ):
            raise ValueError("Qualified exactly bound native transition required")
        cce.authorized_creation(policy.envelope())
        self.guard.check(180)

    def checkpoint(self):
        self.persist(dict(self.record))
        self.session.checkpoint()

    def capture(self):
        self.record["capture_phase"] = "authorization"
        self.checkpoint()
        self.check()
        if self.rows:
            raise ValueError("Fresh transition capture required")
        for role, program in (("primary", INSPECT), ("secondary", SECONDARY_INSPECT)):
            self.record["capture_phase"] = role + "_inventory"
            self.checkpoint()
            rows = self.session.call(role, program, 45)
            expected = {r["container_id"] for r in self.routes if r["host_role"] == role}
            apis = [r for r in rows if r["Config"]["Labels"].get("com.docker.compose.service") == "api"]
            if {r["Id"] for r in apis} != expected or len(apis) != len(expected):
                raise ValueError("Candidate API inventory changed")
            self.record["capture_phase"] = role + "_image_settings"
            self.checkpoint()
            for row in apis:
                env = dict(value.split("=", 1) for value in row["Config"]["Env"])
                if (
                    row["Config"]["Image"] != cce.dependency.INDEX
                    or row["Image"] != cce.dependency.INDEX
                    or not row["State"]["Running"]
                    or any(env.get(k) != v for k, v in cce.contract()["api_settings"].items())
                ):
                    self.record["admission_mismatch"] = {
                        "configured_image_matches": row["Config"]["Image"] == cce.dependency.INDEX,
                        "runtime_image_matches": row["Image"] == cce.dependency.INDEX,
                        "running": row["State"]["Running"],
                        "setting_names": [
                            k for k, v in cce.contract()["api_settings"].items() if env.get(k) != v
                        ],
                    }
                    self.checkpoint()
                    raise ValueError("Candidate API image/settings changed")
            self.rows[role] = apis
            if role == "primary":
                self.record["capture_phase"] = "load_balancer_mount"
                self.checkpoint()
                lbs = [
                    r
                    for r in rows
                    if r["Config"]["Labels"].get("com.docker.compose.service") == "load-balancer"
                ]
                if (
                    len(lbs) != 1
                    or not lbs[0]["State"]["Running"]
                    or [m["Source"] for m in lbs[0]["Mounts"] if m["Destination"] == "/etc/nginx/nginx.conf"]
                    != [self.route_path]
                ):
                    raise ValueError("Owned load-balancer mount required")
                self.lb = lbs[0]
        self.record["capture_phase"] = "route_content"
        self.checkpoint()
        current = self.session.call(
            "primary",
            "import json;from pathlib import Path;print(json.dumps(Path("
            + repr(self.route_path)
            + ").read_text()))",
            45,
        )
        if current != self.original_route:
            raise ValueError("Candidate route policy changed")
        self.record["captured_api_ids"] = {role: [r["Id"] for r in rows] for role, rows in self.rows.items()}
        self.record["load_balancer_id"] = self.lb["Id"]
        self.record["original_route_sha256"] = paid.sha(self.original_route.encode())
        self.record["capture_complete"] = True
        self.checkpoint()

    def reload(self, *, validate_only=False):
        if self.lb is None:
            raise ValueError("No captured load balancer")
        identity = execution_identity(self.lb)
        return self.session.call(
            "primary",
            "validate_only="
            + repr(validate_only)
            + "\nexpected="
            + repr(identity)
            + r"""
import json,subprocess
rows=json.loads(subprocess.check_output(['docker','inspect',expected['Id']],text=True))
if len(rows)!=1 or {k:rows[0][k] for k in expected}!=expected or not rows[0]['State']['Running']:raise ValueError('Load balancer changed')
for args in (() if validate_only else (['nginx','-t'],['nginx','-s','reload'])):
 subprocess.run(['docker','exec',expected['Id'],*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
print(json.dumps({'owned_load_balancer_verified':True,'owned_nginx_reload_verified':not validate_only}))
""",
            45,
        )

    def activate(self, receipts):
        self.check()
        if not self.record.get("capture_complete") or self.record["api_stop_attempted"]:
            raise ValueError("Fresh captured transition required")
        receipt_gate(receipts, self.run)
        self.record["api_stop_attempted"] = True
        self.checkpoint()  # Includes lost stop acknowledgement; restoration remains mandatory.
        for role, rows in self.rows.items():
            proof = self.session.call(role, change_program(rows, "stop"), 60)
            if proof.get("owned_api_transition_verified") is not True or proof.get("running") is not False:
                raise ValueError("Candidate APIs did not stop")
        self.record["all_four_ecs_apis_stopped"] = True
        self.record["route_write_attempted"] = True
        self.record["cce_route_sha256"] = paid.sha(native_route(receipts).encode())
        self.checkpoint()
        self.session.put("primary", self.route_path, native_route(receipts))
        if self.reload().get("owned_nginx_reload_verified") is not True:
            raise ValueError("CCE route reload failed")
        self.record["cce_routing_active"] = True
        self.checkpoint()

    def restored_routes(self, receipts):
        routes = []
        for role, rows in self.rows.items():
            proof = receipts[role]
            expected = {row["Id"] for row in rows}
            ports = proof.get("published_ports", {})
            if set(proof.get("container_ids", [])) != expected or set(ports) != expected:
                raise ValueError("Exact restarted API port receipts required")
            for cid in sorted(expected):
                bindings = ports[cid]
                address = self.session.config[role]["private_ipv4"]
                if (
                    not isinstance(bindings, list)
                    or len(bindings) != 1
                    or bindings[0].get("HostIp") != address
                    or not re.fullmatch(r"810[1-4]", str(bindings[0].get("HostPort", "")))
                ):
                    raise ValueError("Restarted private API publication differs")
                routes.append(
                    {
                        "container_id": cid,
                        "host_role": role,
                        "private_ipv4": address,
                        "port": int(bindings[0]["HostPort"]),
                    }
                )
        if len(routes) != 4 or len({(r["private_ipv4"], r["port"]) for r in routes}) != 4:
            raise ValueError("Four distinct restored API endpoints required")
        return routes

    def restore(self):
        self.session.begin_cleanup()
        errors = []
        receipts = {}
        restored_routes = self.routes
        if self.record["api_stop_attempted"]:
            for role, rows in self.rows.items():
                try:
                    result = self.session.call(role, change_program(rows, "start"), 60)
                    if (
                        result.get("owned_api_transition_verified") is not True
                        or result.get("running") is not True
                    ):
                        raise ValueError("Candidate restart unverified")
                    receipts[role] = result
                except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Restore every captured host independently.
                    errors.append({"operation": "restart_" + role, "type": type(error).__name__})
        if self.record["api_stop_attempted"] and not errors:
            try:
                restored_routes = self.restored_routes(receipts)
                self.record["restored_routes"] = restored_routes
                self.record["published_ports_changed"] = {
                    r["container_id"]: r["port"] for r in restored_routes
                } != {r["container_id"]: r["port"] for r in self.routes}
                self.checkpoint()
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Never route to unverified publications.
                errors.append({"operation": "restart_ports", "type": type(error).__name__})
        if self.record["route_write_attempted"] and not errors:
            try:
                # Verify the captured mount before replacing a partially transferred route.
                if self.reload(validate_only=True).get("owned_load_balancer_verified") is not True:
                    raise ValueError("Load balancer ownership unverified")
                restored_route = nginx_config(restored_routes)
                self.record["restored_route_sha256"] = paid.sha(restored_route.encode())
                self.session.put("primary", self.route_path, restored_route)
                if self.reload().get("owned_nginx_reload_verified") is not True:
                    raise ValueError("Original route reload unverified")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve route failure after attempting API restoration.
                errors.append({"operation": "route_restore", "type": type(error).__name__})
        if self.record["api_stop_attempted"] and not errors:
            try:
                urls = [f"http://{r['private_ipv4']}:{r['port']}/health/ready" for r in restored_routes]
                result = self.session.call(
                    "primary",
                    "urls="
                    + repr(urls)
                    + r"""
import json,time,urllib.request
end=time.monotonic()+45
while True:
 try:
  if all(json.loads(urllib.request.urlopen(u,timeout=3).read())=={'status':'ready'} for u in urls):break
 except (OSError,ValueError):pass
 if time.monotonic()>=end:raise TimeoutError('Original candidate readiness not restored')
 time.sleep(1)
print(json.dumps({'all_four_candidate_apis_ready':True}))
""",
                    60,
                )
                if result.get("all_four_candidate_apis_ready") is not True:
                    raise ValueError("Restored candidate readiness unverified")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Readiness is a required restoration gate.
                errors.append({"operation": "readiness", "type": type(error).__name__})
        self.record["restoration_errors"] = errors
        self.record["candidate_restored"] = not errors
        self.checkpoint()
        if errors:
            raise ValueError("Native transition recovery remains incomplete")
        return dict(self.record)
