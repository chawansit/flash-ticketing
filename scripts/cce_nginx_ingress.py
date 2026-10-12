"""ADR0291 existing-addon routing; no new ELB or addon mutation capability."""
import copy
import hashlib
import re
import time

import cce_dependency_probe as dependency
import work_envelope as policy

DECISION = "ADR0291"
HOURLY_DECISION = "ADR0293"
HOURLY_BASELINE = "docs/capacity/cce/existing-ingress-candidate-result-2026-10-12.json"
HOURLY_BASELINE_SHA256 = "48bf7f5a7995bef34e3d683fd259d4470837576f60c2dbf218fad1b67098c2a0"
CONTROL_ORIGIN = "http://10.1.137.69:8000"
CANDIDATE_ORIGIN = "http://10.2.39.55"
ADDON_PATH = "/api/v1/namespaces/kube-system/services/cceaddon-nginx-ingress-controller"
ADDON_UID = "86d3d67c-cfd3-4311-91f2-c711bd6ceba4"
ELB_ID = "10a8ed8c-1a7b-4068-88a8-6068b64f9130"
INGRESSES = "/apis/networking.k8s.io/v1/ingresses"
CLASS_PATH = "/apis/networking.k8s.io/v1/ingressclasses/nginx"
CONTROLLERS = "/api/v1/namespaces/kube-system/pods?labelSelector=app%3Dnginx-ingress%2Ccomponent%3Dcontroller%2Crelease%3Dcceaddon-nginx-ingress"


def contract():
    return {"decision": DECISION, "factor": "customer_http_entry_point",
            "control_origin": CONTROL_ORIGIN, "candidate_origin": CANDIDATE_ORIGIN,
            "addon_service_uid": ADDON_UID, "elb_id": ELB_ID, "ingress_class": "nginx",
            "new_elbs": 0, "additional_pods": 0, "additional_database_connections": 0,
            "source_sha256": hashlib.sha256(
                __import__("pathlib").Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def selected(goal):
    keys = {"ingress_route_decision", "ingress_route_contract_sha256", "ingress_route_arm",
            "ingress_hourly_decision", "ingress_hourly_prerequisite_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    if (goal.get("ingress_route_decision") != DECISION
            or goal.get("ingress_route_contract_sha256") != policy.digest(contract())
            or goal.get("ingress_route_arm") not in {"control", "candidate"}
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("profile") not in {"cce_paid_comparison", "cce_hourly_qualification"}):
        raise ValueError("Exact bounded ingress comparison required")
    hourly = goal.get("ingress_hourly_decision") is not None
    if hourly:
        if (goal["ingress_hourly_decision"] != HOURLY_DECISION
                or goal.get("ingress_hourly_prerequisite_sha256") != HOURLY_BASELINE_SHA256
                or goal["ingress_route_arm"] != "candidate"
                or goal.get("maximum_paid_stages") != 1):
            raise ValueError("Exact bounded ingress hourly qualification required")
    elif (goal.get("profile") != "cce_paid_comparison"
          or goal.get("maximum_paid_stages") != 2
          or "ingress_hourly_prerequisite_sha256" in goal):
        raise ValueError("Exact bounded ingress comparison required")
    return True


def hourly_baseline(goal):
    """Retained short proof; duration extension changes harness, not application."""
    if not selected(goal) or goal.get("ingress_hourly_decision") != HOURLY_DECISION:
        raise ValueError("Explicit ingress hourly prerequisite required")
    path = policy.ROOT / HOURLY_BASELINE
    if path.is_symlink():
        raise ValueError("Retained regular ingress prerequisite required")
    value = policy.read(path)
    import cce_event_lane_identity as pins
    if (policy.digest(value) != HOURLY_BASELINE_SHA256
            or value.get("status") != "PASSED_RESTORED" or value.get("pass") is not True
            or value.get("arm") != "candidate" or value.get("decision") != DECISION
            or value.get("image") != pins.IMAGE_ID or value.get("configuration") != pins.CONFIG
            or value.get("source") != pins.SOURCE
            or not value.get("measurement_gates") or not all(value["measurement_gates"].values())
            or value.get("customer", {}).get("fulfilled") != 25200
            or any(value.get("customer", {}).get(k) != 0 for k in
                   ("generator_drops", "first_attempt_error_journeys", "final_customer_failures"))
            or value.get("issuance_window", {}).get("unique_paid_issued_tickets", 0) < 25000
            or value.get("financial", {}).get("pass") is not True
            or value.get("financial", {}).get("duplicate_booked_seats") != 0
            or not value.get("financial_checks") or not all(value["financial_checks"].values())
            or any(value.get(k) is not True for k in
                   ("restoration_complete", "integrity_verified", "cleanup_complete", "transport_credentials_cleared"))
            or value.get("ingress_route", {}).get("owned_route_removed") is not True
            or value.get("ingress_route", {}).get("existing_addon_unchanged") is not True):
        raise ValueError("Fully passing restored ingress prerequisite required")
    matches = [e for e in policy.journal(policy.envelope())["experiments"]
               if e.get("ledger") == value["ledger"]]
    if (len(matches) != 1 or matches[0].get("status") != "PASSED_RESTORED"
            or matches[0].get("result_sha256") != value["raw_result_sha256"]
            or matches[0].get("paid_runs_started") != 1):
        raise ValueError("Restored ingress prerequisite ledger required")
    return value


def origin(goal):
    if not selected(goal):
        return CONTROL_ORIGIN
    return CONTROL_ORIGIN if goal["ingress_route_arm"] == "control" else CANDIDATE_ORIGIN


def validate_binding(goal, binding):
    if (not selected(goal) or binding.get("cce_ingress_route_decision") != DECISION
            or binding.get("cce_ingress_route_contract_sha256") != policy.digest(contract())
            or binding.get("cce_ingress_route_arm") != goal["ingress_route_arm"]):
        raise ValueError("Exactly bound ingress entry point required")
    if (goal.get("ingress_hourly_decision") is not None
            and (binding.get("cce_ingress_hourly_decision") != HOURLY_DECISION
                 or binding.get("cce_ingress_hourly_prerequisite_sha256") != HOURLY_BASELINE_SHA256)):
        raise ValueError("Exactly bound hourly ingress prerequisite required")


def authorize_sequence(envelope):
    """A failed control can never authorize the second paid stage."""
    goal = envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    if not selected(goal):
        return
    if goal.get("ingress_hourly_decision") == HOURLY_DECISION:
        hourly_baseline(goal)
        if goal.get("profile") != "cce_hourly_qualification":
            raise ValueError("Fresh hourly ingress scope required")
        # The normal one-stage envelope/guard prevents spent-scope replay.
        return
    entries = [e for e in policy.journal(envelope)["experiments"][goal["ledger_start_index"]:]
               if e.get("profile") == "cce_paid_comparison"]
    state = policy.read(policy.STATE)
    consumed = [e for e in entries
                if max(e.get("paid_runs_started", 0),
                       state.get(e["ledger"], {}).get("paid_runs_started", 0)) > 0]
    if goal["ingress_route_arm"] == "control":
        if consumed:
            raise ValueError("Fresh ingress control required; no replay")
        return
    if len(consumed) != 1:
        raise ValueError("Exactly one passing ingress control required")
    entry = consumed[0]
    path = policy.ROOT / goal.get("ingress_control_report_path", "")
    if (not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve())
            or path.is_symlink() or path.name != "cce-comparison.private.json"):
        raise ValueError("Owned ingress control report required")
    report = policy.read(path)
    bound = state.get(entry["ledger"], {}).get("binding", {})
    if (entry["ledger"] != goal.get("ingress_control_ledger")
            or entry.get("status") != "PASSED_RESTORED"
            or entry.get("result_sha256") != policy.digest(report)
            or report.get("pass") is not True or report.get("restoration_complete") is not True
            or report.get("integrity_verified") is not True
            or not report.get("native", {}).get("measurement_gates")
            or not all(report["native"]["measurement_gates"].values())
            or bound.get("cce_ingress_route_arm") != "control"
            or bound.get("cce_ingress_route_contract_sha256") != policy.digest(contract())
            or bound.get("cce_paid_core_sources") != __import__("cce_paid_stage").identity()):
        raise ValueError("Passing restored unchanged ingress control required")


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    if goal.get("profile") == "cce_hourly_qualification":
        hourly_baseline(goal)
        return {**plan, "kind": "existing_nginx_ingress_hourly_qualification",
                "single_changed_factor": "duration_and_distinct_cohort",
                "ingress_route": contract(), "ingress_route_arm": "candidate",
                "ingress_hourly_decision": HOURLY_DECISION,
                "baseline_evidence": HOURLY_BASELINE, "baseline_sha256": HOURLY_BASELINE_SHA256,
                "baseline_is_passing_control": True, "matched_performance_comparison": False,
                "duration_reason": "One unchanged CCE ingress hour at 84/s; audits and restoration within 90 minutes."}
    return {**plan, "kind": "existing_nginx_ingress_comparison",
            "single_changed_factor": contract()["factor"], "ingress_route": contract(),
            "ingress_route_arm": goal["ingress_route_arm"], "matched_performance_comparison": True,
            "baseline_evidence": "docs/capacity/cce/statistics-order-paid-result-2026-10-12.json",
            "baseline_sha256": policy.digest(policy.read(policy.ROOT /
                "docs/capacity/cce/statistics-order-paid-result-2026-10-12.json")),
            "baseline_is_passing_control": False,
            "duration_reason": "Fresh matched 84/s 300s customer-entry control then conditional candidate."}


def objects(run):
    namespace = dependency.namespace_for(run)
    meta = {"name": "paid-api", "namespace": namespace, "labels": {"codex-owner": run}}
    selector = {"codex-owner": run, "codex-api-route": run}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": copy.deepcopy(meta),
               "spec": {"type": "ClusterIP", "selector": selector,
                        "ports": [{"name": "http", "port": 8000, "targetPort": 8000, "protocol": "TCP"}]}}
    ingress = {"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
               "metadata": {**copy.deepcopy(meta), "annotations": {
                   "nginx.ingress.kubernetes.io/ssl-redirect": "false",
                   "nginx.ingress.kubernetes.io/proxy-next-upstream": "off"}},
               "spec": {"ingressClassName": "nginx", "rules": [{"http": {"paths": [
                   {"path": "/", "pathType": "Prefix", "backend": {
                       "service": {"name": "paid-api", "port": {"number": 8000}}}}]}}]}}
    return service, ingress


def allowed_request(method, path, body, goal):
    if not selected(goal):
        return False
    if method == "GET" and path in {ADDON_PATH, CLASS_PATH, CONTROLLERS, INGRESSES}:
        return True
    match = re.fullmatch(
        r"/(api/v1|apis/networking.k8s.io/v1)/namespaces/(flash-cce-([0-9a-f]{12}))/(services|endpoints|ingresses)(/paid-api)?", path)
    if not match:
        return False
    prefix, _, suffix, resource, named = match.groups()
    if (resource == "ingresses") != (prefix == "apis/networking.k8s.io/v1"):
        return False
    if method == "GET":
        return bool(named)
    if goal["ingress_route_arm"] != "candidate":
        return False
    if method == "DELETE":
        return bool(named and resource != "endpoints" and isinstance(body, dict)
                    and set(body) == {"apiVersion", "kind", "preconditions"}
                    and body.get("apiVersion") == "v1" and body.get("kind") == "DeleteOptions"
                    and set(body.get("preconditions", {})) == {"uid"} and body["preconditions"]["uid"])
    if method == "POST" and not named and resource in {"services", "ingresses"}:
        expected = objects("adr0151-" + suffix)[resource == "ingresses"]
        return body == expected
    return False


def owned(value, expected, uid=None):
    meta = value.get("metadata", {}) if isinstance(value, dict) else {}
    target = expected["metadata"]
    if (meta.get("name") != target["name"] or meta.get("namespace") != target["namespace"]
            or meta.get("labels", {}).get("codex-owner") != target["labels"]["codex-owner"]
            or not meta.get("uid") or (uid is not None and meta["uid"] != uid)):
        raise ValueError("Ingress route ownership changed")
    spec = value.get("spec", {})
    for key, required in expected["spec"].items():
        if spec.get(key) != required:
            raise ValueError("Ingress route specification changed")
    if expected["kind"] == "Ingress" and meta.get("annotations") != target["annotations"]:
        raise ValueError("Ingress route annotations changed")
    return meta["uid"]


def addon_snapshot(request):
    service = request("GET", ADDON_PATH, None)
    meta = service.get("metadata", {}) if isinstance(service, dict) else {}
    spec = service.get("spec", {}) if isinstance(service, dict) else {}
    if (meta.get("uid") != ADDON_UID
            or meta.get("annotations", {}).get("kubernetes.io/elb.id") != ELB_ID
            or spec.get("type") != "LoadBalancer" or spec.get("loadBalancerIP") != "10.2.39.55"
            or service.get("status", {}).get("loadBalancer", {}).get("ingress") !=
                [{"ip": "10.2.39.55", "ipMode": "VIP"}]):
        raise ValueError("Existing addon ELB identity changed")
    cls = request("GET", CLASS_PATH, None)
    if not cls or cls.get("spec", {}).get("controller") != "k8s.io/ingress-nginx":
        raise ValueError("Existing nginx ingress class required")
    pods = request("GET", CONTROLLERS, None)
    items = pods.get("items", []) if isinstance(pods, dict) else []
    if len(items) != 2:
        raise ValueError("Exactly two existing addon controllers required")
    controllers = []
    for pod in items:
        statuses = pod.get("status", {}).get("containerStatuses", [])
        containers = pod.get("spec", {}).get("containers", [])
        ready = pod.get("status", {}).get("conditions", [])
        if (len(containers) != 1 or len(statuses) != 1
                or statuses[0].get("ready") is not True or statuses[0].get("restartCount") != 0
                or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in ready)
                or containers[0].get("resources") != {
                    k: {"cpu": "250m", "memory": "512Mi"} for k in ("requests", "limits")}):
            raise ValueError("Unchanged healthy addon controller resources required")
        controllers.append({"uid": pod["metadata"]["uid"], "spec": pod["spec"],
                            "image_id": statuses[0].get("imageID")})
    return {"service_uid": meta["uid"], "service_spec": spec,
            "service_annotations": meta.get("annotations", {}),
            "class_uid": cls["metadata"]["uid"], "class_spec": cls["spec"],
            "controllers": sorted(controllers, key=lambda p: p["uid"])}


class Route:
    """Installed only in the candidate's namespace; shared addon remains read-only."""
    def __init__(self, deployment, read_http, persist, *, now=time.monotonic, sleep=time.sleep):
        self.deployment, self.request = deployment, deployment.request
        self.read_http, self.persist, self.now, self.sleep = read_http, persist, now, sleep
        self.before = None
        self.attempted, self.uids = {}, {}
        self.receipts = None
        self.removed = False

    def paths(self):
        namespace = self.deployment.namespace
        return ("/api/v1/namespaces/" + namespace + "/services/paid-api",
                "/apis/networking.k8s.io/v1/namespaces/" + namespace + "/ingresses/paid-api")

    def install(self, receipts):
        self.deployment.verify_owner()
        self.deployment.authorize()
        self.before = addon_snapshot(self.request)
        ingress_list = self.request("GET", INGRESSES, None)
        if not isinstance(ingress_list, dict) or ingress_list.get("items") != []:
            raise ValueError("Existing private ingress must be exclusively unused")
        self.persist({"addon_before_sha256": policy.digest(self.before)})
        self.receipts = copy.deepcopy(receipts)
        for path, item in zip(self.paths(), objects(self.deployment.run), strict=True):
            if self.request("GET", path, None) is not None:
                raise ValueError("Never adopt a preexisting ingress route")
            self.attempted[path] = item
            self.persist({"route_creation_attempted": list(self.attempted)})
            value = self.deployment.mutation("POST", path.rsplit("/", 1)[0], item)
            self.uids[path] = owned(value, item)
            self.persist({"route_uids": dict(self.uids)})
        deadline = self.now() + 90
        while True:
            self.deployment.authorize()
            if self.verify(allow_pending=True):
                return
            if self.now() >= deadline:
                raise TimeoutError("CCE ingress backend publication expired")
            self.sleep(2)

    def verify(self, *, allow_pending=False):
        self.deployment.verify_owner()
        if not self.before or addon_snapshot(self.request) != self.before:
            raise ValueError("Existing addon runtime changed during ingress comparison")
        service_path, ingress_path = self.paths()
        for path, item in zip(self.paths(), objects(self.deployment.run), strict=True):
            owned(self.request("GET", path, None), item, self.uids[path])
        ingresses = self.request("GET", INGRESSES, None)
        items = ingresses.get("items", []) if isinstance(ingresses, dict) else []
        if len(items) != 1 or items[0]["metadata"]["uid"] != self.uids[ingress_path]:
            raise ValueError("Foreign ingress appeared during exclusive routing comparison")
        ingress = items[0]
        if ingress.get("status", {}).get("loadBalancer", {}).get("ingress") != [{"ip": "10.2.39.55"}]:
            if allow_pending:
                return False
            raise ValueError("Bound existing ingress VIP publication required")
        endpoints = self.request("GET", service_path.replace("/services/", "/endpoints/"), None)
        addresses, ports = [], []
        for subset in (endpoints or {}).get("subsets", []):
            if subset.get("notReadyAddresses"):
                if allow_pending:
                    return False
                raise ValueError("Unready ingress API backend")
            addresses += subset.get("addresses", [])
            ports += subset.get("ports", [])
        actual = {(a.get("ip"), a.get("targetRef", {}).get("uid"),
                   a.get("targetRef", {}).get("name"), a.get("targetRef", {}).get("kind"))
                  for a in addresses}
        expected = {(r["private_ipv4"], r["pod_uid"], r["pod_name"], "Pod") for r in self.receipts}
        if len(addresses) != 4 or actual != expected or not ports or any(
                p.get("port") != 8000 or p.get("protocol") != "TCP" for p in ports):
            if allow_pending:
                return False
            raise ValueError("Exactly four captured API endpoints required")
        for role in ("primary", "generator"):
            ready = self.read_http(role, "/health/ready")
            denial = self.read_http(role, "/v1/orders/00000000-0000-0000-0000-000000000000")
            if ready != {"status": 200, "body": {"status": "ready"}} or denial.get("status") not in {401, 403}:
                if allow_pending:
                    return False
                raise ValueError("Ingress readiness and customer authorization required")
        self.persist({"route_ready": True, "four_owned_backends": True,
                      "customer_authorization_preserved": True})
        return True

    def cleanup(self):
        if self.removed or not self.attempted:
            return {"owned_route_removed": True, "existing_addon_unchanged":
                    self.before is None or addon_snapshot(self.request) == self.before}
        self.deployment.verify_owner()
        # Ingress removal first, before removing its Service/backends.
        for path in reversed(self.paths()):
            if path not in self.attempted:
                continue
            item = self.attempted[path]
            value = self.request("GET", path, None)
            if value is None:
                continue
            uid = owned(value, item, self.uids.get(path))
            self.request("DELETE", path, {"apiVersion": "v1", "kind": "DeleteOptions",
                                         "preconditions": {"uid": uid}})
            deadline = self.now() + 90
            while self.request("GET", path, None) is not None:
                if self.now() >= deadline:
                    raise TimeoutError("Owned ingress route cleanup expired")
                self.sleep(2)
        if self.attempted:
            deadline = self.now() + 90
            while any(self.read_http(role, "/health/ready").get("status") != 404
                      for role in ("primary", "generator")):
                if self.now() >= deadline:
                    raise TimeoutError("Existing ingress route removal unverified")
                self.sleep(2)
        self.removed = True
        stable = self.before is None or addon_snapshot(self.request) == self.before
        self.persist({"owned_route_removed": True, "existing_addon_unchanged": stable})
        if not stable:
            raise ValueError("Addon runtime drift retained; shared resources were not changed")
        return {"owned_route_removed": True, "existing_addon_unchanged": True}


HTTP_READ = r"""
import json,urllib.request,urllib.error
try:
 with urllib.request.urlopen(origin+path,timeout=3) as response:
  status=response.status; raw=response.read(65537)
except urllib.error.HTTPError as error:
 status=error.code;raw=error.read(65537)
if len(raw)>65536:raise ValueError('Bounded ingress HTTP proof required')
try:body=json.loads(raw)
except ValueError:body=None
print(json.dumps({'status':status,'body':body}))
"""


def http_reader(session, role, path):
    if role not in {"primary", "generator"} or path not in {
            "/health/ready", "/v1/orders/00000000-0000-0000-0000-000000000000"}:
        raise ValueError("Read-only ingress qualification only")
    return session.call(role, "origin=" + repr(CANDIDATE_ORIGIN) + "\npath=" + repr(path) +
                        "\n" + HTTP_READ, 15)
