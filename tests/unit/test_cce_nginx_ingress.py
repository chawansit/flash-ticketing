"""Offline ADR0291 ownership, entry-point and mandatory teardown checks."""
import copy
from types import SimpleNamespace

import cce_nginx_ingress as ingress
import pytest
import work_envelope as policy

RUN = "adr0151-" + "a" * 12
NS = "flash-cce-" + "a" * 12


def goal(arm="candidate"):
    return {"ingress_route_decision": "ADR0291",
            "ingress_route_contract_sha256": policy.digest(ingress.contract()),
            "ingress_route_arm": arm, "extension_decision": "ADR0271",
            "profile": "cce_paid_comparison", "maximum_paid_stages": 2}


@pytest.mark.parametrize("arm,expected", [
    ("control", ingress.CONTROL_ORIGIN), ("candidate", ingress.CANDIDATE_ORIGIN)])
def test_explicit_entry_point(arm, expected):
    assert ingress.origin(goal(arm)) == expected
    assert ingress.origin(None) == ingress.CONTROL_ORIGIN
    malformed = goal(arm)
    malformed["ingress_route_contract_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="bounded ingress"):
        ingress.origin(malformed)


def test_service_excludes_workers_and_never_autocreates_elb():
    service, route = ingress.objects(RUN)
    assert service["spec"]["type"] == "ClusterIP"
    selector = service["spec"]["selector"]
    assert selector == {"codex-owner": RUN, "codex-api-route": RUN}
    assert not all({"codex-owner": RUN, "codex-worker-role": "consumer"}.get(k) == v
                   for k, v in selector.items())
    assert route["spec"]["ingressClassName"] == "nginx"
    assert all("elb" not in k for k in route["metadata"]["annotations"])
    assert route["metadata"]["annotations"]["nginx.ingress.kubernetes.io/proxy-next-upstream"] == "off"


@pytest.mark.parametrize("method,path,body", [
    ("DELETE", ingress.ADDON_PATH, {"preconditions": {"uid": ingress.ADDON_UID}}),
    ("POST", "/api/v1/namespaces/kube-system/services", {}),
    ("GET", "/api/v1/namespaces/kube-system/secrets", None),
    ("GET", "/api/v1/namespaces/other/endpoints/paid-api", None),
    ("POST", "/api/v1/namespaces/" + NS + "/endpoints", {}),
])
def test_no_shared_or_foreign_mutation_capability(method, path, body):
    assert not ingress.allowed_request(method, path, body, goal())


def test_creation_requires_exact_bound_object():
    service, route = ingress.objects(RUN)
    path = "/api/v1/namespaces/" + NS + "/services"
    assert ingress.allowed_request("POST", path, service, goal())
    assert not ingress.allowed_request("POST", path, service, goal("control"))
    service["spec"]["type"] = "LoadBalancer"
    assert not ingress.allowed_request("POST", path, service, goal())
    assert ingress.allowed_request("POST", "/apis/networking.k8s.io/v1/namespaces/" +
                                   NS + "/ingresses", route, goal())


@pytest.fixture
def area(monkeypatch):
    resources, calls, persisted = {}, [], []
    before = {"exact_addon": True}
    monkeypatch.setattr(ingress, "addon_snapshot", lambda request: copy.deepcopy(before))
    receipts = [{"pod_name": f"api-{i}", "pod_uid": f"uid-{i}",
                 "private_ipv4": f"10.3.0.{i + 1}"} for i in range(4)]
    endpoints = {"subsets": [{"addresses": [
        {"ip": r["private_ipv4"], "targetRef": {"uid": r["pod_uid"],
            "name": r["pod_name"], "kind": "Pod"}} for r in receipts],
        "ports": [{"port": 8000, "protocol": "TCP"}]}]}
    clock = [0]

    def request(method, path, body=None):
        calls.append((method, path, copy.deepcopy(body)))
        if path == ingress.INGRESSES:
            return {"items": [copy.deepcopy(v) for v in resources.values() if v["kind"] == "Ingress"]}
        if "/endpoints/" in path:
            return copy.deepcopy(endpoints)
        if method == "GET":
            return copy.deepcopy(resources.get(path))
        if method == "POST":
            key = path + "/paid-api"
            value = copy.deepcopy(body)
            value["metadata"]["uid"] = body["kind"] + "-uid"
            if value["kind"] == "Ingress":
                value["status"] = {"loadBalancer": {"ingress": [{"ip": "10.2.39.55"}]}}
            resources[key] = value
            return copy.deepcopy(value)
        if method == "DELETE":
            assert body["preconditions"]["uid"] == resources[path]["metadata"]["uid"]
            resources.pop(path)
            return {}

    def http(role, path):
        if not any(v["kind"] == "Ingress" for v in resources.values()):
            return {"status": 404, "body": None}
        return {"status": 200, "body": {"status": "ready"}} if path == "/health/ready" else {"status": 401, "body": None}

    def sleep(seconds):
        clock[0] += seconds

    deployment = SimpleNamespace(run=RUN, namespace=NS, request=request, mutation=request,
                                 authorize=lambda: None, verify_owner=lambda: None)
    route = ingress.Route(deployment, http, persisted.append, now=lambda: clock[0], sleep=sleep)
    return route, receipts, resources, calls, endpoints, before


def test_full_route_qualification_and_repeated_cleanup(area):
    route, receipts, resources, calls, _, _ = area
    route.install(receipts)
    assert route.verify()
    assert route.cleanup() == {"owned_route_removed": True, "existing_addon_unchanged": True}
    assert route.cleanup()["owned_route_removed"]
    assert not resources
    deletes = [path for method, path, _ in calls if method == "DELETE"]
    assert deletes == list(reversed(route.paths()))
    assert all("/" + NS + "/" in p for p in deletes)


def test_wrong_pod_uid_cannot_qualify(area):
    route, receipts, _, _, endpoints, _ = area
    endpoints["subsets"][0]["addresses"][0]["targetRef"]["uid"] = "old-pod"
    with pytest.raises(TimeoutError, match="publication"):
        route.install(receipts)
    route.cleanup()


def test_foreign_route_is_not_adopted_or_deleted(area):
    route, receipts, resources, calls, _, _ = area
    resources["foreign"] = {"kind": "Ingress", "metadata": {"uid": "foreign"}}
    with pytest.raises(ValueError, match="exclusively unused"):
        route.install(receipts)
    route.cleanup()
    assert "foreign" in resources
    assert not any(method in {"POST", "DELETE"} for method, _, _ in calls)


def test_replaced_service_blocks_unsafe_deletion(area):
    route, receipts, resources, calls, _, _ = area
    route.install(receipts)
    resources[route.paths()[0]]["metadata"]["uid"] = "replacement"
    with pytest.raises(ValueError, match="ownership"):
        route.verify()
    with pytest.raises(ValueError, match="ownership"):
        route.cleanup()
    assert not any(method == "DELETE" and path == route.paths()[0] for method, path, _ in calls)


def test_ambiguous_owned_creation_is_reconciled_without_replay(area):
    route, receipts, resources, calls, _, _ = area
    original = route.deployment.mutation

    def ambiguous(method, path, body):
        original(method, path, body)
        raise TimeoutError("response lost")

    route.deployment.mutation = ambiguous
    with pytest.raises(TimeoutError, match="response lost"):
        route.install(receipts)
    route.cleanup()
    assert not resources
    assert sum(method == "POST" for method, _, _ in calls) == 1


def test_addon_drift_fails_after_owned_cleanup(area):
    route, receipts, resources, calls, _, before = area
    route.install(receipts)
    before["exact_addon"] = False
    with pytest.raises(ValueError, match="runtime drift"):
        route.cleanup()
    assert not resources
    assert all("kube-system" not in path for method, path, _ in calls if method == "DELETE")
