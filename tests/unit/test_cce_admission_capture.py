"""ADR0233 bounded failure snapshots, ownership races and secret filtering; no cloud."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as cce
from test_cce_api_adapter import lifecycle as adapter_area


@pytest.fixture
def owned(monkeypatch):
    return adapter_area.__wrapped__(monkeypatch)


def filtered(raw):
    module = {}
    exec(compile(cce.DIAGNOSTIC_FILTER, "<admission-filter>", "exec"), module)  # noqa: S102 - execute reviewed generated filter
    return module["filtered_admission_logs"](raw)


def event():
    return {
        "event": "db_acquisition_failure",
        "request_id": "secret-request",
        "role": "payment",
        "reason": "role_limit",
        "capture": "guard_rejection",
        "elapsed_ms": 0.2,
        "native_pool": {
            "pool_size": 2,
            "pool_available": 0,
            "requests_waiting": 8,
            "pool_max": 2,
            "dsn": "secret-dsn",
        },
        "guard": {
            "maximum": 12,
            "used": 10,
            "acquiring": 10,
            "retained": 0,
            "counts": {"general": 0, "payment": 10, "password": 9},
            "limits": {"general": 10, "payment": 10},
            "retained_by_role": {"payment": 0},
            "callback_reserved": 0,
            "partial_timeout_reclaim": False,
            "password": "secret-password",
        },
        "token": "secret-token",
    }


@pytest.mark.parametrize("wrapped", [False, True])
def test_filter_preserves_reason_occupancy_without_arbitrary_secrets(wrapped):
    row = {"fields": event()} if wrapped else event()
    result = filtered((json.dumps(row) + "\nrequest secret-token\n").encode())
    assert result["failure_events"] == 1 and result["malformed_events"] == 0
    assert result["records"][0]["guard"]["used"] == 10
    assert result["records"][0]["reason"] == "role_limit"
    assert "secret" not in json.dumps(result)
    assert "password" not in json.dumps(result)


def test_parser_reports_overflow_and_malformed_instead_of_claiming_complete():
    raw = ((json.dumps(event()) + "\n") * 130 + "db_acquisition_failure broken\n").encode()
    result = filtered(raw)
    assert result["failure_events"] == 130
    assert len(result["records"]) == 128
    assert result["overflow_events"] == 2 and result["malformed_events"] == 1


def test_byte_ceiling_reports_incomplete_and_unknown_fields_are_removed():
    assert filtered(b" " * 8388608)["byte_limit_reached"] is True
    row = event()
    row["reason"] = "secret-token"
    row["guard"]["used"] = float("nan")
    result = filtered(json.dumps(row).encode())
    assert result["malformed_events"] == 1
    assert "used" not in result["records"][0]["guard"]
    assert "secret" not in json.dumps(result)


def prepare(owned):
    deployment, manifests, events, _persisted, stored = owned
    deployment.create(manifests)
    for name in deployment.pod_uids:
        stored[
            deployment.path() + "/pods/" + name + "/log?container=api&sinceSeconds=600&limitBytes=8388608"
        ] = filtered(json.dumps(event()).encode())
    return deployment, stored, events


def test_all_four_owned_pods_captured_without_mutation(owned):
    deployment, _stored, events = prepare(owned)
    events.clear()
    value = deployment.admission_diagnostics()
    assert value["all_pods_captured"] is True and len(value["pods"]) == 4
    assert all(method == "GET" for method, _, _ in events)


@pytest.mark.parametrize("fault", ["uid", "owner", "missing", "race"])
def test_replaced_or_foreign_pod_not_retained(owned, fault):
    deployment, stored, events = prepare(owned)
    path = deployment.path() + "/pods/api-0"
    if fault == "uid":
        stored[path]["metadata"]["uid"] = "replacement"
    elif fault == "owner":
        stored[path]["metadata"]["labels"]["codex-owner"] = "foreign"
    elif fault == "missing":
        stored.pop(path)
    else:
        original = deployment.request

        def request(method, route, body):
            value = original(method, route, body)
            if route.startswith(path + "/log?"):
                stored[path]["metadata"]["uid"] = "replacement"
            return value

        deployment.request = request
    value = deployment.admission_diagnostics()
    assert value["all_pods_captured"] is False
    assert value["errors"] == {"api-0": "ValueError"}
    assert "api-0" not in value["pods"]
    assert len(value["pods"]) == 3
    assert not any(method == "DELETE" for method, _, _ in events)


def test_namespace_replacement_blocks_capture(owned):
    deployment, stored, _events = prepare(owned)
    stored[deployment.path()]["metadata"]["uid"] = "foreign"
    with pytest.raises(ValueError, match="namespace identity"):
        deployment.admission_diagnostics()


def test_bounded_diagnostic_transport_route(monkeypatch):
    monkeypatch.setattr(cce.dependency, "kube_material", lambda path: {"ca.crt": "private"})
    calls = []
    from types import SimpleNamespace

    session = SimpleNamespace(
        call=lambda *args, **kwargs: calls.append(args) or {"value": {"schema_version": 1}}
    )
    transport = cce.KubernetesTransport(session, Path("unused"))
    path = "/api/v1/namespaces/flash-cce-aaaaaaaaaaaa/pods/api-0/log?container=api&sinceSeconds=600&limitBytes=8388608"
    assert transport.request("GET", path)["schema_version"] == 1
    for route in (path.replace("600", "3600"), path.replace("8388608", "9999999")):
        with pytest.raises(ValueError):
            transport.request("GET", route)
    assert len(calls) == 1
