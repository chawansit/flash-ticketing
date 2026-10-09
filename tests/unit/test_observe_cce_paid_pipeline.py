"""Native observer tests: same HTTP metrics and strict process identity, no cloud."""

import copy
import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import observe_cce_paid_pipeline as native
from test_cce_ecs_transition import pod_receipts


def bundle():
    rows = pod_receipts()
    return {
        "decision": "ADR0228",
        "run": rows[0]["startup_proof"]["run"],
        "api_sources": rows[0]["startup_proof"]["sources"],
        "resources": rows[0]["resources"],
        "manifest_digest": rows[0]["image_id"].split("@")[-1],
        "receipts": rows,
    }


def payload(start=123.0, count=1):
    return f'process_start_time_seconds {start}\n# TYPE ticketing_db_acquisition_failures_total counter\nticketing_http_requests_total{{route="/v1/orders/{{order_id}}",method="GET",status="200"}} {count}\n'


def test_native_install_fetches_once_and_preserves_worker_discovery():
    calls = []
    module = SimpleNamespace(
        api_replicas=lambda host, port: [host + ":worker"], parse_api_metrics=lambda value: {"frozen": True}
    )
    native.install(
        module, bundle(), fetch=lambda url, **kwargs: calls.append(url) or io.BytesIO(payload().encode())
    )
    assert len(module.api_replicas()) == 4
    assert module.api_replicas("consumer", 9001) == ["consumer:worker"]
    address = module.api_replicas()[0]
    assert module.api_metrics(address)["frozen"] is True
    assert len(calls) == 1
    assert module.api_metrics(address)["business_http_requests_total"] == 1
    assert len(calls) == 2


@pytest.mark.parametrize(
    "fault", ["duplicate", "public_ip", "run", "source", "resource", "image", "nan", "missing"]
)
def test_native_admission_drift_prevents_metrics(fault):
    value = bundle()
    if fault == "duplicate":
        value["receipts"][1]["pod_uid"] = value["receipts"][0]["pod_uid"]
    elif fault == "public_ip":
        value["receipts"][0]["private_ipv4"] = "8.8.8.8"
    elif fault == "run":
        value["receipts"][0]["startup_proof"]["run"] = "foreign"
    elif fault == "source":
        value["receipts"][0]["startup_proof"]["sources"] = {}
    elif fault == "resource":
        value["receipts"][0]["resources"] = {}
    elif fault == "image":
        value["receipts"][0]["image_id"] = "wrong"
    elif fault == "nan":
        value["receipts"][0]["process_start_time_seconds"] = float("nan")
    else:
        value["receipts"].pop()
    with pytest.raises(ValueError):
        native.validate_receipts(value)


@pytest.mark.parametrize("fault", ["restarted", "reset", "unadmitted", "payload_limit"])
def test_runtime_drift_fails_without_retry(fault):
    response = payload(count=5)
    calls = []
    module = SimpleNamespace(api_replicas=lambda *args: [], parse_api_metrics=lambda value: {})

    def fetch(url, **kwargs):
        calls.append(url)
        return io.BytesIO(response.encode())

    native.install(module, bundle(), fetch=fetch)
    address = module.api_replicas()[0]
    module.api_metrics(address)
    if fault == "restarted":
        response = payload(start=124, count=5)
    elif fault == "reset":
        response = payload(count=4)
    elif fault == "unadmitted":
        address = "10.2.240.99"
    else:
        response = "x" * (native.observer.MAX_PAYLOAD + 1)
    with pytest.raises(ValueError):
        module.api_metrics(address)
    assert len(calls) == (1 if fault == "unadmitted" else 2)


def test_native_and_existing_distribution_gates_are_identical():
    labels = {"one", "two", "three", "four"}
    rows = [
        {
            "utc": f"2026-10-08T12:00:{i:02d}+00:00",
            "api_replicas": {
                label: {"process_start_time_seconds": 1, "business_http_requests_total": i}
                for label in labels
            },
        }
        for i in range(4)
    ]
    args = {"offered_start_utc": rows[0]["utc"], "offered_end_utc": rows[-1]["utc"]}
    result = native.observer.summarize_endpoint_distribution(rows, labels, **args)
    assert result["all_four_replicas_observed"] is True
    broken = copy.deepcopy(rows)
    broken[2]["api_replicas"].pop("one")
    with pytest.raises(ValueError):
        native.observer.summarize_endpoint_distribution(broken, labels, **args)
