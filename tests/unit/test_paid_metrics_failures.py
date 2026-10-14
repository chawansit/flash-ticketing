import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import io
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import cce_metric_admission as capture
import observe_cce_paid_pipeline as observer
import pytest
from test_cce_metric_admission import samples


def test_each_endpoint_attempted_once_with_successes_and_safe_http_status_retained():
    body = io.BytesIO(b"sensitive-response-body")
    error = HTTPError("http://private-endpoint/metrics?secret=hidden", 503, "private message", {}, body)
    calls = []
    def read(address):
        calls.append(address)
        if address == "second":
            raise error
        return {"count": 5}
    result = observer.collect_api_metrics(SimpleNamespace(
        api_replicas=lambda: ["first", "second", "third"], api_metrics=read))
    assert calls == ["first", "second", "third"]
    assert result == {"api_replicas": {"first": {"count": 5}, "third": {"count": 5}},
                      "api_metrics_error": "HTTPError", "api_metrics_failures": [
                          {"endpoint_index": 1, "error_class": "HTTPError", "http_status": 503}]}
    assert body.closed
    assert "sensitive" not in str(result) and "private" not in str(result)


def test_transport_failure_has_no_invented_status_or_reading():
    def read(_):
        raise URLError("secret connection URL")
    result = observer.collect_api_metrics(SimpleNamespace(api_replicas=lambda: ["only"], api_metrics=read))
    assert result == {"api_replicas": {}, "api_metrics_error": "URLError",
                      "api_metrics_failures": [{"endpoint_index": 0, "error_class": "URLError"}]}


def test_endpoint_missing_is_distinct_and_never_qualifies_partial_evidence():
    rows, receipts, start, end = samples()
    rows[4]["api_replicas"].pop(receipts["receipts"][0]["private_ipv4"])
    rows[4]["api_metrics_error"] = "HTTPError"
    with pytest.raises(capture.CaptureError) as failure:
        capture.summarize(rows, receipts, start, end)
    assert failure.value.reason_code == "endpoint_missing"


def test_actual_frozen_collector_accepts_exactly_one_collection_overlay(tmp_path):
    import ast
    import inspect

    import observe_two_host_pipeline as pipeline
    from cce_historical_sources import blob
    path = tmp_path / "frozen.py"
    path.write_bytes(blob(pipeline.FROZEN_NORMALIZED_SHA256))
    module = pipeline.load_frozen(path)
    before = ast.parse(inspect.getsource(module.main))
    observer.install_metrics_collection(module)
    assert callable(module._native_metric_collection)
    assert "_native_metric_collection" in module.main.__code__.co_names
    assert "api_metrics" not in module.main.__code__.co_names
    assert "sample" in module.main.__code__.co_names
    assert len(before.body) == 1


def test_unknown_collection_shape_fails_closed():
    from types import SimpleNamespace
    with pytest.raises(ValueError, match="Exactly one frozen"):
        observer.install_metrics_collection(SimpleNamespace(main=test_unknown_collection_shape_fails_closed))
