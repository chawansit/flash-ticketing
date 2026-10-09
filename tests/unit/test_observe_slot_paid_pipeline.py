import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import observe_slot_paid_pipeline as entry
import slot_failure_evidence as slots
from test_observe_cce_paid_pipeline import bundle, payload

from ticketing.infrastructure.slot_diagnostics import SlotDiagnostics


def test_slot_capture_uses_same_admitted_http_read_and_retains_old_metrics():
    calls = []
    tracker = SlotDiagnostics(4)
    tracker.failure({"role": "payment", "reason": "native_timeout"})
    raw = (payload() + 'ticketing_db_acquisition_failures_total{role="payment",reason="native_timeout"} 1\n'
           + tracker.comment().decode())
    module = SimpleNamespace(api_replicas=lambda host, port: [host], parse_api_metrics=lambda raw: {"original": 1})
    entry.install(module, bundle(), fetch=lambda url, **kw: calls.append(url) or io.BytesIO(raw.encode()))
    address = module.api_replicas()[0]
    metrics = module.api_metrics(address)
    assert len(calls) == 1
    assert metrics["original"] == metrics["business_http_requests_total"] == 1
    assert metrics["db_failure_diagnostics"]["failure_total"] == 1
    assert slots.summarize([{"api_replicas": {address: metrics}}], [address])["failure_total"] == 1
    assert module.api_replicas("consumer", 9001) == ["consumer"]


@pytest.mark.parametrize("fault", ["missing", "overflow", "restarted"])
def test_slot_entry_fails_closed_without_retry(fault):
    tracker = SlotDiagnostics(1)
    if fault == "overflow":
        for _ in range(17):
            tracker.failure({"role": "payment", "reason": "native_timeout"})
    raw = payload(start=124 if fault == "restarted" else 123)
    if fault != "missing":
        raw += tracker.comment().decode()
    calls = []
    module = SimpleNamespace(api_replicas=lambda *args: [], parse_api_metrics=lambda raw: {})
    entry.install(module, bundle(), fetch=lambda url, **kw: calls.append(url) or io.BytesIO(raw.encode()))
    with pytest.raises(ValueError):
        module.api_metrics(module.api_replicas()[0])
    assert len(calls) == 1


def test_entry_restores_original_native_hook_even_after_failure(monkeypatch):
    original = entry.native.install

    def fail(argv):
        assert entry.native.install is not original
        raise ValueError("synthetic argument failure")

    monkeypatch.setattr(entry.native, "main", fail)
    with pytest.raises(ValueError, match="synthetic"):
        entry.main([])
    assert entry.native.install is original
