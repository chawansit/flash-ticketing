"""Metrics transport must retain completeness and legacy failure behavior."""
import copy
from datetime import UTC, datetime, timedelta

import cce_metric_admission as capture
import cce_verification_contract as verification
import pytest
from cce_paid_observers import summarize_independent_diagnostics
from test_cce_paid_lifecycle import area  # noqa: F401 - shared lifecycle fixture
from test_observe_cce_paid_pipeline import bundle
from test_slot_failure_evidence import rolling_rows


def goal():
    return {"extension_decision": "ADR0271", "metric_admission_decision": "ADR0287",
            "metric_admission_contract_sha256": capture.digest(capture.contract()),
            "verification_decision": "ADR0285",
            "verification_contract_sha256": verification.digest(verification.contract())}


def binding():
    return {"cce_live_admission_decision": "ADR0287",
            "cce_metric_admission_contract_sha256": capture.digest(capture.contract())}


def samples():
    receipts = bundle()
    epoch = datetime(2026, 10, 12, tzinfo=UTC)
    rows = []
    for index, item in enumerate(rolling_rows()):
        data = item["api_replicas"]["api"]
        rows.append({"utc": (epoch + timedelta(seconds=index)).isoformat(),
                     "api_replicas": {r["private_ipv4"]: {
                         **copy.deepcopy(data), "process_start_time_seconds": r["process_start_time_seconds"],
                         "business_http_requests_total": index}
                         for r in receipts["receipts"]}})
    return rows, receipts, epoch.isoformat(), (epoch + timedelta(seconds=19)).isoformat()


def test_ring_rollover_is_complete_without_any_logs():
    rows, receipts, start, end = samples()
    result = capture.summarize(rows, receipts, start, end)
    assert result["complete"] and result["failure_total"] == 80
    assert result["additional_kubernetes_log_requests"] == 0
    assert capture.validate_summary(result, binding(), receipts["receipts"])


@pytest.mark.parametrize("fault", [
    "missing_endpoint", "process", "post_window_process", "gap", "time_order", "window",
    "missing_sequence", "conflicting_sequence", "counter_reset", "unsettled", "bad_ring",
])
def test_missing_or_conflicting_evidence_cannot_pass(fault):
    rows, receipts, start, end = samples()
    address = receipts["receipts"][0]["private_ipv4"]
    if fault == "missing_endpoint":
        rows[3]["api_replicas"].pop(address)
    elif fault in {"process", "post_window_process"}:
        rows[-1]["api_replicas"][address]["process_start_time_seconds"] += 1
        if fault == "post_window_process":
            end = rows[-3]["utc"]
    elif fault == "gap":
        del rows[3:6]
    elif fault == "time_order":
        rows[3]["utc"] = rows[2]["utc"]
    elif fault == "window":
        start = "2026-10-11T23:59:56+00:00"
    elif fault == "missing_sequence":
        # Starts after sequence one has disappeared from all four rings.
        rows = rows[16:]
        start = rows[0]["utc"]
    elif fault == "conflicting_sequence":
        rows[-1]["api_replicas"][address]["db_failure_diagnostics"]["records"][0]["captured_at_unix_seconds"] += 1
    elif fault == "counter_reset":
        rows[-1]["api_replicas"][address]["acquisition_failure:payment:native_timeout"] = 0
    elif fault == "unsettled":
        rows[-1]["api_replicas"][address]["acquisition_failure:payment:native_timeout"] += 1
    else:
        rows[3]["api_replicas"][address]["db_failure_diagnostics"]["diagnostic_errors"] = 1
    with pytest.raises(capture.CaptureError):
        capture.summarize(rows, receipts, start, end)


@pytest.mark.parametrize("field", ["metric_admission_decision", "metric_admission_contract_sha256",
                                  "verification_contract_sha256", "extension_decision"])
def test_scope_is_explicit_and_source_bound(field):
    assert not capture.selected(None) and not capture.selected({})
    value = goal()
    assert capture.selected(value)
    value[field] = "wrong"
    with pytest.raises(ValueError):
        capture.selected(value)


def test_orphan_marker_cannot_silently_use_legacy():
    with pytest.raises(ValueError):
        capture.selected({"metric_admission_contract_sha256": "wrong"})


def test_capture_receipt_cannot_be_reused_for_another_process():
    rows, receipts, start, end = samples()
    result = capture.summarize(rows, receipts, start, end)
    changed = copy.deepcopy(receipts["receipts"])
    changed[0]["pod_uid"] = "new-pod"
    with pytest.raises(ValueError):
        capture.validate_summary(result, binding(), changed)


def test_independent_collection_retains_safe_reason_without_skipping_database(monkeypatch):
    rows, receipts, start, end = samples()
    del rows[2:5]
    monkeypatch.setattr("cce_paid_observers.api_cpu", lambda *args, **kw: {"pass": True})
    record = {}
    failures = summarize_independent_diagnostics(
        record, rows, None, receipts, start, end, hourly=False,
        summarize_waits=lambda *args, **kw: {"complete": True},
        summarize_slots=lambda *args, **kw: pytest.fail("Legacy oracle should not be used"),
        metric_admission=True)
    assert failures == [{"operation": "slot_failure_capture", "type": "CaptureError",
                         "reason_code": "window_or_receipt_invalid"}]
    assert record["database_wait_capture"]["complete"]


@pytest.mark.parametrize("complete", [True, False])
def test_lifecycle_uses_bound_trace_and_no_log_requests(area, complete):  # noqa: F811
    value, calls, _, _ = area
    value.stage.guard.binding = binding()
    value.__init__(value.stage, value.resources, value.deployment, value.transition,
                   value.observers, value.manifests, qualify_safety=value.qualify_safety,
                   cleanup_safety=value.cleanup_safety, prepare_fixture=value.prepare_fixture,
                   identities=value.identities, background_before=value.background_before,
                   persist=value.persist, live_admission=True, metric_admission=True)
    assert value.observers.metric_admission is True
    rows, receipts, start, end = samples()
    result = capture.summarize(rows, receipts, start, end)
    result["complete"] = complete
    value.observers.collect = lambda: {"observer_pass": True, "native_api_cpu": {"pass": True},
                                      "slot_failure_capture": result}
    if complete:
        outcome = value.run(None, None, "audit")
        assert outcome["pass"] and outcome["measurement_gates"]["live_admission"]
        assert outcome["admission_diagnostics"]["decision"] == "ADR0287"
    else:
        with pytest.raises(ValueError, match="Complete bound"):
            value.run(None, None, "audit")
        assert not value.record["pass"]
    assert value.live_capture is None and "diagnostics" not in calls
    assert "restore" in calls and "safety_cleanup" in calls and "stage_cleanup" in calls


def test_unbound_capture_cannot_start_lifecycle(area):  # noqa: F811
    value, _, _, _ = area
    with pytest.raises(ValueError, match="binding"):
        capture.validate_binding(value.stage.guard.binding)
