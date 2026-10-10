import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import slot_failure_evidence as collector

from ticketing.infrastructure.slot_diagnostics import SlotDiagnostics


def evidence():
    tracker = SlotDiagnostics(1)
    tracker.checkout("payment")
    tracker.failure({"role": "payment", "reason": "native_timeout"})
    return json.loads(tracker.comment()[len(collector.PREFIX):])


def test_producer_consumer_and_installed_metrics_wrapper():
    tracker = SlotDiagnostics(1)
    tracker.failure({"role": "payment", "reason": "native_timeout"})
    raw = tracker.comment().decode()
    module = SimpleNamespace(parse_api_metrics=lambda payload: {"cpu": 1})
    module.api_metrics = lambda address: module.parse_api_metrics(raw)
    collector.install(module)
    assert module.api_metrics("api") == {"cpu": 1, "db_failure_diagnostics": collector.parse(raw)}


@pytest.mark.parametrize("damage", ["extra_secret", "nan", "unknown_operation", "missing", "overflow", "wrong_sequence"])
def test_malformed_or_incomplete_evidence_rejected(damage):
    value = evidence()
    if damage == "extra_secret":
        value["records"][0]["holders"][0]["customer_id"] = "private"
    elif damage == "nan":
        value["records"][0]["holders"][0]["age_ms"] = float("nan")
    elif damage == "unknown_operation":
        value["records"][0]["holders"][0]["operation"] = "arbitrary-path"
    elif damage == "missing":
        value["records"] = []
    elif damage == "overflow":
        value["overwritten_total"] = 1
    else:
        value["records"][0]["sequence"] = 2
    with pytest.raises(ValueError):
        collector.validate(value)


def test_ring_is_deduplicated_and_matches_independent_failure_counters():
    value = evidence()
    row = {"api_replicas": {"api": {"acquisition_failure:payment:native_timeout": 1,
                                   "db_failure_diagnostics": value}}}
    result = collector.summarize([row, copy.deepcopy(row)], ["api"])
    assert result["complete"] and result["failure_total"] == 1
    assert len(result["replicas"]["api"]) == 1
    row["api_replicas"]["api"]["acquisition_failure:payment:native_timeout"] = 2
    with pytest.raises(ValueError, match="disagree"):
        collector.summarize([row], ["api"])


@pytest.mark.parametrize("raw", ["", collector.PREFIX + "{}\n" + collector.PREFIX + "{}", collector.PREFIX + " " * 65536],
                         ids=["missing", "duplicate", "oversized"])
def test_missing_duplicate_or_oversized_comment_rejected(raw):
    with pytest.raises(ValueError):
        collector.parse(raw)


def test_nonterminal_exposition_race_requires_settled_terminal_coverage():
    value = evidence()
    unsettled = {"api_replicas": {"api": {"acquisition_failure:payment:native_timeout": 1,
        "db_failure_diagnostics": {"schema_version": 1, "failure_total": 0, "overwritten_total": 0,
                                   "diagnostic_errors": 0, "complete": True, "records": []}}}}
    settled = {"api_replicas": {"api": {"acquisition_failure:payment:native_timeout": 1,
                                     "db_failure_diagnostics": value}}}
    assert collector.summarize([unsettled, settled], ["api"])["failure_total"] == 1
    with pytest.raises(ValueError, match="disagree"):
        collector.summarize([unsettled], ["api"])


@pytest.mark.parametrize("fault", ["overflow", "missing", "malformed", "extra_secret"])
def test_invalid_slot_comment_keeps_basic_metrics_but_completeness_still_fails(fault):
    value = evidence()
    if fault == "overflow":
        value.update(failure_total=17, overwritten_total=1, complete=False)
    elif fault == "extra_secret":
        value["customer_secret"] = "never-retain-this"
    raw = collector.PREFIX + json.dumps(value) + "\n"
    if fault == "missing":
        raw = "process_cpu_seconds_total 2\n"
    elif fault == "malformed":
        raw = collector.PREFIX + "{broken-json}\n"
    module = SimpleNamespace(parse_api_metrics=lambda _: {"cpu": 2, "payment_requests_waiting": 3})
    module.api_metrics = lambda _: module.parse_api_metrics(raw)
    collector.install(module)
    observed = module.api_metrics("api")
    assert observed["cpu"] == 2 and observed["payment_requests_waiting"] == 3
    assert "db_failure_diagnostics_error" in observed
    assert "never-retain-this" not in json.dumps(observed)
    with pytest.raises(ValueError, match="Incomplete"):
        collector.summarize([{"api_replicas": {"api": observed}}], ["api"])
    if fault == "overflow":
        assert observed["db_failure_diagnostics_error"]["header"]["overwritten_total"] == 1
        assert observed["db_failure_diagnostics_error"]["header"]["complete"] is False


def test_diagnostic_error_is_bounded_and_omits_arbitrary_payload():
    payload = collector.PREFIX + json.dumps({"private": "x" * collector.MAX_PAYLOAD})
    result = collector.diagnostic_error(payload)
    assert "header" not in result
    assert len(json.dumps(result)) < 400
    assert len(result["payload_prefix_sha256"]) == 64


def rolling_rows():
    tracker = SlotDiagnostics(1)
    rows = []
    for number in range(1, 21):
        tracker.failure({"role": "payment", "reason": "native_timeout"})
        value = json.loads(tracker.comment()[len(collector.PREFIX):])
        rows.append({"api_replicas": {"api": {"acquisition_failure:payment:native_timeout": number,
                                             "db_failure_diagnostics": value}}})
    return rows


def test_rolling_ring_requires_independent_complete_history():
    rows = rolling_rows()
    assert collector.validate(rows[-1]["api_replicas"]["api"]["db_failure_diagnostics"])["complete"] is False
    result = collector.summarize(rows, ["api"])
    assert result["complete"] and result["failure_total"] == 20
    assert [v["sequence"] for v in result["replicas"]["api"]] == list(range(1, 21))
    for missing in (rows[-1:], rows[16:]):
        with pytest.raises(ValueError, match="Missing slot failure history"):
            collector.summarize(missing, ["api"])


def test_rolling_ring_still_rejects_reset_tamper_and_diagnostic_errors():
    rows = rolling_rows()
    with pytest.raises(ValueError, match="counter reset"):
        collector.summarize(rows + [rows[0]], ["api"])
    damaged = copy.deepcopy(rows)
    damaged[-1]["api_replicas"]["api"]["db_failure_diagnostics"]["records"][0]["captured_at_unix_seconds"] += 1
    with pytest.raises(ValueError, match="identity changed"):
        collector.summarize(damaged, ["api"])
    damaged = copy.deepcopy(rows[-1]["api_replicas"]["api"]["db_failure_diagnostics"])
    damaged["diagnostic_errors"] = 1
    with pytest.raises(ValueError):
        collector.validate(damaged)
