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
    collector.install(module)
    assert module.parse_api_metrics(raw) == {"cpu": 1, "db_failure_diagnostics": collector.parse(raw)}


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
