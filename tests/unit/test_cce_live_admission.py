"""ADR0272 burst rollover, secret redaction and exact terminal reconciliation."""
import copy
import json
from types import SimpleNamespace

import cce_live_admission as live
import pytest
import slot_failure_evidence as slots
from test_cce_admission_capture import event, filtered
from test_slot_failure_evidence import rolling_rows


def history():
    return [row["api_replicas"]["api"]["db_failure_diagnostics"]["records"][-1] for row in rolling_rows()]


def evidence(records):
    return {"schema_version": 1, "all_pods_captured": True, "errors": {},
            "pods": {f"api-{i}": {"pod_uid": f"uid-{i}", "schema_version": 1,
                "byte_limit_reached": False, "overflow_events": 0, "malformed_events": 0,
                "failure_events": len(records), "records": [
                    {"role": r["role"], "reason": r["reason"], "slot_ownership": r} for r in records]}
                for i in range(4)}}


def capture(value, clock=None):
    deployment = SimpleNamespace(pod_uids={f"api-{i}": f"uid-{i}" for i in range(4)},
                                 admission_diagnostics=lambda **kw: value)
    instance = live.Capture(deployment, now=clock or (lambda: 1.0))
    instance.started = 0
    return instance


def test_burst_history_recovers_rollover_without_weakening_terminal_gate():
    all_rows = rolling_rows()
    records = [row["api_replicas"]["api"]["db_failure_diagnostics"]["records"][-1] for row in all_rows]
    rows = all_rows[-1:]
    with pytest.raises(ValueError, match="Missing"): slots.summarize(rows, ["api"])
    assert slots.summarize(rows, ["api"], extra_records={"api": records})["failure_total"] == 20
    with pytest.raises(ValueError): slots.summarize(rows, ["api"], extra_records={"api": records[4:]})
    with pytest.raises(ValueError, match="exceeds"): slots.summarize(all_rows[:1], ["api"], extra_records={"api": records})


def test_live_filter_retains_valid_phase_only_and_removes_nested_secret():
    row = event()
    row["slot_ownership"] = history()[0]
    row["reason"] = row["slot_ownership"]["reason"]
    value = filtered(json.dumps(row).encode())
    assert value["records"][0]["slot_ownership"] == row["slot_ownership"]
    assert "secret" not in json.dumps(value)
    row["slot_ownership"]["holders"] = [{"password": "secret"}]
    value = filtered(json.dumps(row).encode())
    assert value["malformed_events"] == 1
    assert "slot_ownership" not in value["records"][0]
    assert "secret" not in json.dumps(value)


def test_overlap_is_deduplicated_and_conflicting_duplicate_rejected():
    value = evidence(history())
    instance = capture(value)
    instance.read(); instance.read()
    assert len(instance.records["api-0"]) == 20
    value["pods"]["api-0"]["records"][0]["slot_ownership"]["captured_at_unix_seconds"] += 1
    with pytest.raises(ValueError, match="Conflicting"): instance.read()


@pytest.mark.parametrize("fault", ["uid", "truncated", "overflow", "malformed", "missing", "gap", "budget"])
def test_incomplete_live_capture_cannot_qualify(fault):
    value = evidence(history())
    clock = [1.0]
    instance = capture(value, lambda: clock[0])
    instance.read()
    if fault == "uid": value["pods"]["api-0"]["pod_uid"] = "replacement"
    elif fault == "truncated": value["pods"]["api-0"]["byte_limit_reached"] = True
    elif fault == "overflow": value["pods"]["api-0"]["overflow_events"] = 1
    elif fault == "malformed": value["pods"]["api-0"]["malformed_events"] = 1
    elif fault == "missing": del value["pods"]["api-0"]
    elif fault == "gap": clock[0] = 22.0
    else: clock[0] = 601.0
    result = instance.finish()
    assert result["complete"] is False and result["errors"]
    with pytest.raises(ValueError): live.endpoints(result, {})


def test_endpoint_mapping_rejects_replaced_receipt():
    instance = capture(evidence(history()))
    result = instance.finish()
    receipts = {"receipts": [{"pod_name": f"api-{i}", "pod_uid": f"uid-{i}", "private_ipv4": f"10.2.0.{i+1}"} for i in range(4)]}
    assert len(live.endpoints(result, receipts)) == 4
    changed = copy.deepcopy(receipts); changed["receipts"][0]["pod_uid"] = "replacement"
    with pytest.raises(ValueError, match="changed"): live.endpoints(result, changed)
