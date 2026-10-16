"""Query attribution must reject lost/reset identities rather than hide costs."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import database_query_cost as cost


def snapshot(calls=2, userid=1):
    row = {"userid": userid, "dbid": 9, "queryid": 12, "toplevel": True,
               "query": "INSERT INTO orders VALUES ($1)"}
    row.update({k: calls for k in cost.COUNTERS})
    return {"captured_at": "2026-10-13T01:00:00+00:00",
                "statistics_info": {"stats_reset": "2026-09-13T00:00:00+00:00", "dealloc": 0},
                "statement_count": 1, "statements": [row]}


def test_role_qualified_delta_and_no_sql_export():
    before, after = snapshot(), snapshot(5)
    after["statements"].append(snapshot(8, userid=2)["statements"][0])
    after["statement_count"] = 2
    result = cost.summarize(before, after)
    assert result["totals"]["calls"] == 11
    assert result["new_query_identities"] == 1
    assert len({r["identity_sha256"] for r in result["queries"]}) == 2
    assert all("query" not in r and "userid" not in r for r in result["queries"])
    assert result["queries"][0]["category"] == "orders_insert"


@pytest.mark.parametrize("fault", ["reset", "eviction", "missing", "regression", "truncation",
                                  "duplicate", "nan", "missing_counter", "missing_identity"])
def test_incomplete_statistics_rejected(fault):
    before, after = snapshot(), snapshot(5)
    if fault == "reset":
        after["statistics_info"]["stats_reset"] = "later"
    elif fault == "eviction":
        after["statistics_info"]["dealloc"] = 1
    elif fault == "missing":
        after["statements"] = []
        after["statement_count"] = 0
    elif fault == "regression":
        after["statements"][0]["calls"] = 1
    elif fault == "truncation":
        after["statement_count"] = 2
    elif fault == "duplicate":
        after["statements"].append(copy.deepcopy(after["statements"][0]))
        after["statement_count"] = 2
    elif fault == "nan":
        after["statements"][0]["wal_bytes"] = "nan"
    elif fault == "missing_counter":
        after["statements"][0].pop("wal_fpi")
    else:
        after["statements"][0].pop("toplevel")
    with pytest.raises(ValueError):
        cost.summarize(before, after)


def test_real_catalog_oid_strings_match_integer_identity():
    before, after = snapshot(), snapshot(5)
    after["statements"][0].update(userid="1", dbid="9")
    assert cost.summarize(before, after)["totals"]["calls"] == 3


@pytest.mark.parametrize("value", ["01", "-1", "1.0", "", None, True, 0, 2**32])
def test_invalid_catalog_oid_rejected(value):
    after = snapshot(5)
    after["statements"][0]["userid"] = value
    with pytest.raises(ValueError):
        cost.summarize(snapshot(), after)



@pytest.mark.parametrize("dispatch_fails,diagnostics_fail", [(False,False),(True,False),(False,True),(True,True)])
def test_after_snapshot_runs_once_and_preserves_original_error(dispatch_fails, diagnostics_fail):
    record, calls = {}, []
    original = ValueError("customer dispatch failure")
    diagnostic = RuntimeError("after snapshot failed")
    def dispatch():
        calls.append("dispatch")
        if dispatch_fails:
            raise original
        return {"pass":False}
    def after():
        calls.append("after")
        if diagnostics_fail:
            raise diagnostic
    if dispatch_fails or diagnostics_fail:
        with pytest.raises(type(original if dispatch_fails else diagnostic)) as caught:
            cost.dispatch_with_after_snapshot(dispatch, after, record)
        assert caught.value is (original if dispatch_fails else diagnostic)
    else:
        assert cost.dispatch_with_after_snapshot(dispatch, after, record) == {"pass":False}
    assert calls == ["dispatch", "after"]
    assert record["after_snapshot_attempted"]
    assert record["after_snapshot_complete"] is not diagnostics_fail
    if diagnostics_fail:
        assert record["after_snapshot_failure_type"] == "RuntimeError"
