"""Four-writer observations must preserve every other startup and summary gate."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_writer_scale_verification as v


@pytest.fixture
def sample(monkeypatch):
    monkeypatch.setattr(v, "validate_bundle", lambda b: None)
    counts = {"writer": 4, "maintenance": 1, "reconciler": 1,
              "simulator": 1, "publisher": 1, "consumer": 6}
    row = {"utc": "now", "issued_tickets": 0, "api_replicas": {i: {} for i in range(4)},
           "consumer_replicas": 6, "writer_replicas": 4, "simulator_replicas": 1,
           "publisher_replicas": 1, "maintenance_replicas": 1, "reconciler_replicas": 1,
           "extended_diagnostics": True, "host_cpu_ticks": [0] * 8, "pgbouncer": {"present": True}}
    row.update({role + "_db_replicas": {str(i): {"process_cpu_seconds_total": 0}
                                       for i in range(count)} for role, count in counts.items()})
    summary = {"api": {"observed_replicas": 4}, "consumer": {"max_replicas": 6},
       "api_metrics_errors": 0, "database_errors": 0, "resources": {"pass": True},
       "role_database": {"writer": {"observed_replicas": 4, "counter_reset_detected": False}},
       "writer_phases": {"counter_reset_detected": False}}
    return row, summary, {"decision": "ADR0315"}


def test_declared_four_writer_gates_pass_without_legacy_projection(sample):
    row, summary, bundle = sample
    original = copy.deepcopy(row)
    assert v.startup(row, bundle, None)["pass"]
    assert v.pipeline(summary, bundle, None)
    assert row == original and row["writer_replicas"] == 4


@pytest.mark.parametrize("field,value", [("writer_replicas", 3), ("consumer_replicas", 5),
                                       ("extended_diagnostics", False), ("pgbouncer", {})])
def test_incomplete_startup_is_rejected(sample, field, value):
    row, _, bundle = sample
    row[field] = value
    assert not v.startup(row, bundle, None)["pass"]


def test_missing_writer_cpu_or_counter_reset_is_rejected(sample):
    row, summary, bundle = sample
    row["writer_db_replicas"].pop("3")
    assert not v.startup(row, bundle, None)["pass"]
    summary["role_database"]["writer"]["counter_reset_detected"] = True
    assert not v.pipeline(summary, bundle, None)


def test_historical_checks_are_delegated_unchanged():
    assert v.startup({}, None, lambda r, n: {"old": n}) == {"old": 6}
    assert v.pipeline({}, None, lambda s: "unchanged") == "unchanged"


@pytest.mark.parametrize("fault", [None, "missing_writer", "missing_ticks", "missing_pool",
                                  "error", "reset", "no_cpu_delta"])
def test_summary_keeps_resource_completeness_and_error_gates(sample, fault):
    from summarize_paid_pipeline import summarize_resources
    row, _, bundle = sample
    row["host_cpu_ticks"] = [100] * 8
    row["pgbouncer"] = {"stats": {"total_xact_count": 1}, "pools": {"sv_active": 1},
                        "maxwait_seconds": 0}
    second = copy.deepcopy(row)
    second["host_cpu_ticks"][0] += 10
    rows = [row, second]
    if fault == "missing_writer":
        second["writer_db_replicas"].pop("3")
    elif fault == "missing_ticks":
        second["host_cpu_ticks"] = []
    elif fault == "missing_pool":
        second.pop("pgbouncer")
    elif fault == "error":
        second["database_error"] = "unavailable"
    elif fault == "reset":
        second["host_cpu_ticks"][0] = 0
    elif fault == "no_cpu_delta":
        second["host_cpu_ticks"][0] = row["host_cpu_ticks"][0]
    original = copy.deepcopy(rows)
    legacy = lambda values: {"resources": summarize_resources(values)}
    result = v.summary(rows, bundle, legacy)
    assert result["resources"]["pass"] is (fault is None)
    assert result["resources"]["legacy_resource_coverage_pass"] is False
    assert rows == original
    assert v.summary(rows, None, legacy) == legacy(rows)
