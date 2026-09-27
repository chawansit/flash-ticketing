import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "wal_path_probe.py"
SPEC = importlib.util.spec_from_file_location("wal_path_probe", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_summary_separates_sql_commit_and_queue_delay_without_connection_info():
    rows = [
        {"success": True, "sql_ms": 3.0, "commit_ms": 20.0,
         "schedule_lag_ms": 0.5, "connection_wait_ms": 0.2},
        {"success": True, "sql_ms": 4.0, "commit_ms": 125.0,
         "schedule_lag_ms": 10.0, "connection_wait_ms": 2.0},
        {"success": False, "error_type": "OperationalError",
         "schedule_lag_ms": 3.0, "connection_wait_ms": 1.0},
    ]
    result = module.summarize_block("pooled", 40, 90, rows, "start", "end")
    assert result["completed"] == 2
    assert result["errors"] == 1
    assert result["commit_over_100_ms"] == 1
    assert result["commit_max_ms"] == 125.0
    assert result["sql_p95_ms"] == 4.0
    assert result["matched_rate"] is True
    assert "dsn" not in result


def test_late_scheduler_fails_comparability_gate():
    rows = [{"success": True, "sql_ms": 1.0, "commit_ms": 2.0,
             "schedule_lag_ms": 101.0, "connection_wait_ms": 0.0}]
    result = module.summarize_block("direct", 40, 90, rows, "start", "end")
    assert result["matched_rate"] is False
    assert module.percentile([], 0.99) is None