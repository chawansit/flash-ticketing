from __future__ import annotations

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "pgbouncer_pressure_observe",
    Path(__file__).resolve().parents[2] / "scripts/pgbouncer_pressure_observe.py",
)
assert spec and spec.loader
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


def test_admin_connection_preserves_credentials_without_returning_them_separately():
    dsn, database = observer.admin_connection(
        "postgresql://user:secret@pgbouncer:5432/ticketing?sslmode=disable"
    )
    assert database == "ticketing"
    assert "dbname=pgbouncer" in dsn
    assert "user=user" in dsn
    assert "password=secret" in dsn
    assert "connect_timeout=2" in dsn


def test_summarize_pools_filters_database_and_combines_wait_precision():
    rows = [
        {"database": "ticketing", "cl_active": 4, "cl_waiting": 2,
         "sv_active": 3, "sv_idle": 1, "maxwait": 1, "maxwait_us": 250_000},
        {"database": "ticketing", "cl_active": 2, "cl_waiting": 1,
         "sv_active": 1, "sv_idle": 2, "maxwait": 0, "maxwait_us": 500_000},
        {"database": "other", "cl_waiting": 99, "maxwait": 10},
    ]
    result = observer.summarize_pools(rows, "ticketing")
    assert result["cl_active"] == 6
    assert result["cl_waiting"] == 3
    assert result["sv_active"] == 4
    assert result["sv_idle"] == 3
    assert result["maxwait_seconds"] == 1.25
    assert result["pools"] == 2


def test_summarize_stats_sums_counters_and_takes_largest_rate():
    rows = [
        {"database": "ticketing", "total_query_count": 10,
         "total_wait_time": 100, "avg_query_count": 4, "avg_wait_time": 20},
        {"database": "ticketing", "total_query_count": 5,
         "total_wait_time": 50, "avg_query_count": 7, "avg_wait_time": 10},
        {"database": "other", "total_query_count": 1000, "avg_wait_time": 999},
    ]
    result = observer.summarize_stats(rows, "ticketing")
    assert result["total_query_count"] == 15
    assert result["total_wait_time"] == 150
    assert result["avg_query_count"] == 7
    assert result["avg_wait_time"] == 20
