import json
from io import BytesIO

import pytest

from scripts import observe_paid_pipeline as observer
from scripts import summarize_paid_pipeline as summary


def test_writer_metrics_and_role_db_are_parsed_from_the_same_scrape(monkeypatch):
    payload = b"""ticketing_reservation_persistence_phase_seconds_count{phase="redis_acknowledge",outcome="ok"} 10
ticketing_reservation_persistence_phase_seconds_sum{phase="redis_acknowledge",outcome="ok"} .1
ticketing_reservation_command_age_seconds_count 10
ticketing_reservation_command_age_seconds_sum 5
ticketing_reservation_command_age_seconds_bucket{le="1"} 10
ticketing_db_pool_in_use 2
ticketing_db_pool_acquire_seconds_count{outcome="ok"} 10
ticketing_db_pool_acquire_seconds_sum{outcome="ok"} .2
process_cpu_seconds_total 3
ticketing_reservation_persistence_phase_seconds_count{phase="private-command",outcome="ok"} 9
ticketing_reservation_persistence_phase_seconds_count{phase="postgres_batch",outcome="ok",command_id="secret"} 9
"""
    calls = []
    monkeypatch.setattr(observer, "api_replicas", lambda *_: ["a", "b"])

    def fetch(url, **_):
        calls.append(url)
        return BytesIO(payload)

    monkeypatch.setattr(observer, "urlopen", fetch)
    row = observer.worker_counters("writer", "reservation_write", "http://writer:9101/metrics")
    assert len(calls) == 2
    assert len(row["writer_db_replicas"]) == 2
    assert row["writer_db_replicas"]["a"]["pool_in_use"] == 2
    assert row["writer_db_replicas"]["a"]["process_cpu_seconds_total"] == 3
    assert row["writer_phases"]["command_age:ok:sum"] == 10
    assert row["writer_phases"]["redis_acknowledge:ok:count"] == 20
    assert "private-command" not in json.dumps(row) and "secret" not in json.dumps(row)


def test_writer_phase_buckets_reject_invalid_or_unknown_dimensions():
    prefix = "ticketing_reservation_command_age_seconds_bucket"
    for suffix in ['{le="NaN"}', '{le="-1"}', '{le="zero"}', '{le="1",actor="secret"}']:
        assert observer.writer_phase_metric(prefix+suffix) is None
    assert observer.writer_phase_metric(prefix+'{le="+Inf"}') == "command_age:ok:bucket:+Inf"


def test_pgbouncer_view_selects_target_and_never_exports_users():
    class Conn:
        def execute(self, query):
            self.query = query
            return self

        def fetchall(self):
            if self.query == "SHOW POOLS":
                return [
                    {"database": "target", "user": "private", "cl_waiting": 2, "sv_active": 24,
                     "maxwait": 1, "maxwait_us": 500000},
                    {"database": "other", "cl_waiting": 999, "maxwait": 999},
                ]
            return [{"database": "target", "total_query_count": 100},
                    {"database": "other", "total_query_count": 999}]

    result = observer.pgbouncer_view(Conn(), "target")
    assert result["pools"]["cl_waiting"] == 2 and result["pools"]["sv_active"] == 24
    assert result["maxwait_seconds"] == 1.5 and result["stats"]["total_query_count"] == 100
    assert "private" not in json.dumps(result) and "target" not in json.dumps(result)
    with pytest.raises(ValueError, match="missing"):
        observer.pgbouncer_view(Conn(), "absent")


def resource_row(ticks, counter=10):
    row = {"extended_diagnostics": True, "host_cpu_ticks": ticks,
            "pgbouncer": {"pools": {"cl_waiting": 2}, "maxwait_seconds": .1,
                          "stats": {"total_query_count": counter}}}
    for role, count in {"writer": 3, "maintenance": 1, "reconciler": 1, "simulator": 1,
                        "publisher": 1, "consumer": 6}.items():
        row[f"{role}_replicas"] = count
        row[f"{role}_db_replicas"] = {str(i): {"process_cpu_seconds_total": 0}
                                     for i in range(count)}
    return row


def test_host_cpu_and_admin_counters_have_bounded_summary():
    before = [100, 0, 100, 100, 100, 0, 0, 0]
    after = [120, 0, 110, 150, 120, 0, 0, 0]
    result = summary.summarize_resources([resource_row(before), resource_row(after, 30)])
    assert result["pass"]
    assert result["host_cpu_peak_percent"] == 30
    assert result["host_iowait_peak_percent"] == 20
    assert result["pgbouncer_counter_deltas"]["total_query_count"] == 20
    assert result["pgbouncer_maxwait_peak_ms"] == 100
    assert not summary.summarize_resources([resource_row(after), resource_row(before)])["pass"]
    changed = resource_row(after)
    changed["writer_db_replicas"].pop("2")
    assert not summary.summarize_resources([resource_row(before), changed])["pass"]
    missing = resource_row(after)
    del missing["pgbouncer"]
    assert not summary.summarize_resources([resource_row(before), missing])["pass"]
    assert not summary.summarize_resources([resource_row(before),
            {**resource_row(after), "writer_metrics_error": "OSError"}])["pass"]


def test_role_db_reset_invalidates_attribution_even_if_counter_recovers():
    rows = [{"writer_db_replicas": {"private": {"process_cpu_seconds_total": value,
             "duration:db_commit:all:count": value, "duration:db_commit:all:sum": value/10}}}
            for value in [10, 2, 20]]
    result = summary.summarize_db_replicas(rows, "writer_db_replicas")
    assert result["counter_reset_detected"]
    assert not result["duration_mean_ms"] and not result["counter_deltas"]
    assert "private" not in json.dumps(result)


def test_extended_startup_requires_admin_cpu_and_every_role_replica():
    row = {"utc": "2026-10-03T00:00:00+00:00", "issued_tickets": 0,
           "api_replicas": {str(i): {} for i in range(4)}, "extended_diagnostics": True,
           "host_cpu_ticks": [0]*8, "pgbouncer": {"pools": {"sv_active": 0}}}
    for role, count in {"writer": 3, "maintenance": 1, "reconciler": 1, "simulator": 1,
                        "publisher": 1, "consumer": 6}.items():
        row[f"{role}_replicas"] = count
        row[f"{role}_db_replicas"] = {str(i): {"process_cpu_seconds_total": 0}
                                     for i in range(count)}
    assert observer.pipeline_startup_view(row, 6)["pass"]
    assert not observer.pipeline_startup_view({**row, "writer_db_replicas": {}}, 6)["pass"]
    assert not observer.pipeline_startup_view({**row, "pgbouncer": {}}, 6)["pass"]
    assert not observer.pipeline_startup_view({**row, "host_cpu_ticks": []}, 6)["pass"]
