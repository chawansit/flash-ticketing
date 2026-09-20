import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rds_wait_observe.py"
SPEC = importlib.util.spec_from_file_location("rds_wait_observe", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class FakeCursor:
    def __init__(self, rows, row=None):
        self.rows, self.row = rows, row

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row


class FakeConn:
    def __init__(self):
        self.calls = []

    def execute(self, sql):
        self.calls.append(sql)
        if "pg_stat_activity" in sql:
            return FakeCursor([("active", "IO", "WALSync", 2), ("idle", "Client", "ClientRead", 3),
                               ("active", "IO", "WALSync", 1)])
        if "pg_stat_wal" in sql:
            return FakeCursor([], (15, 11, 4.0, 2.0, 8192, 3))
        return FakeCursor([], (5, 2, 100.0, 30.0))


def test_read_only_wait_sample_is_aggregate_and_has_no_sql_or_ids():
    conn = FakeConn()
    row = module.sample_connection(conn)
    assert row["activity"] == {
        "states": {"active": 3, "idle": 3},
        "wait_types": {"IO": 3, "Client": 3},
        "wait_events": {"WALSync": 3},
    }
    assert row["wal"] == {
        "writes": 15, "syncs": 11, "write_ms": 4.0, "sync_ms": 2.0,
        "bytes": 8192, "buffers_full": 3,
    }
    assert row["checkpointer"] == {
        "timed": 5, "requested": 2, "write_ms": 100.0, "sync_ms": 30.0,
    }
    assert len(conn.calls) == 3
    assert all(sql.lstrip().startswith("SELECT") for sql in conn.calls)
    assert "query_ms" in row

def test_long_stage_duration_is_accepted_but_still_bounded(tmp_path, monkeypatch, capsys):
    import sys

    import pytest

    monkeypatch.delenv("RDS_DATABASE_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["rds_wait_observe.py", "--seconds", "1800", "--output", str(tmp_path / "waits.jsonl")])
    with pytest.raises(SystemExit) as accepted:
        module.main()
    assert accepted.value.code == 2
    assert "Set RDS_DATABASE_URL" in capsys.readouterr().err

    monkeypatch.setattr(sys, "argv", ["rds_wait_observe.py", "--seconds", "3601", "--output", str(tmp_path / "waits.jsonl")])
    with pytest.raises(SystemExit) as rejected:
        module.main()
    assert rejected.value.code == 2
    assert "Use 1..3600 seconds" in capsys.readouterr().err
