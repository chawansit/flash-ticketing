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
            return FakeCursor([("active", "IO", 2), ("idle", "Client", 3),
                               ("active", "IO", 1)])
        return FakeCursor([], (15, 11, 4.0, 2.0))


def test_read_only_wait_sample_is_aggregate_and_has_no_sql_or_ids():
    conn = FakeConn()
    row = module.sample_connection(conn)
    assert row["activity"] == {
        "states": {"active": 3, "idle": 3},
        "wait_types": {"IO": 3, "Client": 3},
    }
    assert row["wal"] == {"writes": 15, "syncs": 11, "write_ms": 4.0, "sync_ms": 2.0}
    assert len(conn.calls) == 2
    assert all(sql.lstrip().startswith("SELECT") for sql in conn.calls)
    assert "query_ms" in row
