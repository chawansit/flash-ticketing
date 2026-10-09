import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))

from rds_preflight import (
    REQUIRED_INDEXES,
    REQUIRED_TABLES,
    migration_checksums,
    percentile,
    validate_schema,
    validate_server,
)


def server_snapshot(**overrides):
    value = {
        "server_version_num": 150000,
        "in_recovery": False,
        "transaction_read_only": "off",
        "encoding": "UTF8",
        "max_connections": 100,
        "ssl": True,
    }
    value.update(overrides)
    return value


def test_percentile_uses_nearest_rank():
    assert percentile([5, 1, 4, 2, 3], 0.95) == 5
    assert percentile([], 0.95) is None


def test_server_gate_accepts_primary_tls_target():
    assert validate_server(server_snapshot(), 15, 40) == []


def test_server_gate_names_each_failed_requirement():
    failures = validate_server(
        server_snapshot(
            server_version_num=140000,
            in_recovery=True,
            transaction_read_only="on",
            encoding="LATIN1",
            max_connections=20,
            ssl=False,
        ),
        15,
        40,
    )
    assert failures == [
        "server_version",
        "primary_required",
        "read_write_required",
        "utf8_required",
        "connection_budget",
        "tls_required",
    ]


def test_schema_gate_checks_migrations_tables_and_indexes(tmp_path):
    migration = tmp_path / "001.sql"
    migration.write_text("SELECT 1;", encoding="utf-8")
    expected = migration_checksums(tmp_path)
    assert validate_schema(expected, expected, REQUIRED_TABLES, REQUIRED_INDEXES) == []
    assert validate_schema({}, expected, set(), set()) == [
        "migration_checksums",
        "required_tables",
        "required_indexes",
    ]
