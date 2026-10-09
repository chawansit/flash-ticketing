"""ADR0180 confidentiality, strict startup and connection-budget regressions."""
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import diagnostic_connection as diagnostic


def spec():
    ca = str(Path("tmp/diagnostic-ca.pem").resolve())
    value = {"host": "10.0.0.1", "port": 5432, "dbname": "ticketing", "user": "monitor",
             "password": "private-fixture", "sslmode": "verify-full", "sslrootcert": ca}
    return diagnostic.specification(value, expected_database="ticketing",
                                    expected_identity=diagnostic.identity("monitor", "ticketing"), ca_path=ca, expected_endpoint=("10.0.0.1", 5432))


@pytest.mark.parametrize("field,value", [("host", "8.8.8.8"), ("host", "invalid"), ("port", True),
    ("port", 5433), ("dbname", "other"), ("user", "other"), ("password", ""),
    ("sslmode", "require"), ("sslrootcert", "/other.pem"), ("options", "-c default_transaction_read_only=off")])
def test_spec_rejects_unbound_or_unverified_fields(field, value):
    original = spec()
    values = {k: v for k, v in original.parameters.items()
              if k not in {"autocommit", "connect_timeout", "prepare_threshold", "options"}}
    values[field] = value
    with pytest.raises(ValueError):
        diagnostic.specification(values, expected_database="ticketing", expected_identity=original.identity_sha256,
                                 ca_path=original.parameters["sslrootcert"], expected_endpoint=("10.0.0.1", 5432))


class Connection:
    def __init__(self, row):
        self.row, self.closed, self.statements = row, False, []

    def transaction(self):
        return nullcontext()

    def execute(self, query):
        self.statements.append(query)
        return self

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


def test_only_one_data_connection_changes_and_other_driver_calls_stay_intact():
    connection = Connection(("monitor", "ticketing", True, True, True))
    calls = []

    def original(*args, **kwargs):
        calls.append((args, kwargs))
        return connection

    driver = SimpleNamespace(connect=original, Error=RuntimeError)
    module = SimpleNamespace(psycopg=driver)
    installed = diagnostic.install(module, "private-application-dsn", spec())
    assert module.psycopg.connect("private-application-dsn", autocommit=True) is connection
    assert calls[0][0] == () and calls[0][1]["sslmode"] == "verify-full"
    assert calls[0][1]["options"] == diagnostic.READ_ONLY_OPTIONS
    assert connection.statements[:2] == ["SET TRANSACTION READ ONLY", "SET LOCAL statement_timeout='100ms'"]
    module.psycopg.connect("pgbouncer-admin-dsn", autocommit=True, prepare_threshold=None)
    assert calls[1] == (("pgbouncer-admin-dsn",), {"autocommit": True, "prepare_threshold": None})
    assert driver.connect is original and module.psycopg.Error is RuntimeError
    with pytest.raises(ValueError):
        installed("private-application-dsn", autocommit=True)
    assert len(calls) == 2
    assert "private-fixture" not in repr(spec())
    with pytest.raises(TypeError):
        spec().parameters["sslmode"] = "disable"


@pytest.mark.parametrize("row", [("other", "ticketing", True, True, True), ("monitor", "other", True, True, True),
    ("monitor", "ticketing", False, True, True), ("monitor", "ticketing", True, False, True),
    ("monitor", "ticketing", True, True, False), ("monitor", "ticketing", True, True, 1), None])
def test_startup_mismatch_closes_connection_and_never_falls_back(row):
    connection = Connection(row)
    calls = []
    adapter = diagnostic.DiagnosticConnect(lambda **kwargs: calls.append(kwargs) or connection, "app", spec())
    with pytest.raises(ValueError):
        adapter("app", autocommit=True)
    assert connection.closed and len(calls) == 1
    with pytest.raises(ValueError):
        adapter("app", autocommit=True)
    assert len(calls) == 1


def test_connection_failure_is_not_retried_or_hidden():
    calls = []

    def unavailable(**kwargs):
        calls.append(kwargs)
        raise OSError("fixed transport failure")

    adapter = diagnostic.DiagnosticConnect(unavailable, "app", spec())
    with pytest.raises(OSError):
        adapter("app", autocommit=True)
    with pytest.raises(ValueError):
        adapter("app", autocommit=True)
    assert len(calls) == 1


def test_unexpected_data_connection_options_stop_before_connect():
    calls = []
    adapter = diagnostic.DiagnosticConnect(lambda **kwargs: calls.append(kwargs), "app", spec())
    with pytest.raises(ValueError):
        adapter("app", autocommit=False)
    assert not calls


@pytest.mark.parametrize("change", ["missing_bundle", "missing_binding", "scoped_mode", "wrong_decision", "unbound_inventory"])
def test_observer_rejects_partial_or_wrong_scope_before_reading_credentials(change, monkeypatch):
    import observe_two_host_pipeline as observer
    from status_refresh_contract import digest

    binding = {"decision": "ADR0180", "database": "ticketing", "identity_sha256": "a" * 64,
               "endpoint": ["10.0.0.1", 5432], "bundle_sha256": "b" * 64, "ca_sha256": "c" * 64}
    inventory = {"diagnostic_connection_binding": binding, "status_refresh_contract": {"decision": "ADR0174"}}
    args = SimpleNamespace(diagnostic_connection_bundle=Path("tmp/diagnostic.private.json"),
                           database_wait_diagnostics=True, inventory=Path("tmp/inventory.private.json"),
                           approved_inventory_sha256=digest(inventory))
    if change == "missing_bundle":
        args.diagnostic_connection_bundle = None
    elif change == "missing_binding":
        inventory.pop("diagnostic_connection_binding")
    elif change == "scoped_mode":
        args.database_wait_diagnostics = False
    elif change == "wrong_decision":
        inventory["status_refresh_contract"]["decision"] = "ADR0177"
    else:
        args.approved_inventory_sha256 = "0" * 64
    calls = []
    monkeypatch.setattr(diagnostic, "load_bundle", lambda *a, **kw: calls.append(kw))
    with pytest.raises(ValueError):
        observer.install_diagnostic_connection(SimpleNamespace(), inventory, args)
    assert not calls


def test_observer_binds_bundle_and_ca_without_changing_global_driver(monkeypatch):
    import observe_two_host_pipeline as observer
    from status_refresh_contract import digest

    current = spec()
    binding = {"decision": "ADR0180", "database": "ticketing", "identity_sha256": current.identity_sha256,
               "endpoint": ["10.0.0.1", 5432], "bundle_sha256": "b" * 64, "ca_sha256": "c" * 64}
    inventory = {"diagnostic_connection_binding": binding, "status_refresh_contract": {"decision": "ADR0174"}}
    args = SimpleNamespace(diagnostic_connection_bundle=Path("tmp/diagnostic.private.json"),
                           database_wait_diagnostics=True, inventory=Path("tmp/inventory.private.json"),
                           approved_inventory_sha256=digest(inventory))
    calls = []
    monkeypatch.setattr(diagnostic, "load_bundle", lambda *a, **kw: calls.append(kw) or current)
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://fixture@10.0.0.2/ticketing")
    original = SimpleNamespace(connect=lambda *a, **kw: None)
    module = SimpleNamespace(psycopg=original)
    installed = observer.install_diagnostic_connection(module, inventory, args)
    assert installed.connections_created == 0 and module.psycopg is not original
    assert calls[0]["expected_ca_sha256"] == "c" * 64
    assert calls[0]["expected_endpoint"] == ("10.0.0.1", 5432)
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://fixture@10.0.0.2/other")
    with pytest.raises(ValueError):
        observer.install_diagnostic_connection(SimpleNamespace(psycopg=original), inventory, args)
