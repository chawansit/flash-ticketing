"""ADR0180 real TLS and read-only enforcement using an isolated local fixture."""
import os
import sys
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import diagnostic_connection as diagnostic
from database_wait_evidence import Collector

pytestmark = pytest.mark.integration


def test_separate_connection_verifies_tls_reads_statistics_and_rejects_writes():
    url = os.environ.get("TEST_DIAGNOSTIC_TLS_DATABASE_URL")
    if not url:
        pytest.skip("Explicit isolated local TLS PostgreSQL fixture required")
    settings = conninfo_to_dict(url)
    assert settings['host'] == '127.0.0.1' and settings['sslmode'] == 'verify-full'
    values = {key: settings.get(key, '') for key in ['host', 'dbname', 'user', 'password', 'sslmode', 'sslrootcert']}
    values['port'] = int(settings['port'])
    values['password'] = 'isolated-trust-fixture'
    proof = diagnostic.identity(values['user'], values['dbname'])
    spec = diagnostic.specification(values, expected_database=values['dbname'], expected_identity=proof,
                                    ca_path=values['sslrootcert'], expected_endpoint=(values['host'], values['port']))
    with psycopg.connect(url, autocommit=True) as admin:
        assert admin.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()[0]
        admin.execute("CREATE TEMP TABLE diagnostic_write_guard (value int)")
        adapter = diagnostic.DiagnosticConnect(psycopg.connect, 'application-fixture', spec)
        with adapter('application-fixture', autocommit=True) as connection:
            assert connection.execute("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").fetchone()[0]
            assert Collector().collect(connection)['complete']
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                connection.execute("CREATE TABLE diagnostic_forbidden_write (value int)")
            assert connection.execute("SELECT current_setting('default_transaction_read_only')").fetchone()[0] == 'on'
        assert adapter.connections_created == 1
        with pytest.raises(ValueError):
            adapter('application-fixture', autocommit=True)
        assert admin.execute("SELECT to_regclass('public.diagnostic_forbidden_write')").fetchone()[0] is None
    broken = dict(values, sslrootcert=str(Path(values['sslrootcert']).with_name('untrusted-ca.pem')))
    wrong_spec = diagnostic.specification(broken, expected_database=values['dbname'], expected_identity=proof,
                                          ca_path=broken['sslrootcert'], expected_endpoint=(values['host'], values['port']))
    denied = diagnostic.DiagnosticConnect(psycopg.connect, 'application-fixture', wrong_spec)
    with pytest.raises(psycopg.OperationalError):
        denied('application-fixture', autocommit=True)
    with pytest.raises(ValueError):
        denied('application-fixture', autocommit=True)
