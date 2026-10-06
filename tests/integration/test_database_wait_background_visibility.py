"""Reproduce null-role activity using only an isolated local PostgreSQL worker."""
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import application_database_wait_evidence as scoped
from database_wait_evidence import Collector

pytestmark = pytest.mark.integration


def test_autovacuum_has_unknown_role_and_masked_backend_without_statistics_privilege():
    url = os.environ.get("TEST_DIAGNOSTIC_BACKGROUND_DATABASE_URL")
    if not url:
        pytest.skip("Explicit isolated background-worker PostgreSQL fixture required")
    assert conninfo_to_dict(url).get("host") in {"127.0.0.1", "localhost"}
    role = "diagnostic_background_" + uuid4().hex[:12]
    table = "diagnostic_vacuum_" + uuid4().hex[:12]
    with psycopg.connect(url, autocommit=True) as admin:
        assert admin.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()[0]
        assert admin.execute("SELECT setting::int FROM pg_settings WHERE name='autovacuum_naptime'").fetchone()[0] <= 1
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        try:
            admin.execute(sql.SQL("CREATE TABLE {} (payload text) WITH (autovacuum_vacuum_threshold=1, autovacuum_vacuum_scale_factor=0, autovacuum_analyze_threshold=1000000, autovacuum_vacuum_cost_limit=1, autovacuum_vacuum_cost_delay=20)").format(sql.Identifier(table)))
            admin.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN payload SET STORAGE PLAIN").format(sql.Identifier(table)))
            admin.execute(sql.SQL("INSERT INTO {} SELECT repeat(md5(i::text),32) FROM generate_series(1,6000) i").format(sql.Identifier(table)))
            admin.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
            admin.execute("SELECT pg_stat_force_next_flush()")
            admin.execute("SELECT 1")
            deadline = time.monotonic() + 30
            while True:
                rows = admin.execute("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND backend_type='autovacuum worker' AND usesysid IS NULL").fetchall()
                if rows:
                    worker_pid = rows[0][0]
                    break
                assert time.monotonic() < deadline, "Bounded local autovacuum reproduction did not start"
                time.sleep(.1)
            proof = scoped.RoleCoverage(scoped.role_digest(role, "postgres"), tuple(sorted(scoped.EXPECTED_REPLICAS.items())))
            with psycopg.connect(make_conninfo(url, user=role), autocommit=True) as observer:
                hidden = scoped.Collector(proof).collect(observer)
                assert not hidden["application_visibility_complete"]
                assert hidden["error_code"] == "application_visibility_missing"
                context = hidden["failure_context"]
                assert context["unknown_sessions"] >= 1
                assert context["restricted_application_sessions"] == 0
                assert any(g["backend_type"] == "unavailable" and g["own_role"] is None and g["state_hidden"] for g in context["backend_groups"])
                assert admin.execute("SELECT count(*) FROM pg_stat_activity WHERE pid=%s AND backend_type='autovacuum worker' AND usesysid IS NULL", (worker_pid,)).fetchone()[0] == 1
                admin.execute(sql.SQL("GRANT pg_read_all_stats TO {}").format(sql.Identifier(role)))
                visible = scoped.Collector(proof).collect(observer)
                assert not visible["application_visibility_complete"]
                assert visible["error_code"] == "application_visibility_missing"
                assert any(g["backend_type"] == "autovacuum worker" and g["own_role"] is None and not g["state_hidden"] for g in visible["failure_context"]["backend_groups"])
                # Statistics access reveals the worker, but the scoped unknown-role policy stays strict.
                assert Collector().collect(observer)["complete"]
                assert admin.execute("SELECT count(*) FROM pg_stat_activity WHERE pid=%s AND backend_type='autovacuum worker'", (worker_pid,)).fetchone()[0] == 1
                assert role not in str(hidden) and role not in str(visible)
        finally:
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
