"""Real PostgreSQL privilege coverage; only an explicitly isolated local fixture."""
import os
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import application_database_wait_evidence as scoped
from database_wait_evidence import ACTIVITY_SQL, Collector, bounded_query, failure_context

pytestmark = pytest.mark.integration


def test_foreign_session_is_hidden_until_statistics_privilege_granted():
    url = os.environ.get("TEST_DIAGNOSTIC_DATABASE_URL")
    if not url:
        pytest.skip("Explicit isolated local PostgreSQL fixture required")
    settings = conninfo_to_dict(url)
    assert settings.get("host") in {"127.0.0.1", "localhost"}
    observer_role = "diagnostic_observer_" + uuid4().hex[:12]
    foreign_role = "diagnostic_foreign_" + uuid4().hex[:12]
    created = []
    with psycopg.connect(url, autocommit=True) as admin:
        assert admin.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()[0]
        try:
            for role in (observer_role, foreign_role):
                admin.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
                created.append(role)
            with psycopg.connect(make_conninfo(url, user=foreign_role), autocommit=True) as foreign:
                foreign.execute("SELECT 1")
                with psycopg.connect(make_conninfo(url, user=observer_role), autocommit=True) as observer:
                    raw_activity = bounded_query(observer, ACTIVITY_SQL)
                    groups = failure_context(raw_activity)["restricted_backends"]
                    assert raw_activity["restricted_sessions"] >= 1
                    hidden = Collector().collect(observer)
                    assert not hidden["complete"]
                    assert hidden["error_phase"] == "capabilities"
                    assert hidden["error_code"] == "statistics_privilege_missing"
                    assert any(g["backend_type"] == "unavailable" and g["own_role"] is False for g in groups)
                    # Replica policy is synthetic here; private container role binding is tested separately.
                    proof = scoped.RoleCoverage(scoped.role_digest(observer_role, "postgres"),
                                                tuple(sorted(scoped.EXPECTED_REPLICAS.items())))
                    with psycopg.connect(make_conninfo(url, user=observer_role), autocommit=True) as own:
                        own.execute("SELECT 1")
                        scoped_value = scoped.Collector(proof).collect(observer)
                    assert scoped_value["application_visibility_complete"]
                    assert scoped_value["application_sessions"] >= 1
                    assert scoped_value["restricted_foreign_sessions"] >= 1
                    assert not scoped_value["full_database_visibility_complete"]
                    assert "complete" not in scoped_value
                    assert observer_role not in str(scoped_value) and foreign_role not in str(scoped_value)
                    assert observer_role not in str(hidden) and foreign_role not in str(hidden)
                    admin.execute(sql.SQL("GRANT pg_read_all_stats TO {}").format(sql.Identifier(observer_role)))
                    collector = Collector()
                    visible = collector.collect(observer)
                    assert visible["complete"] and visible["restricted_sessions"] == 0
                    admin.execute(sql.SQL("REVOKE pg_read_all_stats FROM {}").format(sql.Identifier(observer_role)))
                    revoked = collector.collect(observer)
                    assert not revoked["complete"]
                    assert revoked["error_code"] == "activity_visibility_or_bound"
        finally:
            for role in reversed(created):
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
