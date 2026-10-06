"""ADR0176 application-role diagnostics; ADR0177 supplies the bounded profile."""
import hashlib
import json
import re
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

import database_wait_evidence as full

SCOPE = "application_role"
EXPECTED_REPLICAS = {"api": 4, "consumer": 6, "reservation-writer": 3, "maintenance": 1,
                     "publisher": 1, "reconciler": 1, "simulator": 1, "confirmation": 1}
COUNTS = ("sessions_total", "application_sessions", "foreign_sessions", "unknown_sessions",
          "restricted_application_sessions", "restricted_foreign_sessions", "active", "idle_in_transaction")
ACTIVITY_SQL = """WITH activity AS MATERIALIZED (
 SELECT state, wait_event_type, wait_event,
        usesysid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AS own_role
 FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid())
SELECT json_build_object(
 'utc',clock_timestamp(), 'observer_role',current_user, 'database',current_database(),
 'sessions_total',count(*),
 'application_sessions',count(*) FILTER (WHERE own_role IS TRUE),
 'foreign_sessions',count(*) FILTER (WHERE own_role IS FALSE),
 'unknown_sessions',count(*) FILTER (WHERE own_role IS NULL),
 'restricted_application_sessions',count(*) FILTER (WHERE own_role IS TRUE AND state IS NULL),
 'restricted_foreign_sessions',count(*) FILTER (WHERE own_role IS FALSE AND state IS NULL),
 'active',count(*) FILTER (WHERE own_role IS TRUE AND state='active'),
 'idle_in_transaction',count(*) FILTER (WHERE own_role IS TRUE AND state LIKE 'idle in transaction%'),
 'waits',coalesce((SELECT json_agg(w) FROM (
   SELECT coalesce(wait_event_type,'None') AS type,coalesce(wait_event,'None') AS event,count(*) AS count
   FROM activity WHERE own_role IS TRUE AND state='active'
   GROUP BY wait_event_type,wait_event ORDER BY wait_event_type,wait_event LIMIT 64) w),'[]'::json))
FROM activity"""


def role_digest(role, database):
    if not isinstance(role, str) or not role or not isinstance(database, str) or not database:
        raise ValueError("Known role and database required")
    return hashlib.sha256(json.dumps([role, database], separators=(",", ":")).encode()).hexdigest()


def configured_identity(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {"postgresql", "postgres"} or not parsed.username or not parsed.path.strip("/"):
        raise ValueError("Explicit database role and database required")
    return role_digest(unquote(parsed.username), unquote(parsed.path[1:]))


@dataclass(frozen=True)
class RoleCoverage:
    identity_sha256: str
    replicas: tuple


def prove_role_coverage(rows, expected_counts, observer_url):
    """Compare private inspected environments; publish no role or connection string."""
    if (not isinstance(expected_counts, dict) or expected_counts != EXPECTED_REPLICAS
            or any(not isinstance(k, str) or type(v) is not int or v <= 0 for k, v in expected_counts.items())):
        raise ValueError("Exact four-API and worker replica coverage required")
    identity, counts, ids = configured_identity(observer_url), Counter(), set()
    for row in rows:
        service = row["Config"]["Labels"]["com.docker.compose.service"]
        if service not in expected_counts:
            continue  # Infrastructure without database application sessions is outside this binding.
        if row["State"]["Running"] is not True or row["Id"] in ids:
            raise ValueError("Distinct running database application replicas required")
        ids.add(row["Id"])
        env = dict(item.split("=", 1) for item in row["Config"]["Env"] if "=" in item)
        if configured_identity(env.get("DATABASE_URL", "")) != identity:
            raise ValueError("Application database role differs")
        counts[service] += 1
    if dict(counts) != expected_counts:
        raise ValueError("Complete API and database worker role coverage required")
    return RoleCoverage(identity, tuple(sorted(counts.items())))


def partition(activity):
    result = {}
    for key in COUNTS:
        value = activity[key]
        if type(value) is not int or value < 0:
            raise ValueError("Integer session counts required")
        result[key] = value
    if (result["sessions_total"] != sum(result[k] for k in ("application_sessions", "foreign_sessions", "unknown_sessions"))
            or result["restricted_foreign_sessions"] > result["foreign_sessions"]
            or sum(result[k] for k in ("active", "idle_in_transaction", "restricted_application_sessions")) > result["application_sessions"]):
        raise ValueError("Consistent role partitions required")
    if result["unknown_sessions"] or result["restricted_application_sessions"]:
        raise ValueError("Complete known application visibility required")
    return result


def checked_binding(binding):
    if (not isinstance(binding, RoleCoverage) or not isinstance(binding.identity_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", binding.identity_sha256)
            or binding.replicas != tuple(sorted(EXPECTED_REPLICAS.items()))
            or any(type(count) is not int for _, count in binding.replicas)):
        raise ValueError("Prequalified application role binding required")
    return binding


def sanitize(activity, stats, binding):
    checked_binding(binding)
    if role_digest(activity["observer_role"], activity["database"]) != binding.identity_sha256:
        raise ValueError("Observer role or database differs from application binding")
    counts = partition(activity)
    # The shared counter sanitizer validates this explicitly scoped application count.
    # Foreign counts are retained separately and never presented as unrestricted global coverage.
    core = full.sanitize({"utc": activity["utc"], "restricted_sessions": counts["restricted_application_sessions"],
                          "active": counts["active"], "idle_in_transaction": counts["idle_in_transaction"],
                          "waits": activity["waits"]}, stats)
    core.pop("restricted_sessions")
    if sum(w["count"] for w in core["waits"]) > counts["active"]:
        raise ValueError("Wait count exceeds scoped active sessions")
    return {**core, **counts, "diagnostic_scope": SCOPE, "role_identity_sha256": binding.identity_sha256,
            "application_visibility_complete": True,
            "full_database_visibility_complete": counts["restricted_foreign_sessions"] == 0}


class Collector:
    def __init__(self, binding):
        self.binding = checked_binding(binding)
        self.capability = None

    def collect(self, conn):
        started, phase = time.monotonic(), "capabilities"
        try:
            if self.capability is None:
                value = full.bounded_query(conn, full.CAPABILITY_SQL)
                if (type(value.get("server_version_num")) is not int or not 170000 <= value["server_version_num"] < 190000
                        or any(value.get(k) is not True for k in ("wal_view", "checkpointer_view", "track_activities"))
                        or any(type(value.get(k)) is not bool for k in ("full_statistics_visibility", "track_wal_io_timing", "track_io_timing"))):
                    raise ValueError("Scoped diagnostic capabilities unavailable")
                self.capability = value
            phase = "activity"
            activity = full.bounded_query(conn, ACTIVITY_SQL)
            phase = "statistics"
            stats = full.bounded_query(conn, full.STATS_SQL)
            phase = "validation"
            result = sanitize(activity, stats, self.binding)
        except Exception as exc:  # noqa: BLE001 - fixed categories only; no SQL, role or exception text.
            code = {"Complete known application visibility required": "application_visibility_missing",
                    "Observer role or database differs from application binding": "role_binding_changed",
                    "Consistent role partitions required": "partition_invalid"}.get(str(exc), "scoped_diagnostics_invalid")
            result = {"diagnostic_scope": SCOPE, "application_visibility_complete": False,
                      "full_database_visibility_complete": False, "error_phase": phase,
                      "error_type": type(exc).__name__, "error_code": code}
        result["collection_ms"] = (time.monotonic() - started) * 1000
        if result["collection_ms"] > full.MAX_OVERHEAD_MS:
            result.update(application_visibility_complete=False, overhead_budget_exceeded=True)
        return result


def summarize(path, binding):
    """New scoped gate; never emit the historical generic complete=true field."""
    checked_binding(binding)
    path = Path(path)
    if path.stat().st_size > full.MAX_BYTES:
        raise ValueError("Diagnostic trace byte bound exceeded")
    previous, epochs, errors = None, {}, Counter()
    totals = {g: {} for g in ("wal", "checkpointer", "database")}
    peaks, rows, max_ms, max_foreign, masked_rows, timing = {}, 0, 0, 0, 0, set()
    continuity, global_visibility = True, True
    with path.open("rb") as stream:
        while line := stream.readline(full.MAX_LINE + 1):
            if len(line) > full.MAX_LINE or rows >= full.MAX_ROWS:
                raise ValueError("Diagnostic trace row or line bound exceeded")
            row = json.loads(line); rows += 1
            item = row.get("application_database_wait_diagnostics", {})
            if (item.get("diagnostic_scope") != SCOPE or item.get("application_visibility_complete") is not True
                    or item.get("role_identity_sha256") != binding.identity_sha256):
                errors["MissingOrInvalidScope"] += 1
                global_visibility = False
                continue
            counts = partition(item)
            activity = {"utc": item["activity_utc"], "restricted_sessions": counts["restricted_application_sessions"],
                        "active": counts["active"], "idle_in_transaction": counts["idle_in_transaction"], "waits": item["waits"]}
            stats = {"utc": item["statistics_utc"], **{k: item[k] for k in
                     ("wal", "checkpointer", "database", "track_wal_io_timing", "track_io_timing")}}
            checked = full.sanitize(activity, stats)
            if sum(w["count"] for w in checked["waits"]) > counts["active"]:
                raise ValueError("Wait count exceeds scoped active sessions")
            is_global = counts["restricted_foreign_sessions"] == 0
            if item.get("full_database_visibility_complete") is not is_global:
                raise ValueError("Global visibility flag contradicts masked sessions")
            global_visibility &= is_global
            max_foreign = max(max_foreign, counts["restricted_foreign_sessions"])
            masked_rows += not is_global
            max_ms = max(max_ms, full.number(item["collection_ms"]))
            current = full.stamp(checked["statistics_utc"])
            timing.add(checked["wal_timing_available"])
            if previous and not 0 < (current - previous[0]).total_seconds() <= 2:
                continuity = False
            for wait in checked["waits"]:
                key = wait["type"] + ":" + wait["event"]
                peaks[key] = max(peaks.get(key, 0), wait["count"])
            for group, deltas in totals.items():
                epoch = checked[group]["stats_reset"]
                if group in epochs and epoch != epochs[group]:
                    continuity = False
                epochs[group] = epoch
                for key, value in checked[group].items():
                    if key == "stats_reset" or value is None:
                        continue
                    deltas.setdefault(key, 0)
                    if previous:
                        before = previous[1][group].get(key)
                        if before is None or value < before:
                            continuity = False
                        else:
                            deltas[key] += value - before
            previous = (current, checked)
    complete = rows >= 2 and not errors and continuity and max_ms <= full.MAX_OVERHEAD_MS
    return {"decision": "ADR0176", "diagnostic_scope": SCOPE,
            "application_database_wait_evidence_complete": complete,
            "full_database_visibility_complete": complete and global_visibility,
            "role_identity_sha256": binding.identity_sha256, "bound_replicas": dict(binding.replicas),
            "rows": rows, "error_counts": dict(errors), "counter_continuity": continuity,
            "max_collection_ms": max_ms, "masked_foreign_sample_count": masked_rows,
            "masked_foreign_sessions_peak": max_foreign, "wal_timing_available_throughout": timing == {True},
            "counter_deltas": totals if continuity and not errors else None,
            "application_active_wait_peaks": peaks,
            "scope": "Application-role activity only; WAL/checkpoint/database counters remain aggregate. Foreign invisibility is explicit; causality and global activity completeness are not inferred."}


def binding_record(binding):
    checked_binding(binding)
    return {"decision": "ADR0176", "diagnostic_scope": SCOPE,
            "role_identity_sha256": binding.identity_sha256, "bound_replicas": dict(binding.replicas)}


def binding_from_record(value):
    if (not isinstance(value, dict) or set(value) != {"decision", "diagnostic_scope", "role_identity_sha256", "bound_replicas"}
            or value["decision"] != "ADR0176" or value["diagnostic_scope"] != SCOPE
            or not isinstance(value["bound_replicas"], dict)):
        raise ValueError("Exact application role coverage record required")
    return checked_binding(RoleCoverage(value["role_identity_sha256"], tuple(sorted(value["bound_replicas"].items()))))


def startup(row, binding=None):
    item = row.get("application_database_wait_diagnostics", {})
    if (item.get("diagnostic_scope") != SCOPE or item.get("application_visibility_complete") is not True
            or type(item.get("full_database_visibility_complete")) is not bool):
        raise ValueError("Application database diagnostics unavailable before dispatch")
    if binding is not None and item.get("role_identity_sha256") != checked_binding(binding).identity_sha256:
        raise ValueError("Scoped startup role binding differs")
    if not isinstance(item.get("role_identity_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", item["role_identity_sha256"]):
        raise ValueError("Scoped startup identity missing")
    counts = partition(item)
    if item["full_database_visibility_complete"] is not (counts["restricted_foreign_sessions"] == 0):
        raise ValueError("Global visibility contradicts masked foreign sessions")
    if full.number(item["collection_ms"]) > full.MAX_OVERHEAD_MS:
        raise ValueError("Scoped startup overhead exceeded")


def install(module, binding):
    original, collector = module.sample, Collector(binding)

    def sample(conn, show_ids):
        result = original(conn, show_ids)
        result["application_database_wait_diagnostics"] = collector.collect(conn)
        return result

    module.sample = sample


def preflight_program(binding, directory):
    value = binding_record(binding)
    return ("import sys,json,os,psycopg;sys.path.insert(0," + repr(directory) + ")\n"
            "from application_database_wait_evidence import Collector,binding_from_record,startup\n"
            "binding=binding_from_record(" + repr(value) + ")\n"
            "with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,connect_timeout=5) as conn:\n"
            " collector=Collector(binding);records=[collector.collect(conn) for _ in range(2)]\n"
            " for item in records:startup({'application_database_wait_diagnostics':item},binding)\n"
            " print(json.dumps({'pass':True,'diagnostic_scope':'application_role',"
            "'role_identity_sha256':binding.identity_sha256,'max_collection_ms':max(r['collection_ms'] for r in records),"
            "'full_database_visibility_complete':all(r['full_database_visibility_complete'] for r in records)}))\n")
