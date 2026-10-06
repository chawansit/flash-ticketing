"""ADR0173 bounded, read-only diagnostics using the existing observer connection."""
import json
import math
import re
import time
from datetime import datetime
from pathlib import Path

MAX_ROWS = 2000
MAX_BYTES = 64 * 2**20
MAX_LINE = 2 * 2**20
MAX_OVERHEAD_MS = 250
WAL_KEYS = ("wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full", "wal_write", "wal_sync", "wal_write_time", "wal_sync_time")
CHECKPOINT_KEYS = ("num_timed", "num_requested", "num_done", "write_time", "sync_time", "buffers_written")
DATABASE_KEYS = ("xact_commit", "xact_rollback", "blks_read", "blks_hit", "blk_read_time", "blk_write_time", "deadlocks", "temp_bytes")
CAPABILITY_SQL = """SELECT json_build_object(
  'server_version_num',current_setting('server_version_num')::int,
  'wal_view',to_regclass('pg_catalog.pg_stat_wal') IS NOT NULL,
  'checkpointer_view',to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL,
  'track_activities',current_setting('track_activities')='on',
  'track_wal_io_timing',current_setting('track_wal_io_timing')='on',
  'track_io_timing',current_setting('track_io_timing')='on')"""
ACTIVITY_SQL = """SELECT json_build_object(
 'utc',clock_timestamp(),
 'restricted_sessions',count(*) FILTER (WHERE state IS NULL),
 'restricted_backends',coalesce((SELECT json_agg(b) FROM (
   SELECT backend_type,usename=current_user AS own_role,count(*) AS count
   FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state IS NULL
   GROUP BY backend_type,usename=current_user ORDER BY backend_type LIMIT 16) b),'[]'::json),
 'active',count(*) FILTER (WHERE state='active'),
 'idle_in_transaction',count(*) FILTER (WHERE state LIKE 'idle in transaction%'),
 'waits',coalesce((SELECT json_agg(w) FROM (
   SELECT coalesce(wait_event_type,'None') AS type,coalesce(wait_event,'None') AS event,count(*) AS count
   FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active'
   GROUP BY wait_event_type,wait_event ORDER BY wait_event_type,wait_event LIMIT 64) w),'[]'::json))
 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()"""


def projection(alias, keys):
    return "json_build_object(" + ",".join(repr(k) + ",to_jsonb(" + alias + ")->" + repr(k)
                                            for k in (*keys, "stats_reset")) + ")"


STATS_SQL = "SELECT json_build_object('utc',clock_timestamp(),'wal'," + projection("w", WAL_KEYS) + ", 'checkpointer'," + projection("c", CHECKPOINT_KEYS) + ", 'database'," + projection("d", DATABASE_KEYS) + ", 'track_wal_io_timing',current_setting('track_wal_io_timing')='on', 'track_io_timing',current_setting('track_io_timing')='on') FROM pg_stat_wal w CROSS JOIN pg_stat_checkpointer c CROSS JOIN pg_stat_database d WHERE d.datname=current_database()"


def bounded_query(conn, sql):
    if conn.autocommit is not True:
        raise ValueError("Existing autocommit observer connection required")
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout='100ms'")
        conn.execute("SET TRANSACTION READ ONLY")
        return conn.execute(sql).fetchone()[0]


def capabilities(conn):
    result = bounded_query(conn, CAPABILITY_SQL)
    if (type(result.get("server_version_num")) is not int or not 170000 <= result["server_version_num"] < 190000
            or any(result.get(k) is not True for k in ("wal_view", "checkpointer_view", "track_activities"))
            or any(type(result.get(k)) is not bool for k in ("track_wal_io_timing", "track_io_timing"))):
        raise ValueError("PostgreSQL 17/18 diagnostic views and activity collection required")
    return result


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("Finite nonnegative diagnostic value required")
    return value


def stamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Aware database timestamp required")
    return result


def sanitize(activity, stats):
    stamp(activity["utc"])
    stamp(stats["utc"])
    result = {"activity_utc": activity["utc"], "statistics_utc": stats["utc"],
              **{k: number(activity[k]) for k in ("restricted_sessions", "active", "idle_in_transaction")}, "waits": []}
    if result["restricted_sessions"] != 0 or not isinstance(activity["waits"], list) or len(activity["waits"]) > 64:
        raise ValueError("Full bounded activity visibility required")
    for wait in activity["waits"]:
        if any(not isinstance(wait.get(k), str) or not re.fullmatch(r"[A-Za-z0-9_]{1,64}", wait[k]) for k in ("type", "event")):
            raise ValueError("Catalog wait labels required")
        result["waits"].append({"type": wait["type"], "event": wait["event"], "count": number(wait["count"])})
    for group, keys in (("wal", WAL_KEYS), ("checkpointer", CHECKPOINT_KEYS), ("database", DATABASE_KEYS)):
        epoch = stats[group]["stats_reset"]
        if epoch is not None:
            stamp(epoch)
        result[group] = {"stats_reset": epoch}
        for key in keys:
            value = stats[group].get(key)
            # Version-specific absent columns are explicit nulls, never synthesized zeroes.
            if value is None and not (group == "wal" and key in WAL_KEYS[4:] or group == "checkpointer" and key == "num_done"):
                raise ValueError("Required statistics counter absent")
            result[group][key] = None if value is None else number(value)
    for key in ("track_wal_io_timing", "track_io_timing"):
        if type(stats[key]) is not bool:
            raise ValueError("Explicit timing availability required")
        result[key] = stats[key]
    result["wal_timing_available"] = stats["track_wal_io_timing"] and all(result["wal"][k] is not None for k in WAL_KEYS[-2:])
    return result


def failure_context(activity):
    """Retain only bounded category counts; do not export error text or identities."""
    result = {}
    for key in ("restricted_sessions", "active", "idle_in_transaction"):
        try:
            result[key] = number(activity[key])
        except (ValueError, KeyError, TypeError):
            pass
    groups = activity.get("restricted_backends", [])
    if isinstance(groups, list) and len(groups) <= 16:
        safe = []
        for group in groups:
            if (not isinstance(group, dict) or not isinstance(group.get("backend_type"), str)
                    or not re.fullmatch(r"[A-Za-z_ ]{1,64}", group["backend_type"])
                    or group.get("own_role") is not None and type(group["own_role"]) is not bool):
                continue
            try:
                safe.append({"backend_type": group["backend_type"], "own_role": group.get("own_role"),
                             "count": number(group["count"])})
            except (ValueError, KeyError, TypeError):
                pass
        result["restricted_backends"] = safe
    return result


class Collector:
    def __init__(self):
        self.capability = None

    def collect(self, conn):
        started = time.monotonic()
        phase, activity = "capabilities", None
        try:
            if self.capability is None:
                self.capability = capabilities(conn)
            phase = "activity"
            activity = bounded_query(conn, ACTIVITY_SQL)
            phase = "statistics"
            stats = bounded_query(conn, STATS_SQL)
            phase = "validation"
            result = sanitize(activity, stats)
            result.update(complete=True, capabilities=self.capability)
        except Exception as exc:  # noqa: BLE001 - classify only; never export DB error text
            result = {"complete": False, "error_type": type(exc).__name__, "error_phase": phase}
            codes = {"Full bounded activity visibility required": "activity_visibility_or_bound",
                     "Catalog wait labels required": "wait_label",
                     "Required statistics counter absent": "missing_counter",
                     "Finite nonnegative diagnostic value required": "invalid_counter",
                     "Aware database timestamp required": "timestamp",
                     "Explicit timing availability required": "timing_availability"}
            result["error_code"] = codes.get(str(exc), "unclassified")
            if phase == "validation" and isinstance(activity, dict):
                result["failure_context"] = failure_context(activity)
        result["collection_ms"] = (time.monotonic() - started) * 1000
        if result["collection_ms"] > MAX_OVERHEAD_MS:
            result.update(complete=False, overhead_budget_exceeded=True)
        return result


def install(module):
    original = module.sample
    collector = Collector()

    def sample(conn, show_ids):
        result = original(conn, show_ids)
        result["database_wait_diagnostics"] = collector.collect(conn)
        return result

    module.sample = sample


def summarize(path):
    path = Path(path)
    if path.stat().st_size > MAX_BYTES:
        raise ValueError("Diagnostic trace byte bound exceeded")
    previous = None
    epochs = {}
    totals = {g: {} for g in ("wal", "checkpointer", "database")}
    peaks = {}
    timing = set()
    errors = {}
    rows = 0
    max_ms = 0
    continuity = True
    with path.open("rb") as stream:
        while line := stream.readline(MAX_LINE + 1):
            if len(line) > MAX_LINE or rows >= MAX_ROWS:
                raise ValueError("Diagnostic trace row or line bound exceeded")
            row = json.loads(line)
            rows += 1
            item = row.get("database_wait_diagnostics", {})
            if item.get("complete") is not True:
                kind = item.get("error_type", "MissingOrOverhead")
                errors[kind] = errors.get(kind, 0) + 1
                continue
            max_ms = max(max_ms, number(item["collection_ms"]))
            # Revalidate raw evidence without retaining arbitrary input fields.
            activity = {"utc": item["activity_utc"], **{k: item[k] for k in ("restricted_sessions", "active", "idle_in_transaction", "waits")}}
            stats = {"utc": item["statistics_utc"], **{k: item[k] for k in ("wal", "checkpointer", "database", "track_wal_io_timing", "track_io_timing")}}
            checked = sanitize(activity, stats)
            current = stamp(checked["statistics_utc"])
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
    complete = rows >= 2 and not errors and continuity and max_ms <= MAX_OVERHEAD_MS
    return {"decision": "ADR0173", "complete": complete, "rows": rows, "error_counts": errors,
            "counter_continuity": continuity, "max_collection_ms": max_ms,
            "wal_timing_available_throughout": timing == {True},
            "counter_deltas": totals if continuity else None, "observed_active_wait_peaks": peaks,
            "scope": "Observer lifetime, cluster WAL/checkpoints and current-database activity; one-second snapshots may miss brief waits. Correlation does not prove causality."}


def preflight_program():
    source = Path(__file__).read_text(encoding="utf-8")
    return source + "\nimport os,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,connect_timeout=5) as conn:\n collector=Collector()\n records=[collector.collect(conn) for _ in range(2)]\n assert all(r['complete'] for r in records),'Database diagnostics unavailable'\n print(json.dumps({'pass':True,'capabilities':collector.capability,'max_collection_ms':max(r['collection_ms'] for r in records)}))\n"
