"""ADR0301 supplementary query deltas; no connections or load are created here."""
import hashlib
import math

IDENTITY = ("userid", "dbid", "queryid", "toplevel")
COUNTERS = ("calls", "total_exec_time", "rows", "shared_blks_hit", "shared_blks_read",
            "shared_blks_dirtied", "shared_blks_written", "wal_records", "wal_fpi", "wal_bytes")
SNAPSHOT_SQL = """SELECT json_build_object(
 'captured_at',clock_timestamp(),
 'statistics_info',(SELECT row_to_json(i) FROM public.pg_stat_statements_info i),
 'statement_count',(SELECT count(*) FROM public.pg_stat_statements
   WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())),
 'statements',(SELECT coalesce(json_agg(row),'[]'::json) FROM (
   SELECT * FROM public.pg_stat_statements
   WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
   ORDER BY userid,dbid,queryid,toplevel LIMIT 5000) row))"""
READ_ONLY_BODY = """
 result=conn.execute(SQL).fetchone()[0]
 print(json.dumps(result,default=str))
"""


def oid(value):
    # PostgreSQL JSON serializes OID catalog columns as decimal strings.
    if type(value) is str:
        if not value.isascii() or not value.isdigit() or len(value) > 10:
            raise ValueError("Canonical positive catalog OID required")
        parsed = int(value)
        if str(parsed) != value:
            raise ValueError("Canonical positive catalog OID required")
        value = parsed
    if type(value) is not int or not 0 < value < 2**32:
        raise ValueError("Canonical positive catalog OID required")
    return value


def key(row):
    if type(row.get("queryid")) is not int or type(row.get("toplevel")) is not bool:
        raise ValueError("Complete role-qualified query identity required")
    return oid(row.get("userid")), oid(row.get("dbid")), row["queryid"], row["toplevel"]


def number(value):
    if type(value) not in (int, float, str):
        raise ValueError("Numeric cumulative counter required")
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError("Finite nonnegative cumulative counter required")
    return value


def indexed(snapshot):
    info = snapshot.get("statistics_info", {})
    rows = snapshot.get("statements")
    if (not info.get("stats_reset") or type(info.get("dealloc")) is not int
            or type(snapshot.get("statement_count")) is not int
            or not isinstance(rows, list) or len(rows) != snapshot["statement_count"]
            or len(rows) > 5000):
        raise ValueError("Complete bounded query snapshot required")
    result = {}
    for row in rows:
        identity = key(row)
        if identity in result or not isinstance(row.get("query"), str):
            raise ValueError("Unique query identity and private text required")
        for counter in COUNTERS:
            number(row.get(counter))
        result[identity] = row
    return result


def category(query):
    query = " ".join(query.lower().split())
    if query.startswith("with cohort"):
        return "paid_cohort_observer"
    for table in ("outbox_events", "consumer_inbox", "orders", "holds", "tickets",
                  "bookings", "order_items", "reservation_commands", "payment_attempts",
                  "payment_callbacks", "idempotency_records", "event_seats", "seat_refresh_requests"):
        for command in ("insert", "update", "delete"):
            marker = command + (" into " if command == "insert" else
                                " from " if command == "delete" else " ") + table
            if query.startswith(marker):
                return table + "_" + command
    if "skip locked" in query and "payment_attempts" in query:
        return "callback_dispatch_claim"
    if "from event_seats" in query:
        return "seat_projection_read"
    command = query.split(" ", 1)[0].upper()
    return command if command in {"SELECT", "BEGIN", "COMMIT", "ROLLBACK", "SET"} else "OTHER"


def summarize(before, after):
    left, right = indexed(before), indexed(after)
    if (before["statistics_info"]["stats_reset"] != after["statistics_info"]["stats_reset"]
            or before["statistics_info"]["dealloc"] != after["statistics_info"]["dealloc"]
            or set(left) - set(right)):
        raise ValueError("Reset, eviction or missing queries prevents attribution")
    rows = []
    for identity, current in right.items():
        previous = left.get(identity)
        delta = {c: number(current[c]) - (number(previous[c]) if previous else 0)
                 for c in COUNTERS}
        if any(v < 0 for v in delta.values()):
            raise ValueError("Counter regression prevents attribution")
        if not delta["calls"]:
            continue
        fingerprint = hashlib.sha256(repr(identity).encode()).hexdigest()
        rows.append({"identity_sha256": fingerprint, "queryid": str(identity[2]),
                     "toplevel": identity[3], "category": category(current["query"]),
                     "mean_exec_ms": delta["total_exec_time"] / delta["calls"], **delta})
    return {"decision": "ADR0301", "complete": True,
            "span": "dispatch boundary through customer completion; includes launch lead-in and grace",
            "before_utc": before["captured_at"], "after_utc": after["captured_at"],
            "statistics_reset": before["statistics_info"]["stats_reset"],
            "new_query_identities": len(set(right) - set(left)),
            "totals": {c: sum(r[c] for r in rows) for c in COUNTERS},
            "queries": sorted(rows, key=lambda r: r["total_exec_time"], reverse=True)}



def dispatch_with_after_snapshot(dispatch, collect_after, record):
    """Attempt diagnostics before cleanup; preserve an original dispatch failure."""
    primary_error = None
    try:
        return dispatch()
    except BaseException as error:
        primary_error = error
        raise
    finally:
        record["after_snapshot_attempted"] = True
        try:
            collect_after()
            record["after_snapshot_complete"] = True
        except (Exception, KeyboardInterrupt) as error:
            record["after_snapshot_complete"] = False
            record["after_snapshot_failure_type"] = type(error).__name__
            if primary_error is None:
                raise
