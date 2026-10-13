"""ADR0285 integrate the ADR0282 read-only terminal audit; no load authority."""
import inspect
import time
from uuid import UUID

import audit_checkout_smoke
from cce_transaction_recovery import RELATIONSHIP_SQL
from worker_separation_audits import RELATIONSHIP_NAMES

TOTAL_SECONDS = 600
TRANSPORT_SECONDS = 780  # Includes at most 150 seconds of hold expiry wait.


def expectations(show_ids, expected, paid, callbacks):
    if (not isinstance(show_ids, list) or len(show_ids) not in {1, 60, 84, 102, 1008}
            or len(set(show_ids)) != len(show_ids)
            or any(not isinstance(v, str) or str(UUID(v)) != v for v in show_ids)
            or type(expected) is not int or not 0 <= expected <= len(show_ids) * 300
            or type(paid) is not int or not 0 <= paid <= expected
            or type(callbacks) is not int or not 1 <= callbacks <= 10):
        raise ValueError("Exact bounded one-ticket audit expectations required")


def relationship_queries():
    sql = RELATIONSHIP_SQL.replace("cohort AS (", "cohort AS NOT MATERIALIZED (", 1).replace(
        "scoped_bookings AS (", "scoped_bookings AS NOT MATERIALIZED (", 1)
    if sql.replace(" AS NOT MATERIALIZED (", " AS (") != RELATIONSHIP_SQL:
        raise ValueError("Relationship predicates changed")
    prefix, tail = sql.split("\n SELECT\n", 1)
    parts, begin, depth, quoted = [], 0, 0, False
    for index, ch in enumerate(tail):
        if ch == "'":
            quoted = not quoted
        elif not quoted:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append(tail[begin:index].strip())
                begin = index + 1
    parts.append(tail[begin:].strip())
    if len(parts) != len(RELATIONSHIP_NAMES) or depth != 0 or quoted:
        raise ValueError("Exact seven complete predicates required")
    return [prefix + "\n SELECT " + part for part in parts]


def set_timeout(conn, deadline, maximum):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Bounded financial audit deadline")
    milliseconds = max(1, int(min(maximum, remaining) * 1000))
    conn.execute("SET LOCAL statement_timeout=" + str(milliseconds))


def financial_snapshot(conn, show_ids, expected, paid, callbacks, queries, aggregate_audit):
    expectations(show_ids, expected, paid, callbacks)
    deadline = time.monotonic() + TOTAL_SECONDS
    failed = "aggregate"
    aggregate, relationships = None, {}
    try:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            conn.execute("SET LOCAL lock_timeout='2s'")
            aggregate = aggregate_audit(conn, show_ids, expected, paid, callbacks, deadline=deadline)
            if len(queries) != len(RELATIONSHIP_NAMES):
                raise ValueError("Exact seven audit predicates required")
            for index, query in enumerate(queries):
                failed = RELATIONSHIP_NAMES[index]
                set_timeout(conn, deadline, 180)
                row = conn.execute(query, (show_ids,) * query.count("%s")).fetchone()
                if row is None or len(row) != 1 or type(row[0]) is not int or row[0] < 0:
                    raise ValueError("One nonnegative integer audit count required")
                relationships[failed] = row[0]
    except Exception as error:  # noqa: BLE001 - Retain safe failure identity, never DB error text.
        return {"pass": False, "hold_deadlines_elapsed": False, "counts": aggregate,
                "relationships": relationships, "failed_predicate": failed,
                "failure_type": type(error).__name__}
    checks = {
        "post_ttl_complete": relationships["holds_not_past_ttl"] == 0,
        "payments_durable": aggregate["pass"] is True,
        "ticket_relationships_valid": relationships["unique_issued_tickets"] == paid
            and all(relationships[k] == 0 for k in RELATIONSHIP_NAMES[:5]),
    }
    return {**aggregate, "counts": aggregate, "relationships": relationships,
            "checks": checks, "hold_deadlines_elapsed": checks["post_ttl_complete"],
            "pass": all(checks.values())}


def program(show_ids, expected, expected_paid=None, callbacks=1):
    paid = expected if expected_paid is None else expected_paid
    expectations(show_ids, expected, paid, callbacks)
    aggregate = inspect.getsource(audit_checkout_smoke.audit)
    signature = "callback_duplicates=3):"
    timeout = 'conn.execute("SET LOCAL statement_timeout = \'20s\'")'
    if aggregate.count(signature) != 1 or aggregate.count(timeout) != 1:
        raise ValueError("Canonical aggregate audit changed")
    aggregate = aggregate.replace(signature, "callback_duplicates=3, *, deadline):", 1).replace(
        timeout, "set_timeout(conn, deadline, 60)", 1)
    source = "import json,os,time,psycopg\nfrom uuid import UUID\n"
    source += "TOTAL_SECONDS=" + repr(TOTAL_SECONDS) + "\nRELATIONSHIP_NAMES=" + repr(RELATIONSHIP_NAMES) + "\n"
    source += "\n".join(inspect.getsource(f) for f in (expectations, set_timeout))
    source += "\n" + aggregate + "\n" + inspect.getsource(financial_snapshot)
    source += "\nshows=" + repr(show_ids) + "\nqueries=" + repr(relationship_queries())
    source += "\nexpected=" + repr(expected) + "\npaid=" + repr(paid) + "\ncallbacks=" + repr(callbacks)
    source += """
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,connect_timeout=5) as conn:
 with conn.transaction():
  conn.execute('SET TRANSACTION READ ONLY')
  conn.execute("SET LOCAL statement_timeout='10s'")
  delay=conn.execute("SELECT greatest(coalesce(extract(epoch FROM max(expires_at)-clock_timestamp()),0),0) FROM holds WHERE event_id=ANY(%s::uuid[])",(shows,)).fetchone()[0]
 if delay>150:raise TimeoutError('Cohort TTL exceeds bounded audit window')
 time.sleep(float(delay)+0.05)
 result=financial_snapshot(conn,shows,expected,paid,callbacks,queries,audit)
print(json.dumps(result))
"""
    compile(source, "bounded-financial-audit", "exec")
    return source
