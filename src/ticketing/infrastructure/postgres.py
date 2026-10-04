import logging
from contextlib import ExitStack, contextmanager
from time import perf_counter as monotonic

from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ticketing.observability import (
    DB_COMMIT_SECONDS,
    DB_CONNECTION_HOLD_SECONDS,
    DB_ERRORS,
    DB_POOL_ACQUIRING,
    DB_POOL_IN_USE,
    DB_POOL_RETURN_SECONDS,
    DB_POOL_SECONDS,
    DB_POOL_STATE,
    DB_QUERY_SECONDS,
    DB_ROLLBACK_SECONDS,
    DB_SECONDS,
    DB_TRANSACTION_BODY_SECONDS,
)

logger = logging.getLogger(__name__)
SLOW_DB_PHASE_SECONDS = 0.1


def record_slow_db_phase(phase: str, duration: float, outcome: str = "ok") -> None:
    if duration >= SLOW_DB_PHASE_SECONDS:
        logger.warning(
            "slow_db_phase",
            extra={
                "fields": {
                    "event": "slow_db_phase",
                    "phase": phase,
                    "duration_ms": round(duration * 1000, 3),
                    "outcome": outcome,
                }
            },
        )


class MeasuredCursor(Cursor):
    def execute(self, query, params=None, **kwargs):
        command = (
            query.lstrip().split(None, 1)[0].upper() if isinstance(query, str) and query.strip() else "OTHER"
        )
        if command not in {"SELECT", "INSERT", "UPDATE", "DELETE", "SET", "BEGIN", "COMMIT", "ROLLBACK"}:
            command = "OTHER"
        started = monotonic()
        try:
            return super().execute(query, params, **kwargs)
        finally:
            DB_QUERY_SECONDS.labels(command).observe(monotonic() - started)


class Postgres:
    def __init__(self, url: str, maximum: int = 12, wait_ms: int = 150, maximum_waiting: int | None = None):
        self.pool = ConnectionPool(
            url,
            open=True,
            min_size=1,
            max_size=maximum,
            timeout=wait_ms / 1000,
            max_waiting=maximum if maximum_waiting is None else maximum_waiting,
            kwargs={"row_factory": dict_row, "prepare_threshold": None, "cursor_factory": MeasuredCursor},
        )

    @contextmanager
    def transaction(self):
        def _record_error(exc):
            state = getattr(exc, "sqlstate", None)
            if state or type(exc).__name__ in {"PoolTimeout", "TooManyRequests"}:
                DB_ERRORS.labels(state or type(exc).__name__).inc()

        error_recorded = {"value": False}
        try:
            with self.connection() as conn:
                started = monotonic()
                body_started = monotonic()
                body_done = None
                transaction_started = False
                try:
                    conn.execute("BEGIN")
                    transaction_started = True
                    conn.execute(
                        "SELECT set_config('lock_timeout', '75ms', true), "
                        "set_config('statement_timeout', '1500ms', true), "
                        "set_config('idle_in_transaction_session_timeout', '3s', true)"
                    )

                    yield conn

                    body_done = monotonic()
                    DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)

                    commit_started = monotonic()
                    commit_outcome = "error"
                    try:
                        conn.commit()
                        commit_outcome = "ok"
                    finally:
                        commit_duration = monotonic() - commit_started
                        DB_COMMIT_SECONDS.observe(commit_duration)
                        record_slow_db_phase("commit", commit_duration, commit_outcome)
                except Exception as exc:
                    error_recorded["value"] = True
                    body_done = body_done or monotonic()
                    DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)

                    rollback_started = monotonic()
                    try:
                        if transaction_started:
                            conn.rollback()
                    finally:
                        DB_ROLLBACK_SECONDS.observe(monotonic() - rollback_started)

                    _record_error(exc)
                    raise
                finally:
                    if body_done is None:
                        body_done = monotonic()
                        DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)
                    DB_SECONDS.observe(monotonic() - started)
        except Exception as exc:
            if not error_recorded["value"]:
                _record_error(exc)
            raise

    @contextmanager
    def connection(self):
        start, outcome = monotonic(), "error"
        conn = None
        acquired_at = None
        DB_POOL_ACQUIRING.inc()
        try:
            conn = self.pool.getconn()
            acquired_at = monotonic()
            outcome = "ok"
            DB_POOL_SECONDS.labels(outcome).observe(acquired_at - start)
            DB_POOL_ACQUIRING.dec()
            self.sample_pool()
            DB_POOL_IN_USE.inc()
            try:
                with conn:
                    yield conn
            finally:
                return_started = monotonic()
                return_outcome = "error"
                try:
                    if conn is not None:
                        self.pool.putconn(conn)
                    return_outcome = "ok"
                finally:
                    return_duration = monotonic() - return_started
                    DB_POOL_RETURN_SECONDS.observe(return_duration)
                    record_slow_db_phase("pool_return", return_duration, return_outcome)
                DB_CONNECTION_HOLD_SECONDS.observe(monotonic() - acquired_at)
                if conn is not None:
                    DB_POOL_IN_USE.dec()
                self.sample_pool()
        finally:
            if outcome == "error":
                DB_POOL_SECONDS.labels(outcome).observe(monotonic() - start)
                # Keep acquisition telemetry consistent when checkout fails.
                DB_POOL_ACQUIRING.dec()
                self.sample_pool()
            else:
                # On success, return path already decremented DB_POOL_ACQUIRING.
                pass

    def sample_pool(self):
        roles = getattr(self, "_metric_pool_roles", None)
        waiter_limits = getattr(self, "_metric_waiter_limits", {})
        pools = roles if roles is not None else {"general": self.pool}
        states = {role: pool.get_stats() for role, pool in pools.items()}
        for name in ("pool_size", "pool_available", "requests_waiting", "pool_max"):
            DB_POOL_STATE.labels(name).set(sum(stats.get(name, 0) for stats in states.values()))
            if roles is not None:
                for role in ("general", "payment"):
                    DB_POOL_STATE.labels(f"{role}_{name}").set(states.get(role, {}).get(name, 0))
        if roles is not None:
            DB_POOL_STATE.labels("pool_max_waiting").set(sum(waiter_limits.values()))
            for role in ("general", "payment"):
                DB_POOL_STATE.labels(f"{role}_max_waiting").set(waiter_limits.get(role, 0))

    def close(self):
        self.pool.close()


def api_pool_budgets(maximum: int, maximum_waiting: int | None, payment_maximum: int = 0,
                     payment_maximum_waiting: int = 0):
    """Partition existing API ceilings; no zero (unlimited) enabled queue."""
    waiting = maximum if maximum_waiting is None else maximum_waiting
    if maximum < 1 or waiting < 1 or not 0 <= payment_maximum < maximum:
        raise ValueError("Invalid API pool budget")
    if payment_maximum_waiting < 0 or (payment_maximum_waiting and (
        not payment_maximum or payment_maximum_waiting >= waiting
    )):
        raise ValueError("Invalid explicit payment waiter budget")
    if not payment_maximum:
        return {"general": {"maximum": maximum, "maximum_waiting": waiting}, "payment": None}
    if waiting < 2:
        raise ValueError("Payment partition requires at least two total waiter slots")
    payment_waiting = payment_maximum_waiting or max(1, waiting * payment_maximum // maximum)
    return {
        "general": {"maximum": maximum - payment_maximum, "maximum_waiting": waiting - payment_waiting},
        "payment": {"maximum": payment_maximum, "maximum_waiting": payment_waiting},
    }


def create_api_databases(url, maximum, wait_ms, maximum_waiting, payment_maximum=0, payment_maximum_waiting=0):
    """Return general/payment adapters, sharing one pool when disabled."""
    budgets = api_pool_budgets(maximum, maximum_waiting, payment_maximum, payment_maximum_waiting)
    with ExitStack() as resources:
        general = Postgres(url, wait_ms=wait_ms, **budgets["general"])
        resources.callback(general.close)
        payment = general
        if budgets["payment"] is not None:
            payment = Postgres(url, wait_ms=wait_ms, **budgets["payment"])
            resources.callback(payment.close)
        roles = {"general": general.pool}
        if payment is not general:
            roles["payment"] = payment.pool
        general._metric_pool_roles = payment._metric_pool_roles = roles
        limits = {role: budget["maximum_waiting"] for role, budget in budgets.items() if budget is not None}
        general._metric_waiter_limits = payment._metric_waiter_limits = limits
        resources.pop_all()
        return general, payment
