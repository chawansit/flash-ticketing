from contextlib import contextmanager
from time import perf_counter as monotonic

from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ticketing.observability import (
    DB_COMMIT_SECONDS,
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
    def __init__(self, url: str, maximum: int = 12):
        self.pool = ConnectionPool(
            url,
            open=True,
            min_size=1,
            max_size=maximum,
            timeout=0.15,
            max_waiting=maximum,
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
                    conn.execute("SET LOCAL lock_timeout = '75ms'")
                    conn.execute("SET LOCAL statement_timeout = '1500ms'")
                    conn.execute("SET LOCAL idle_in_transaction_session_timeout = '3s'")
                    transaction_started = True

                    yield conn

                    body_done = monotonic()
                    DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)

                    commit_started = monotonic()
                    try:
                        conn.commit()
                    finally:
                        DB_COMMIT_SECONDS.observe(monotonic() - commit_started)
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
        DB_POOL_ACQUIRING.inc()
        try:
            conn = self.pool.getconn()
            outcome = "ok"
            DB_POOL_ACQUIRING.dec()
            self.sample_pool()
            DB_POOL_IN_USE.inc()
            try:
                with conn:
                    yield conn
            finally:
                return_started = monotonic()
                if conn is not None:
                    self.pool.putconn(conn)
                DB_POOL_RETURN_SECONDS.observe(monotonic() - return_started)
                if conn is not None:
                    DB_POOL_IN_USE.dec()
                self.sample_pool()
        finally:
            DB_POOL_SECONDS.labels(outcome).observe(monotonic() - start)
            if outcome == "error":
                # Keep acquisition telemetry consistent when checkout fails.
                DB_POOL_ACQUIRING.dec()
                self.sample_pool()
            else:
                # On success, return path already decremented DB_POOL_ACQUIRING.
                pass

    def sample_pool(self):
        stats = self.pool.get_stats()
        for name in ("pool_size", "pool_available", "requests_waiting", "pool_max"):
            DB_POOL_STATE.labels(name).set(stats.get(name, 0))

    def close(self):
        self.pool.close()
