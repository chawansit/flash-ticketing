from contextlib import contextmanager
from time import perf_counter as monotonic

from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ticketing.observability import (
    DB_ERRORS,
    DB_POOL_ACQUIRING,
    DB_POOL_IN_USE,
    DB_POOL_SECONDS,
    DB_POOL_STATE,
    DB_QUERY_SECONDS,
    DB_SECONDS,
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
        try:
            with self.connection() as conn:
                started = monotonic()
                try:
                    with conn.transaction():
                        conn.execute("SET LOCAL lock_timeout = '75ms'")
                        conn.execute("SET LOCAL statement_timeout = '1500ms'")
                        conn.execute("SET LOCAL idle_in_transaction_session_timeout = '3s'")
                        yield conn
                finally:
                    DB_SECONDS.observe(monotonic() - started)
        except Exception as exc:
            state = getattr(exc, "sqlstate", None)
            if state or type(exc).__name__ in {"PoolTimeout", "TooManyRequests"}:
                DB_ERRORS.labels(state or type(exc).__name__).inc()
            raise

    @contextmanager
    def connection(self):
        start, outcome = monotonic(), "error"
        DB_POOL_ACQUIRING.inc()
        try:
            conn = self.pool.getconn()
            outcome = "ok"
        finally:
            DB_POOL_SECONDS.labels(outcome).observe(monotonic() - start)
            DB_POOL_ACQUIRING.dec()
            self.sample_pool()
        DB_POOL_IN_USE.inc()
        try:
            with conn:
                yield conn
        finally:
            self.pool.putconn(conn)
            DB_POOL_IN_USE.dec()
            self.sample_pool()

    def sample_pool(self):
        stats = self.pool.get_stats()
        for name in ("pool_size", "pool_available", "requests_waiting", "pool_max"):
            DB_POOL_STATE.labels(name).set(stats.get(name, 0))

    def close(self):
        self.pool.close()
