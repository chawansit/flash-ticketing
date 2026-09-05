from contextlib import contextmanager
from time import monotonic

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ticketing.observability import DB_ERRORS, DB_SECONDS


class Postgres:
    def __init__(self, url: str, maximum: int = 12):
        self.pool = ConnectionPool(
            url,
            open=True,
            min_size=1,
            max_size=maximum,
            timeout=0.15,
            max_waiting=maximum,
            kwargs={"row_factory": dict_row, "prepare_threshold": None},
        )

    @contextmanager
    def transaction(self):
        try:
            with self.pool.connection() as conn:
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

    def close(self):
        self.pool.close()
