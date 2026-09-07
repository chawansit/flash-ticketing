import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
from time import perf_counter as monotonic

from prometheus_client import Counter, Gauge, Histogram

REQUESTS = Counter("ticketing_http_requests_total", "HTTP requests", ["route", "method", "status"])
LATENCY = Histogram("ticketing_http_seconds", "HTTP latency", ["route"])
OUTCOMES = Counter("ticketing_outcomes_total", "Business outcomes", ["operation", "outcome"])
WORKER_ERRORS = Counter("ticketing_worker_errors_total", "Worker errors", ["role"])
OUTBOX_AGE = Gauge("ticketing_outbox_oldest_seconds", "Oldest unpublished event age")
REFRESH_PENDING = Gauge("ticketing_cache_refresh_pending", "Events awaiting cache refresh")
REFRESH_AGE = Gauge("ticketing_cache_refresh_oldest_seconds", "Oldest dirty cache request age")
DB_SECONDS = Histogram("ticketing_db_transaction_seconds", "Database transaction duration")
DB_ERRORS = Counter("ticketing_db_errors_total", "Database errors", ["type"])
REQUEST_ID = ContextVar("request_id", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        result = {
            "time": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        result.update(getattr(record, "fields", {}))
        if record.exc_info:
            result["exception"] = self.formatException(record.exc_info)
        return json.dumps(result, default=str)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


# Fixed labels only: no SQL text, identifiers or request payloads.
DB_QUERY_SECONDS = Histogram(
    "ticketing_db_query_seconds", "Client execute time including network/pooler", ["command"]
)
DB_POOL_SECONDS = Histogram(
    "ticketing_db_pool_acquire_seconds", "Pool acquisition including failures", ["outcome"]
)
DB_POOL_ACQUIRING = Gauge("ticketing_db_pool_acquiring", "Threads acquiring a connection")
DB_POOL_IN_USE = Gauge("ticketing_db_pool_in_use", "Connections checked out")
DB_POOL_STATE = Gauge("ticketing_db_pool_state", "Last sampled pool state", ["state"])
WORK_SECONDS = Counter(
    "ticketing_worker_busy_seconds_total", "Operation wall time including I/O", ["operation"]
)
WORK_ACTIVE = Gauge("ticketing_worker_active", "Concurrent worker operations", ["operation"])
WORK_CALLS = Counter("ticketing_worker_operations_total", "Completed worker calls", ["operation", "outcome"])
CACHE_ROWS = Counter("ticketing_cache_rows_total", "Rows sent to cache", ["mode"])
# Reconciliation scheduling. Labels are fixed vocabularies: never event or seat identifiers.
RECONCILE_BACKLOG = Gauge(
    "ticketing_reconciliation_backlog", "Active events due for reconciliation, counted up to a cap"
)
RECONCILE_OVERDUE = Gauge(
    "ticketing_reconciliation_overdue_seconds", "Age of the oldest reconciliation deadline already passed"
)
RECONCILE_TRACKED = Gauge(
    "ticketing_reconciliation_tracked_events", "Events currently held in the reconciliation schedule"
)
RECONCILE_EVENTS = Counter(
    "ticketing_reconciliation_events_total", "Scheduled reconciliations by outcome", ["outcome"]
)
RECONCILE_FAILURES = Counter(
    "ticketing_reconciliation_failures_total", "Reconciliation failures by stage", ["stage"]
)
RECONCILE_SECONDS = Histogram(
    "ticketing_reconciliation_seconds",
    "Per-event reconciliation wall time including Redis I/O",
    ["outcome"],
)
RECONCILE_RECOVERED = Counter(
    "ticketing_reconciliation_recovered_leases_total", "Expired reconciliation leases reclaimed"
)


def measured_work(operation):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start, outcome = monotonic(), "ok"
            WORK_ACTIVE.labels(operation).inc()
            try:
                return fn(*args, **kwargs)
            except Exception:
                outcome = "error"
                raise
            finally:
                WORK_ACTIVE.labels(operation).dec()
                WORK_SECONDS.labels(operation).inc(monotonic() - start)
                WORK_CALLS.labels(operation, outcome).inc()

        return wrapped

    return decorate
