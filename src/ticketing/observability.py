import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime

from prometheus_client import Counter, Gauge, Histogram

REQUESTS = Counter("ticketing_http_requests_total", "HTTP requests", ["route", "method", "status"])
LATENCY = Histogram("ticketing_http_seconds", "HTTP latency", ["route"])
OUTCOMES = Counter("ticketing_outcomes_total", "Business outcomes", ["operation", "outcome"])
WORKER_ERRORS = Counter("ticketing_worker_errors_total", "Worker errors", ["role"])
OUTBOX_AGE = Gauge("ticketing_outbox_oldest_seconds", "Oldest unpublished event age")
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
