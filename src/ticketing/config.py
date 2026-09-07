import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://ticketing:ticketing@localhost:5432/ticketing")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    jwt_secret: str = os.getenv("JWT_SECRET", "local-development-secret-change-me")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "local-webhook-secret-change-me")
    environment: str = os.getenv("ENVIRONMENT", "development")
    hold_seconds: int = int(os.getenv("HOLD_SECONDS", "120"))
    pool_max: int = int(os.getenv("DB_POOL_MAX", "12"))
    reserve_concurrency: int = int(os.getenv("RESERVE_CONCURRENCY", "8"))
    publisher_batch_size: int = int(os.getenv("PUBLISHER_BATCH_SIZE", "32"))
    simulator_concurrency: int = int(os.getenv("SIMULATOR_CONCURRENCY", "4"))
    refresh_cooldown_ms: int = int(os.getenv("REFRESH_COOLDOWN_MS", "250"))
    worker_port: int = int(os.getenv("WORKER_METRICS_PORT", "9101"))
    # Target reconciliation period per active event. Must stay below the 30-second Redis
    # seat-map TTL, which only a full snapshot extends. This is a scheduling target, not a
    # freshness guarantee: see ticketing_reconciliation_overdue_seconds.
    reconcile_interval_seconds: int = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "20"))
    # Events are proactively reconciled from this long before sale_starts until this long
    # after sale_ends. Only fields present in the events table are used.
    reconcile_window_seconds: int = int(os.getenv("RECONCILE_WINDOW_SECONDS", "300"))
    reconcile_batch_size: int = int(os.getenv("RECONCILE_BATCH_SIZE", "8"))
    reconcile_budget_ms: int = int(os.getenv("RECONCILE_BUDGET_MS", "500"))
    reconcile_lease_seconds: int = int(os.getenv("RECONCILE_LEASE_SECONDS", "30"))
    reconcile_backoff_ms: int = int(os.getenv("RECONCILE_BACKOFF_MS", "1000"))
    reconcile_seed_batch: int = int(os.getenv("RECONCILE_SEED_BATCH", "200"))

    def validate(self):
        if self.environment != "development" and (
            self.jwt_secret.startswith("local-") or self.webhook_secret.startswith("local-")
        ):
            raise RuntimeError("Configure JWT_SECRET and WEBHOOK_SECRET outside development")
        if self.hold_seconds < 1 or self.pool_max < 1 or self.reserve_concurrency < 1:
            raise RuntimeError("Invalid positive configuration")
        if not 1 <= self.publisher_batch_size <= 100:
            raise RuntimeError("PUBLISHER_BATCH_SIZE must be between 1 and 100")
        if not 1 <= self.simulator_concurrency <= self.pool_max:
            raise RuntimeError("SIMULATOR_CONCURRENCY must fit DB_POOL_MAX")
        if not 1 <= self.refresh_cooldown_ms <= 5000:
            raise RuntimeError("REFRESH_COOLDOWN_MS must be between 1 and 5000")
        if not 1 <= self.reconcile_interval_seconds < 30:
            raise RuntimeError("RECONCILE_INTERVAL_SECONDS must be below the 30-second cache TTL")
        if not 0 <= self.reconcile_window_seconds <= 86400:
            raise RuntimeError("RECONCILE_WINDOW_SECONDS must be between 0 and 86400")
        if not 1 <= self.reconcile_batch_size <= 100:
            raise RuntimeError("RECONCILE_BATCH_SIZE must be between 1 and 100")
        if not 50 <= self.reconcile_budget_ms <= 5000:
            raise RuntimeError("RECONCILE_BUDGET_MS must be between 50 and 5000")
        if not 5 <= self.reconcile_lease_seconds <= 300:
            raise RuntimeError("RECONCILE_LEASE_SECONDS must be between 5 and 300")
        if not 100 <= self.reconcile_backoff_ms <= 60000:
            raise RuntimeError("RECONCILE_BACKOFF_MS must be between 100 and 60000")
        if not 1 <= self.reconcile_seed_batch <= 5000:
            raise RuntimeError("RECONCILE_SEED_BATCH must be between 1 and 5000")
