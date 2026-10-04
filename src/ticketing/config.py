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
    order_status_cache_ms: int = int(os.getenv("ORDER_STATUS_CACHE_MS", "0"))
    hold_seconds: int = int(os.getenv("HOLD_SECONDS", "120"))
    pool_max: int = int(os.getenv("DB_POOL_MAX", "12"))
    pool_max_waiting: int | None = (
        int(os.environ["DB_POOL_MAX_WAITING"]) if "DB_POOL_MAX_WAITING" in os.environ else None
    )
    api_payment_pool_max: int = int(os.getenv("API_PAYMENT_POOL_MAX", "0"))
    pool_wait_ms: int = int(os.getenv("DB_POOL_WAIT_MS", "150"))
    seatmap_ttl_seconds: int = int(os.getenv("SEATMAP_TTL_SECONDS", "120"))
    reserve_concurrency: int = int(os.getenv("RESERVE_CONCURRENCY", "12"))
    reservation_mode: str = os.getenv("RESERVATION_MODE", "postgres")
    redis_reserve_concurrency: int = int(os.getenv("REDIS_RESERVE_CONCURRENCY", "128"))
    redis_reservation_replica_acks: int = int(os.getenv("REDIS_RESERVATION_REPLICA_ACKS", "0"))
    redis_reservation_wait_ms: int = int(os.getenv("REDIS_RESERVATION_WAIT_MS", "100"))
    redis_reservation_max_backlog: int = int(os.getenv("REDIS_RESERVATION_MAX_BACKLOG", "10000"))
    redis_reservation_max_command_age_seconds: int = int(
        os.getenv("REDIS_RESERVATION_MAX_COMMAND_AGE_SECONDS", "0")
    )
    reservation_writer_batch_size: int = int(os.getenv("RESERVATION_WRITER_BATCH_SIZE", "1"))
    publisher_batch_size: int = int(os.getenv("PUBLISHER_BATCH_SIZE", "32"))
    consumer_batch_size: int = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    consumer_batch_wait_ms: int = int(os.getenv("CONSUMER_BATCH_WAIT_MS", "10"))
    simulator_dispatch_mode: str = os.getenv("SIMULATOR_DISPATCH_MODE", "batch")
    simulator_concurrency: int = int(os.getenv("SIMULATOR_CONCURRENCY", "4"))
    refresh_cooldown_ms: int = int(os.getenv("REFRESH_COOLDOWN_MS", "250"))
    refresh_batch_size: int = int(os.getenv("REFRESH_BATCH_SIZE", "16"))
    expiry_batch_size: int = int(os.getenv("EXPIRY_BATCH_SIZE", "8"))
    worker_port: int = int(os.getenv("WORKER_METRICS_PORT", "9101"))
    # Target reconciliation period per active event. Must stay below the seatmap TTL
    # used for read-side keepalive, which is refreshed by successful reads and full rebuilds.
    # This is a scheduling target, not a hard freshness guarantee: see
    # ticketing_reconciliation_overdue_seconds.
    reconcile_interval_seconds: int = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "20"))
    # Events are proactively reconciled from this long before sale_starts until this long
    # after sale_ends. Only fields present in the events table are used.
    reconcile_window_seconds: int = int(os.getenv("RECONCILE_WINDOW_SECONDS", "300"))
    reconcile_batch_size: int = int(os.getenv("RECONCILE_BATCH_SIZE", "8"))
    reconcile_budget_ms: int = int(os.getenv("RECONCILE_BUDGET_MS", "500"))
    reconcile_lease_seconds: int = int(os.getenv("RECONCILE_LEASE_SECONDS", "30"))
    reconcile_backoff_ms: int = int(os.getenv("RECONCILE_BACKOFF_MS", "1000"))
    reconcile_seed_batch: int = int(os.getenv("RECONCILE_SEED_BATCH", "200"))

    @property
    def hold_admission_limit(self):
        if self.reservation_mode == "redis-first":
            return self.redis_reserve_concurrency
        return self.reserve_concurrency

    def validate(self):
        if self.environment != "development" and (
            self.jwt_secret.startswith("local-") or self.webhook_secret.startswith("local-")
        ):
            raise RuntimeError("Configure JWT_SECRET and WEBHOOK_SECRET outside development")
        if (
            self.hold_seconds < 1
            or self.pool_max < 1
            or self.reserve_concurrency < 1
            or self.seatmap_ttl_seconds < 1
        ):
            raise RuntimeError("Invalid positive configuration")
        if not 0 <= self.order_status_cache_ms <= 3000:
            raise RuntimeError("ORDER_STATUS_CACHE_MS must be between 0 and 3000")
        if self.pool_max_waiting is not None and not 1 <= self.pool_max_waiting <= 64:
            raise RuntimeError("DB_POOL_MAX_WAITING must be between 1 and 64")
        if not 50 <= self.pool_wait_ms <= 1000:
            raise RuntimeError("DB_POOL_WAIT_MS must be between 50 and 1000")
        if not 0 <= self.api_payment_pool_max < self.pool_max:
            raise RuntimeError("API_PAYMENT_POOL_MAX must be nonnegative and below DB_POOL_MAX")
        if self.api_payment_pool_max and (self.pool_max_waiting or self.pool_max) < 2:
            raise RuntimeError("API payment partition requires at least two total waiter slots")
        if self.reservation_mode not in {"postgres", "redis-first"}:
            raise RuntimeError("RESERVATION_MODE must be postgres or redis-first")
        if not 1 <= self.redis_reserve_concurrency <= 10000:
            raise RuntimeError("REDIS_RESERVE_CONCURRENCY must be between 1 and 10000")
        if not 0 <= self.redis_reservation_replica_acks <= 5:
            raise RuntimeError("REDIS_RESERVATION_REPLICA_ACKS must be between 0 and 5")
        if (
            self.reservation_mode == "redis-first"
            and self.environment != "development"
            and self.redis_reservation_replica_acks < 1
        ):
            raise RuntimeError("redis-first production mode requires a Redis replica acknowledgement")
        if not 10 <= self.redis_reservation_wait_ms <= 1000:
            raise RuntimeError("REDIS_RESERVATION_WAIT_MS must be between 10 and 1000")
        if not 100 <= self.redis_reservation_max_backlog <= 1000000:
            raise RuntimeError("REDIS_RESERVATION_MAX_BACKLOG must be between 100 and 1000000")
        if self.redis_reservation_max_command_age_seconds < 0:
            raise RuntimeError("REDIS_RESERVATION_MAX_COMMAND_AGE_SECONDS must be nonnegative")
        if (
            self.redis_reservation_max_command_age_seconds
            and self.redis_reservation_max_command_age_seconds > self.hold_seconds - 30
        ):
            raise RuntimeError(
                "REDIS_RESERVATION_MAX_COMMAND_AGE_SECONDS must leave 30 seconds before hold expiry"
            )
        if not 1 <= self.reservation_writer_batch_size <= 8:
            raise RuntimeError("RESERVATION_WRITER_BATCH_SIZE must be between 1 and 8")
        if not 1 <= self.publisher_batch_size <= 100:
            raise RuntimeError("PUBLISHER_BATCH_SIZE must be between 1 and 100")
        if not 1 <= self.consumer_batch_size <= 100:
            raise RuntimeError("CONSUMER_BATCH_SIZE must be between 1 and 100")
        if not 1 <= self.consumer_batch_wait_ms <= 100:
            raise RuntimeError("CONSUMER_BATCH_WAIT_MS must be between 1 and 100")
        if self.simulator_dispatch_mode not in {"batch", "refill"}:
            raise RuntimeError("SIMULATOR_DISPATCH_MODE must be batch or refill")
        if not 1 <= self.simulator_concurrency <= self.pool_max:
            raise RuntimeError("SIMULATOR_CONCURRENCY must fit DB_POOL_MAX")
        if not 1 <= self.refresh_cooldown_ms <= 5000:
            raise RuntimeError("REFRESH_COOLDOWN_MS must be between 1 and 5000")
        if not 1 <= self.refresh_batch_size <= 100:
            raise RuntimeError("REFRESH_BATCH_SIZE must be between 1 and 100")
        if not 1 <= self.expiry_batch_size <= 100:
            raise RuntimeError("EXPIRY_BATCH_SIZE must be between 1 and 100")
        if not 1 <= self.reconcile_interval_seconds < self.seatmap_ttl_seconds:
            raise RuntimeError("RECONCILE_INTERVAL_SECONDS must be below SEATMAP_TTL_SECONDS")
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
