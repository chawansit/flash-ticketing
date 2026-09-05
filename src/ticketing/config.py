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
    worker_port: int = int(os.getenv("WORKER_METRICS_PORT", "9101"))

    def validate(self):
        if self.environment != "development" and (
            self.jwt_secret.startswith("local-") or self.webhook_secret.startswith("local-")
        ):
            raise RuntimeError("Configure JWT_SECRET and WEBHOOK_SECRET outside development")
        if self.hold_seconds < 1 or self.pool_max < 1 or self.reserve_concurrency < 1:
            raise RuntimeError("Invalid positive configuration")
