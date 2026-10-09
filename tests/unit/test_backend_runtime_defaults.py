"""Reviewable defaults cannot silently turn on optional financial behavior."""
import json
import os
import subprocess
import sys
from dataclasses import replace

import pytest

from ticketing.config import Settings


def test_clean_environment_keeps_optional_features_off_and_budgets_bounded():
    # Settings reads the environment at import; use a fresh process to check real defaults.
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "PYTHONPATH", "SYSTEMROOT", "WINDIR", "HOME", "TMP", "TEMP"}}
    fields = {"reservation_mode": "postgres", "hold_seconds": 120, "pool_max": 12,
              "api_payment_pool_max": 0, "api_pool_shared_waiting": False,
              "api_partial_timeout_reclaim": False, "payment_confirmation_async": False,
              "order_status_cache_ms": 0, "order_status_event_refresh": False,
              "order_status_event_refresh_dedup": False, "order_status_poll_ms": 0,
              "reservation_write_pipeline": False, "reservation_writer_batch_size": 1,
              "simulator_dispatch_mode": "batch", "simulator_concurrency": 4}
    code = ("import json;from ticketing.config import Settings;s=Settings();s.validate();"
            + "print(json.dumps({k:getattr(s,k) for k in " + repr(list(fields)) + "}))")
    actual = subprocess.check_output([sys.executable, "-c", code], env=env, text=True)
    assert json.loads(actual) == fields


@pytest.mark.parametrize("overrides", [
    {"api_partial_timeout_reclaim": True, "api_pool_shared_waiting": False},
    {"order_status_event_refresh": True, "order_status_cache_ms": 0},
    {"order_status_event_refresh_dedup": True, "order_status_event_refresh": False},
    {"api_payment_pool_max": 12, "pool_max": 12},
    {"confirmation_concurrency": 3, "pool_max": 2, "payment_confirmation_async": True},
    {"redis_reservation_max_command_age_seconds": 100, "hold_seconds": 120},
    {"reservation_mode": "redis-first", "environment": "production",
     "jwt_secret": "test-jwt", "webhook_secret": "test-hook", "redis_reservation_replica_acks": 0},
])
def test_invalid_feature_combinations_fail_before_serving(overrides):
    with pytest.raises(RuntimeError):
        replace(Settings(), **overrides).validate()
