"""Isolate archived fake cloud receipts from the operator's active goal."""
import copy

import pytest

HISTORICAL_MODULES = {"test_cce_api_adapter", "test_cce_ecs_transition",
                      "test_cce_paid_observers", "test_cce_transaction_profile", "test_observe_cce_paid_pipeline",
                      "test_cce_paid_lifecycle", "test_cce_hourly_integration",
                      "test_cce_hourly_observer_bootstrap"}


@pytest.fixture(autouse=True)
def isolate_historical_cloud_scope(request, monkeypatch):
    if request.module.__name__ in HISTORICAL_MODULES:
        import work_envelope as policy
        original, live_path = policy.envelope, policy.ENVELOPE
        def isolated():
            value = copy.deepcopy(original())
            if policy.ENVELOPE == live_path:
                goal = value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
                for key in ("extension_decision", "hourly_qualification_decision", "hourly_adapters_sha256"):
                    goal.pop(key, None)
            return value
        monkeypatch.setattr(policy, "envelope", isolated)
