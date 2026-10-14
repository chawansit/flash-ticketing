"""Isolate archived fake cloud receipts from the operator's active goal."""
import copy

import pytest

HISTORICAL_MODULES = {"test_cce_api_adapter", "test_cce_worker_rebalance", "test_cce_ecs_transition",
                      "test_cce_paid_observers", "test_cce_transaction_profile", "test_observe_cce_paid_pipeline",
                      "test_cce_paid_lifecycle", "test_cce_paid_stage", "test_cce_hourly_integration",
                      "test_cce_hourly_observer_bootstrap", "test_cce_native_database_bridge",
                      "test_cce_complete_verification", "test_cce_metric_admission",
                      "test_paid_metrics_failures"}


@pytest.fixture(autouse=True)
def isolate_historical_cloud_scope(request, monkeypatch):
    if request.module.__name__ in HISTORICAL_MODULES:
        import work_envelope as policy
        original, live_path = policy.envelope, policy.ENVELOPE
        def isolated():
            value = copy.deepcopy(original())
            if policy.ENVELOPE == live_path:
                goal = value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
                authority = {"decision", "profile", "maximum_paid_stages", "maximum_pods",
                             "pod_vcpu", "pod_memory_gib", "offered_journeys_per_second",
                             "offered_seconds", "maximum_experiment_seconds", "acquisition_budget",
                             "unlimited_cumulative_time_explicitly_authorized", "spending_allowance",
                             "ledger_start_index", "authorization", "authorized_at_utc",
                             "authorized_at_bangkok"}
                for key in set(goal) - authority:
                    goal.pop(key)
            return value
        monkeypatch.setattr(policy, "envelope", isolated)
