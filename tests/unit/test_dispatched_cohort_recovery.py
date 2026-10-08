"""Recovery never changes failed throughput gates or accepts uncertain money/tickets."""
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import dispatched_cohort_recovery as recovery
import work_envelope as policy
from safety_diagnostic_abort_recovery import QUEUE_KEYS


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def cohort(tmp_path):
    key, binding = recovery.KEY, {"test": "bound"}
    entry = {"ledger": key, "profile": "shared_callback_rate_probe", "status": "RECOVERY_REQUIRED",
        "binding_sha256": policy.digest(binding), "reports": [{"pass": True}, {"run": recovery.RUN, "pass": False}]}
    state = {key: {"binding": binding, "paid_runs_started": 1, "paid_protocols_started": 1}}
    report = {"run": recovery.RUN, "experiment_decision": "ADR0219", "pass": False, "binding": binding,
        "arms": {"candidate": {"restoration_complete": True, "capacity_stages_started": 1}}}
    queues = {"pass": True, "kafka_members": 1, **dict.fromkeys(QUEUE_KEYS, 0)}
    saved = {**dict.fromkeys(("restore_pass", "primary_runtime_semantics_restored", "secondary_resources_removed",
        "generator_idle_after", "credential_snapshots_removed"), True), "primary_api_count": 4, "restored_global_queues": queues}
    counts = {**dict.fromkeys(("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"), 24331),
        **dict.fromkeys(("pending_orders", "expired_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
        "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"), 0),
        "pass": False, "expected": 25200, "expected_paid": 25200, "hold_deadlines_elapsed": True}
    fixture = {"fixture_identity": {"shows": 84, "show_ids": [str(i) for i in range(84)]}}
    fixture["fixture_identity_sha256"] = policy.digest(fixture["fixture_identity"])
    stage = {"customers_dispatched": True, "retired_shows": 84, "private_cleanup_pass": True,
        "diagnostic_credentials_removed": True, "cleanup_errors": [], "financial": counts, "fixture_identity": fixture,
        "customer": {"scheduled": 25200, "generator_drops": 869, "retry_attempts": 0, "outcomes": {"fulfilled": 24331},
        **dict.fromkeys(("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets"), 24331)}}
    owned = tmp_path / "tmp" / recovery.RUN; arm = owned / recovery.ARM
    write(owned / "comparison-summary.json", report); write(arm / "state.json", saved)
    write(arm / "candidate/stage.private.json", stage); write(arm / "candidate/fixture-identity.json", fixture)
    financial = {"pass": True, "checks": {"post_ttl_complete": True, "payments_durable": True, "ticket_relationships_valid": True},
        "counts": {**counts, "pass": True, "expected": 24331, "expected_paid": 24331}, "fixture_hold_rows": 24331,
        "relationships": {"unique_issued_tickets": 24331, "holds_not_past_ttl": 0, **dict.fromkeys(("hold_relationship_errors",
        "booking_relationship_errors", "order_item_relationship_errors", "inventory_relationship_errors", "fulfilled_ticket_relationship_errors"), 0)}}
    fresh = {"ledger": key, "binding_sha256": entry["binding_sha256"], "original_result_sha256": policy.digest(report),
        "fixture_identity_sha256": fixture["fixture_identity_sha256"], "verified_at_utc": datetime.now(UTC).isoformat(),
        "new_paid_runs_started": 0, "actual_elapsed_seconds": 120, "fixture_financial_audit": financial, "queue_counts": queues,
        **dict.fromkeys(("pass", "runtime_unchanged", "original_runtime_restored", "zero_double_booking", "all_queues_zero",
        "kafka_drained", "generator_idle", "original_failed_gate_preserved"), True)}
    fresh_path = tmp_path / "tmp/recovery.json"; write(fresh_path, fresh)
    return tmp_path, entry, state, report, fresh_path, fresh


def test_original_failure_preserved(cohort):
    root, entry, state, report, path, _fresh = cohort
    before = copy.deepcopy((entry, report))
    receipt = recovery.verify(root, entry, state, path)
    assert (entry, report) == before and not receipt["capacity_qualified"] and not receipt["original_experiment_pass"]
    assert receipt["dispatched_paid_tickets"] == 24331 and receipt["undispatched"] == 869
    assert len(receipt["retained_artifact_sha256"]) == 5


@pytest.mark.parametrize("drift", ["stale", "future", "tickets", "relationship", "ttl", "queue", "restore", "binding", "dispatch", "planned", "missing", "accounting", "duplicate"])
def test_uncertainty_blocks(cohort, drift):
    root, entry, state, report, path, fresh = cohort
    if drift == "stale": fresh["verified_at_utc"] = (datetime.now(UTC) - timedelta(seconds=121)).isoformat()
    elif drift == "future": fresh["verified_at_utc"] = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
    elif drift == "tickets": fresh["fixture_financial_audit"]["counts"]["tickets"] -= 1
    elif drift == "relationship": fresh["fixture_financial_audit"]["relationships"]["booking_relationship_errors"] = 1
    elif drift == "ttl": fresh["fixture_financial_audit"]["relationships"]["holds_not_past_ttl"] = 1
    elif drift == "queue": fresh["queue_counts"]["pending_callback_deliveries"] = 1
    elif drift == "restore": fresh["runtime_unchanged"] = False
    elif drift == "binding": fresh["binding_sha256"] = "changed"
    elif drift == "dispatch": fresh["new_paid_runs_started"] = 1
    elif drift == "accounting": fresh["actual_elapsed_seconds"] = -1
    elif drift == "duplicate": fresh["fixture_financial_audit"]["counts"]["duplicate_booked_seats"] = 1
    elif drift == "planned":
        report["pass"] = True; write(root / "tmp" / recovery.RUN / "comparison-summary.json", report)
    else: fresh["fixture_financial_audit"]["relationships"].pop("hold_relationship_errors")
    write(path, fresh)
    with pytest.raises(ValueError): recovery.verify(root, entry, state, path)


def test_tamper_blocks_resolution(cohort):
    root, entry, state, _report, path, _fresh = cohort
    receipt = recovery.verify(root, entry, state, path)
    target = root / "docs/capacity/flash-sale-opening/recovery.json"; write(target, receipt)
    import hashlib
    records = {"verified_paid_recoveries": {recovery.KEY: {"entry_sha256": policy.digest(entry),
        "receipt_path": target.relative_to(root).as_posix(), "receipt_sha256": hashlib.sha256(target.read_bytes()).hexdigest()}}}
    assert recovery.resolved(records, entry, root)
    path.write_text("{}")
    assert not recovery.resolved(records, entry, root)
