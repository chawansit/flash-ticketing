"""ADR0182 rejects ambiguous dispatch, financial loss, cleanup drift and stale probes."""
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import pre_dispatch_abort_recovery as recovery
import work_envelope as envelope


@pytest.fixture
def facts():
    report = {"pass": False, "capacity_stages_started": 0, "arms": {"control": {"restoration_complete": True}}}
    financial = dict.fromkeys(("expected", "expected_paid", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"), 1)
    financial.update(dict.fromkeys(("pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries", "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"), 0))
    financial.update({"pass": True, "hold_deadlines_elapsed": True})
    safety = dict.fromkeys(("pass", "one_durable_owner_before_payment", "cross_host_hold_replay", "cross_host_payment_replay", "other_actor_denied", "customer_ticket_confirmed"), True)
    safety.update(wave_requests=100, accepted_holds=1)
    queues = dict.fromkeys(recovery.QUEUE_KEYS, 0); queues["pass"] = True
    state = dict.fromkeys(("restore_pass", "primary_runtime_semantics_restored", "secondary_resources_removed", "generator_idle_after", "credential_snapshots_removed"), True)
    state.update(capacity_stages_started=0, primary_api_count=4, safety=safety, post_ttl_financial=financial,
                 safety_confirmation_receipts={"pass": True}, restored_global_queues=queues)
    stage = {"customers_dispatched": False, "diagnostic_credentials_removed": True, "private_cleanup_pass": True, "cleanup_errors": []}
    return report, state, stage, "ValueError: Exact qualified inventory receipt required"


def test_only_known_receipt_abort_with_complete_safety_can_recover(facts):
    assert recovery.retained_pass(*facts)


@pytest.mark.parametrize("failure", ["dispatch", "reserved", "bool_counter", "customer_result", "different_error", "lost_payment", "double_booking", "ttl", "authorization", "hold_owners", "cleanup", "queue", "restore"])
def test_uncertain_or_failed_correctness_cannot_be_reclassified(facts, failure):
    report, state, stage, error = copy.deepcopy(facts)
    if failure == "dispatch": stage["customers_dispatched"] = True
    elif failure == "reserved": state["capacity_stages_started"] = 1
    elif failure == "bool_counter": state["post_ttl_financial"]["tickets"] = True
    elif failure == "customer_result": stage["customer"] = {}
    elif failure == "different_error": error = "ValueError: other failure"
    elif failure == "lost_payment": state["post_ttl_financial"]["tickets"] = 0
    elif failure == "double_booking": state["post_ttl_financial"]["duplicate_booked_seats"] = 1
    elif failure == "ttl": state["post_ttl_financial"]["hold_deadlines_elapsed"] = False
    elif failure == "authorization": state["safety"]["other_actor_denied"] = False
    elif failure == "hold_owners": state["safety"]["accepted_holds"] = 2
    elif failure == "cleanup": stage["diagnostic_credentials_removed"] = False
    elif failure == "queue": state["restored_global_queues"]["pending_refunds"] = 1
    else: state["restore_pass"] = False
    with pytest.raises(ValueError):
        recovery.retained_pass(report, state, stage, error)


def test_receipt_and_original_entry_tampering_keep_recovery_blocked(tmp_path):
    entry = {"ledger": "bounded_diagnostic_placement__" + "a" * 12, "binding_sha256": "b" * 64, "status": "RECOVERY_REQUIRED"}
    path = tmp_path / "docs/capacity/flash-sale-opening/recovery.json"; path.parent.mkdir(parents=True)
    receipt = {"decision": "ADR0182", "ledger": entry["ledger"], "binding_sha256": entry["binding_sha256"], "gates": dict.fromkeys(recovery.GATES, True)}
    path.write_text(json.dumps(receipt))
    import hashlib
    records = {"verified_aborts": {entry["ledger"]: {"entry_sha256": envelope.digest(entry), "receipt_path": path.relative_to(tmp_path).as_posix(), "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}}
    assert recovery.resolved(records, entry, tmp_path)
    receipt["gates"]["fresh_credentials_absent"] = False; path.write_text(json.dumps(receipt))
    assert not recovery.resolved(records, entry, tmp_path)
    assert not recovery.resolved(records, dict(entry, status="ACTIVE"), tmp_path)


@pytest.mark.parametrize("failure", [None, "stale", "binding", "queue", "generator", "credential"])
def test_full_retained_artifacts_and_fresh_proof_are_required(tmp_path, facts, failure):
    binding = {"configuration_sha256": "a" * 64}
    entry = {"ledger": "bounded_diagnostic_placement__" + "b" * 12, "binding_sha256": envelope.digest(binding),
             "profile": "diagnostic_placement", "status": "RECOVERY_REQUIRED",
             "reports": [{"run": "adr0151-" + "c" * 12, "pass": True}, {"run": "adr0151-" + "d" * 12, "pass": False}]}
    dry = tmp_path / "tmp" / entry["reports"][0]["run"]; dry.mkdir(parents=True)
    (dry / "comparison-summary.json").write_text(json.dumps({"pass": True, "binding": binding, "arms": {k: {"restoration_complete": True} for k in ("control", "candidate")}}))
    parent = tmp_path / "tmp" / entry["reports"][1]["run"]
    directory = parent / ("adr0151-arm-" + "e" * 12); (directory / "control").mkdir(parents=True)
    report, state, stage, error = facts
    report.update(run=entry["reports"][1]["run"], binding=binding)
    report["arms"]["control"]["evidence_directory"] = directory.relative_to(tmp_path).as_posix()
    for path, value in ((parent / "comparison-summary.json", report), (directory / "state.json", state), (directory / "control/stage.private.json", stage)):
        path.write_text(json.dumps(value))
    (directory / "control/failure.private.log").write_text(error)
    now = datetime.now(UTC)
    fresh = {"ledger": entry["ledger"], "binding_sha256": entry["binding_sha256"], "captured_at_utc": now.isoformat(),
             "queues": state["restored_global_queues"], **dict.fromkeys(("runtime_unchanged", "primary_four", "secondary_empty", "generator_idle", "credentials_absent"), True)}
    if failure == "stale": fresh["captured_at_utc"] = (now - timedelta(seconds=121)).isoformat()
    elif failure == "binding": fresh["binding_sha256"] = "f" * 64
    elif failure == "queue": fresh["queues"]["pending_confirmation_receipts"] = 1
    elif failure == "generator": fresh["generator_idle"] = False
    elif failure == "credential": fresh["credentials_absent"] = False
    if failure:
        with pytest.raises(ValueError): recovery.verify_artifacts(tmp_path, entry, fresh, now=now)
    else:
        receipt = recovery.verify_artifacts(tmp_path, entry, fresh, now=now)
        assert receipt["gates"] == dict.fromkeys(recovery.GATES, True)
        assert len(receipt["retained_artifact_sha256"]) == 5


@pytest.mark.parametrize("status,error,expected", [(1,"Error: No such object: ",True),
    (1,"error: no such object: ",True),(1,"Error response from daemon: No such container: ",True),
    (0,"Error: No such object: ",False),(True,"Error: No such object: ",False),
    (1,"permission denied: ",False),(1,"Cannot connect to daemon: ",False)])
def test_missing_container_requires_exact_identity_and_accepts_documented_case_variants(status,error,expected):
    cid="a"*64
    assert recovery.missing_container_observation(status,error+cid,cid) is expected
    assert not recovery.missing_container_observation(status,error+cid,"b"*64)
