"""The batch comparison changes only the four native writers' bounded setting."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_native_workers as workers
import cce_recovery_headroom as h
from test_cce_recovery_headroom import goal as parent


def goal():
    return {**parent(), "payment_slot_arm": "control", "maximum_pods": 19,
            "headroom_slot_correction_decision": "ADR0314",
            "headroom_slot_failure_sha256": h.FAILURE_SHA256,
            "reservation_writer_scale_decision": "ADR0315",
            "reservation_writer_failure_sha256": h.SLOT_SHA256,
            "writer_batch_decision": "ADR0316", "writer_batch_size": 8,
            "writer_batch_failure_sha256": h.WRITER_SHA256,
            "baseline_sha256": h.WRITER_SHA256,
            "committed_status_arm": "candidate"}


def test_exact_contract_and_legacy_runtime_view_preserve_actual_scope():
    actual = goal()
    assert h.writer_batch_selected(actual)
    assert h.writer_batch_selected(h.runtime_view(actual))
    assert actual["offered_journeys_per_second"] == 102
    assert h.writer_batch_contract()["physical_connections"] == 24
    old = {k:v for k,v in actual.items() if k not in h.BATCH_KEYS}
    old["baseline_sha256"] = h.SLOT_SHA256
    assert h.selected(old) and not h.writer_batch_selected(old)


@pytest.mark.parametrize("key,value", [
    ("writer_batch_size", 4), ("writer_batch_size", 16), ("writer_batch_size", True),
    ("writer_batch_decision", "ADR0315"), ("writer_batch_failure_sha256", "0"*64),
    ("maximum_pods", 20), ("baseline_sha256", h.SLOT_SHA256)])
def test_partial_or_expanded_scope_is_rejected(key, value):
    with pytest.raises(ValueError):
        h.writer_batch_selected({**goal(), key:value})


@pytest.mark.parametrize("role", ["reservation-writer", "consumer", "projection-consumer",
                                 "publisher", "maintenance", "reconciler", "simulator"])
def test_only_writer_setting_changes(monkeypatch, role):
    import cce_committed_status_profile as committed
    import cce_transaction_profile as transaction
    actual = goal()
    old = {k:v for k,v in actual.items() if k not in h.BATCH_KEYS}
    old["baseline_sha256"] = h.SLOT_SHA256
    values = {"DATABASE_URL": "postgresql://test:test@pgbouncer:5432/ticketing",
              "ORDER_STATUS_COMMITTED_SNAPSHOT": "0", "RESERVATION_WRITER_BATCH_SIZE": "4"}
    if role == "simulator":
        values["API_URL"] = "http://load-balancer:8000"
    monkeypatch.setattr(committed, "selected", lambda value: True)
    monkeypatch.setattr(transaction, "active", lambda: h.runtime_view(old))
    before = workers.environment(role, values, "10.1.2.3", "ADR0315")
    monkeypatch.setattr(transaction, "active", lambda: h.runtime_view(actual))
    after = workers.environment(role, values, "10.1.2.3", "ADR0315")
    assert {k for k in before if before[k] != after[k]} == (
        {"RESERVATION_WRITER_BATCH_SIZE"} if role == "reservation-writer" else set())
    assert values["RESERVATION_WRITER_BATCH_SIZE"] == "4"
    if role == "reservation-writer":
        assert after["RESERVATION_WRITER_BATCH_SIZE"] == "8"


def test_wrong_original_batch_is_rejected(monkeypatch):
    import cce_committed_status_profile as committed
    import cce_transaction_profile as transaction
    monkeypatch.setattr(committed, "selected", lambda value: True)
    monkeypatch.setattr(transaction, "active", lambda: h.runtime_view(goal()))
    with pytest.raises(ValueError):
        workers.environment("reservation-writer",
            {"DATABASE_URL": "postgresql://test:test@pgbouncer:5432/ticketing",
             "ORDER_STATUS_COMMITTED_SNAPSHOT": "0", "RESERVATION_WRITER_BATCH_SIZE": "8"},
             "10.1.2.3", "ADR0315")
