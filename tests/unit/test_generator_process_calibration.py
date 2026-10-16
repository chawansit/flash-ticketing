import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from calibrate_generator_processes import allocation, prepare_runtime, verdict, verify_runtime


@pytest.mark.parametrize("shards", [2, 4])
def test_allocations_are_disjoint_and_sufficient(shards):
    parts = allocation("http://127.0.0.1:4567", 160, 45, shards)
    shows = [s for part in parts for s in part["show_ids"]]
    viewers = [v for part in parts for v in part["viewer_tokens"]]
    assert len(shows) == len(set(shows))
    assert len(viewers) == len(set(viewers)) == 7200
    assert all(len(p["show_ids"]) * 300 >= 7200 // shards for p in parts)


@pytest.mark.parametrize(
    "origin,rate,shards",
    [
        ("https://example.com", 160, 4),
        ("http://10.2.39.55", 160, 4),
        ("http://127.0.0.1:4567", 300, 4),
        ("http://127.0.0.1:4567", 160, 8),
    ],
)
def test_rejects_real_origins_or_unapproved_profiles(origin, rate, shards):
    with pytest.raises(ValueError):
        allocation(origin, rate, 45, shards)


def evidence():
    row = {
        k: 2
        for k in (
            "scheduled",
            "dispatched",
            "completed",
            "fulfilled_by_deadline",
            "distinct_orders",
            "distinct_tickets",
        )
    }
    row.update(generator_drops=0, pass_=True, dispatch_lag_p95_ms=5, dispatch_lag_max_ms=10)
    row["pass"] = row.pop("pass_")
    stats = dict(
        protocol_errors=0,
        duplicate_seat_attempts=0,
        loop_lag_p95_ms=1,
        handler_p95_ms=31,
        synthetic_orders=4,
        unique_assigned_seats=4,
    )
    return [row.copy(), row.copy()], stats


def test_missing_or_late_or_invalid_responder_cannot_pass():
    rows, stats = evidence()
    assert verdict(rows, [0, 0], 4, stats)["pass"]
    assert not verdict(rows, [0, 1], 4, stats)["pass"]
    assert not verdict(rows[:1], [0], 4, stats)["pass"]
    rows[0]["dispatch_lag_max_ms"] = 501
    assert not verdict(rows, [0, 0], 4, stats)["pass"]
    rows, stats = evidence()
    stats["protocol_errors"] = 1
    assert not verdict(rows, [0, 0], 4, stats)["pass"]


def test_runtime_binds_the_sealed_paid_leaf_and_rejects_drift(tmp_path):
    directory = tmp_path / "runtime"
    receipt = prepare_runtime(directory)
    assert verify_runtime(directory) == receipt
    assert receipt["source_sha256"]["paid_ticket_load_generator.py"] == (
        "0b2368445965ab9c5ac36171e3d336ecedffcd15dc6d38a3e66150a3e9c28ea6"
    )
    (directory / "paid_ticket_load_generator.py").write_text("drift", encoding="utf-8")
    with pytest.raises(ValueError, match="Exact prepared"):
        verify_runtime(directory)


def test_empty_responder_is_a_failed_control_not_an_exception():
    rows, stats = evidence()
    stats["handler_p95_ms"] = None
    assert not verdict([{}, {}], [1, 1], 4, stats)["pass"]
