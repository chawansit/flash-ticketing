from scripts.paid_ticket_sharded_generator import aggregate, split_manifest


def fixture():
    return {
        "schema_version": 1,
        "environment": "development",
        "show_ids": [f"show-{i}" for i in range(12)],
        "viewer_tokens": [f"private-token-{i}" for i in range(10)],
        "seat_offset": 0,
        "seats_per_show": 300,
    }


def shard_result():
    return {
        "scheduled": 1800, "dispatched": 1800, "generator_drops": 0,
        "completed": 1800, "fulfilled": 1800, "fulfilled_by_deadline": 1800,
        "distinct_orders": 1800, "distinct_tickets": 1800, "retry_attempts": 0,
        "hold_http_p95_ms": 20, "transport_phase_p95_ms": {
            "holds": {"pre_send_ms": 5, "response_wait_ms": 8, "connect_ms": 2},
        },
        "transport_phase_samples": {"holds": {"pre_send_ms": 1800}},
        "outcomes": {"fulfilled": 1800}, "physical_http_attempts": {"holds": 1800},
        "pass": True,
    }


def test_two_shards_have_disjoint_inventory_and_viewers():
    parts = split_manifest(fixture(), 2, 1800)
    assert len(parts) == 2
    assert set(parts[0]["show_ids"]).isdisjoint(parts[1]["show_ids"])
    assert set(parts[0]["viewer_tokens"]).isdisjoint(parts[1]["viewer_tokens"])
    assert len(parts[0]["show_ids"]) == len(parts[1]["show_ids"]) == 6
    assert all(len(part["show_ids"]) * part["seats_per_show"] == 1800 for part in parts)


def test_shard_rejects_insufficient_seats():
    try:
        split_manifest(fixture(), 2, 1801)
    except ValueError as exc:
        assert "insufficient distinct seats" in str(exc)
    else:
        raise AssertionError("Expected per-shard inventory check")


def test_aggregate_uses_worse_shard_p95_and_requires_both_pass():
    first = shard_result()
    second = shard_result()
    second["hold_http_p95_ms"] = 40
    second["transport_phase_p95_ms"]["holds"]["pre_send_ms"] = 12
    result = aggregate([first, second], 60, 60, 500, [0, 0])
    assert result["pass"]
    assert result["dispatched"] == result["fulfilled_by_deadline"] == 3600
    assert result["hold_http_p95_ms"] == 40
    assert result["transport_phase_p95_ms"]["holds"]["pre_send_ms"] == 12
    assert result["transport_phase_samples"]["holds"]["pre_send_ms"] == 3600
    assert result["latency_summary_mode"] == "worst_shard_p95_not_combined_percentile"
    second["generator_drops"] = 1
    second["pass"] = False
    assert not aggregate([first, second], 60, 60, 500, [0, 1])["pass"]


def test_aggregate_keeps_new_status_get_transport_routes():
    first = shard_result()
    second = shard_result()
    for row in (first, second):
        row["transport_phase_p95_ms"]["orders"] = {"pre_send_ms": 4000, "response_wait_ms": 150}
        row["transport_phase_samples"]["orders"] = {"pre_send_ms": 500, "response_wait_ms": 500}
    result = aggregate([first, second], rate=60, seconds=60, concurrency=500, exit_codes=[1, 1])
    assert result["transport_phase_p95_ms"]["orders"]["pre_send_ms"] == 4000
    assert result["transport_phase_samples"]["orders"]["pre_send_ms"] == 1000
