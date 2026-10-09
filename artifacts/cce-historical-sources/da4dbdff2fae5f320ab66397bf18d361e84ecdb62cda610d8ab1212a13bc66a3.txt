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


def test_aggregate_preserves_release_phases_and_separate_shard_pressure():
    first, second = shard_result(), shard_result()
    for row, value in [(first, 10), (second, 20)]:
        row["transport_phase_p95_ms"]["holds"]["stream_close_ms"] = value
        row["transport_phase_samples"]["holds"]["stream_close_ms"] = 1800
        row["active_journeys_peak"] = value
        row["lifecycle"] = {"loop_lag_p95_ms": value}
    result = aggregate([first, second], 60, 60, 500, [0, 0])
    assert result["transport_phase_p95_ms"]["holds"]["stream_close_ms"] == 20
    assert result["transport_phase_samples"]["holds"]["stream_close_ms"] == 3600
    assert [row["active_journeys_peak"] for row in result["shards"]] == [10, 20]
    assert result["shards"][1]["lifecycle"]["loop_lag_p95_ms"] == 20


def test_run_preserves_total_budgets_diagnostics_and_private_cleanup(monkeypatch):
    import asyncio
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from scripts import paid_ticket_sharded_generator as generator

    launches, private_paths = [], []

    class Child:
        returncode = None

        async def wait(self):
            self.returncode = 0
            return 0

        def terminate(self):
            raise AssertionError("A completed child must not be terminated")

    async def launch(*argv, **kwargs):
        options = {argv[i]: argv[i + 1] for i in range(len(argv) - 1)
                   if argv[i].startswith("--") and argv[i] != "--lifecycle-diagnostics"}
        launches.append((argv, options))
        manifest_path = Path(options["--manifest"])
        private_paths.append(manifest_path.parent)
        private_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert len(private_manifest["show_ids"]) == 6
        result = shard_result()
        result.update({name: 2 for name in ["scheduled", "dispatched", "completed", "fulfilled",
            "fulfilled_by_deadline", "distinct_orders", "distinct_tickets"]})
        result["lifecycle"] = {"loop_lag_p95_ms": len(launches)}
        result["http_client_count"] = int(options["--http-client-count"])
        result["http_connection_budgets"] = [32, 32, 31, 31, 31, 31, 31, 31]
        Path(options["--output"]).write_text(json.dumps(result), encoding="utf-8")
        return Child()

    monkeypatch.setattr(generator.asyncio, "create_subprocess_exec", launch)
    result = asyncio.run(generator.run(SimpleNamespace(rate=4, seconds=1, concurrency=500,
        completion_deadline_seconds=121, origin="http://192.0.2.1:8000", poll_seconds=1,
        duplicates=1, http_client_count=8, lifecycle_diagnostics=True), fixture()))
    assert result["pass"] and len(launches) == 2
    assert all("--lifecycle-diagnostics" in argv for argv, _ in launches)
    assert sum(int(options["--rate"]) for _, options in launches) == 4
    assert sum(int(options["--concurrency"]) for _, options in launches) == 500
    assert sum(int(options["--http-max-connections"]) for _, options in launches) == 500
    assert sum(int(options["--http-client-count"]) for _, options in launches) == 16
    assert launches[0][1]["--start-at-epoch"] == launches[1][1]["--start-at-epoch"]
    assert [row["lifecycle"]["loop_lag_p95_ms"] for row in result["shards"]] == [1, 2]
    assert result["http_clients_per_shard"] == [8, 8]
    assert all(sum(budgets) == 250 for budgets in result["http_connection_budgets_per_shard"])
    assert all(not path.exists() for path in private_paths)
