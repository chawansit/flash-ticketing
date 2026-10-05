import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_two_host_paid_comparison as runner


def write_trace(path, name, rows):
    (path / (name + ".jsonl")).write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_finally_summary_preserves_original_failed_customer_financial_and_restore_state(monkeypatch, tmp_path):
    record = {"pass": False, "failure_type": "ConnectionResetError",
              "customer": {"pass": False, "errors": 324},
              "financial": {"pass": False, "duplicate_booked_seats": 0}, "restore_pass": False,
              "offered_start_utc": "start", "offered_end_utc": "end"}
    write_trace(tmp_path, "pipeline", [{"sample": 1}, {"sample": 2}])
    write_trace(tmp_path, "kafka", [{"sample": 3}])
    monkeypatch.setattr(runner, "summarize_pipeline", lambda rows: {"samples": len(rows)})
    monkeypatch.setattr(runner, "summarize_kafka", lambda rows: {"samples": len(rows)})
    captured = []

    def distribution(rows, inventory, **window):
        captured.append((rows, inventory, window))
        return {"all_four_replicas_observed": True}

    monkeypatch.setattr(runner, "summarize_distribution", distribution)
    runner.retain_observer_summaries(tmp_path, record, {"apis": [1, 2, 3, 4]})
    assert record["pipeline_summary"] == {"samples": 2}
    assert record["kafka_summary"] == {"samples": 1}
    assert record["pass"] is False and record["restore_pass"] is False
    assert record["failure_type"] == "ConnectionResetError"
    assert record["financial"]["pass"] is False and record["customer"]["errors"] == 324
    assert captured[0][2] == {"offered_start_utc": "start", "offered_end_utc": "end"}


@pytest.mark.parametrize("broken", ["", "{partial\n", '{"ok":1}\n{partial'])
def test_broken_trace_is_explicit_and_other_summary_still_retained(monkeypatch, tmp_path, broken):
    (tmp_path / "pipeline.jsonl").write_text(broken)
    write_trace(tmp_path, "kafka", [{"sample": 1}])
    monkeypatch.setattr(runner, "summarize_kafka", lambda rows: {"samples": len(rows)})
    record = {"pass": False, "failure_type": "OriginalFailure"}
    runner.retain_observer_summaries(tmp_path, record)
    assert "pipeline_summary" not in record
    assert record["summary_collection_errors"]["pipeline"] in {"ValueError", "JSONDecodeError"}
    assert record["kafka_summary"] == {"samples": 1}
    assert record["pass"] is False and record["failure_type"] == "OriginalFailure"


def test_missing_traces_are_explicit(tmp_path):
    record = {"pass": False}
    runner.retain_observer_summaries(tmp_path, record)
    assert record["summary_collection_errors"] == {"pipeline": "FileNotFoundError", "kafka": "FileNotFoundError"}


@pytest.mark.parametrize("inventory,window", [(None, True), ({"apis": [1]}, False)])
def test_distribution_is_not_inferred_without_declared_inventory_and_window(monkeypatch, tmp_path, inventory, window):
    write_trace(tmp_path, "pipeline", [{"sample": 1}])
    write_trace(tmp_path, "kafka", [{"sample": 1}])
    monkeypatch.setattr(runner, "summarize_pipeline", lambda rows: {})
    monkeypatch.setattr(runner, "summarize_kafka", lambda rows: {})
    monkeypatch.setattr(runner, "summarize_distribution", lambda *_a, **_k: pytest.fail("must not infer window"))
    record = {"pass": False, **({"offered_start_utc": "start", "offered_end_utc": "end"} if window else {})}
    runner.retain_observer_summaries(tmp_path, record, inventory)
    assert "distribution" not in record


def test_distribution_failure_cannot_hide_original_failure(monkeypatch, tmp_path):
    write_trace(tmp_path, "pipeline", [{"sample": 1}])
    write_trace(tmp_path, "kafka", [{"sample": 1}])
    monkeypatch.setattr(runner, "summarize_pipeline", lambda rows: {})
    monkeypatch.setattr(runner, "summarize_kafka", lambda rows: {})

    def missing_window(*_args, **_kwargs):
        raise ValueError("insufficient bracketing")

    monkeypatch.setattr(runner, "summarize_distribution", missing_window)
    record = {"pass": False, "failure_type": "EOFError", "offered_start_utc": "start", "offered_end_utc": "end"}
    runner.retain_observer_summaries(tmp_path, record, {"apis": [1]})
    assert record["summary_collection_errors"]["distribution"] == "ValueError"
    assert record["failure_type"] == "EOFError" and record["pass"] is False


@pytest.mark.parametrize("changed_worker", [False, True])
def test_live_inventory_verifies_all_worker_roles_before_any_customer_dispatch(tmp_path, changed_worker):
    from copy import deepcopy
    from types import SimpleNamespace

    import collect_two_host_inventory as collector
    from prepare_two_host_scaling import API_SETTINGS, BACKGROUND
    from test_two_host_paid_runner import env
    from test_two_host_scaling_preparation import inventory

    fixture = inventory()
    baseline = env()
    rows, secondary = [], []
    images = {role: "sha256:" + str(i) * 64 for i, role in enumerate(BACKGROUND, 1)}
    for role, count in {"api": 2, **{k: v["replicas"] for k, v in BACKGROUND.items()},
                        "kafka": 1, "load-balancer": 1, "pgbouncer": 1}.items():
        for _index in range(count):
            values = {"DB_POOL_MAX": "8", "RESERVATION_WRITER_BATCH_SIZE": "4",
                      "SIMULATOR_CONCURRENCY": "8", "SIMULATOR_DISPATCH_MODE": "refill",
                      "DEFAULT_POOL_SIZE": "24", "RESERVE_POOL_SIZE": "0", "MAX_CLIENT_CONN": "160"}
            if role == "api":
                values = {**baseline, **API_SETTINGS}
            rows.append({"Id": f"{len(rows)+1:064x}", "Image": images.get(role, collector.IMAGE),
                         "State": {"Running": True, "StartedAt": "2026-10-05T00:00:00Z"},
                         "Config": {"Env": [k+"="+v for k, v in values.items()],
                                    "Labels": {"com.docker.compose.service": role, "com.docker.compose.project": "flash-ticketing"}}})
    apis = [row for row in rows if row["Config"]["Labels"]["com.docker.compose.service"] == "api"]
    routes = []
    for i, original in enumerate(fixture["apis"]):
        row = deepcopy(apis[i % 2])
        if i >= 2:
            row["Id"] = f"{i+100:064x}"
            row["Config"]["Labels"]["com.docker.compose.project"] = "flash-ticketing-api-secondary"
            row["Config"]["Env"] = [k+"="+v for k, v in {**baseline, **API_SETTINGS,
                    "DATABASE_URL": baseline["DATABASE_URL"].replace("pgbouncer", fixture["hosts"]["primary"]["private_ipv4"])}.items()]
            secondary.append(row)
        routes.append({k: original[k] for k in ("host_role", "private_ipv4", "port")} | {"container_id": row["Id"]})
    if changed_worker:
        next(row for row in rows if row["Config"]["Labels"]["com.docker.compose.service"] == "consumer")["Image"] = collector.IMAGE
    checked = []

    def call(role, code, *_args):
        if code == collector.INSPECT:
            return rows
        if "docker','ps','-aq'" in code:
            return secondary
        if "product_uuid" in code:
            return {**fixture["hosts"][role], "addresses": [fixture["hosts"][role]["private_ipv4"]], "load_processes": 0}
        assert "def inspect()" in code and "source_hashes_match" in code
        # The embedded guard itself is executed with synthetic Docker in the separate source-proof tests.
        namespace = {}
        exec(code.split("def inspect():")[0], namespace)  # noqa: S102
        checked.append(namespace["role"])
        return {"started_at": namespace["started"], "source_identity": {"source_hashes_match": True, "ready": True},
                "container_id": namespace["cid"], "role": namespace["role"], "image_id": namespace["image"]}

    session = SimpleNamespace(config={k: {"private_ipv4": v["private_ipv4"]} for k,v in fixture["hosts"].items()},
                              call=call, api=lambda *_a: {"pass": True, "kafka_members": 6})
    if changed_worker:
        with pytest.raises(ValueError, match="Background role image differs"):
            collector.observe(session, "candidate", routes, baseline, expected_worker_images=images)
        assert not checked
    else:
        result, view, _primary, _secondary = collector.observe(session, "candidate", routes, baseline, expected_worker_images=images)
        assert len(result["worker_sources"]) == 13
        assert checked[-4:] == ["api"] * 4
        assert checked.count("consumer") == 6 and checked.count("reservation-writer") == 3
        assert view["inventory_contract_pass"] is True


def test_stage_failure_runs_finally_summaries_without_dispatch_or_success_rewrite(monkeypatch, tmp_path):
    from types import SimpleNamespace

    local = tmp_path / "candidate"
    calls = []

    def remote(*args):
        calls.append(args)
        if len(calls) == 1:
            write_trace(local, "pipeline", [{"sample": 1}])
            write_trace(local, "kafka", [{"sample": 2}])
            raise ConnectionResetError("synthetic observer transport failure")
        return {"private_manifests_removed": True}

    monkeypatch.setattr(runner, "summarize_pipeline", lambda rows: {"samples": len(rows)})
    monkeypatch.setattr(runner, "summarize_kafka", lambda rows: {"samples": len(rows)})
    session = SimpleNamespace(config={"generator": {"repo": "/owned/generator"}}, call=remote,
                              state={}, checkpoint=lambda: None)
    stage = runner.Stages(False, {})
    with pytest.raises(ConnectionResetError):
        stage(session, "candidate", [{"host_role": "primary", "container_id": "1" * 64}], {}, "/owned", tmp_path)
    retained = json.loads((local / "stage.private.json").read_text())
    assert retained["pipeline_summary"] == {"samples": 1}
    assert retained["kafka_summary"] == {"samples": 1}
    assert retained["pass"] is False and retained["customers_dispatched"] is False
    assert retained["failure_type"] == "ConnectionResetError"
    assert stage.results["candidate"] == retained
