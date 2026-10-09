import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from scripts import collect_paid_control_cpu as collector


@pytest.fixture
def run_dir(tmp_path):
    root = tmp_path / "checkout-20261004T000000Z-abcdef"
    root.mkdir()
    return root


def state(root, phases=(), error=None):
    return {"run": root.name, "phases": list(phases), "error": error}


class Clock:
    def __init__(self, on_sleep=lambda _: None):
        self.now = 0
        self.on_sleep = on_sleep

    def clock(self):
        return self.now

    def sleep(self, duration):
        self.now += duration
        self.on_sleep(self.now)


def test_delayed_checkpoint_creation_and_partial_writes_wait_for_dispatch(run_dir):
    path = run_dir / "state.json"
    def advance(now):
        if now == 1:
            path.write_text('{"run":')
        elif now == 2:
            path.write_text(json.dumps(state(run_dir)))
        elif now == 3:
            path.write_text(json.dumps(state(run_dir, ["generator-script-directory"])))
    clock = Clock(advance)
    result = collector.wait_for_dispatch(
        run_dir, timeout=5, interval=1, clock=clock.clock, sleep=clock.sleep,
    )
    assert result["phases"] == ["generator-script-directory"]
    assert clock.now == 3


@pytest.mark.parametrize("content", [None, '{"run":', ""])
def test_missing_or_partial_checkpoint_times_out_in_one_bound(run_dir, content):
    if content is not None:
        (run_dir / "state.json").write_text(content)
    clock = Clock()
    with pytest.raises(TimeoutError):
        collector.wait_for_dispatch(run_dir, timeout=3, clock=clock.clock, sleep=clock.sleep)
    assert clock.now == 3


@pytest.mark.parametrize("phases,error", [
    (["generator-script-directory"], "failed"),
    (["generator-script-directory", "probe"], None),
    (["rollback"], None),
])
def test_failed_or_finished_control_never_starts_ssh(run_dir, monkeypatch, phases, error):
    (run_dir / "state.json").write_text(json.dumps(state(run_dir, phases, error)))
    key = run_dir / "identity"
    key.touch()
    monkeypatch.setattr(collector.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("SSH started"))
    with pytest.raises(RuntimeError):
        collector.collect(run_dir, "root@example", key)


@pytest.mark.parametrize("value", [[], {}, {"run": "wrong", "phases": [], "error": None},
                                  {"phases": "ready", "error": None}])
def test_complete_invalid_checkpoint_fails_closed(run_dir, value):
    (run_dir / "state.json").write_text(json.dumps(value))
    with pytest.raises(ValueError):
        collector.wait_for_dispatch(run_dir)


def samples():
    start = datetime(2026, 10, 4, tzinfo=UTC)
    return {
        "pass": True, "replica_counts": collector.EXPECTED_REPLICAS,
        "samples": [{
            "utc": (start + timedelta(seconds=i * 5)).isoformat(), "elapsed_seconds": i * 5,
            "cpu_usec_by_role": {"api": i * 10_000_000, "consumer": i * 2_500_000,
                                 "reservation-writer": i * 1_000_000},
            "host_ticks": [i * 10, i * 2, i * 8, i * 70, i * 10, 0, 0, 0],
        } for i in range(49)],
    }


def test_complete_cpu_window_derives_rates_from_elapsed_counter_deltas():
    summary = collector.summarize_cpu(samples())
    assert summary["window_seconds"] == 240
    assert summary["host_cpu_mean_percent"] == 20
    assert summary["aggregate_cpu_cores_by_role"] == {
        "api": 2, "consumer": .5, "reservation-writer": .2,
    }


@pytest.mark.parametrize("defect", [
    "short", "topology", "role_missing", "counter_reset", "tick_reset", "clock_reset",
    "nonfinite", "collection_error", "duration", "failed",
])
def test_incomplete_or_uncertain_cpu_evidence_is_rejected(defect):
    data = samples()
    if defect == "short":
        data["samples"].pop()
    elif defect == "topology":
        data["replica_counts"] = {**collector.EXPECTED_REPLICAS, "api": 3}
    elif defect == "role_missing":
        del data["samples"][2]["cpu_usec_by_role"]["api"]
    elif defect == "counter_reset":
        data["samples"][2]["cpu_usec_by_role"]["api"] = 0
    elif defect == "tick_reset":
        data["samples"][2]["host_ticks"][0] = 0
    elif defect == "clock_reset":
        data["samples"][2]["elapsed_seconds"] = 1
    elif defect == "nonfinite":
        data["samples"][2]["elapsed_seconds"] = float("nan")
    elif defect == "collection_error":
        data["samples"][2]["collection_error"] = "cgroup_missing"
    elif defect == "duration":
        data["samples"][-1]["elapsed_seconds"] = 1000
    else:
        data["pass"] = False
    with pytest.raises(ValueError):
        collector.summarize_cpu(data)


@pytest.mark.parametrize("outcome", ["ok", "failure", "timeout", "incomplete"])
def test_transport_evidence_and_failure_are_retained(run_dir, monkeypatch, outcome):
    (run_dir / "state.json").write_text(json.dumps(state(run_dir, ["generator-script-directory"])))
    key = run_dir / "identity"
    key.touch()
    def execute(command, **kwargs):
        assert kwargs["timeout"] == 265
        assert kwargs["input"] == collector.REMOTE_CODE
        assert command[-3:] == ["root@example", "python3", "-"]
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(command, 265, stderr=b"timeout detail")
        data = samples()
        if outcome == "incomplete":
            data["samples"].pop()
        return subprocess.CompletedProcess(command, int(outcome == "failure"),
                                           json.dumps(data), "retained diagnostic")
    monkeypatch.setattr(collector.subprocess, "run", execute)
    if outcome == "ok":
        assert collector.collect(run_dir, "root@example", key)["pass"]
        assert (run_dir / "container-cpu-summary.json").exists()
    else:
        with pytest.raises((RuntimeError, ValueError)):
            collector.collect(run_dir, "root@example", key)
        assert not (run_dir / "container-cpu-summary.json").exists()
    assert (run_dir / "container-cpu-sampling.stderr").read_text()
    if outcome == "incomplete":
        assert len(json.loads((run_dir / "container-cpu-samples.json").read_text())["samples"]) == 48
