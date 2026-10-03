import importlib.util
import json
from pathlib import Path

import pytest


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path("scripts") / (name + ".py"))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


summary = module("summarize_api_stacks")
sampler = module("sample_api_stacks")


def profile(samples=None, weights=None):
    return {
        "shared": {
            "frames": [
                {"name": "run", "file": "/private/tools/anyio/_backends/_asyncio.py"},
                {"name": "get_order", "file": "/private/app/ticketing/api.py"},
                {"name": "loads", "file": "/usr/lib/python3.12/json/__init__.py"},
            ]
        },
        "profiles": [
            {
                "type": "sampled",
                "unit": "none",
                "samples": [[0, 1, 2], [0, 2]] if samples is None else samples,
                "weights": [3, 1] if weights is None else weights,
            }
        ],
    }


def test_weighted_routes_root_to_leaf_and_private_path_removal():
    result = summary.summarize([profile()])
    assert result["sample_observations"] == 2
    assert result["total_sample_weight"] == 4
    assert result["route_stack_presence"][0]["symbol"] == "order_status"
    assert result["route_stack_presence"][0]["sample_weight_percent"] == 75
    assert result["top_leaf_frames"][0]["symbol"] == "json/__init__.py:loads"
    assert result["top_leaf_frames"][0]["sample_weight_percent"] == 100
    assert "/private" not in json.dumps(result)


def test_recursive_inclusive_frames_count_once_and_units_must_match():
    result = summary.summarize([profile([[0, 1, 1, 2]], [1])])
    order = next(v for v in result["top_inclusive_frames"] if v["symbol"].endswith(":get_order"))
    assert order["sample_weight_percent"] == 100
    other = profile()
    other["profiles"][0]["unit"] = "seconds"
    with pytest.raises(ValueError, match="inconsistent"):
        summary.summarize([profile(), other])


@pytest.mark.parametrize(
    ("samples", "weights"),
    [
        ([], []),
        ([[0]], []),
        ([[99]], [1]),
        ([[-1]], [1]),
        ([[True]], [1]),
        ([[]], [1]),
        ([[0]], [0]),
        ([[0]], [-1]),
        ([[0]], [float("nan")]),
    ],
)
def test_invalid_or_empty_profiles_fail_closed(samples, weights):
    with pytest.raises(ValueError):
        summary.summarize([profile(samples, weights)])


def test_offloaded_webhook_is_attributed_only_when_domain_frame_present():
    assert (
        summary.route_category(
            [
                {"name": "callback", "file": "/app/ticketing/application/reservations.py"},
                {"name": "execute", "file": "/site/psycopg/cursor.py"},
            ]
        )
        == "payment_callback"
    )
    assert summary.route_category([{"name": "callback", "file": "/generic/framework.py"}]) == "unattributed"
    assert summary.public_file(r"C:\\private\\ticketing\\api.py") == "ticketing/api.py"


@pytest.mark.parametrize(("seconds", "rate"), [(0, 25), (121, 25), (120, 0), (120, 26)])
def test_collection_budget_rejects_unapproved_attachment(seconds, rate):
    with pytest.raises(ValueError):
        sampler.validate_bounds(seconds, rate)


def test_target_failure_creates_typed_failure_without_attachment(tmp_path, monkeypatch):
    def fail(_repo):
        raise ValueError("Mismatched application")

    monkeypatch.setattr(sampler, "targets", fail)
    monkeypatch.setattr(sampler.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("must not attach"))
    result = sampler.collect(Path("unused"), tmp_path / "trace", Path("unused"))
    assert not result["pass"]
    assert result["owned_profilers_exited"]
    assert json.loads((tmp_path / "trace" / "collection.json").read_text()) == result


def test_replaced_target_terminates_and_reaps_owned_profilers(tmp_path, monkeypatch):
    selected = [{"container": str(i), "pid": i, "start": "original"} for i in range(4)]
    processes = []

    class FakeProcess:
        returncode = None
        terminated = False
        reaped = False

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        def wait(self, timeout):
            self.reaped = True
            return self.returncode

    def spawn(*_args, **_kwargs):
        process = FakeProcess()
        processes.append(process)
        return process

    monkeypatch.setattr(sampler, "targets", lambda _repo: selected)
    monkeypatch.setattr(sampler, "process_start", lambda _pid: "replaced")
    monkeypatch.setattr(sampler, "child_cpu", lambda: 0)
    monkeypatch.setattr(sampler.subprocess, "check_output", lambda *_args, **_kwargs: "py-spy 0.4.2")
    monkeypatch.setattr(sampler.subprocess, "Popen", spawn)
    result = sampler.collect(Path("unused"), tmp_path / "trace", Path("unused"))
    assert not result["pass"]
    assert result["error_type"] == "ValueError"
    assert len(processes) == 4
    assert all(process.terminated and process.reaped for process in processes)
    assert result["owned_profilers_exited"]


def test_fileless_profiler_thread_frames_are_supported():
    document = profile()
    document["shared"]["frames"][0].pop("file")
    result = summary.summarize([document])
    assert any(row["symbol"] == "<unknown>:run" for row in result["top_inclusive_frames"])


def test_public_summary_removes_profiler_thread_identifiers():
    document = profile()
    document["shared"]["frames"][0] = {"name": "thread (12345678)"}
    result = summary.summarize([document])
    assert "12345678" not in json.dumps(result)
    assert any(row["symbol"] == "<unknown>:thread" for row in result["top_inclusive_frames"])
