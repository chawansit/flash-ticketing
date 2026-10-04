import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "observation_overhead", Path(__file__).resolve().parents[2] / "scripts/measure_observation_overhead.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def counters():
    return {
        "elapsed": 0.0,
        "cpu": {"api": 1000000},
        "api_process": 1.0,
        "ticks": [10, 0, 10, 80, 0, 0, 0, 0],
    }, {
        "elapsed": 60.0,
        "cpu": {"api": 13000000},
        "api_process": 7.0,
        "ticks": [130, 0, 130, 440, 0, 0, 0, 0],
    }


def test_distinguishes_api_process_from_container_cpu():
    before, after = counters()
    v = module.summarize_window(before, after)
    assert v["cpu_cores_by_role"]["api"] == pytest.approx(0.2)
    assert v["api_process_cpu_cores"] == pytest.approx(0.1)
    assert v["host_cpu_percent"] == pytest.approx(40)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(elapsed=0),
        lambda v: v.update(elapsed=float("nan")),
        lambda v: v.update(api_process=0),
        lambda v: v["cpu"].update(api=0),
        lambda v: v["cpu"].update(consumer=1),
        lambda v: v["cpu"].update(api=float("inf")),
        lambda v: v["ticks"].__setitem__(0, 0),
        lambda v: v["ticks"].__setitem__(0, float("nan")),
    ],
)
def test_rejects_incomplete_or_reset_cpu_counters(mutation):
    before, after = counters()
    mutation(after)
    with pytest.raises(ValueError):
        module.summarize_window(before, after)


def phases(values):
    return [
        {
            "phase": name,
            "host_cpu_percent": value * 25,
            "api_process_cpu_cores": value / 2,
            "cpu_cores_by_role": {"api": value},
        }
        for name, value in zip(module.PHASES, values)
    ]


def test_negative_difference_remains_visible_and_is_not_a_saving_claim():
    result = module.effects(phases([0.4, 0.3, 0.4, 0.3, 0.4]))
    assert result["observers"]["container_cpu_delta_cores"] == pytest.approx(-0.1)
    assert not result["observers"]["positive_direction_above_idle_drift"]


def test_background_drift_prevents_positive_attribution():
    result = module.effects(phases([0.2, 0.5, 0.6, 0.5, 0.6]))
    assert result["observers"]["container_cpu_delta_cores"] == pytest.approx(0.1)
    assert result["observers"]["bracketing_idle_drift_cores"] == pytest.approx(0.4)
    assert not result["observers"]["positive_direction_above_idle_drift"]


def test_positive_direction_requires_reversal_and_complete_order():
    rows = phases([0.2, 0.5, 0.2, 0.6, 0.2])
    assert module.effects(rows)["observers"]["positive_direction_above_idle_drift"]
    with pytest.raises(ValueError):
        module.effects(rows[:-1])
    rows.reverse()
    with pytest.raises(ValueError):
        module.effects(rows)


def test_nonfinite_role_summary_cannot_be_attributed():
    rows = phases([0.2, 0.5, 0.2, 0.6, 0.2])
    rows[1]["cpu_cores_by_role"]["api"] = float("nan")
    with pytest.raises(ValueError):
        module.effects(rows)


def test_existing_container_artifact_is_preserved_on_preflight_failure(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    (repo / "tmp").mkdir(parents=True)
    output = repo / "tmp" / "adr0146-collision"
    roles = {f"{role}{i}": role for role, n in module.EXPECTED.items() for i in range(n)}
    monkeypatch.setitem(
        sys.modules,
        "sample_api_stacks",
        SimpleNamespace(targets=lambda _: [{"container": "api0", "pid": 123, "start": "1"}]),
    )
    original_read = Path.read_text
    original_is_file = Path.is_file
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda p, *a, **kw: (
            "0::/fake" if str(p).replace("\\", "/") == "/proc/123/cgroup" else original_read(p, *a, **kw)
        ),
    )
    monkeypatch.setattr(
        Path,
        "is_file",
        lambda p: True if str(p).replace("\\", "/").startswith("/sys/fs/cgroup/") else original_is_file(p),
    )
    monkeypatch.setattr(module.os, "sysconf", lambda _: 100, raising=False)

    def output_for(args, **_):
        if args == ["docker", "ps", "-q"]:
            return " ".join(roles)
        if args[:2] == ["docker", "inspect"]:
            return json.dumps(
                [
                    {
                        "Config": {
                            "Labels": {
                                "com.docker.compose.project": "flash-ticketing",
                                "com.docker.compose.service": roles[args[2]],
                            }
                        },
                        "State": {"Pid": 123},
                        "Image": "image",
                    }
                ]
            )
        if args[:5] == ["docker", "exec", "api0", "python", "-c"]:
            return "{}"
        raise AssertionError(f"Unexpected operation: {args}")

    operations = []

    def existing_third_path(args, **_):
        operations.append(args)
        assert args[:6] == ["docker", "exec", "api0", "test", "!", "-e"]
        if len(operations) == 3:
            raise subprocess.CalledProcessError(1, args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(module.subprocess, "check_output", output_for)
    monkeypatch.setattr(module.subprocess, "run", existing_third_path)
    with pytest.raises(subprocess.CalledProcessError):
        module.run(repo, repo / "fixture.json", output, repo / "observer.py")
    assert len(operations) == 3
    assert not (output / "result.json").exists()
