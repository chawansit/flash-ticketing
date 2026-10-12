"""ADR0296 composed supervision must cover the actual early hourly window."""
import ast
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from run_two_host_paid_comparison import job_program


def supervisor(arguments, directory, hourly):
    tree = ast.parse(job_program(arguments, str(directory), hourly=hourly))
    call = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute) and node.func.attr == "Popen")
    return ast.literal_eval(call.args[0])[2]


@pytest.mark.parametrize("name,flags", [
    ("observe_cce_paid_pipeline", ["--hourly", "--early-hourly-bootstrap"]),
    ("observe_slot_paid_pipeline", ["--hourly", "--early-hourly-bootstrap"]),
    ("kafka_lag_observe", []),
    ("observe_projection_kafka", []),
])
def test_extended_supervision_covers_early_setup_hour_and_exit(tmp_path, monkeypatch, name, flags):
    args = ["python", "/owned/"+name+".py", "--seconds", "4200", *flags]
    calls = []
    monkeypatch.setattr("subprocess.run", lambda args, **kw: calls.append((args, kw)) or SimpleNamespace(returncode=0))
    exec(supervisor(args, tmp_path, True), {})  # noqa: S102 - execute only repository-generated supervisor
    forwarded, options = calls.pop()
    assert forwarded == args
    assert options["timeout"] == 4260
    # Reproduce ADR0295: actual offered end is 3798 seconds after collector launch.
    offered_end = 171.468 + 24.644 + 3600 + 2
    assert 3780 < offered_end < options["timeout"]
    assert (tmp_path/("job-"+name+"-exit.json")).read_text() == '{"returncode": 0}'


@pytest.mark.parametrize("hourly,seconds,expected", [(False,480,600), (True,3780,3780)])
def test_legacy_observers_retain_supervision(tmp_path, monkeypatch, hourly, seconds, expected):
    args = ["python", "/owned/observe_slot_paid_pipeline.py", "--seconds", str(seconds)]
    calls = []
    monkeypatch.setattr("subprocess.run", lambda args, **kw: calls.append(kw) or SimpleNamespace(returncode=0))
    exec(supervisor(args, tmp_path, hourly), {})  # noqa: S102 - execute only repository-generated supervisor
    assert calls[0]["timeout"] == expected


def test_customer_generator_cannot_inherit_observer_extension(tmp_path, monkeypatch):
    args = ["python", "/owned/cce_ingress_hourly_generator.py", "--seconds", "4200"]
    calls = []
    monkeypatch.setattr("subprocess.run", lambda args, **kw: calls.append(kw) or SimpleNamespace(returncode=1))
    exec(supervisor(args, tmp_path, True), {})  # noqa: S102 - execute only repository-generated supervisor
    assert calls[0]["timeout"] == 3780


@pytest.mark.parametrize("hourly,flags", [
    (False, ["--seconds","4200","--early-hourly-bootstrap"]),
    (True, ["--seconds","3780","--early-hourly-bootstrap"]),
    (True, ["--seconds","4200"]),
    (True, ["--seconds","4200","--seconds","4200","--early-hourly-bootstrap"]),
])
def test_unqualified_native_extension_rejected(tmp_path, hourly, flags):
    with pytest.raises(ValueError):
        job_program(["python","/owned/observe_slot_paid_pipeline.py",*flags],str(tmp_path),hourly=hourly)


def test_timeout_exception_is_not_reported_as_success(tmp_path, monkeypatch):
    import subprocess
    args = ["python","/owned/kafka_lag_observe.py","--seconds","4200"]
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])
    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        exec(supervisor(args,tmp_path,True),{})  # noqa: S102 - execute only repository-generated supervisor
    assert not (tmp_path/"job-kafka_lag_observe-exit.json").exists()
