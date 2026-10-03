import subprocess

import pytest

from scripts.unattended_capacity_stage import Transport


@pytest.mark.parametrize("operation", ["remote", "copy_from", "copy_to"])
def test_every_transport_uses_bounded_liveness_and_explicit_identity(monkeypatch, tmp_path, operation):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    transport = Transport("ssh", "scp", tmp_path, identity_file=tmp_path / "owned-key")
    if operation == "remote":
        transport.remote("phase", "host", ["true"], timeout=20)
    elif operation == "copy_from":
        transport.copy_from("phase", "host", "/owned/source", tmp_path / "dest")
    else:
        transport.copy_to("phase", tmp_path / "source", "host", "/owned/dest")
    assert len(calls) == 1
    command, options = calls[0]
    for value in ["BatchMode=yes", "ServerAliveInterval=15", "ServerAliveCountMax=3",
                  "ConnectTimeout=10", "ConnectionAttempts=1", "IdentitiesOnly=yes"]:
        assert command[command.index(value) - 1] == "-o"
    assert command[command.index("-i") + 1] == str(tmp_path / "owned-key")
    assert options["stdin"] == subprocess.DEVNULL


def test_remote_failure_is_never_hidden_or_retried(monkeypatch, tmp_path):
    calls = []

    def fail(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, stdout="completed failed result", stderr="")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="failed with exit code 1"):
        Transport("ssh", "scp", tmp_path).remote("control", "host", ["false"], timeout=30)
    assert len(calls) == 1
    assert (tmp_path / "control.log").read_text() == "completed failed result"


def test_parent_timeout_remains_a_failure_without_retry(monkeypatch, tmp_path):
    calls = []

    def fail(command, **kwargs):
        calls.append(command)
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.TimeoutExpired):
        Transport("ssh", "scp", tmp_path).remote("control", "host", ["sleep", "60"], timeout=20)
    assert len(calls) == 1
