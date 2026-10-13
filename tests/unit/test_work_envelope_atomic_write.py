"""A transient Windows rename lock never changes atomic journal semantics."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import work_envelope as policy


def locked():
    error = PermissionError("sharing violation")
    error.winerror = 5
    return error


def test_transient_lock_reuses_fsynced_pending_file(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text('{"old":true}')
    replace = policy.os.replace
    calls = []
    def transient(source, target):
        calls.append((source, target))
        if len(calls) == 1:
            assert json.loads(path.read_text()) == {"old": True}
            raise locked()
        replace(source, target)
    monkeypatch.setattr(policy.os, "replace", transient)
    monkeypatch.setattr(policy.time, "sleep", lambda _: None)
    policy.write(path, {"new": True})
    assert len(calls) == 2 and calls[0] == calls[1]
    assert json.loads(path.read_text()) == {"new": True}
    assert not list(tmp_path.glob("*.pending"))


def test_permanent_denial_preserves_old_state_and_pending_evidence(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text('{"old":true}')
    calls = []
    def denied(source, target):
        calls.append((source, target))
        raise locked()
    monkeypatch.setattr(policy.os, "replace", denied)
    monkeypatch.setattr(policy.time, "sleep", lambda _: None)
    with pytest.raises(PermissionError):
        policy.write(path, {"new": True})
    assert len(calls) == 6 and len(set(calls)) == 1
    assert json.loads(path.read_text()) == {"old": True}
    pending = list(tmp_path.glob("*.pending"))
    assert len(pending) == 1 and json.loads(pending[0].read_text()) == {"new": True}
