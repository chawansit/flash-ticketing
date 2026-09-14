#!/usr/bin/env python3
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "unattended_capacity_stage.py"
SPEC = importlib.util.spec_from_file_location("unattended_capacity_stage", SCRIPT)
assert SPEC and SPEC.loader
stage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stage)


class FakeTransport:
    instances: ClassVar[list] = []
    fail_phase: ClassVar[str | None] = None

    def __init__(self, ssh, scp, log_dir):
        self.calls = []
        self.private_manifest = None
        self.__class__.instances.append(self)

    def remote(self, phase, host, argv, check=True, timeout=None):
        self.calls.append(("remote", phase))
        if phase == self.fail_phase:
            raise RuntimeError("token=should-not-leak")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def copy_from(self, phase, host, remote, local, recursive=False):
        self.calls.append(("copy_from", phase))
        local = Path(local)
        if phase == "manifest-from-backend":
            local.write_text('{"private": true}', encoding="utf-8")
            self.private_manifest = local
        elif phase == "generator-evidence":
            target = local / "public" / "load"
            target.mkdir(parents=True)
            (target / "summary.json").write_text(
                json.dumps(
                    {
                        "rate": 750,
                        "seconds": 1,
                        "generator_drops": 0,
                        "transport_errors": {},
                        "worst_worker_read_p95_ms": 8,
                        "worst_worker_hold_p95_ms": 50,
                        "workload_gate_pass": True,
                    }
                ),
                encoding="utf-8",
            )
        elif phase == "backend-evidence":
            target = local / "public"
            target.mkdir(parents=True)
            (target / "durability.json").write_text(
                json.dumps(
                    {
                        "durability_and_expiry_pass": True,
                        "overlapping_load_hold_intervals": 0,
                        "queues_snapshot": {"outbox": 0, "refresh": 0, "dead_letters": 0},
                        "audited_holds": 1,
                    }
                ),
                encoding="utf-8",
            )
            (target / "admission.json").write_text(
                '{"admission_rejections": 0, "pass": true}', encoding="utf-8"
            )
            (target / "rollback.json").write_text(
                '{"restored_admission": 4, "pass": true}', encoding="utf-8"
            )

    def copy_to(self, phase, local, host, remote, recursive=False):
        self.calls.append(("copy_to", phase))


def argv(output: Path) -> list[str]:
    return [
        "unattended_capacity_stage.py",
        "--backend-host",
        "backend-test",
        "--generator-host",
        "generator-test",
        "--origin",
        "http://private.test:8000",
        "--output",
        str(output),
        "--rate",
        "750",
        "--seconds",
        "1",
        "--shows",
        "4",
        "--seats",
        "300",
        "--viewers",
        "40",
        "--admission-candidate",
        "5",
        "--admission-rollback",
        "4",
        "--hold-expiry-wait",
        "1",
    ]


def test_redacts_credentials():
    value = stage.redact(
        "password=hello token:abc Authorization=Bearer-value "
        "postgresql://user:private@database/test Bearer ey.private"
    )
    assert "hello" not in value
    assert "abc" not in value
    assert "private@" not in value
    assert "ey.private" not in value


def test_success_orders_phases_cleans_manifest_and_passes(monkeypatch, tmp_path):
    FakeTransport.instances.clear()
    FakeTransport.fail_phase = None
    monkeypatch.setattr(stage, "Transport", FakeTransport)
    monkeypatch.setattr(stage.time, "sleep", lambda _: None)
    monkeypatch.setattr(sys, "argv", argv(tmp_path / "result"))

    stage.main()

    fake = FakeTransport.instances[-1]
    phases = [phase for kind, phase in fake.calls if kind == "remote"]
    assert phases.index("prepare") < phases.index("deploy") < phases.index("preflight")
    assert phases.index("load") < phases.index("durability-audit") < phases.index("rollback")
    assert phases[-2:] == ["generator-cleanup", "backend-cleanup"]
    assert fake.private_manifest is not None and not fake.private_manifest.exists()
    assert json.loads((tmp_path / "result" / "stage-result.json").read_text())["pass"] is True


def test_preflight_failure_short_circuits_and_rolls_back(monkeypatch, tmp_path):
    FakeTransport.instances.clear()
    FakeTransport.fail_phase = "preflight"
    monkeypatch.setattr(stage, "Transport", FakeTransport)
    monkeypatch.setattr(stage.time, "sleep", lambda _: None)
    monkeypatch.setattr(sys, "argv", argv(tmp_path / "failed"))

    with pytest.raises(SystemExit):
        stage.main()

    fake = FakeTransport.instances[-1]
    phases = [phase for kind, phase in fake.calls if kind == "remote"]
    assert "load" not in phases
    assert "rollback" in phases
    assert phases[-2:] == ["generator-cleanup", "backend-cleanup"]
    assert fake.private_manifest is not None and not fake.private_manifest.exists()
    result = json.loads((tmp_path / "failed" / "stage-result.json").read_text())
    assert result["pass"] is False
    assert "should-not-leak" not in json.dumps(result)


def test_cleanup_failure_does_not_skip_remaining_cleanup(monkeypatch, tmp_path):
    FakeTransport.instances.clear()
    FakeTransport.fail_phase = "generator-cleanup"
    monkeypatch.setattr(stage, "Transport", FakeTransport)
    monkeypatch.setattr(stage.time, "sleep", lambda _: None)
    monkeypatch.setattr(sys, "argv", argv(tmp_path / "cleanup-failed"))

    with pytest.raises(SystemExit):
        stage.main()

    fake = FakeTransport.instances[-1]
    phases = [phase for kind, phase in fake.calls if kind == "remote"]
    assert phases[-2:] == ["generator-cleanup", "backend-cleanup"]
    result = json.loads((tmp_path / "cleanup-failed" / "stage-result.json").read_text())
    assert result["pass"] is False
    assert "generator cleanup" in result["error"]
