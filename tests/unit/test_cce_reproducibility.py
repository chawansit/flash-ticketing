"""Portable historical CCE assets and drift rejection; no cloud credentials."""
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_frozen_harness as frozen
import cce_historical_sources as sources
import cce_paid_stage as paid
import check_cce_reproducibility as check
import generator_completion_probe_contract as historical
from prepare_generator_completion import PARENT_SHA256, corrected_source


def test_reproduction_contract_is_offline_and_preserves_workloads():
    result = check.verify()
    assert result["pass"] and result["frozen_parent_files"] == 78
    assert result["cloud_calls"] == result["customer_dispatches"] == 0
    assert result["workload_profiles"]["hourly"] == {"rate": 84, "seconds": 3600, "tickets": 302400}
    assert result["historical_images_match_pr6_combined_source"] is False
    assert result["capacity_improvement_measured"] is result["production_qualified"] is False


def test_assets_work_without_historical_git_object_or_temporary_export(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Historical Git access is not portable")
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    parent = frozen.frozen_bundle()
    assert hashlib.sha256(parent["scripts/paid_ticket_load_generator.py"]).hexdigest() == PARENT_SHA256
    runtime = paid.generator_bundle()
    assert runtime["scripts/paid_ticket_load_generator.py"] == corrected_source(parent["scripts/paid_ticket_load_generator.py"])
    assert runtime["scripts/checkout_journey_probe.py"] == frozen.qualified_helper()


@pytest.mark.parametrize("relative", ["scripts/paid_ticket_load_generator.py", "qualified/checkout_journey_probe.py"])
def test_changed_frozen_source_is_rejected(tmp_path, monkeypatch, relative):
    shutil.copytree(frozen.ASSETS, tmp_path / "assets")
    monkeypatch.setattr(frozen, "ASSETS", tmp_path / "assets")
    path = frozen.ASSETS / relative
    path.write_bytes(path.read_bytes() + b"\n# drift\n")
    with pytest.raises(ValueError, match="source drift"):
        frozen.qualified_helper() if relative.startswith("qualified/") else frozen.frozen_bundle()


def test_manifest_changes_are_rejected(tmp_path, monkeypatch):
    shutil.copytree(frozen.ASSETS, tmp_path / "assets")
    monkeypatch.setattr(frozen, "ASSETS", tmp_path / "assets")
    path = frozen.ASSETS / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["parent_revision"] = "0" * 40
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="78-file manifest"):
        frozen.frozen_bundle()


@pytest.mark.parametrize("path", ["../outside.py", "/tmp/outside.py", "scripts\\outside.py"])
def test_asset_paths_cannot_escape(path):
    with pytest.raises(ValueError, match="Canonical"):
        frozen.read_asset(path, "a" * 64)


@pytest.mark.parametrize("kind", ["altered_file", "missing_file", "reused_permission", "escaped_path"])
def test_promotion_drift_fails_before_contract_or_cloud_access(tmp_path, monkeypatch, kind):
    path = tmp_path / "input.py"
    path.write_text("original")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    lock = {"decision": "ADR0238", "cloud_execution_authorized": False, "files": {"input.py": digest}}
    if kind == "altered_file":
        path.write_text("changed")
    elif kind == "missing_file":
        path.unlink()
    elif kind == "reused_permission":
        lock["cloud_execution_authorized"] = True
    else:
        lock["files"] = {"../outside.py": digest}
    manifest = tmp_path / "lock.json"
    manifest.write_text(json.dumps(lock))
    monkeypatch.setattr(check, "ROOT", tmp_path)
    monkeypatch.setattr(check, "LOCK", manifest)
    monkeypatch.setattr(check.adapter, "contract", lambda: pytest.fail("Contract access after invalid promotion input"))
    with pytest.raises((ValueError, FileNotFoundError)):
        check.verify()


def test_complete_deployment_contract_without_historical_git_or_tmp(monkeypatch):
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: pytest.fail("Historical Git access"))
    data = historical.plan()
    contract = historical.GeneratorCompletionProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])
    assert len(contract.images) == 8
    assert contract.source_map("consumer") == data["consumer_runtime_source_sha256"]
    assert contract.source_map("reservation-writer") == data["writer_runtime_source_sha256"]
    assert all(contract.settings(role)["PAYMENT_CONFIRMATION_ASYNC"] == "0" for role in ("api", "consumer", "simulator"))


def test_packaged_export_keeps_complete_file_set_and_hash_checks(tmp_path, monkeypatch):
    snapshot = sources.manifest()
    directory, expected = next(iter(snapshot["exports"].items()))
    sources.verify_export(sources.ROOT / directory, expected)
    changed = dict(expected)
    changed.pop(next(iter(changed)))
    with pytest.raises(ValueError, match="complete source map"):
        sources.verify_export(sources.ROOT / directory, changed)
    key = next(iter(expected.values()))
    shutil.copytree(sources.ASSETS, tmp_path / "assets")
    monkeypatch.setattr(sources, "ASSETS", tmp_path / "assets")
    (sources.ASSETS / (key + ".txt")).write_text("tampered")
    with pytest.raises(ValueError, match="snapshot drift"):
        sources.verify_export(sources.ROOT / directory, expected)


def test_unknown_legacy_paths_cannot_materialize_or_read_a_scope():
    with pytest.raises(ValueError, match="Unknown historical"):
        sources.read_source_file(sources.ROOT / "tmp/unknown/manifest.private.json")


def test_cloud_entry_checks_lock_before_credentials_or_dispatch(monkeypatch, tmp_path):
    import run_work_envelope as entry
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(entry.getpass, "getpass", lambda *args: pytest.fail("Credentials read before identity check"))
    def reject():
        raise ValueError("Promotion identity drift")
    monkeypatch.setattr(check, "verify", reject)
    with pytest.raises(ValueError, match="identity drift"):
        entry.execute(tmp_path / "config", tmp_path / "proof", tmp_path,
                      profile_name="cce_paid_comparison", diagnostic_target=tmp_path / "target",
                      cce_kubeconfig=tmp_path / "kubeconfig", cce_snapshot=tmp_path / "snapshot")


def test_historical_verification_does_not_use_live_short_goal(monkeypatch):
    import cce_transaction_profile as transaction
    monkeypatch.setattr(transaction, "active", lambda: {"extension_decision": "ADR0255"})
    assert check.verify()["pass"] is True
    with pytest.raises(ValueError, match="Separate exact hourly"):
        check.hourly.plan()
