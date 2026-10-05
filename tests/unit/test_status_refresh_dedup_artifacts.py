import hashlib
import json
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_status_refresh_dedup as dedup


def artifact_copy(tmp_path, monkeypatch):
    target = tmp_path / "artifact"
    target.mkdir()
    for name in ("manifest.json", "adr0156.patch"):
        (target / name).write_bytes((dedup.ARTIFACTS / name).read_bytes())
    monkeypatch.setattr(dedup, "ARTIFACTS", target)
    return target


def test_dedup_runtime_keeps_every_unmodified_parent_module():
    parent, data = dedup.base.manifest(), dedup.manifest()
    assert set(data["runtime_source_sha256"]) == set(parent["runtime_source_sha256"])
    assert len(data["overlay_sha256"]) == 8
    for name, sha in parent["runtime_source_sha256"].items():
        if name not in data["overlay_sha256"]:
            assert data["runtime_source_sha256"][name] == sha


@pytest.mark.parametrize("field", ["schema", "parent", "dependencies", "runtime", "overlay", "patch"])
def test_modified_candidate_contract_fails_before_export(tmp_path, monkeypatch, field):
    target = artifact_copy(tmp_path, monkeypatch)
    data = json.loads((target / "manifest.json").read_text())
    if field == "schema":
        data["schema"] = True
    elif field == "parent":
        data["parent_manifest_sha256"] = "0" * 64
    elif field == "dependencies":
        data["dependency_input_sha256"]["requirements.lock"] = "0" * 64
    elif field == "runtime":
        data["runtime_source_sha256"]["src/ticketing/api.py"] = "0" * 64
    elif field == "overlay":
        data["overlay_sha256"].pop(".env.example")
    else:
        (target / "adr0156.patch").write_bytes((target / "adr0156.patch").read_bytes() + b"changed")
    (target / "manifest.json").write_text(json.dumps(data))
    with pytest.raises(ValueError):
        dedup.manifest()


@pytest.mark.parametrize("mode", ["escape", "symlink"])
def test_matching_digest_does_not_allow_unsafe_patch_targets(tmp_path, monkeypatch, mode):
    target = artifact_copy(tmp_path, monkeypatch)
    data = json.loads((target / "manifest.json").read_text())
    patch = (target / "adr0156.patch").read_bytes()
    if mode == "escape":
        patch = patch.replace(b"a/compose.yaml b/compose.yaml", b"a/../../escape b/../../escape", 1)
    else:
        patch += b"new mode 120000\n"
    (target / "adr0156.patch").write_bytes(patch)
    data["patch_sha256"] = hashlib.sha256(patch).hexdigest()
    (target / "manifest.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="patch paths|overlay only"):
        dedup.manifest()


def test_existing_output_is_not_overwritten(tmp_path):
    marker = tmp_path / "keep"
    marker.write_text("retained")
    with pytest.raises(ValueError, match="Fresh owned"):
        dedup.prepare(tmp_path)
    assert marker.read_text() == "retained"


def test_compose_inherits_disabled_dedup_flag():
    config = yaml.safe_load((dedup.base.ROOT / "compose.yaml").read_text(encoding="utf-8"))
    roles = {name: service["environment"] for name, service in config["services"].items()
             if "ORDER_STATUS_EVENT_REFRESH_DEDUP" in service.get("environment", {})}
    assert "api" in roles and "consumer" in roles
    assert all(env["ORDER_STATUS_EVENT_REFRESH_DEDUP"] == "${ORDER_STATUS_EVENT_REFRESH_DEDUP:-0}"
               for env in roles.values())
