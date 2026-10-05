import copy
import csv
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_status_refresh_artifacts as artifacts
import status_refresh_image_install as installer


@pytest.fixture
def metadata():
    return artifacts.manifest()


def test_fresh_git_export_reproduces_all_qualified_bytes_without_old_tmp_tree(
    tmp_path, monkeypatch, metadata
):
    monkeypatch.setattr(artifacts, "ROOT", artifacts.ROOT)
    # Real export and patch application, under a fresh owned path; never reads the ignored old tree.
    output = artifacts.ROOT / "tmp" / ("adr0152-test-" + uuid4().hex[:12])
    output = artifacts.owned_output(output)
    source, expected = artifacts.export_source(output, metadata)
    assert len(expected) == 207
    assert len(metadata["runtime_source_sha256"]) == 19
    for path, digest in metadata["overlay_sha256"].items():
        assert artifacts.sha((source / path).read_bytes()) == digest
    assert (
        artifacts.sha((source / "src/ticketing/api.py").read_bytes())
        == metadata["parent_runtime_source_sha256"]["src/ticketing/api.py"]
    )
    artifacts.verify_tree(source, expected)
    (source / "src/ticketing/api.py").write_text("drift")
    with pytest.raises(ValueError, match="drift"):
        artifacts.verify_tree(source, expected)


@pytest.mark.parametrize("drift", ["digest", "qualified", "path", "extra-path"])
def test_artifact_patch_or_source_drift_rejected_before_export(tmp_path, monkeypatch, drift):
    source = artifacts.ARTIFACTS
    copied = tmp_path / "artifacts"
    copied.mkdir()
    data = json.loads((source / "manifest.json").read_text())
    patch = (source / "adr0149.patch").read_bytes()
    if drift == "digest":
        patch += b"\n"
    elif drift == "qualified":
        data["overlay_sha256"]["src/ticketing/workers.py"] = "0" * 64
    else:
        if drift == "path":
            patch = patch.replace(b"a/src/ticketing/workers.py", b"a/../../secret")
        else:
            patch += b"diff --git a/.env b/.env\n"
        data["patch_sha256"] = hashlib.sha256(patch).hexdigest()
    (copied / "manifest.json").write_text(json.dumps(data))
    (copied / "adr0149.patch").write_bytes(patch)
    monkeypatch.setattr(artifacts, "ARTIFACTS", copied)
    with pytest.raises(ValueError):
        artifacts.manifest()


def test_source_tree_rejects_extra_file_and_source_output_cannot_overwrite(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.py").write_text("ok")
    expected = {"a.py": artifacts.sha(b"ok")}
    artifacts.verify_tree(source, expected)
    (source / "secret.env").write_text("must not copy")
    with pytest.raises(ValueError):
        artifacts.verify_tree(source, expected)
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    with pytest.raises(ValueError):
        artifacts.owned_output(tmp_path / "outside")
    owned = artifacts.owned_output(tmp_path / "tmp/owned")
    with pytest.raises(ValueError):
        artifacts.owned_output(owned)


@pytest.mark.parametrize("user", ["", "root", "0", "ticketing", "10001:10001"])
def test_build_recipe_preserves_user_and_installs_offline_without_pip(user):
    text = artifacts.dockerfile({"Config": {"User": user}}, "local:parent", "a" * 64)
    assert "pip" not in text and "apt" not in text
    if user in {"", "root", "0"}:
        assert "USER" not in text
    else:
        assert text.splitlines()[-1] == "USER " + user
        assert "USER root" in text


@pytest.mark.parametrize("config", [{"User": "root\nRUN bad"}, {"User": "root", "OnBuild": ["RUN bad"]}])
def test_unsafe_parent_build_configuration_rejected(config):
    with pytest.raises(ValueError):
        artifacts.dockerfile({"Config": config}, "local:parent", "a" * 64)


@pytest.mark.parametrize("drift", ["layers", "user", "env", "cmd", "entrypoint", "label", "healthcheck"])
def test_derived_image_retains_all_inherited_configuration_and_parent_layers(drift):
    parent = {
        "RootFS": {"Layers": ["base"]},
        "Config": {
            "User": "10001",
            "Env": ["A=B"],
            "Cmd": ["api"],
            "Entrypoint": None,
            "Labels": {"base": "yes"},
            "Healthcheck": None,
        },
    }
    candidate = copy.deepcopy(parent)
    candidate["RootFS"]["Layers"].append("overlay")
    candidate["Config"]["Labels"][artifacts.LABEL] = "a" * 64
    artifacts.verify_derived(parent, candidate, "a" * 64)
    if drift == "layers":
        candidate["RootFS"]["Layers"][0] = "other"
    else:
        key = {
            "user": "User",
            "env": "Env",
            "cmd": "Cmd",
            "entrypoint": "Entrypoint",
            "healthcheck": "Healthcheck",
        }.get(drift)
        if key:
            candidate["Config"][key] = "changed"
        else:
            candidate["Config"]["Labels"][artifacts.LABEL] = "b" * 64
    with pytest.raises(ValueError):
        artifacts.verify_derived(parent, candidate, "a" * 64)


@pytest.mark.parametrize("failed", [False, True])
def test_partial_build_never_writes_deployment_receipt_and_shared_parents_deduplicated(
    tmp_path, monkeypatch, metadata, failed
):
    parents = dict.fromkeys(artifacts.ROLES, artifacts.IMAGE)
    parents["consumer"] = "sha256:" + "a" * 64
    monkeypatch.setattr(artifacts, "inspect_image", lambda *_a: {})
    monkeypatch.setattr(artifacts, "probe", lambda *_a, **_kw: {})
    calls = []

    def build_one(_folder, _data, parent):
        calls.append(parent)
        if failed and len(calls) == 2:
            raise ValueError("build failed")
        return "sha256:" + ("b" if parent == artifacts.IMAGE else "c") * 64, {}

    monkeypatch.setattr(artifacts, "build_one", build_one)
    if failed:
        with pytest.raises(ValueError, match="build failed"):
            artifacts.build(tmp_path, metadata, parents)
        assert not (tmp_path / "artifact-receipt.json").exists()
    else:
        receipt = artifacts.build(tmp_path, metadata, parents)
        assert len(calls) == 2 and set(receipt["images"]) == set(artifacts.ROLES)
        assert receipt["images"]["api"] == receipt["images"]["publisher"]


def test_missing_parent_aborts_before_any_build_or_receipt(tmp_path, monkeypatch, metadata):
    monkeypatch.setattr(artifacts, "inspect_image", lambda *_a: (_ for _ in ()).throw(ValueError("missing")))
    monkeypatch.setattr(artifacts, "build_one", lambda *_a: pytest.fail("no builds allowed"))
    with pytest.raises(ValueError, match="missing"):
        artifacts.build(tmp_path, metadata, dict.fromkeys(artifacts.ROLES, artifacts.IMAGE))
    assert not (tmp_path / "artifact-receipt.json").exists()


@pytest.mark.parametrize(
    "parents", [None, {}, {"api": "local:tag"}, dict.fromkeys(artifacts.ROLES, "sha256:" + "a" * 64)]
)
def test_test_parent_or_incomplete_role_map_cannot_emit_cloud_receipt(parents):
    with pytest.raises(ValueError):
        artifacts.validate_parents(parents)


def test_installer_updates_both_roots_clears_bytecode_and_preserves_distribution(tmp_path, monkeypatch):
    app, installed = tmp_path / "app/src/ticketing", tmp_path / "site/ticketing"
    info = tmp_path / "site/flash_ticketing-0.1.0.dist-info"
    info.mkdir(parents=True)
    metadata = info / "METADATA"
    metadata.write_text("Name: flash-ticketing\nVersion: 0.1.0\n")
    record = info / "RECORD"
    record.write_text(
        "ticketing/__init__.py,,\nticketing/mod.py,,\nticketing/__pycache__/mod.pyc,,\nflash_ticketing-0.1.0.dist-info/METADATA,,\nflash_ticketing-0.1.0.dist-info/RECORD,,\n"
    )
    for root in (app, installed):
        root.mkdir(parents=True)
        (root / "__init__.py").write_text("")
        (root / "mod.py").write_text("old = True\n")
        (root / "__pycache__").mkdir()
        (root / "__pycache__/mod.pyc").write_bytes(b"stale")
    dep = tmp_path / "app/requirements.lock"
    dep.write_text("pinned\n")
    data = {
        "parent_runtime_source_sha256": {
            "src/ticketing/__init__.py": installer.sha(b""),
            "src/ticketing/mod.py": installer.sha(b"old = True\n"),
        },
        "runtime_source_sha256": {
            "src/ticketing/__init__.py": installer.sha(b""),
            "src/ticketing/mod.py": installer.sha(b"new = True\n"),
            "src/ticketing/projector.py": installer.sha(b"project = True\n"),
        },
        "dependency_input_sha256": {"requirements.lock": installer.sha(dep.read_bytes())},
    }
    payload = tmp_path / "payload"
    source = payload / "src/ticketing"
    source.mkdir(parents=True)
    for name, text in (("__init__.py", ""), ("mod.py", "new = True\n"), ("projector.py", "project = True\n")):
        (source / name).write_text(text)
    (payload / "manifest.json").write_text(json.dumps(data))
    fake_dist = SimpleNamespace(
        files=[Path("flash_ticketing-0.1.0.dist-info/RECORD")], locate_file=lambda p: installed.parent / p
    )
    monkeypatch.setattr(installer, "roots", lambda: (app, installed, fake_dist))
    monkeypatch.setattr(installer, "Path", lambda p: tmp_path / "app" if p == "/app" else Path(p))
    result = installer.install(payload)
    assert result["app_and_installed_modules_verified"] == 3
    for root in (app, installed):
        assert (root / "projector.py").read_bytes() == (source / "projector.py").read_bytes()
        assert not list(root.rglob("*.pyc"))
    assert metadata.read_text() == "Name: flash-ticketing\nVersion: 0.1.0\n"
    rows = list(csv.reader(record.read_text().splitlines()))
    assert any(r[0] == "ticketing/projector.py" and r[1].startswith("sha256=") for r in rows)
    assert not any(r[0].endswith(".pyc") for r in rows)


@pytest.mark.parametrize("drift", ["secret", "installer", "runtime"])
def test_minimal_build_context_rejects_secret_and_installer_drift(tmp_path, metadata, drift):
    context = tmp_path / "context"
    context.mkdir()
    source = artifacts.ROOT / json.loads(artifacts.PLAN.read_text())["isolated_source_directory"]
    for path in metadata["runtime_source_sha256"]:
        target = context / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((source / path).read_bytes())
    (context / "install.py").write_bytes(artifacts.INSTALLER.read_bytes())
    (context / "manifest.json").write_text(json.dumps(metadata, sort_keys=True) + "\n")
    recipe = "FROM local:parent\n"
    (context / "Dockerfile").write_text(recipe)
    artifacts.validate_context(context, metadata, recipe)
    changed = {"secret": "secret.env", "installer": "install.py", "runtime": "src/ticketing/api.py"}[drift]
    (context / changed).write_text("unexpected content")
    with pytest.raises(ValueError):
        artifacts.validate_context(context, metadata, recipe)


@pytest.mark.parametrize("bad", ["traversal", "symlink", "duplicate", "drive"])
def test_source_archive_rejects_unsafe_or_duplicate_members_before_patch(
    tmp_path, monkeypatch, metadata, bad
):
    blob = io.BytesIO()
    with tarfile.open(fileobj=blob, mode="w") as archive:
        name = {"traversal": "src/../../outside", "drive": "C:/outside"}.get(bad, "src/ticketing/x.py")
        info = tarfile.TarInfo(name)
        if bad == "symlink":
            info.type = tarfile.SYMTYPE
            info.linkname = "../../outside"
            archive.addfile(info)
        else:
            info.size = 2
            archive.addfile(info, io.BytesIO(b"ok"))
            if bad == "duplicate":
                archive.addfile(info, io.BytesIO(b"ok"))
    monkeypatch.setattr(
        artifacts,
        "command",
        lambda args, **_kw: blob.getvalue() if args[1] == "archive" else pytest.fail("patch must never run"),
    )
    with pytest.raises(ValueError):
        artifacts.export_source(tmp_path, metadata)
    assert not (tmp_path.parent / "outside").exists()
