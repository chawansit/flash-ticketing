import hashlib
import io
import json
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import stage_status_refresh_images as stage

OWNER = "/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa"


@pytest.mark.parametrize("size", [True, 0, -1, stage.MAX_ARCHIVE + 1])
def test_creation_rejects_unbounded_archive(size):
    with pytest.raises(ValueError):
        stage.create_owner_program(OWNER, size)


@pytest.mark.parametrize("digest", ["short", "g" * 64])
def test_seal_rejects_invalid_digest(digest):
    with pytest.raises(ValueError):
        stage.seal_program(OWNER, 4, digest)


@pytest.mark.parametrize("drift", [None, "digest", "size", "extra"])
def test_archive_seal_checks_contents_before_any_load(tmp_path, capsys, drift):
    owner = tmp_path / "adr0153-parents-aaaaaaaaaaaa"
    owner.mkdir()
    p = owner / "images.tar"
    p.write_bytes(b"known")
    digest = hashlib.sha256(b"known").hexdigest()
    size = 5
    if drift == "digest":
        digest = "a" * 64
    if drift == "size":
        size = 6
    if drift == "extra":
        (owner / "unowned").write_text("preserve")
    program = stage.seal_program(OWNER, size, digest).replace(
        "Path(" + repr(OWNER) + ")", "Path(" + repr(str(owner)) + ")"
    )
    # Windows cannot express Unix0600. Emulate only the mode check; retain all other real stat/hash checks.
    program = program.replace("stat.S_IMODE(s.st_mode)!=0o600", "False")
    if drift:
        with pytest.raises(ValueError):
            exec(program, {})  # noqa: S102
        assert p.exists()
    else:
        exec(program, {})  # noqa: S102
        receipt = json.loads(capsys.readouterr().out)
        assert receipt["sha256"] == digest and receipt["seal"]["size"] == 5


@pytest.mark.parametrize("drift", [None, "seal", "digest"])
def test_load_never_runs_for_changed_seal_or_content(tmp_path, monkeypatch, capsys, drift):
    owner = tmp_path / "adr0153-parents-aaaaaaaaaaaa"
    owner.mkdir()
    p = owner / "images.tar"
    p.write_bytes(b"known")
    s = p.stat()
    receipt = {
        "owner": OWNER,
        "sha256": hashlib.sha256(b"known").hexdigest(),
        "seal": {
            "dev": s.st_dev,
            "ino": s.st_ino,
            "size": s.st_size,
            "mtime_ns": s.st_mtime_ns,
            "uid": s.st_uid,
            "mode": stat.S_IMODE(s.st_mode),
        },
    }
    if drift == "seal":
        receipt["seal"]["ino"] += 1
    if drift == "digest":
        receipt["sha256"] = "a" * 64
    image = "sha256:" + "a" * 64
    program = stage.load_program(receipt, [image]).replace(
        "Path(" + repr(OWNER) + ")", "Path(" + repr(str(owner)) + ")"
    )
    program = program.replace("stat.S_IMODE(s.st_mode)!=0o600", "False")
    calls = []
    monkeypatch.setattr(stage.command.__globals__["subprocess"], "run", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(
        stage.command.__globals__["subprocess"], "check_output", lambda *a, **kw: json.dumps([{"Id": image}])
    )
    if drift:
        with pytest.raises(ValueError):
            exec(program, {})  # noqa: S102
        assert calls == []
    else:
        exec(program, {})  # noqa: S102
        assert len(calls) == 1 and json.loads(capsys.readouterr().out)["loaded_images"] == [image]


@pytest.mark.parametrize("drift", [None, "digest", "size", "existing"])
def test_upload_is_exclusive_and_checks_bytes_without_retry(tmp_path, drift):
    path = tmp_path / "images.tar"
    path.write_bytes(b"known archive")
    size = path.stat().st_size
    digest = stage.hash_file(path)
    if drift == "digest":
        digest = "a" * 64
    if drift == "size":
        size += 1
    destination = io.BytesIO()
    destination.set_pipelined = lambda _value: None
    written = []
    destination.close = lambda: written.append(destination.getvalue())
    calls = []

    def opened(remote, mode):
        calls.append((remote, mode))
        if drift == "existing":
            raise FileExistsError("preserve existing")
        return destination

    closed = []
    sftp = SimpleNamespace(
        open=opened,
        chmod=lambda *a: None,
        get_channel=lambda: SimpleNamespace(settimeout=lambda n: None),
        close=lambda: closed.append(True),
    )
    session = SimpleNamespace(clients={"primary": SimpleNamespace(open_sftp=lambda: sftp)})
    if drift:
        with pytest.raises((ValueError, FileExistsError)):
            stage.upload(session, "primary", OWNER, path, size, digest)
        assert len(calls) <= 1
    else:
        result = stage.upload(session, "primary", OWNER, path, size, digest)
        assert result == {"bytes": size, "sha256": digest}
        assert written == [path.read_bytes()] and calls == [(OWNER + "/images.tar", "wx")]
    assert closed == ([] if drift == "size" else [True])


def test_generated_programs_compile_and_never_deploy_services():
    seal = {"owner": OWNER, "sha256": "a" * 64, "seal": {"size": 1}}
    for code in (
        stage.create_owner_program(OWNER, 1),
        stage.seal_program(OWNER, 1, "a" * 64),
        stage.load_program(seal, ["sha256:" + "a" * 64]),
        stage.secondary_inventory(),
    ):
        compile(code, "staging_remote", "exec")
        assert "'compose'" not in code and "'pull'" not in code and "'up'" not in code
    assert "flash-ticketing-api-secondary" in stage.secondary_inventory()
