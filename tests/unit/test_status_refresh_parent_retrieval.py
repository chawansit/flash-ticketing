import hashlib
import io
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import fetch_status_refresh_parents as fetch
from prepare_status_refresh_artifacts import IMAGE


def inventory():
    return {
        "revision": fetch.REVISION,
        "groups": {
            r: [{"image": IMAGE, "id": f"{i:064x}", "started_at": "now"} for i in range(n)]
            for r, n in fetch.NORMAL_COUNTS.items()
        },
    }


@pytest.mark.parametrize("drift", [None, "revision", "count", "same-role-image", "api-image"])
def test_only_original_normal_topology_can_export_parents(drift):
    data = inventory()
    if drift == "revision":
        data["revision"] = "changed"
    elif drift == "count":
        data["groups"]["api"].pop()
    elif drift == "same-role-image":
        data["groups"]["api"][0]["image"] = "sha256:" + "a" * 64
    elif drift == "api-image":
        for row in data["groups"]["api"]:
            row["image"] = "sha256:" + "a" * 64
    if drift:
        with pytest.raises(ValueError):
            fetch.parents_from_inventory(data)
    else:
        parents = fetch.parents_from_inventory(data)
        assert len(parents) == 7 and parents["reservation-writer"] == parents["api"]


@pytest.mark.parametrize(
    "owner",
    [
        "/tmp/other",
        "/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa/../other",
        "relative/tmp/adr0153-parents-aaaaaaaaaaaa",
        "/root/repo/other/adr0153-parents-aaaaaaaaaaaa",
    ],
)
def test_unsafe_remote_export_owner_rejected_before_remote_code(owner):
    with pytest.raises(ValueError):
        fetch.export_program(owner, fetch.parents_from_inventory(inventory()))
    with pytest.raises(ValueError):
        fetch.cleanup_program({"owner": owner})


@pytest.mark.parametrize("changed", [False, True])
def test_cleanup_removes_only_exact_sealed_archive_and_empty_owner(tmp_path, changed):
    owner = tmp_path / "adr0153-parents-aaaaaaaaaaaa"
    owner.mkdir()
    archive = owner / "images.tar"
    archive.write_bytes(b"original archive")
    s = archive.stat()
    seal = {
        "dev": s.st_dev,
        "ino": s.st_ino,
        "size": s.st_size,
        "mtime_ns": s.st_mtime_ns,
        "uid": s.st_uid,
        "mode": stat.S_IMODE(s.st_mode),
    }
    program = fetch.cleanup_program({"owner": "/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa", "seal": seal})
    program = program.replace(
        "Path('/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa')", "Path(" + repr(str(owner)) + ")"
    )
    if changed:
        archive.write_bytes(b"changed archive")
        with pytest.raises(ValueError, match="identity differs"):
            exec(program, {})  # noqa: S102 - execute self-generated cleanup only against this owned fixture
        assert archive.exists()
    else:
        exec(program, {})  # noqa: S102 - execute self-generated cleanup only against this owned fixture
        assert not owner.exists()


@pytest.mark.parametrize("drift", [None, "hash", "size", "exceeded", "existing"])
def test_transfer_accepts_only_exclusive_size_and_hash_verified_file(tmp_path, drift):
    raw = b"known archive"
    receipt = {
        "owner": "/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "seal": {"size": len(raw)},
    }
    target = tmp_path / "images.tar"
    if drift == "hash":
        receipt["sha256"] = "a" * 64
    elif drift == "size":
        raw = raw[:-1]
    elif drift == "exceeded":
        raw += b"unexpected"
    elif drift == "existing":
        target.write_bytes(b"must preserve")
    source = io.BytesIO(raw)
    source.stat = lambda: SimpleNamespace(st_size=receipt["seal"]["size"])
    closed = []
    sftp = SimpleNamespace(
        open=lambda *_a: source,
        get_channel=lambda: SimpleNamespace(settimeout=lambda _n: None),
        close=lambda: closed.append(True),
    )
    session = SimpleNamespace(clients={"primary": SimpleNamespace(open_sftp=lambda: sftp)})
    if drift:
        with pytest.raises((ValueError, FileExistsError)):
            fetch.transfer(session, receipt, target)
        if drift == "existing":
            assert target.read_bytes() == b"must preserve"
    else:
        result = fetch.transfer(session, receipt, target)
        assert result["sha256"] == receipt["sha256"] and target.read_bytes() == raw
    assert closed == [True]


def test_inventory_and_export_code_are_compilable_and_have_no_deployment():
    parents = fetch.parents_from_inventory(inventory())
    code = fetch.export_program("/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa", parents)
    compile(code, "remote_export", "exec")
    compile(fetch.inventory_program("/root/repo"), "remote_inventory", "exec")
    assert "'save'" in code and "'--output'" in code and "'pull'" not in code and "'compose'" not in code
