"""ADR0238 verified public source snapshots; no old scope is materialized."""
import hashlib
import io
import json
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "artifacts/cce-historical-sources"


def manifest():
    data = json.loads((ASSETS / "manifest.json").read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or data.get("decision") != "ADR0238" or data.get("base_revision") != "deb330ec91e553640d1d0ba10e92aa8f29cd86dc":
        raise ValueError("Exact public historical source snapshot required")
    return data


def blob(digest):
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("Canonical source hash required")
    path = ASSETS / (digest + ".txt")
    if path.is_symlink():
        raise ValueError("Historical source symlink forbidden")
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("Historical source snapshot drift")
    return raw


def parent_file(relative):
    return blob(manifest()["base"][relative])


def parent_archive():
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:") as archive:
        for name, digest in sorted(manifest()["base"].items()):
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or "\\" in name:
                raise ValueError("Canonical historical source path required")
            raw = blob(digest)
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(raw), 0o644
            archive.addfile(info, io.BytesIO(raw))
    return output.getvalue()


def export_map(source):
    relative = Path(source).resolve().relative_to(ROOT.resolve()).as_posix()
    data = manifest()["exports"].get(relative)
    if data is None:
        raise ValueError("Unknown historical source export")
    return data


def read_source_file(path):
    path = Path(path)
    if path.is_file():
        if path.is_symlink():
            raise ValueError("Source symlink forbidden")
        return path.read_bytes().replace(b"\r\n", b"\n")
    relative = path.resolve().relative_to(ROOT.resolve()).as_posix()
    data = manifest()
    if relative in data["receipts"]:
        return blob(data["receipts"][relative])
    for directory, files in data["exports"].items():
        if relative.startswith(directory + "/"):
            return blob(files[relative[len(directory) + 1:]])
    raise ValueError("Unknown historical source file")


def verify_export(source, expected):
    actual = export_map(source)
    if actual != expected:
        raise ValueError("Historical complete source map differs")
    for digest in actual.values():
        blob(digest)
