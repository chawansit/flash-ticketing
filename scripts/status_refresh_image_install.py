"""Offline image-only source installation/verification for ADR0152."""

import base64
import csv
import hashlib
import importlib.metadata
import json
import shutil
import sys
import sysconfig
from pathlib import Path


def sha(data):
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def roots():
    app = Path("/app/src/ticketing")
    dist = importlib.metadata.distribution("flash-ticketing")
    installed = Path(dist.locate_file("ticketing"))
    purelib = Path(sysconfig.get_path("purelib"))
    if app.is_symlink() or installed.is_symlink() or installed.resolve() != purelib.resolve() / "ticketing":
        raise ValueError("Expected installed package root required")
    if not app.is_dir() or not installed.is_dir():
        raise ValueError("Both package roots required")
    return app, installed, dist


def package_files(root):
    files = list(root.rglob("*"))
    if any(p.is_symlink() for p in files):
        raise ValueError("Package symlink rejected")
    return {p.relative_to(root).as_posix(): p for p in files if p.is_file() and p.suffix == ".py"}


def verify(manifest, *, parent):
    expected = manifest["parent_runtime_source_sha256" if parent else "runtime_source_sha256"]
    relative = {p.removeprefix("src/ticketing/"): v for p, v in expected.items()}
    app, installed, _dist = roots()
    for root in (app, installed):
        actual = package_files(root)
        if set(actual) != set(relative) or any(sha(actual[p].read_bytes()) != h for p, h in relative.items()):
            raise ValueError("Complete package source differs")
    if any(sha((Path("/app") / p).read_bytes()) != h for p, h in manifest["dependency_input_sha256"].items()):
        raise ValueError("Frozen dependency inputs differ")
    migrations = manifest.get("migration_sha256", {})
    if not parent and any(sha((Path("/app") / p).read_bytes()) != h for p, h in migrations.items()):
        raise ValueError("Pinned additive migration differs")
    return {"app_and_installed_modules_verified": len(expected), "dependency_inputs_match": True,
            **({"migration_inputs_match": True} if migrations else {})}


def install(payload):
    manifest = json.loads((payload / "manifest.json").read_text())
    verify(manifest, parent=True)
    expected = manifest["runtime_source_sha256"]
    paths = {p.removeprefix("src/ticketing/"): h for p, h in expected.items()}
    source = payload / "src/ticketing"
    supplied = package_files(source)
    if set(supplied) != set(paths) or any(sha(supplied[p].read_bytes()) != h for p, h in paths.items()):
        raise ValueError("Qualified payload source differs")
    app, installed, dist = roots()
    for root in (app, installed):
        for relative, path in supplied.items():
            target = root / relative
            if not target.resolve().is_relative_to(root.resolve()):
                raise ValueError("Source path escapes package")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
            target.chmod(0o644)
        for cache in list(root.rglob("__pycache__")):
            if not cache.resolve().is_relative_to(root.resolve()) or cache.is_symlink():
                raise ValueError("Unsafe package bytecode cache")
            shutil.rmtree(cache)
        for cached in root.rglob("*.pyc"):
            cached.unlink()
    # Preserve distribution identity/dependencies while updating source hashes and removing retired bytecode rows.
    records = [f for f in (dist.files or []) if f.as_posix().endswith(".dist-info/RECORD")]
    if len(records) != 1:
        raise ValueError("Single installed distribution RECORD required")
    record = Path(dist.locate_file(records[0]))
    if record.is_symlink() or not record.resolve().is_relative_to(installed.parent.resolve()):
        raise ValueError("Unsafe installed RECORD")
    rows = list(csv.reader(record.read_text().splitlines()))
    replacements = {}
    for relative in paths:
        raw = (installed / relative).read_bytes()
        digest = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()
        replacements["ticketing/" + relative] = ["ticketing/" + relative, "sha256=" + digest, str(len(raw))]
    retained = [
        r
        for r in rows
        if r[0] not in replacements and not (r[0].startswith("ticketing/") and r[0].endswith(".pyc"))
    ]
    with record.open("w", newline="") as stream:
        csv.writer(stream).writerows(sorted([*retained, *replacements.values()]))
    for relative, expected_hash in manifest.get("migration_sha256", {}).items():
        if relative != "migrations/009_payment_confirmation_receipts.sql":
            raise ValueError("Only explicitly pinned additive receipt migration allowed")
        source_migration = payload / relative
        target = Path("/app") / relative
        if source_migration.is_symlink() or target.is_symlink() or sha(source_migration.read_bytes()) != expected_hash:
            raise ValueError("Migration payload drift")
        if target.exists() and sha(target.read_bytes()) != expected_hash:
            raise ValueError("Never overwrite a different migration")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_migration.read_bytes())
        target.chmod(0o644)
    return verify(manifest, parent=False)


if __name__ == "__main__":
    print(json.dumps(install(Path(sys.argv[1]))))
