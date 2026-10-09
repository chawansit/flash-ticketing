"""ADR0163 frozen five-file admission export; offline images only, no cloud calls."""

import argparse
import ast
import io
import json
import re
import subprocess
import tarfile
from pathlib import Path

import prepare_async_confirmation as parent

base = parent.base
ARTIFACTS = base.ROOT / "artifacts/partial-timeout-reclamation"
PATHS = {"src/ticketing/" + name for name in (
    "api.py", "config.py", "http.py", "observability.py", "infrastructure/postgres.py")}


def manifest():
    prior = parent.manifest()
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    patch = (ARTIFACTS / "adr0163.patch").read_bytes()
    keys = {"schema", "decision", "base_revision", "implementation_revision", "parent_manifest_sha256",
            "patch_sha256", "overlay_sha256", "runtime_source_sha256", "protected_source_sha256"}
    if (set(data) != keys or type(data["schema"]) is not int or data["schema"] != 1
            or data["decision"] != "ADR0163" or data["base_revision"] != base.REVISION
            or data["implementation_revision"] != "d444cc78e7d29c95d2c1878a40fdd6ba1d6ed569"
            or data["parent_manifest_sha256"] != base.sha((parent.ARTIFACTS / "manifest.json").read_bytes())
            or data["patch_sha256"] != base.sha(patch) or set(data["overlay_sha256"]) != PATHS
            or any(not isinstance(h, str) or not re.fullmatch(r"[0-9a-f]{64}", h)
                   for h in data["overlay_sha256"].values())):
        raise ValueError("Exact frozen admission overlay required")
    expected = {**prior["runtime_source_sha256"], **data["overlay_sha256"]}
    protected = {"src/ticketing/" + name for name in (
        "application/reservations.py", "infrastructure/reservations.py",
        "infrastructure/cache.py", "infrastructure/payment_confirmation.py")}
    if (data["runtime_source_sha256"] != expected
            or data["protected_source_sha256"] != {p: prior["runtime_source_sha256"][p] for p in protected}):
        raise ValueError("Complete runtime and unchanged authority map required")
    headers = re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE)
    if len(headers) != len(PATHS) or set(headers) != {(p, p) for p in PATHS}:
        raise ValueError("Five unique allowlisted text targets required")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")) and line[4:] not in {prefix + p for p in PATHS for prefix in ("a/", "b/")}:
            raise ValueError("Patch escaped allowlist")
        if line.startswith(("old mode ", "new mode ", "new file mode ", "deleted file mode ",
                            "rename from ", "rename to ", "copy from ", "copy to ")):
            raise ValueError("Text-only overlay required")
    return data



def expected_source_map():
    """Derive every input from immutable Git and pinned overlays, not a mutable receipt."""
    from cce_historical_sources import parent_archive
    raw = parent_archive()
    expected = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > 10000 or sum(m.size for m in members) > 128 * 1024 * 1024:
            raise ValueError("Bounded immutable source archive required")
        for member in members:
            if member.isdir():
                continue
            path = Path(member.name)
            if not member.isfile() or path.is_absolute() or ".." in path.parts or ":" in member.name or member.name in expected:
                raise ValueError("Immutable archive contains unsafe input")
            expected[member.name] = base.sha(archive.extractfile(member).read())
    for data in (base.manifest(), parent.parent.manifest(), parent.manifest(), manifest()):
        expected.update(data["overlay_sha256"])
    return expected


def method_ast(raw, cls, name):
    tree = ast.parse(raw)
    body = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls).body
    return ast.dump(next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name), include_attributes=False)


def verify_port(original, candidate, revision):
    if method_ast(original, "Postgres", "transaction") != method_ast(candidate, "Postgres", "transaction"):
        raise ValueError("Financial transaction changed")
    for cls, name in (("SharedAcquisitionBudget", "release"), ("SharedAcquisitionBudget", "snapshot")):
        if method_ast(original, cls, name) != method_ast(candidate, cls, name):
            raise ValueError("Original accounting recovery changed")
    qualified = subprocess.check_output(
        ["git", "show", revision + ":src/ticketing/infrastructure/postgres.py"], cwd=base.ROOT, timeout=15
    ).decode()
    for cls, name in (("Postgres", "connection"), ("SharedAcquisitionBudget", "_refresh"),
                      ("SharedAcquisitionBudget", "failure_snapshot"), ("AcquisitionLimitedPool", "getconn"),
                      ("AcquisitionLimitedPool", "_record_failure")):
        if method_ast(qualified, cls, name) != method_ast(candidate, cls, name):
            raise ValueError("Qualified reclamation/diagnostic method differs")
    if "_groups" in candidate or "self.callback_reserved" in candidate:
        raise ValueError("Callback reserve code excluded from isolated guard")


def prepare(output):
    overlay = manifest()
    output, data, prior = parent.prepare(output)
    source = output / "source"
    pg = "src/ticketing/infrastructure/postgres.py"
    original = (source / pg).read_text()
    for check in (True, False):
        args = ["git", "apply", "--whitespace=error", "--directory=" + source.relative_to(base.ROOT).as_posix()]
        if check:
            args.append("--check")
        base.command([*args, str(ARTIFACTS / "adr0163.patch")])
    for name in PATHS:
        p = source / name
        p.write_bytes(p.read_bytes().replace(b"\r\n", b"\n"))
    expected = {**prior["source_file_sha256"], **overlay["overlay_sha256"]}
    base.verify_tree(source, expected)
    verify_port(original, (source / pg).read_text(), overlay["implementation_revision"])
    if "api_callback_acquisition_reserve" in (source / "src/ticketing/config.py").read_text():
        raise ValueError("Unqualified callback reserve wiring excluded")
    data.update(overlay_sha256={**data["overlay_sha256"], **overlay["overlay_sha256"]},
                runtime_source_sha256=overlay["runtime_source_sha256"], patch_sha256=overlay["patch_sha256"])
    context = output / "image-context"
    for name in data["runtime_source_sha256"]:
        (context / name).write_bytes((source / name).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    receipt = {**prior, "decision": "ADR0163", "source_file_sha256": expected,
               "source_files_verified": len(expected), "runtime_modules_verified": len(data["runtime_source_sha256"]),
               "financial_transaction_ast_unchanged": True, "qualified_method_ast_parity": True,
               "callback_reserve_wiring_excluded": True, "patch_sha256": overlay["patch_sha256"],
               "source_manifest_sha256": base.digest({"base_revision": base.REVISION,
                                                     "runtime_source_sha256": data["runtime_source_sha256"]})}
    (output / "admission-source-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return output, data, receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--parents", type=Path)
    args = parser.parse_args()
    if args.build and args.parents is None:
        parser.error("Offline build requires immutable original parent map")
    parents = json.loads(args.parents.read_text()) if args.parents else None
    if args.build:
        base.validate_parents(parents)
    output, data, receipt = prepare(args.output)
    images = base.build(output, data, parents) if args.build else None
    if images:
        for key in ("images", "parent_images"):
            images[key]["confirmation"] = images[key]["simulator"]
        (output / "artifact-receipt.json").write_text(json.dumps(images, indent=2) + "\n")
    print(json.dumps({"phase": "offline-images-built" if images else "isolated-admission-source-prepared",
                      "source_files_verified": receipt["source_files_verified"],
                      "runtime_modules_verified": receipt["runtime_modules_verified"],
                      "images_built": len(set(images["images"].values())) if images else 0, "cloud_calls": 0}))


if __name__ == "__main__":
    main()
