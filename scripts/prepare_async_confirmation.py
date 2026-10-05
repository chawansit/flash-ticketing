"""Reproduce ADR0160 from qualified ADR0156; optional offline images, no cloud calls."""

import argparse
import ast
import hashlib
import json
import re
from pathlib import Path

import prepare_status_refresh_dedup as parent

base = parent.base
ARTIFACTS = base.ROOT / "artifacts/async-payment-confirmation"


def financial_body(raw, name):
    tree = ast.parse(raw)
    matches = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name]
    if len(matches) != 1:
        raise ValueError("Unique financial callback required")
    node = matches[0]
    body = node.body[0].body if name == "callback" else node.body[1:]
    return [ast.dump(n, include_attributes=False) for n in body]


def manifest():
    prior = parent.manifest()
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    expected_paths = {
        "src/ticketing/api.py", "src/ticketing/config.py", "src/ticketing/workers.py",
        "src/ticketing/observability.py", "src/ticketing/application/ports.py",
        "src/ticketing/infrastructure/reservations.py",
        "src/ticketing/application/payment_confirmation.py",
        "src/ticketing/infrastructure/payment_confirmation.py",
        "scripts/checkout_journey_probe.py", "migrations/009_payment_confirmation_receipts.sql",
        "tests/unit/test_payment_confirmation.py", "tests/integration/test_async_payment_confirmation.py",
        "tests/integration/test_payment_commit_recovery.py",
    }
    patch = (ARTIFACTS / "adr0160.patch").read_bytes()
    if (data.get("schema") != 1 or data.get("decision") != "ADR0160"
            or data.get("base_revision") != base.REVISION
            or data.get("parent_manifest_sha256") != base.sha((parent.ARTIFACTS / "manifest.json").read_bytes())
            or data.get("patch_sha256") != hashlib.sha256(patch).hexdigest()
            or set(data.get("overlay_sha256", {})) != expected_paths
            or data.get("migration_sha256") != {p: h for p, h in data["overlay_sha256"].items() if p.startswith("migrations/")}):
        raise ValueError("Pinned receipt overlay and qualified parent required")
    runtime = {**prior["runtime_source_sha256"], **{p: h for p, h in data["overlay_sha256"].items() if p.startswith("src/")}}
    if data.get("runtime_source_sha256") != runtime:
        raise ValueError("Complete inherited runtime map required")
    headers = re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE)
    if len(headers) != len(expected_paths) or set(headers) != {(p, p) for p in expected_paths}:
        raise ValueError("Exact bounded text patch required")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")) and line[4:] not in {"/dev/null", *(prefix + p for p in expected_paths for prefix in ("a/", "b/"))}:
            raise ValueError("Patch target escapes allowlist")
        if line.startswith(("old mode ", "new mode ", "rename from ", "rename to ", "copy from ", "copy to ")):
            raise ValueError("Text overlay only")
    return data


def prepare(output):
    overlay = manifest()
    output, data, prior = parent.prepare(output)
    source = output / "source"
    old = (source / "src/ticketing/infrastructure/reservations.py").read_text()
    for check in (True, False):
        args = ["git", "apply", "--whitespace=error", "--directory=" + source.relative_to(base.ROOT).as_posix()]
        if check:
            args.append("--check")
        base.command([*args, str(ARTIFACTS / "adr0160.patch")])
    for name in overlay["overlay_sha256"]:
        p = source / name
        p.write_bytes(p.read_bytes().replace(b"\r\n", b"\n"))
    expected = {**prior["source_file_sha256"], **overlay["overlay_sha256"]}
    base.verify_tree(source, expected)
    new = (source / "src/ticketing/infrastructure/reservations.py").read_text()
    if financial_body(old, "callback") != financial_body(new, "apply_callback"):
        raise ValueError("Financial transaction body changed")
    if "api_callback_acquisition_reserve" in (source / "src/ticketing/config.py").read_text():
        raise ValueError("Unqualified callback reserve wiring excluded")
    data.update(overlay_sha256=overlay["overlay_sha256"], runtime_source_sha256=overlay["runtime_source_sha256"],
                migration_sha256=overlay["migration_sha256"], patch_sha256=overlay["patch_sha256"])
    context = output / "image-context"
    for name in [*data["runtime_source_sha256"], *data["migration_sha256"]]:
        destination = context / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((source / name).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    receipt = {**prior, "decision": "ADR0160", "source_file_sha256": expected,
               "source_files_verified": len(expected), "runtime_modules_verified": len(data["runtime_source_sha256"]),
               "financial_transaction_body_ast_unchanged": True, "callback_reserve_wiring_excluded": True,
               "patch_sha256": overlay["patch_sha256"],
               "source_manifest_sha256": base.digest({"base_revision": base.REVISION, "runtime_source_sha256": data["runtime_source_sha256"]})}
    (output / "async-source-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
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
    print(json.dumps({"phase": "local-images-built" if images else "isolated-receipt-source-prepared",
                      "source_files_verified": receipt["source_files_verified"],
                      "runtime_modules_verified": receipt["runtime_modules_verified"],
                      "images_built": len(set(images["images"].values())) if images else 0,
                      "cloud_calls": 0}))


if __name__ == "__main__":
    main()
