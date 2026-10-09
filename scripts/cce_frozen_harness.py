"""ADR0238 portable, hash-checked historical load assets; never executes them."""
import hashlib
import json
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "artifacts/cce-frozen-harness"
PLAN = ROOT / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json"
REVISION = "deb330ec91e553640d1d0ba10e92aa8f29cd86dc"
QUALIFIED_HELPER_SHA256 = "10b14c979133188d42aaba5a9a91ab599101922e2d284e85dbe6c03244e3a5bc"


def read_asset(relative, expected):
    parts = PurePosixPath(relative)
    if parts.is_absolute() or ".." in parts.parts or "\\" in relative:
        raise ValueError("Canonical frozen asset path required")
    path = ASSETS / relative
    if path.is_symlink() or any(p.is_symlink() for p in path.parents if p != ASSETS.parent):
        raise ValueError("Frozen asset symlinks are forbidden")
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Frozen asset source drift: " + relative)
    return raw


def frozen_bundle():
    hashes = json.loads(PLAN.read_text(encoding="utf-8"))["harness_source_sha256"]
    manifest = json.loads((ASSETS / "manifest.json").read_text(encoding="utf-8"))
    if manifest != {"schema_version": 1, "decision": "ADR0238", "parent_revision": REVISION,
                    "parent_files": hashes, "qualified_helper_sha256": QUALIFIED_HELPER_SHA256} or len(hashes) != 78:
        raise ValueError("Exact frozen 78-file manifest required")
    return {path: read_asset(path, expected) for path, expected in hashes.items()}


def qualified_helper():
    return read_asset("qualified/checkout_journey_probe.py", QUALIFIED_HELPER_SHA256)
