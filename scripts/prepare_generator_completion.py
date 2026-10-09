"""ADR0226 deterministic two-edit correction of the pinned generator source."""
import difflib
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts/generator-completion"
PATH = "scripts/paid_ticket_load_generator.py"
PARENT_SHA256 = "0dec129dec0aa122bcb3642013b22e7c0d5127eb42023a7ddc8905f184b83c8f"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def corrected_source(raw):
    raw = raw.replace(b"\r\n", b"\n")
    if sha(raw) != PARENT_SHA256:
        raise ValueError("Exact retained frozen generator required")
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    text = raw.decode()
    replacements = [
        ("        def completed_task(task):\n            update_occupancy()",
         "        def completed_task(task):\n            if task not in active:\n                return\n            update_occupancy()"),
        ("            if len(active) >= args.concurrency:\n",
         "            for finished_task in tuple(active):\n                if finished_task.done():\n                    completed_task(finished_task)\n            if len(active) >= args.concurrency:\n"),
    ]
    for before, after in replacements:
        if text.count(before) != 1:
            raise ValueError("Frozen generator cleanup contract differs")
        text = text.replace(before, after, 1)
    candidate = text.encode()
    patch = ("diff --git a/" + PATH + " b/" + PATH + "\n" + "".join(difflib.unified_diff(
        raw.decode().splitlines(True), text.splitlines(True), fromfile="a/" + PATH, tofile="b/" + PATH))).encode()
    expected = {"schema": 1, "decision": "ADR0226", "parent_sha256": PARENT_SHA256,
        "candidate_sha256": sha(candidate), "patch_sha256": sha(patch), "changed_paths": [PATH],
        "scope": "Only idempotent completion cleanup and pre-admission completed-task reaping; frozen workload/backend/budgets unchanged."}
    if data != expected or (ARTIFACTS / "adr0226.patch").read_bytes() != patch:
        raise ValueError("Exact two-edit generator patch and manifest required")
    compile(candidate, PATH, "exec")
    return candidate


def corrected_bundle(bundle):
    return {**bundle, PATH: corrected_source(bundle[PATH])}
