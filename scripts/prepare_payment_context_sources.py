"""ADR0249 one payment-method overlay on the accepted 22-module API source."""
import ast
import hashlib
import json
from pathlib import Path

from prepare_slot_comparison_sources import pairs as original_pairs
from stage_status_refresh_images import new_stage_output

ROOT = Path(__file__).resolve().parents[1]
RECEIPT = "docs/capacity/cce/slot-comparison-images-2026-10-09.json"
EVIDENCE = "docs/capacity/cce/payment-context-query-profile-2026-10-09.json"
MODULE = "src/ticketing/infrastructure/reservations.py"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def payment_method(source):
    cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef)
               and n.name == "PostgresReservations")
    return next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "initiate_payment")


def pairs():
    receipt = json.loads((ROOT / RECEIPT).read_text())
    evidence = json.loads((ROOT / EVIDENCE).read_text())
    old, old_plan = original_pairs()
    control = old["control"]
    expected = receipt["images"]["control"]
    if {name: sha(raw) for name, raw in control.items()} != expected["runtime_sources_sha256"]:
        raise ValueError("Accepted control sources drifted")
    path = ROOT / MODULE
    if path.is_symlink():
        raise ValueError("Payment source symlink forbidden")
    before, current = control[MODULE].decode(), path.read_text()
    old_method, new_method = payment_method(before), payment_method(current)
    old_lines, new_lines = before.splitlines(keepends=True), current.splitlines(keepends=True)
    patched = "".join(old_lines[:old_method.lineno - 1]
                      + new_lines[new_method.lineno - 1:new_method.end_lineno]
                      + old_lines[old_method.end_lineno:]).encode()
    if sha(patched) != evidence["candidate_source_proof"]["candidate_runtime_sources_sha256"][MODULE]:
        raise ValueError("Exact reviewed payment method required")
    candidate = {**control, MODULE: patched}
    if [k for k in control if candidate[k] != control[k]] != [MODULE]:
        raise ValueError("Only payment context module may differ")
    return {"control": control, "candidate": candidate}, {
        "decision": "ADR0249", "parent_image": expected["registry_image"],
        "parent_manifest_digest": expected["registry_manifest_digest"],
        "parent_configuration_digest": expected["registry_configuration_digest"],
        "parent_runtime_user": expected["runtime_user"],
        "parent_runtime_source_sha256": expected["runtime_sources_sha256"],
        "dependency_input_sha256": old_plan["dependency_input_sha256"],
        "comparison_factor": "post_lock_payment_context", "cloud_load_started": False,
        "runtime_sources": {arm: {name: sha(raw) for name, raw in values.items()}
                            for arm, values in (("control", control), ("candidate", candidate))}}


def prepare():
    sources, plan = pairs()
    output = new_stage_output()
    output.mkdir(exist_ok=False)
    target = output / "candidate"
    target.mkdir()
    for name, raw in sources["candidate"].items():
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
    manifest = {"parent_runtime_source_sha256": plan["parent_runtime_source_sha256"],
                "runtime_source_sha256": plan["runtime_sources"]["candidate"],
                "dependency_input_sha256": plan["dependency_input_sha256"]}
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", newline="\n")
    (target / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes().replace(b"\r\n", b"\n"))
    (target / "Dockerfile").write_text("FROM " + plan["parent_image"] + "\nUSER root\n"
        "COPY . /tmp/payment-context-payload\n"
        "RUN python /tmp/payment-context-payload/install.py /tmp/payment-context-payload\n"
        "USER " + plan["parent_runtime_user"] + "\n", newline="\n")
    (output / "source-plan.json").write_text(json.dumps(plan, indent=2) + "\n", newline="\n")
    return {"output": str(output), "source_count": len(sources["candidate"]), "cloud_calls": 0}


if __name__ == "__main__":
    print(json.dumps(prepare()))
