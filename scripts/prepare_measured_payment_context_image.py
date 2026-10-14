"""ADR0319: one reviewed payment-method delta on the exact measured shared image."""
import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_committed_status_profile as parent_profile
from prepare_payment_context_sources import MODULE, pairs, payment_method
from prepare_shared_application_image import ROOT, docker, sha


def overlay(parent):
    receipt = parent_profile.image()
    expected = receipt["runtime_source_sha256"]
    if {name: sha(raw) for name, raw in parent.items()} != expected:
        raise ValueError("Complete measured parent source map required")
    historical, _ = pairs()
    before = parent[MODULE].decode()
    old = payment_method(before)
    reviewed_control = payment_method(historical["control"][MODULE])
    if ast.dump(old) != ast.dump(reviewed_control):
        raise ValueError("Measured payment method differs from reviewed control")
    reviewed = historical["candidate"][MODULE].decode()
    new = payment_method(reviewed)
    lines, replacement = before.splitlines(keepends=True), reviewed.splitlines(keepends=True)
    patched = "".join(lines[:old.lineno - 1] + replacement[new.lineno - 1:new.end_lineno]
                      + lines[old.end_lineno:]).encode()
    original_tree, candidate_tree = ast.parse(before), ast.parse(patched)
    def without_payment(tree):
        owner = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                     and n.name == "PostgresReservations")
        owner.body = [n for n in owner.body if getattr(n, "name", None) != "initiate_payment"]
        return ast.dump(tree)
    if (without_payment(original_tree) != without_payment(candidate_tree)
            or ast.dump(payment_method(patched)) != ast.dump(new)):
        raise ValueError("Only the reviewed payment method may change")
    result = {**parent, MODULE: patched}
    if [name for name in parent if sha(parent[name]) != sha(result[name])] != [MODULE]:
        raise ValueError("Single payment module delta required")
    return result


def prepare(output):
    receipt = parent_profile.image()
    image = receipt["registry_image"]
    info = json.loads(docker("image", "inspect", image))[0]
    if (info["Id"] != receipt["local_image_id"] or image not in info.get("RepoDigests", [])
            or (info["Os"], info["Architecture"]) != ("linux", "amd64")):
        raise ValueError("Exact immutable linux/amd64 measured image required")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", image)
    try:
        docker("cp", container + ":/app/src", str(output / "src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container + ":/app/" + name, str(output / name))
    finally:
        docker("rm", container)
    parent = {p.relative_to(output).as_posix(): p.read_bytes()
              for p in (output / "src/ticketing").rglob("*.py")}
    candidate = overlay(parent)
    for name, raw in candidate.items():
        (output / name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    identity = sha(json.dumps(runtime, sort_keys=True).encode())
    manifest = {"decision": "ADR0319", "parent_registry_image": image,
                "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
                "parent_source_identity_sha256": receipt["source_identity_sha256"],
                "runtime_source_sha256": runtime, "source_identity_sha256": identity,
                "changed_modules": [MODULE], "changed_method": "PostgresReservations.initiate_payment",
                "dependency_input_sha256": receipt["dependency_input_sha256"]}
    (output / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    dockerfile = (f"FROM {image}\nUSER root\nCOPY . /tmp/payment-context-payload\n"
                  "RUN python /tmp/payment-context-payload/install.py /tmp/payment-context-payload "
                  "&& cp /tmp/payment-context-payload/manifest.json /app/shared-image.json "
                  "&& rm -r /tmp/payment-context-payload\n"
                  f'LABEL org.flash-ticketing.decision="ADR0319" org.flash-ticketing.source-sha256="{identity}"\n'
                  "USER ticketing\n")
    (output / "Dockerfile").write_bytes(dockerfile.encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Private preparation receipt must remain under workspace tmp")
    output = ROOT / "tmp" / ("adr0319-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0319-" + manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "local_tag": tag, "payload_path": str(output),
               "registry_published": False, "cloud_deployed": False,
               "cloud_load_started": False, "capacity_improvement_measured": False}
    if args.build:
        subprocess.run(["docker", "build", "--network=none", "--provenance=false", "-t", tag, str(output)],
                       check=True)
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes((json.dumps(receipt, indent=2) + "\n").encode())
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
