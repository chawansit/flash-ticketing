"""ADR0324: bound fulfillment transaction batches on the exact measured image."""

import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_explicit_start_profile as parent_profile
from prepare_shared_application_image import ROOT, docker, sha


def overlay(parent):
    expected = parent_profile.image()["runtime_source_sha256"]
    if {name: sha(raw) for name, raw in parent.items()} != expected:
        raise ValueError("Complete immutable ADR0323 source map required")
    modules = ("src/ticketing/config.py", "src/ticketing/workers.py")
    candidate = {**parent, **{name: (ROOT / name).read_bytes().replace(b"\r\n", b"\n") for name in modules}}
    for name, raw in candidate.items():
        ast.parse(raw, filename=name)
    if {name for name in candidate if sha(candidate[name]) != expected[name]} != set(modules):
        raise ValueError("Only fulfillment grouping and configuration may change")
    return candidate


def prepare(output):
    receipt = parent_profile.image()
    image = receipt["registry_image"]
    info = json.loads(docker("image", "inspect", image))[0]
    if (
        info["Id"] != receipt["local_image_id"]
        or image not in info.get("RepoDigests", [])
        or (info["Os"], info["Architecture"]) != ("linux", "amd64")
    ):
        raise ValueError("Exact immutable linux/amd64 measured image required")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", image)
    try:
        docker("cp", container + ":/app/src", str(output / "src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container + ":/app/" + name, str(output / name))
    finally:
        docker("rm", container)
    parent = {
        p.relative_to(output).as_posix(): p.read_bytes() for p in (output / "src/ticketing").rglob("*.py")
    }
    candidate = overlay(parent)
    for name, raw in candidate.items():
        (output / name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    identity = sha(json.dumps(runtime, sort_keys=True).encode())
    manifest = {
        "decision": "ADR0324",
        "parent_registry_image": image,
        "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
        "parent_source_identity_sha256": receipt["source_identity_sha256"],
        "runtime_source_sha256": runtime,
        "source_identity_sha256": identity,
        "changed_modules": ["src/ticketing/config.py", "src/ticketing/workers.py"],
        "changed_factor": "bounded_fulfillment_transaction_grouping",
        "dependency_input_sha256": receipt["dependency_input_sha256"],
    }
    (output / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    dockerfile = (
        f"FROM {image}\nUSER root\nCOPY . /tmp/fulfillment-batch-payload\n"
        "RUN python /tmp/fulfillment-batch-payload/install.py /tmp/fulfillment-batch-payload "
        "&& cp /tmp/fulfillment-batch-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/fulfillment-batch-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0324" org.flash-ticketing.source-sha256="{identity}"\n'
        "USER ticketing\n"
    )
    (output / "Dockerfile").write_bytes(dockerfile.encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Private preparation receipt must remain under workspace tmp")
    output = ROOT / "tmp" / ("adr0324-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0324-" + manifest["source_identity_sha256"][:12]
    receipt = {
        **manifest,
        "local_tag": tag,
        "payload_path": str(output),
        "registry_published": False,
        "cloud_deployed": False,
        "cloud_load_started": False,
        "capacity_improvement_measured": False,
    }
    if args.build:
        subprocess.run(
            ["docker", "buildx", "build", "--platform=linux/amd64", "--network=none", "--provenance=false", "--sbom=false", "--output=type=docker,oci-mediatypes=false", "--load", "-t", tag, str(output)], check=True
        )
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes((json.dumps(receipt, indent=2) + "\n").encode())
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
