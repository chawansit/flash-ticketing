"""Build ADR0336 from the complete measured ADR0324 source, changing one keyword."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from prepare_shared_application_image import ROOT, docker, sha

RECEIPT = ROOT / "docs/capacity/cce/fulfillment-transaction-batching-local-2026-10-14.json"
MODULE = "src/ticketing/api.py"
BEFORE = b"\ndef actor(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]):"
AFTER = BEFORE.replace(b"\ndef actor", b"\nasync def actor")


def overlay(parent, receipt):
    if {name: sha(raw) for name, raw in parent.items()} != receipt["runtime_source_sha256"]:
        raise ValueError("Complete measured ADR0324 source required")
    raw = parent[MODULE]
    if raw.count(BEFORE) != 1 or AFTER in raw:
        raise ValueError("Exact synchronous authentication dependency required")
    return {**parent, MODULE: raw.replace(BEFORE, AFTER, 1)}


def prepare(output):
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    image = receipt["registry_image"]
    info = json.loads(docker("image", "inspect", image))[0]
    if (info["Id"] != receipt["local_image_id"] or image not in info.get("RepoDigests", [])
            or (info["Os"], info["Architecture"]) != ("linux", "amd64")):
        raise ValueError("Exact immutable measured Linux image required")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", image)
    try:
        docker("cp", container+":/app/src", str(output/"src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container+":/app/"+name, str(output/name))
    finally:
        docker("rm", container)
    parent = {p.relative_to(output).as_posix(): p.read_bytes()
              for p in (output/"src/ticketing").rglob("*.py")}
    candidate = overlay(parent, receipt)
    if (ROOT/MODULE).read_bytes().replace(b"\r\n", b"\n") != candidate[MODULE].replace(b"\r\n", b"\n"):
        raise ValueError("Reviewed working API must match the exact one-keyword candidate")
    for name, raw in candidate.items():
        (output/name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    manifest = {
        "decision": "ADR0336", "parent_registry_image": image,
        "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
        "parent_source_identity_sha256": receipt["source_identity_sha256"],
        "runtime_source_sha256": runtime,
        "source_identity_sha256": sha(json.dumps(runtime, sort_keys=True).encode()),
        "changed_modules": [MODULE], "changed_factor": "local_jwt_dependency_thread_handoff",
        "dependency_input_sha256": receipt["dependency_input_sha256"],
    }
    (output/"manifest.json").write_bytes((json.dumps(manifest, indent=2)+"\n").encode())
    (output/"install.py").write_bytes((ROOT/"scripts/status_refresh_image_install.py").read_bytes())
    (output/".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    (output/"Dockerfile").write_bytes((
        f"FROM {image}\nUSER root\nCOPY . /tmp/auth-dispatch-payload\n"
        "RUN python /tmp/auth-dispatch-payload/install.py /tmp/auth-dispatch-payload "
        "&& cp /tmp/auth-dispatch-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/auth-dispatch-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0336" org.flash-ticketing.source-sha256="{manifest["source_identity_sha256"]}"\n'
        "USER ticketing\n").encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to((ROOT/"tmp").resolve()):
        raise ValueError("Preparation receipt must remain under workspace tmp")
    output = ROOT/"tmp"/("adr0336-image-"+uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0336-"+manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "local_tag": tag, "payload_path": str(output),
               "registry_published": False, "cloud_deployed": False, "cloud_load_started": False,
               "capacity_improvement_measured": False}
    if args.build:
        subprocess.run(["docker", "buildx", "build", "--platform=linux/amd64", "--network=none",
                        "--provenance=false", "--sbom=false", "--output=type=docker,oci-mediatypes=false",
                        "--load", "-t", tag, str(output)], check=True)
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes((json.dumps(receipt, indent=2)+"\n").encode())
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
