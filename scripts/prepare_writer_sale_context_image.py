"""ADR0321: remove one writer lookup on the exact measured shared image."""

import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_committed_status_profile as parent_profile
from prepare_shared_application_image import ROOT, docker, sha

MODULE = "src/ticketing/infrastructure/reservations.py"
OLD_SALE = '        sale = conn.execute(\n            "SELECT * FROM events WHERE id=%s", (payload["event_id"],)\n        ).fetchone()\n        if not sale:\n            raise Failure("EVENT_NOT_FOUND", 404)\n'
CLOCK = '        now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]\n'
CONTEXT = '        sale = conn.execute(\n            "SELECT *, clock_timestamp() AS now FROM events WHERE id=%s", (payload["event_id"],)\n        ).fetchone()\n        if not sale:\n            raise Failure("EVENT_NOT_FOUND", 404)\n        now = sale["now"]\n'


def method(source):
    owner = next(
        n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == "PostgresReservations"
    )
    return next(n for n in owner.body if getattr(n, "name", None) == "_persist_reservation_command")


def overlay(parent):
    receipt = parent_profile.image()
    expected = receipt["runtime_source_sha256"]
    if {name: sha(raw) for name, raw in parent.items()} != expected:
        raise ValueError("Complete measured parent source map required")
    source = parent[MODULE].decode()
    node = method(source)
    lines = source.splitlines(keepends=True)
    before = "".join(lines[node.lineno - 1 : node.end_lineno])
    if before.count(OLD_SALE) != 1 or before.count(CLOCK) != 1:
        raise ValueError("Exactly one reviewed writer event/clock lookup required")
    after = before.replace(OLD_SALE, "").replace(CLOCK, CONTEXT)
    patched = "".join(lines[: node.lineno - 1]) + after + "".join(lines[node.end_lineno :])

    def without_writer(value):
        tree = ast.parse(value)
        owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PostgresReservations")
        owner.body = [n for n in owner.body if getattr(n, "name", None) != "_persist_reservation_command"]
        return ast.dump(tree)

    if without_writer(source) != without_writer(patched):
        raise ValueError("Only the reservation writer method may change")
    return {**parent, MODULE: patched.encode()}


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
        "decision": "ADR0321",
        "parent_registry_image": image,
        "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
        "parent_source_identity_sha256": receipt["source_identity_sha256"],
        "runtime_source_sha256": runtime,
        "source_identity_sha256": identity,
        "changed_modules": [MODULE],
        "changed_method": "PostgresReservations._persist_reservation_command",
        "dependency_input_sha256": receipt["dependency_input_sha256"],
    }
    (output / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    dockerfile = (
        f"FROM {image}\nUSER root\nCOPY . /tmp/writer-sale-context-payload\n"
        "RUN python /tmp/writer-sale-context-payload/install.py /tmp/writer-sale-context-payload "
        "&& cp /tmp/writer-sale-context-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/writer-sale-context-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0321" org.flash-ticketing.source-sha256="{identity}"\n'
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
    output = ROOT / "tmp" / ("adr0321-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0321-" + manifest["source_identity_sha256"][:12]
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
            ["docker", "build", "--network=none", "--provenance=false", "-t", tag, str(output)], check=True
        )
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes((json.dumps(receipt, indent=2) + "\n").encode())
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
