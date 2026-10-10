"""ADR0265: derive a two-module event-lane candidate from the measured immutable image."""
import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from prepare_shared_application_image import ROOT, docker, sha

PARENT_RECEIPT = ROOT / "docs/capacity/cce/shared-application-image-consumer-restored-2026-10-10.json"
MODULES = ("src/ticketing/config.py", "src/ticketing/workers.py")
CHANGED_FUNCTIONS = {"consume_kafka_messages", "consume_iteration", "main"}
ADDED_NAMES = {"EVENT_TOPIC", "FULFILLMENT_GROUP", "PROJECTION_GROUP", "BUSINESS_EVENTS",
               "CONSUMER_LANES", "event_consumer_identity", "create_event_consumer", "consume_lane_events"}


def named_nodes(raw):
    result = {}
    for node in ast.parse(raw).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            key = node.name
        elif isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            key = node.targets[0].id
        else:
            key = ast.dump(node)
        if key in result:
            raise ValueError("Duplicate source definition")
        result[key] = node
    return result


def verify_delta(parent, candidate):
    old, new = named_nodes(parent[MODULES[1]]), named_nodes(candidate[MODULES[1]])
    if set(old) - set(new) or set(new) - set(old) != ADDED_NAMES:
        raise ValueError("Unexpected worker definitions")
    changed = {name for name in old if ast.dump(old[name]) != ast.dump(new[name])}
    if changed != CHANGED_FUNCTIONS:
        raise ValueError("Worker delta escaped consumer routing/lifecycle")
    before, after = ast.parse(parent[MODULES[0]]), ast.parse(candidate[MODULES[0]])
    settings = next(n for n in after.body if isinstance(n, ast.ClassDef) and n.name == "Settings")
    flags = [n for n in settings.body if isinstance(n, ast.AnnAssign)
             and isinstance(n.target, ast.Name) and n.target.id == "event_consumer_separation"]
    if len(flags) != 1:
        raise ValueError("Exactly one separation flag required")
    settings.body.remove(flags[0])
    if ast.dump(before) != ast.dump(after):
        raise ValueError("Configuration delta escaped separation flag")


def prepare(output):
    receipt = json.loads(PARENT_RECEIPT.read_text(encoding="utf-8"))
    image = receipt["registry_image"]
    info = json.loads(docker("image", "inspect", image))[0]
    if image not in info.get("RepoDigests", []) or (info["Os"], info["Architecture"]) != ("linux", "amd64"):
        raise ValueError("Measured immutable linux/amd64 parent required")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", image)
    try:
        docker("cp", container + ":/app/src", str(output / "src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container + ":/app/" + name, str(output / name))
    finally:
        docker("rm", container)
    parent = {p.relative_to(output).as_posix(): p.read_bytes() for p in (output / "src/ticketing").rglob("*.py")}
    if {name: sha(raw) for name, raw in parent.items()} != receipt["runtime_source_sha256"]:
        raise ValueError("Complete measured parent source required")
    candidate = {**parent, **{name: (ROOT / name).read_bytes().replace(b"\r\n", b"\n") for name in MODULES}}
    verify_delta(parent, candidate)
    for name, raw in candidate.items():
        ast.parse(raw, filename=name)
        (output / name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    identity = sha(json.dumps(runtime, sort_keys=True).encode())
    manifest = {
        "decision": "ADR0265", "parent_registry_image": image,
        "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
        "parent_receipt_sha256": sha(PARENT_RECEIPT.read_bytes()),
        "runtime_source_sha256": runtime, "source_identity_sha256": identity,
        "changed_modules": list(MODULES),
        "dependency_input_sha256": receipt["dependency_input_sha256"],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_text("**/__pycache__\n**/*.pyc\n", encoding="utf-8")
    (output / "Dockerfile").write_text(
        f"FROM {image}\nUSER root\nCOPY . /tmp/event-lane-payload\n"
        "RUN python /tmp/event-lane-payload/install.py /tmp/event-lane-payload "
        "&& cp /tmp/event-lane-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/event-lane-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0265" org.flash-ticketing.source-sha256="{identity}"\n'
        "USER ticketing\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    output = ROOT / "tmp" / ("adr0265-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0265-" + manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "local_tag": tag, "registry_published": False,
               "cloud_deployed": False, "cloud_load_started": False,
               "capacity_improvement_measured": False}
    if args.build:
        subprocess.run(["docker", "build", "--network=none", "--provenance=false", "-t", tag, str(output)], check=True)
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
