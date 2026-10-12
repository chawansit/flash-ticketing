"""ADR0297: isolate committed-status publication on the measured immutable image."""
import argparse
import ast
import copy
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from prepare_event_lane_image import named_nodes
from prepare_shared_application_image import ROOT, docker, sha

PARENT_RECEIPT = ROOT / "docs/capacity/cce/event-lane-fetch-image-2026-10-10.json"
MODULES = ("src/ticketing/config.py", "src/ticketing/workers.py",
           "src/ticketing/infrastructure/order_status_projector.py", "src/ticketing/observability.py")
FLAG = "order_status_committed_snapshot"
ORIGINAL_REVISION = "e8a9421e40a007778c9f85b7f94cb8cf18afd13f"


def settings_node(raw):
    return next(n for n in ast.parse(raw).body if isinstance(n, ast.ClassDef) and n.name == "Settings")


def config_overlay(parent, current):
    baseline = settings_node(parent)
    candidate = settings_node(current)
    field = next(n for n in candidate.body if isinstance(n, ast.AnnAssign) and n.target.id == FLAG)
    validation = next(n for n in candidate.body if isinstance(n, ast.FunctionDef) and n.name == "validate")
    guard = next(n for n in validation.body if isinstance(n, ast.If)
                 and "ORDER_STATUS_COMMITTED_SNAPSHOT" in ast.unparse(n))
    # Transplant exactly the new field and dependency guard, leaving all unrelated baseline flags intact.
    lines = parent.decode().splitlines(keepends=True)
    base_validate = next(n for n in baseline.body if isinstance(n, ast.FunctionDef) and n.name == "validate")
    field_anchor = next(n for n in baseline.body if isinstance(n, ast.AnnAssign)
                        and n.target.id == "order_status_event_refresh_dedup")
    additions = [(field_anchor.end_lineno, "    " + ast.unparse(field) + "\n"),
                 (base_validate.body[0].lineno - 1,
                  "\n".join("        " + line for line in ast.unparse(guard).splitlines()) + "\n")]
    for index, raw in sorted(additions, reverse=True):
        lines[index:index] = [raw]
    result = "".join(lines).encode()
    tree = ast.parse(result)
    selected = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Settings")
    selected.body = [n for n in selected.body if not (
        isinstance(n, ast.AnnAssign) and n.target.id == FLAG)]
    validation = next(n for n in selected.body if isinstance(n, ast.FunctionDef) and n.name == "validate")
    validation.body = [n for n in validation.body if not (
        isinstance(n, ast.If) and "ORDER_STATUS_COMMITTED_SNAPSHOT" in ast.unparse(n))]
    if ast.dump(tree) != ast.dump(ast.parse(parent)):
        raise ValueError("Configuration changes escaped the single default-off flag")
    return result


def overlay(parent):
    result = copy.copy(parent)
    result[MODULES[0]] = config_overlay(parent[MODULES[0]], (ROOT / MODULES[0]).read_bytes())
    for name in MODULES[1:]:
        original = subprocess.run(["git", "show", ORIGINAL_REVISION + ":" + name], cwd=ROOT,
                                  capture_output=True, check=True).stdout
        if sha(original) != sha(parent[name]):
            raise ValueError("Edited module did not start from the measured image: " + name)
        result[name] = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
        before, after = named_nodes(parent[name]), named_nodes(result[name])
        if name == MODULES[1]:
            allowed = {"consume_event", "_consume_event_transaction", "main"}
        elif name == MODULES[3]:
            allowed = {"measured_consumer_event"}
        else:
            old_imports = {ast.dump(v) for v in before.values() if isinstance(v, ast.ImportFrom)}
            new_imports = {ast.dump(v) for v in after.values() if isinstance(v, ast.ImportFrom)}
            expected_import = ast.dump(ast.parse(
                "from ticketing.infrastructure.order_status_cache import scalar, valid_order").body[0])
            original_import = ast.dump(ast.parse(
                "from ticketing.infrastructure.order_status_cache import scalar").body[0])
            if new_imports != (old_imports - {original_import}) | {expected_import}:
                raise ValueError("Unexpected projector import")
            before = {k: v for k, v in before.items() if not isinstance(v, ast.ImportFrom)}
            after = {k: v for k, v in after.items() if not isinstance(v, ast.ImportFrom)}
            old_methods = before["CommittedOrderStatusProjector"].body
            new_methods = after["CommittedOrderStatusProjector"].body
            old_methods = {n.name: n for n in old_methods if isinstance(n, ast.FunctionDef)}
            new_methods = {n.name: n for n in new_methods if isinstance(n, ast.FunctionDef)}
            if (set(new_methods) - set(old_methods) != {"begin_capture", "publish_committed"}
                    or set(old_methods) - set(new_methods)
                    or any(ast.dump(n) != ast.dump(new_methods[k])
                           for k, n in old_methods.items() if k != "__init__")):
                raise ValueError("Only capture/publication and its constructor may change")
            allowed = {"CommittedOrderStatusProjector"}
        actual = {k for k in before.keys() & after.keys() if ast.dump(before[k]) != ast.dump(after[k])}
        if set(before) != set(after) or actual != allowed:
            raise ValueError("Publication delta escaped its reviewed definitions: " + name)
    changed = [name for name in result if sha(result[name]) != sha(parent[name])]
    if set(changed) != set(MODULES):
        raise ValueError("Only the four reviewed publication/configuration modules may change")
    for name, raw in result.items():
        ast.parse(raw, filename=name)
    return result


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
    parent = {p.relative_to(output).as_posix(): p.read_bytes()
              for p in (output / "src/ticketing").rglob("*.py")}
    if {name: sha(raw) for name, raw in parent.items()} != receipt["runtime_source_sha256"]:
        raise ValueError("Complete measured parent source required")
    candidate = overlay(parent)
    for name, raw in candidate.items():
        (output / name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    identity = sha(json.dumps(runtime, sort_keys=True).encode())
    manifest = {"decision": "ADR0297", "parent_registry_image": image,
                "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
                "parent_receipt_sha256": sha(PARENT_RECEIPT.read_bytes()),
                "runtime_source_sha256": runtime, "source_identity_sha256": identity,
                "changed_modules": list(MODULES), "dependency_input_sha256": receipt["dependency_input_sha256"]}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_text("**/__pycache__\n**/*.pyc\n", encoding="utf-8")
    (output / "Dockerfile").write_text(
        f"FROM {image}\nUSER root\nCOPY . /tmp/committed-status-payload\n"
        "RUN python /tmp/committed-status-payload/install.py /tmp/committed-status-payload "
        "&& cp /tmp/committed-status-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/committed-status-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0297" org.flash-ticketing.source-sha256="{identity}"\n'
        "USER ticketing\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    output = ROOT / "tmp" / ("adr0297-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0297-" + manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "local_tag": tag, "payload_path": str(output),
               "registry_published": False, "cloud_deployed": False,
               "cloud_load_started": False, "capacity_improvement_measured": False}
    if args.build:
        subprocess.run(["docker", "build", "--network=none", "--provenance=false", "-t", tag, str(output)],
                       check=True)
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"local_tag": tag, "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
