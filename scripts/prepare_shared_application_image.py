"""ADR0258: package the accepted runtime once for every application role."""
import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from cce_historical_sources import blob

ROOT = Path(__file__).resolve().parents[1]
ACCEPTED = ROOT / "docs/capacity/cce/customer-recovery-image-2026-10-10.json"
CONFIG = "src/ticketing/config.py"
OLD = b'        if not 1 <= self.simulator_concurrency <= self.pool_max:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must fit DB_POOL_MAX")\n'
NEW = b'        # HTTP delivery runs outside SQL transactions; bound its slots separately.\n        if type(self.simulator_concurrency) is not int or not 1 <= self.simulator_concurrency <= 32:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must be an integer between 1 and 32")\n'


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def source_patch(files, expected):
    if {name: sha(raw) for name, raw in files.items()} != expected:
        raise ValueError("Complete accepted runtime source map required")
    if files[CONFIG].count(OLD) != 1:
        raise ValueError("Exactly one accepted simulator validation anchor required")
    result = {**files, CONFIG: files[CONFIG].replace(OLD, NEW, 1)}
    for name, raw in result.items():
        ast.parse(raw, filename=name)
    if [name for name in result if sha(result[name]) != expected[name]] != [CONFIG]:
        raise ValueError("Only simulator configuration validation may change")
    return result



WRITER_SOURCES = {
    "src/ticketing/config.py": "e3d141654092a7c7f528e11ceb163ec14cf035349f9d9040dd2e617b5f1f2591",
    "src/ticketing/workers.py": "dec31b79c22f2c3acbd533e768435bac6787ab76a71a197b0ef1aef4a5e0d4a1",
    "src/ticketing/infrastructure/reservations.py": "681e6b4f01e5236cb07b0b25c9f874c75d00e1c6c484c432d262a23762ce7c91",
}


def method_span(source, owner, name):
    cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == owner)
    node = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return node.lineno - 1, node.end_lineno


def once(source, before, after):
    if source.count(before) != 1:
        raise ValueError("Single accepted source anchor required")
    return source.replace(before, after, 1)


def merge_writer(files):
    result = dict(files)
    field = '    reservation_write_pipeline: bool = os.getenv("RESERVATION_WRITE_PIPELINE", "0") == "1"'
    writer_config = blob(WRITER_SOURCES[CONFIG]).decode()
    if field not in writer_config.splitlines():
        raise ValueError("Accepted writer field differs")
    config = files[CONFIG].decode()
    result[CONFIG] = once(config, '    reservation_writer_batch_size:', field + '\n    reservation_writer_batch_size:').encode()
    name = "src/ticketing/workers.py"
    writer = blob(WRITER_SOURCES[name]).decode()
    before = '    store = PostgresReservations(db, cache, settings.hold_seconds)'
    after = ('    store = PostgresReservations(\n'
             '        db, cache, settings.hold_seconds,\n'
             '        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,\n'
             '    )')
    if writer.count(after) != 1:
        raise ValueError("Accepted writer construction differs")
    result[name] = once(files[name].decode(), before, after).encode()
    name = "src/ticketing/infrastructure/reservations.py"
    source = files[name].decode()
    writer = blob(WRITER_SOURCES[name]).decode()
    for method in ("__init__", "_persist_reservation_command"):
        start, end = method_span(source, "PostgresReservations", method)
        wstart, wend = method_span(writer, "PostgresReservations", method)
        lines = source.splitlines(True)
        source = ''.join(lines[:start] + writer.splitlines(True)[wstart:wend] + lines[end:])
    source = once(source, "import json\n", "import json\nfrom contextlib import nullcontext\n")
    result[name] = source.encode()
    for name, raw in result.items():
        ast.parse(raw, filename=name)
    changed = {name for name in files if files[name] != result[name]}
    if changed != set(WRITER_SOURCES):
        raise ValueError("Writer merge escaped its three-module scope")
    return result


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def prepare(output):
    accepted = json.loads(ACCEPTED.read_text())["images"]["control"]
    parent = accepted["registry_image"]
    info = json.loads(docker("image", "inspect", parent))[0]
    if parent not in info.get("RepoDigests", []):
        raise ValueError("Immutable accepted parent must be available locally")
    if info["Os"] != "linux" or info["Architecture"] != "amd64":
        raise ValueError("Accepted platform must be linux/amd64")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", parent)
    try:
        docker("cp", container + ":/app/src", str(output / "src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container + ":/app/" + name, str(output / name))
    finally:
        docker("rm", container)
    files = {p.relative_to(output).as_posix(): p.read_bytes()
             for p in (output / "src/ticketing").rglob("*.py")}
    candidate = merge_writer(source_patch(files, accepted["runtime_sources_sha256"]))
    for name, raw in candidate.items():
        (output / name).write_bytes(raw)
    runtime = {name: sha(raw) for name, raw in candidate.items()}
    identity = sha(json.dumps(runtime, sort_keys=True).encode())
    manifest = {
        "decision": "ADR0258", "parent_registry_image": parent,
        "parent_runtime_source_sha256": accepted["runtime_sources_sha256"],
        "runtime_source_sha256": runtime, "source_identity_sha256": identity,
        "writer_source_inputs_sha256": WRITER_SOURCES,
        "dependency_input_sha256": {name: sha((output / name).read_bytes())
                                    for name in ("requirements.lock", "pyproject.toml")},
    }
    (output / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    (output / "Dockerfile").write_bytes((
        f"FROM {parent}\nUSER root\nCOPY . /tmp/shared-payload\n"
        "RUN python /tmp/shared-payload/install.py /tmp/shared-payload "
        "&& cp /tmp/shared-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/shared-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0258" org.flash-ticketing.source-sha256="{identity}"\n'
        "USER ticketing\n"
    ).encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    output = ROOT / "tmp" / ("adr0258-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0258-" + manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "local_tag": tag, "registry_published": False,
               "cloud_deployed": False, "cloud_load_started": False,
               "capacity_improvement_measured": False}
    if args.build:
        subprocess.run(["docker", "build", "--network=none", "--provenance=false",
                        "-t", tag, str(output)], check=True)
        receipt["local_image_id"] = json.loads(docker("image", "inspect", tag))[0]["Id"]
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_bytes((json.dumps(receipt, indent=2) + "\n").encode())
    print(json.dumps({"build_context": str(output), "local_tag": tag,
                      "source_identity_sha256": manifest["source_identity_sha256"],
                      "built": args.build, "cloud_deployed": False}))


if __name__ == "__main__":
    main()
