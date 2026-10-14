"""ADR0320: isolated writer transaction pipeline on the measured shared image."""

import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_committed_status_profile as parent_profile
from prepare_shared_application_image import ROOT, docker, sha

PATCHES = {
    "src/ticketing/infrastructure/reservations.py": [
        (
            "    def __init__(self, db: Database, cache: SeatCache, hold_seconds=120, *, persist_write_pipeline=False):\n        self.db, self.cache, self.hold_seconds = db, cache, hold_seconds\n        self.persist_write_pipeline = persist_write_pipeline\n",
            '    def __init__(\n        self, db: Database, cache: SeatCache, hold_seconds=120, *,\n        persist_write_pipeline=False, persist_transaction_pipeline=False,\n    ):\n        if persist_transaction_pipeline and not persist_write_pipeline:\n            raise ValueError("Reservation transaction pipeline requires independent-write pipeline")\n        self.db, self.cache, self.hold_seconds = db, cache, hold_seconds\n        self.persist_write_pipeline = persist_write_pipeline\n        self.persist_transaction_pipeline = persist_transaction_pipeline\n',
        ),
        (
            "        if len(commands) == 1:\n",
            '        options = {"pipeline": True} if self.persist_transaction_pipeline else {}\n        if len(commands) == 1:\n',
        ),
        (
            "                with self.db.transaction() as conn:\n                    durable = self._persist_reservation_command(conn, *commands[0])\n",
            "                with self.db.transaction(**options) as conn:\n                    durable = self._persist_reservation_command(conn, *commands[0])\n",
        ),
        (
            "        outcomes = []\n        with self.db.transaction() as conn:\n            for payload, response in commands:\n",
            "        outcomes = []\n        with self.db.transaction(**options) as conn:\n            for payload, response in commands:\n",
        ),
    ],
    "src/ticketing/config.py": [
        (
            '    reservation_write_pipeline: bool = os.getenv("RESERVATION_WRITE_PIPELINE", "0") == "1"\n',
            '    reservation_write_pipeline: bool = os.getenv("RESERVATION_WRITE_PIPELINE", "0") == "1"\n    reservation_transaction_pipeline: bool = os.getenv("RESERVATION_TRANSACTION_PIPELINE", "0") == "1"\n',
        ),
        (
            "        if not 1 <= self.reservation_writer_batch_size <= 8:\n",
            '        if self.reservation_transaction_pipeline and not self.reservation_write_pipeline:\n            raise RuntimeError("RESERVATION_TRANSACTION_PIPELINE requires RESERVATION_WRITE_PIPELINE")\n        if not 1 <= self.reservation_writer_batch_size <= 8:\n',
        ),
    ],
    "src/ticketing/workers.py": [
        (
            '        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,\n',
            '        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,\n        persist_transaction_pipeline=role == "reservation-writer" and settings.reservation_transaction_pipeline,\n',
        )
    ],
}


def overlay(parent):
    receipt = parent_profile.image()
    if {name: sha(raw) for name, raw in parent.items()} != receipt["runtime_source_sha256"]:
        raise ValueError("Complete measured parent source map required")
    candidate = dict(parent)
    for name, patches in PATCHES.items():
        raw = candidate[name]
        for old, new in patches:
            if raw.count(old.encode()) != 1:
                raise ValueError("Exactly one reviewed writer pipeline anchor required")
            raw = raw.replace(old.encode(), new.encode(), 1)
        ast.parse(raw, filename=name)
        candidate[name] = raw
    if {name for name in parent if sha(parent[name]) != sha(candidate[name])} != set(PATCHES):
        raise ValueError("Exactly the three writer configuration modules may change")
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
        "decision": "ADR0320",
        "parent_registry_image": image,
        "parent_runtime_source_sha256": receipt["runtime_source_sha256"],
        "parent_source_identity_sha256": receipt["source_identity_sha256"],
        "runtime_source_sha256": runtime,
        "source_identity_sha256": identity,
        "changed_modules": sorted(PATCHES),
        "dependency_input_sha256": receipt["dependency_input_sha256"],
        "candidate_flag": "RESERVATION_TRANSACTION_PIPELINE",
        "candidate_default": False,
    }
    (output / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    dockerfile = (
        f"FROM {image}\nUSER root\nCOPY . /tmp/writer-pipeline-payload\n"
        "RUN python /tmp/writer-pipeline-payload/install.py /tmp/writer-pipeline-payload "
        "&& cp /tmp/writer-pipeline-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/writer-pipeline-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0320" org.flash-ticketing.source-sha256="{identity}"\n'
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
    output = ROOT / "tmp" / ("adr0320-image-" + uuid4().hex[:12])
    manifest = prepare(output)
    tag = "flash-ticketing-shared:adr0320-" + manifest["source_identity_sha256"][:12]
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
