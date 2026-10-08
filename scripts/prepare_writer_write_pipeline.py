"""ADR0225 reproducible frozen writer-only export and offline image build."""
import copy
import json
import re
from pathlib import Path

import prepare_partial_timeout_reclamation as parent
from status_refresh_contract import digest

base = parent.base
ARTIFACTS = base.ROOT / "artifacts/writer-write-pipeline"
CHANGED = ['src/ticketing/config.py', 'src/ticketing/infrastructure/reservations.py', 'src/ticketing/workers.py']


def manifest():
    prior = parent.manifest()
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    patch = (ARTIFACTS / "adr0225.patch").read_bytes()
    keys = {"schema", "decision", "parent_decision", "parent_manifest_sha256", "patch_sha256",
            "changed_paths", "runtime_source_sha256", "scope"}
    if (set(data) != keys or type(data["schema"]) is not int or data["schema"] != 1
            or data["decision"] != "ADR0225" or data["parent_decision"] != "ADR0163"
            or data["parent_manifest_sha256"] != base.sha((parent.ARTIFACTS / "manifest.json").read_bytes())
            or data["patch_sha256"] != base.sha(patch) or data["changed_paths"] != CHANGED
            or re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE) != [(p, p) for p in CHANGED]):
        raise ValueError("Exact writer-only frozen patch required")
    expected = copy.deepcopy(prior["runtime_source_sha256"])
    for p in CHANGED:
        h = data["runtime_source_sha256"].get(p)
        if h == prior["runtime_source_sha256"][p] or not isinstance(h, str) or not re.fullmatch(r"[0-9a-f]{64}", h):
            raise ValueError("Each allowlisted writer runtime file must change")
        expected[p] = h
    if data["runtime_source_sha256"] != expected:
        raise ValueError("Writer patch escaped its three-file allowlist")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")) and line[4:] not in {prefix + p for prefix in ("a/", "b/") for p in CHANGED}:
            raise ValueError("Patch escaped the worker allowlist")
        if line.startswith(("old mode ", "new mode ", "new file mode ", "deleted file mode ",
                            "rename from ", "rename to ", "copy from ", "copy to ")):
            raise ValueError("Text-only worker correction required")
    return data


def expected_source_map():
    return {**parent.expected_source_map(), **{p: manifest()["runtime_source_sha256"][p] for p in CHANGED}}


def verify_source(source):
    source = Path(source).resolve()
    if not source.is_relative_to((base.ROOT / "tmp").resolve()):
        raise ValueError("Owned frozen export required")
    base.verify_tree(source, expected_source_map())
    return manifest()["runtime_source_sha256"]


def prepare(output):
    overlay = manifest()
    output, data, _prior = parent.prepare(output)
    source = output / "source"
    for check in (True, False):
        args = ["git", "apply", "--whitespace=error", "--directory=" + source.relative_to(base.ROOT).as_posix()]
        if check:
            args.append("--check")
        base.command([*args, str(ARTIFACTS / "adr0225.patch")])
    for p in CHANGED:
        (source / p).write_bytes((source / p).read_bytes().replace(b"\r\n", b"\n"))
    verify_source(source)
    data.update(parent_runtime_source_sha256=parent.manifest()["runtime_source_sha256"],
                runtime_source_sha256=overlay["runtime_source_sha256"], patch_sha256=overlay["patch_sha256"],
                overlay_sha256={**data["overlay_sha256"], **{p: overlay["runtime_source_sha256"][p] for p in CHANGED}})
    context = output / "image-context"
    for p in CHANGED:
        (context / p).write_bytes((source / p).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    return output, data


def build(output):
    import orders_event_index_probe_contract as baseline
    output, data = prepare(output)
    folder = output / "writer-build"
    folder.mkdir()
    old = baseline.plan()["artifact_receipt"]
    image, _proof = base.build_one(folder, data, old["images"]["reservation-writer"])
    artifact = copy.deepcopy(old)
    artifact["images"]["reservation-writer"] = image
    proof = {"decision": "ADR0225", "writer_parent_image": old["images"]["reservation-writer"],
             "writer_image": image, "runtime_source_sha256": data["runtime_source_sha256"],
             "writer_source_manifest_sha256": digest({"base_revision": base.REVISION,
                                                        "runtime_source_sha256": data["runtime_source_sha256"]}),
             "isolated_source_directory": (output / "source").relative_to(base.ROOT).as_posix(),
             "artifact_receipt": artifact, "cloud_calls": 0}
    (output / "writer-image-receipt.json").write_text(json.dumps(proof, indent=2) + "\n")
    return proof
