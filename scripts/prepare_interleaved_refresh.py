"""ADR0222 reproducible frozen consumer-only export and offline image build."""
import copy
import json
import re
from pathlib import Path

import prepare_partial_timeout_reclamation as parent
from status_refresh_contract import digest

base = parent.base
ARTIFACTS = base.ROOT / "artifacts/interleaved-seat-refresh"
WORKER = "src/ticketing/workers.py"


def manifest():
    prior = parent.manifest()
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    patch = (ARTIFACTS / "adr0222.patch").read_bytes()
    keys = {"schema", "decision", "parent_decision", "parent_manifest_sha256", "patch_sha256",
            "changed_paths", "runtime_source_sha256", "scope"}
    if (set(data) != keys or type(data["schema"]) is not int or data["schema"] != 1
            or data["decision"] != "ADR0222" or data["parent_decision"] != "ADR0163"
            or data["parent_manifest_sha256"] != base.sha((parent.ARTIFACTS / "manifest.json").read_bytes())
            or data["patch_sha256"] != base.sha(patch) or data["changed_paths"] != [WORKER]
            or re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE) != [(WORKER, WORKER)]):
        raise ValueError("Exact consumer-only frozen patch required")
    expected = copy.deepcopy(prior["runtime_source_sha256"])
    expected[WORKER] = data["runtime_source_sha256"].get(WORKER)
    if (data["runtime_source_sha256"] != expected or expected[WORKER] == prior["runtime_source_sha256"][WORKER]
            or not isinstance(expected[WORKER], str) or not re.fullmatch(r"[0-9a-f]{64}", expected[WORKER])):
        raise ValueError("Only the consumer worker source may change")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")) and line[4:] not in {"a/" + WORKER, "b/" + WORKER}:
            raise ValueError("Patch escaped the worker allowlist")
        if line.startswith(("old mode ", "new mode ", "new file mode ", "deleted file mode ",
                            "rename from ", "rename to ", "copy from ", "copy to ")):
            raise ValueError("Text-only worker correction required")
    return data


def expected_source_map():
    return {**parent.expected_source_map(), WORKER: manifest()["runtime_source_sha256"][WORKER]}


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
        base.command([*args, str(ARTIFACTS / "adr0222.patch")])
    (source / WORKER).write_bytes((source / WORKER).read_bytes().replace(b"\r\n", b"\n"))
    verify_source(source)
    data.update(parent_runtime_source_sha256=parent.manifest()["runtime_source_sha256"],
                runtime_source_sha256=overlay["runtime_source_sha256"], patch_sha256=overlay["patch_sha256"],
                overlay_sha256={**data["overlay_sha256"], WORKER: overlay["runtime_source_sha256"][WORKER]})
    context = output / "image-context"
    (context / WORKER).write_bytes((source / WORKER).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    return output, data


def build(output):
    import shared_callback_rate_probe_contract as baseline
    output, data = prepare(output)
    folder = output / "consumer-build"
    folder.mkdir()
    old = baseline.plan()["artifact_receipt"]
    image, _proof = base.build_one(folder, data, old["images"]["consumer"])
    artifact = copy.deepcopy(old)
    artifact["images"]["consumer"] = image
    proof = {"decision": "ADR0222", "consumer_parent_image": old["images"]["consumer"],
             "consumer_image": image, "runtime_source_sha256": data["runtime_source_sha256"],
             "consumer_source_manifest_sha256": digest({"base_revision": base.REVISION,
                                                        "runtime_source_sha256": data["runtime_source_sha256"]}),
             "isolated_source_directory": (output / "source").relative_to(base.ROOT).as_posix(),
             "artifact_receipt": artifact, "cloud_calls": 0}
    (output / "consumer-image-receipt.json").write_text(json.dumps(proof, indent=2) + "\n")
    return proof
