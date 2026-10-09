"""ADR0156: reproducible default-off dedup source; optional offline images, no cloud calls."""

import argparse
import hashlib
import json
import re
from pathlib import Path

import prepare_status_refresh_artifacts as base

ARTIFACTS = base.ROOT / "artifacts/status-refresh-dedup"


def manifest():
    parent = base.manifest()
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    keys = {"schema", "decision", "base_revision", "parent_manifest_sha256", "parent_patch_sha256",
            "patch_sha256", "overlay_sha256", "runtime_source_sha256", "dependency_input_sha256"}
    if (set(data) != keys or type(data["schema"]) is not int or data["schema"] != 1
            or data["decision"] != "ADR0156" or data["base_revision"] != base.REVISION
            or data["parent_manifest_sha256"] != base.sha((base.ARTIFACTS / "manifest.json").read_bytes())
            or data["parent_patch_sha256"] != parent["patch_sha256"]
            or data["dependency_input_sha256"] != parent["dependency_input_sha256"]
            or set(data["overlay_sha256"]) != set(parent["overlay_sha256"])):
        raise ValueError("Exact qualified parent and deduplication overlay required")
    expected_runtime = {**parent["runtime_source_sha256"],
                        **{p: h for p, h in data["overlay_sha256"].items() if p.startswith("src/")}}
    if (data["runtime_source_sha256"] != expected_runtime
            or any(not isinstance(h, str) or not re.fullmatch("[0-9a-f]{64}", h)
                   for h in data["overlay_sha256"].values())):
        raise ValueError("Full runtime map or overlay digests differ")
    patch = (ARTIFACTS / "adr0156.patch").read_bytes()
    if hashlib.sha256(patch).hexdigest() != data["patch_sha256"]:
        raise ValueError("Pinned deduplication patch digest differs")
    headers = re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE)
    if (len(headers) != len(data["overlay_sha256"]) or len(set(headers)) != len(headers)
            or any(a != b or a not in data["overlay_sha256"] for a, b in headers)):
        raise ValueError("Exact allowlisted patch paths required")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")) and line[4:] not in {
                prefix + p for p in data["overlay_sha256"] for prefix in ("a/", "b/")}:
            raise ValueError("Patch target escapes allowlist")
        if line.startswith(("old mode ", "new mode ", "new file mode ", "deleted file mode ",
                            "rename from ", "rename to ", "copy from ", "copy to ")):
            raise ValueError("Text overlay only")
    return data


def prepare(output):
    overlay = manifest()  # Verify both pinned artifacts before creating any candidate context.
    output, data, parent_receipt = base.prepare(output)
    source = output / "source"
    directory = source.relative_to(base.ROOT).as_posix()
    for check in (True, False):
        args = ["git", "apply", "--whitespace=error", "--directory=" + directory]
        if check:
            args.append("--check")
        base.command([*args, str(ARTIFACTS / "adr0156.patch")])
    # Git on Windows can write CRLF. The pinned artifact and image source use canonical LF bytes.
    for name in overlay["overlay_sha256"]:
        p = source / name
        p.write_bytes(p.read_bytes().replace(b"\r\n", b"\n"))
    expected = {**parent_receipt["source_file_sha256"], **overlay["overlay_sha256"]}
    base.verify_tree(source, expected)
    data.update(overlay_sha256=overlay["overlay_sha256"],
                runtime_source_sha256=overlay["runtime_source_sha256"],
                patch_sha256=overlay["patch_sha256"])
    context = output / "image-context"
    for name in data["runtime_source_sha256"]:
        (context / name).write_bytes((source / name).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    receipt = {**parent_receipt, "kind": "isolated_status_refresh_dedup_source", "decision": "ADR0156",
               "parent_source_manifest_sha256": parent_receipt["source_manifest_sha256"],
               "patch_sha256": overlay["patch_sha256"], "source_file_sha256": expected,
               "source_manifest_sha256": base.digest({"base_revision": base.REVISION,
                                                     "runtime_source_sha256": data["runtime_source_sha256"]})}
    (output / "dedup-source-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return output, data, receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--parents", type=Path)
    args = parser.parse_args()
    if args.build and args.parents is None:
        parser.error("Explicit build requires original per-role parent map")
    parents = json.loads(args.parents.read_text()) if args.parents else None
    if args.build:
        base.validate_parents(parents)
    output, data, receipt = prepare(args.output)
    images = base.build(output, data, parents) if args.build else None
    print(json.dumps({"phase": "local-images-built" if images else "isolated-dedup-source-prepared",
                      "source_files_verified": receipt["source_files_verified"],
                      "runtime_modules_verified": receipt["runtime_modules_verified"],
                      "images_built": len(set(images["images"].values())) if images else 0,
                      "cloud_calls": 0}))


if __name__ == "__main__":
    main()
