"""ADR0152: reproduce qualified source; explicitly derive offline local images."""

import argparse
import copy
import hashlib
import io
import json
import re
import subprocess
import tarfile
from pathlib import Path

from runtime_source_identity import source_identity_program
from status_refresh_contract import IMAGE, LABEL, LOCAL, PLAN, REVISION, ROLES, ROOT, digest

ARTIFACTS = ROOT / "artifacts/status-refresh"
INSTALLER = ROOT / "scripts/status_refresh_image_install.py"
EXPORT_PATHS = (
    "src",
    "tests",
    "scripts",
    "migrations",
    "pyproject.toml",
    "requirements.lock",
    "Dockerfile",
    ".env.example",
    "compose.yaml",
)


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def command(args, *, cwd=ROOT, log=None, timeout=30):
    result = subprocess.run(args, cwd=cwd, capture_output=True, timeout=timeout, check=False)
    if log is not None:
        log.write_bytes(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {result.returncode}); retained evidence required")
    return result.stdout


def manifest():
    data = json.loads((ARTIFACTS / "manifest.json").read_text())
    report, plan = json.loads(LOCAL.read_text()), json.loads(PLAN.read_text())
    if (
        data["schema"] != 1
        or data["base_revision"] != REVISION
        or data["overlay_sha256"] != report["isolated_candidate"]["changed_source_sha256"]
        or data["runtime_source_sha256"] != plan["expected_runtime_source_sha256"]
    ):
        raise ValueError("Qualified isolated manifest differs")
    patch = (ARTIFACTS / "adr0149.patch").read_bytes()
    if hashlib.sha256(patch).hexdigest() != data["patch_sha256"]:
        raise ValueError("Pinned patch digest differs")
    headers = re.findall(r"^diff --git a/(\S+) b/(\S+)$", patch.decode(), re.MULTILINE)
    if (
        len(headers) != len(data["overlay_sha256"])
        or len(set(headers)) != len(headers)
        or any(a != b or a not in data["overlay_sha256"] for a, b in headers)
    ):
        raise ValueError("Exact allowlisted patch paths required")
    for line in patch.decode().splitlines():
        if line.startswith(("--- ", "+++ ")):
            value = line[4:]
            if value != "/dev/null" and value not in {
                prefix + p for p in data["overlay_sha256"] for prefix in ("a/", "b/")
            }:
                raise ValueError("Patch path escapes allowlist")
    from cce_historical_sources import manifest as parent_manifest
    from cce_historical_sources import parent_file
    expected_parent = {}
    files = [p for p in parent_manifest()["base"] if p.startswith("src/ticketing/")]
    for path in files:
        if path.endswith(".py"):
            expected_parent[path] = sha(parent_file(path))
    expected_runtime = {
        **expected_parent,
        **{p: h for p, h in data["overlay_sha256"].items() if p.startswith("src/")},
    }
    if expected_runtime != data["runtime_source_sha256"]:
        raise ValueError("Complete frozen/qualified module set differs")
    source_identity_program(expected_parent)
    source_identity_program(expected_runtime)
    for path, expected in data["dependency_input_sha256"].items():
        if (
            path not in {"pyproject.toml", "requirements.lock"}
            or sha(parent_file(path)) != expected
        ):
            raise ValueError("Frozen dependency inputs differ")
    if set(data["dependency_input_sha256"]) != {"pyproject.toml", "requirements.lock"}:
        raise ValueError("Complete frozen dependency inputs required")
    data["parent_runtime_source_sha256"] = expected_parent
    return data


def owned_output(output):
    output = output.resolve()
    if (
        not output.is_relative_to((ROOT / "tmp").resolve())
        or output == (ROOT / "tmp").resolve()
        or output.exists()
    ):
        raise ValueError("Fresh owned output beneath repository tmp required")
    output.mkdir(parents=True, mode=0o700)
    return output


def export_source(output, data):
    source = output / "source"
    source.mkdir()
    from cce_historical_sources import parent_archive
    raw = parent_archive()
    expected = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > 10000 or sum(m.size for m in members) > 128 * 1024 * 1024:
            raise ValueError("Bounded source archive required")
        for member in members:
            path = Path(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or member.name.startswith(("/", "\\"))
                or ":" in member.name
            ):
                raise ValueError("Unsafe source archive path")
            destination = source / path
            if not destination.resolve().is_relative_to(source.resolve()):
                raise ValueError("Source archive escaped output")
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            elif member.isfile() and member.name not in expected:
                payload = archive.extractfile(member).read().replace(b"\r\n", b"\n")
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(payload)
                expected[member.name] = sha(payload)
            else:
                raise ValueError("Archive symlink/special/duplicate rejected")
    # Prefix every patch target with this exact fresh directory; never use --unsafe-paths or the current src tree.
    directory = source.relative_to(ROOT).as_posix()
    for check in (True, False):
        args = ["git", "apply", "--whitespace=error", "--directory=" + directory]
        if check:
            args.append("--check")
        command(
            [*args, str(ARTIFACTS / "adr0149.patch")],
            log=output / ("patch-check.txt" if check else "patch-apply.txt"),
        )
    expected.update(data["overlay_sha256"])
    verify_tree(source, expected)
    return source, expected


def verify_tree(source, expected):
    if not source.exists():
        from cce_historical_sources import verify_export
        verify_export(source, expected)
        return
    paths = list(source.rglob("*"))
    if any(p.is_symlink() for p in paths):
        raise ValueError("Source tree symlink rejected")
    files = {p.relative_to(source).as_posix(): p for p in paths if p.is_file()}
    if set(files) != set(expected) or any(sha(files[p].read_bytes()) != h for p, h in expected.items()):
        raise ValueError("Exported source drift or unqualified file")


def prepare(output):
    data = manifest()
    output = owned_output(output)
    source, expected = export_source(output, data)
    context = output / "image-context"
    context.mkdir()
    for path in data["runtime_source_sha256"]:
        destination = context / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((source / path).read_bytes())
    (context / "manifest.json").write_text(json.dumps(data, sort_keys=True) + "\n")
    (context / "install.py").write_bytes(INSTALLER.read_bytes().replace(b"\r\n", b"\n"))
    receipt = {
        "kind": "isolated_status_refresh_source",
        "base_revision": REVISION,
        "patch_sha256": data["patch_sha256"],
        "source_files_verified": len(expected),
        "source_file_sha256": expected,
        "runtime_modules_verified": len(data["runtime_source_sha256"]),
        "source_manifest_sha256": digest(
            {"base_revision": REVISION, "runtime_source_sha256": data["runtime_source_sha256"]}
        ),
        "installer_sha256": sha(INSTALLER.read_bytes()),
        "cloud_calls": 0,
        "images_built": 0,
    }
    (output / "source-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return output, data, receipt


def inspect_image(image):
    if not isinstance(image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ValueError("Immutable local parent/image required")
    result = json.loads(command(["docker", "image", "inspect", image]))
    if (
        len(result) != 1
        or result[0]["Id"] != image
        or result[0].get("Os") != "linux"
        or result[0].get("Architecture") != "amd64"
    ):
        raise ValueError("Expected immutable Linux amd64 image required")
    return result[0]


def image_probe(data, *, parent):
    installer = INSTALLER.read_text().partition('if __name__ == "__main__":')[0]
    expected = data["parent_runtime_source_sha256" if parent else "runtime_source_sha256"]
    program = source_identity_program(expected).replace("print(json.dumps(proof))", "")
    return (
        installer
        + "\n"
        + program
        + "\nextra=verify("
        + repr(data)
        + ",parent="
        + repr(parent)
        + ")\n"
        + "if proof.get('source_hashes_match') is not True:raise ValueError('Import/bytecode differs')\n"
        + "print(json.dumps({'source_identity':proof,**extra}))\n"
    )


def probe(image, data, *, parent, log):
    proof = json.loads(
        command(
            [
                "docker",
                "run",
                "--rm",
                "--network=none",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--entrypoint",
                "python",
                image,
                "-c",
                image_probe(data, parent=parent),
            ],
            log=log,
            timeout=45,
        )
    )
    if proof.get("source_identity", {}).get("source_hashes_match") is not True:
        raise ValueError("Image source proof missing")
    return proof


def dockerfile(parent, tag, source_manifest):
    user = parent["Config"].get("User", "")
    if (
        not isinstance(user, str)
        or not re.fullmatch(r"[A-Za-z0-9_.:-]{0,80}", user)
        or parent["Config"].get("OnBuild")
    ):
        raise ValueError("Safe inherited user and no ONBUILD required")
    change_user = bool(user) and user not in {"root", "0", "root:root", "0:0"}
    lines = ["FROM " + tag]
    if change_user:
        lines.append("USER root")
    lines += [
        "LABEL " + LABEL + '="' + source_manifest + '"',
        "COPY . /tmp/adr0152-payload/",
        "RUN python /tmp/adr0152-payload/install.py /tmp/adr0152-payload && rm -rf /tmp/adr0152-payload",
    ]
    if change_user:
        lines.append("USER " + user)
    return "\n".join(lines) + "\n"


def verify_derived(parent, candidate, source_manifest):
    layers = parent["RootFS"]["Layers"]
    if not layers or candidate["RootFS"]["Layers"][: len(layers)] != layers:
        raise ValueError("Original parent layers lost")
    expected, actual = copy.deepcopy(parent["Config"]), copy.deepcopy(candidate["Config"])
    labels = dict(expected.get("Labels") or {})
    labels[LABEL] = source_manifest
    expected["Labels"] = labels
    if expected != actual:
        raise ValueError("Inherited runtime configuration changed")


def build_one(output, data, parent_id, *, timeout=300):
    parent = inspect_image(parent_id)
    probe(parent_id, data, parent=True, log=output / "parent-source-proof.json")
    tag = "flash-ticketing-adr0152-parent:" + parent_id.removeprefix("sha256:")
    command(["docker", "tag", parent_id, tag])
    context = output / "context"
    context.mkdir()
    # Only qualified payload bytes and installer go into the image, never tests/config/secrets.
    for path in [*data["runtime_source_sha256"], *data.get("migration_sha256", {}), "manifest.json", "install.py"]:
        supplied = output.parent / "image-context" / path
        destination = context / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(supplied.read_bytes())
    source_manifest = digest(
        {"base_revision": REVISION, "runtime_source_sha256": data["runtime_source_sha256"]}
    )
    (context / "Dockerfile").write_text(dockerfile(parent, tag, source_manifest))
    validate_context(context, data, dockerfile(parent, tag, source_manifest))
    image_id_file = output / "image-id.txt"
    command(
        ["docker", "build", "--network=none", "--pull=false", "--iidfile", str(image_id_file), str(context)],
        log=output / "build.log",
        timeout=timeout,
    )
    image_id = image_id_file.read_text().strip()
    candidate = inspect_image(image_id)
    verify_derived(parent, candidate, source_manifest)
    proof = probe(image_id, data, parent=False, log=output / "candidate-source-proof.json")
    return image_id, proof


def validate_context(context, data, expected_dockerfile):
    expected = {
        **data["runtime_source_sha256"],
        **data.get("migration_sha256", {}),
        "install.py": sha(INSTALLER.read_bytes()),
        "manifest.json": sha((json.dumps(data, sort_keys=True) + "\n").encode()),
        "Dockerfile": sha(expected_dockerfile.encode()),
    }
    verify_tree(context, expected)


def validate_parents(parents):
    if not isinstance(parents, dict) or set(parents) != set(ROLES) or parents.get("api") != IMAGE:
        raise ValueError("Complete original per-role parents and frozen cloud API image required")
    if any(not isinstance(p, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", p) for p in parents.values()):
        raise ValueError("Immutable per-role parents required")


def build(output, data, parents):
    validate_parents(parents)
    # Every original must be present and pass before any candidate image build.
    for parent_id in sorted(set(parents.values())):
        inspect_image(parent_id)
        probe(parent_id, data, parent=True, log=output / ("preflight-" + parent_id[-12:] + ".json"))
    derived = {}
    for parent_id in sorted(set(parents.values())):
        folder = output / ("build-" + parent_id.removeprefix("sha256:"))
        folder.mkdir()
        derived[parent_id], _proof = build_one(folder, data, parent_id)
    receipt = {
        "images": {r: derived[p] for r, p in parents.items()},
        "parent_images": parents,
        "source_manifest_sha256": digest(
            {"base_revision": REVISION, "runtime_source_sha256": data["runtime_source_sha256"]}
        ),
    }
    (output / "artifact-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


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
        validate_parents(parents)
    output, data, receipt = prepare(args.output)
    if args.build:
        built = build(output, data, parents)
        print(
            json.dumps(
                {
                    "phase": "local-images-built",
                    "unique_images": len(set(built["images"].values())),
                    "cloud_calls": 0,
                }
            )
        )
    else:
        print(
            json.dumps(
                {
                    "phase": "isolated-source-prepared",
                    "source_files_verified": receipt["source_files_verified"],
                    "runtime_modules_verified": receipt["runtime_modules_verified"],
                    "images_built": 0,
                    "cloud_calls": 0,
                }
            )
        )


if __name__ == "__main__":
    main()
