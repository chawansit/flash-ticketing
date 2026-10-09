"""ADR0241 matched source payloads from the verified historical API, not a full branch image."""
import ast
import hashlib
import json
from pathlib import Path

import cce_api_adapter as native
import cce_dependency_probe as parent
import cce_historical_sources as historical
from stage_status_refresh_images import new_stage_output

ROOT = Path(__file__).resolve().parents[1]
OVERLAY = ("src/ticketing/api.py", "src/ticketing/config.py", "src/ticketing/http.py",
           "src/ticketing/infrastructure/postgres.py", "src/ticketing/infrastructure/slot_diagnostics.py")
POSTGRES = "src/ticketing/infrastructure/postgres.py"
CANDIDATE_BEGIN = """                    # Psycopg starts non-autocommit transactions before the first query.
                    # A borrowed autocommit adapter still needs an explicit boundary.
                    if getattr(conn, "autocommit", False):
                        conn.execute("BEGIN")
"""
CONTROL_BEGIN = '                    conn.execute("BEGIN")\n'
EXPORT = "tmp/adr0163-offline-images/source"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def pairs():
    legacy = native.legacy_contract()["api_sources"]
    candidate = {name: historical.blob(digest) for name, digest in legacy.items()}
    export = historical.manifest()["exports"][EXPORT]
    if any(export.get(name) != digest for name, digest in legacy.items()):
        raise ValueError("Complete pinned historical API export required")
    for name in OVERLAY:
        source = ROOT / name
        if source.is_symlink():
            raise ValueError("Source symlink forbidden")
        candidate[name] = source.read_bytes().replace(b"\r\n", b"\n")
    # This later worker-only setting did not exist in the historical API package.
    # Do not silently include another PR6 change in a matched API comparison.
    config = "src/ticketing/config.py"
    unrelated = b'    reservation_write_pipeline: bool = os.getenv("RESERVATION_WRITE_PIPELINE", "0") == "1"\n'
    if candidate[config].count(unrelated) != 1:
        raise ValueError("Exact isolated configuration correction required")
    candidate[config] = candidate[config].replace(unrelated, b"")
    # ADR0251 changes worker dispatch validation only. Preserve the reviewed API pair.
    new_bound = b'        # HTTP delivery runs outside SQL transactions; bound its slots separately.\n        if type(self.simulator_concurrency) is not int or not 1 <= self.simulator_concurrency <= 32:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must be an integer between 1 and 32")\n'
    old_bound = b'        if not 1 <= self.simulator_concurrency <= self.pool_max:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must fit DB_POOL_MAX")\n'
    if candidate[config].count(new_bound) == 1:
        candidate[config] = candidate[config].replace(new_bound, old_bound, 1)
    elif candidate[config].count(old_bound) != 1:
        raise ValueError("Exact isolated simulator validation correction required")
    raw = candidate[POSTGRES].decode()
    if raw.count(CANDIDATE_BEGIN) != 1:
        raise ValueError("Exact reviewed transaction startup factor required")
    for name, payload in candidate.items():
        ast.parse(payload, filename=name)
    control = {**candidate, POSTGRES: raw.replace(CANDIDATE_BEGIN, CONTROL_BEGIN).encode()}
    if [name for name in candidate if candidate[name] != control[name]] != [POSTGRES]:
        raise ValueError("Only transaction startup may differ between arms")
    dependencies = {name: export[name] for name in ("pyproject.toml", "requirements.lock")}
    plan = {"decision": "ADR0241", "parent_image": parent.IMAGE, "parent_manifest_digest": parent.MANIFEST,
            "parent_runtime_source_sha256": legacy, "dependency_input_sha256": dependencies,
            "common_environment_changes": {"DB_FAILURE_DIAGNOSTICS": "1"},
            "unchanged_legacy_modules": sorted(set(legacy) - set(OVERLAY)),
            "comparison_factor": "redundant_explicit_begin", "cloud_execution_authorized_by_this_plan": False,
            "cloud_load_started": False, "capacity_improvement_measured": False,
            "runtime_sources": {arm: {name: sha(raw) for name, raw in sources.items()}
                                for arm, sources in (("control", control), ("candidate", candidate))}}
    return {"control": control, "candidate": candidate}, plan


def prepare(parent_config):
    sources, plan = pairs()
    raw_config = Path(parent_config).read_bytes()
    if "sha256:" + sha(raw_config) != parent.CONFIG:
        raise ValueError("Exact immutable parent configuration required")
    user = json.loads(raw_config)["config"]["User"]
    if not user or user in {"root", "0"} or any(c in user for c in "\r\n "):
        raise ValueError("Exact parent config and non-root runtime user required")
    plan["parent_runtime_user"] = user
    output = new_stage_output()
    output.mkdir(parents=False, exist_ok=False)
    for arm, files in sources.items():
        target = output / arm
        target.mkdir()
        for name, raw in files.items():
            destination = target / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
        manifest = {"parent_runtime_source_sha256": plan["parent_runtime_source_sha256"],
                    "runtime_source_sha256": plan["runtime_sources"][arm],
                    "dependency_input_sha256": plan["dependency_input_sha256"]}
        (target / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", newline="\n")
        installer = ROOT / "scripts/status_refresh_image_install.py"
        (target / "install.py").write_bytes(installer.read_bytes().replace(b"\r\n", b"\n"))
        dockerfile = ("FROM " + plan["parent_image"] + "\nUSER root\nCOPY . /tmp/slot-payload\n"
                      "RUN python /tmp/slot-payload/install.py /tmp/slot-payload\nUSER " + user + "\n")
        (target / "Dockerfile").write_text(dockerfile, newline="\n")
    (output / "source-plan.json").write_text(json.dumps(plan, indent=2) + "\n", newline="\n")
    return {"output": str(output), "source_count_per_arm": len(sources["control"]),
            "unchanged_legacy_modules": len(plan["unchanged_legacy_modules"]), "cloud_calls": 0}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-config", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.parent_config), indent=2))
