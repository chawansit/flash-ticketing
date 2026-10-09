"""ADR0238 offline reproduction contract check; no credentials or cloud actions."""
import hashlib
import json
from pathlib import Path

import cce_api_adapter as adapter
import cce_dependency_probe as dependency
import cce_frozen_harness as frozen
import cce_paid_stage as paid
import generator_completion_probe_contract as historical
import run_cce_hourly_qualification as hourly
from cce_paid_profiles import HOURLY, SHORT

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "docs/capacity/cce/reproduction-lock-2026-10-09.json"


def verify():
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    if lock.get("decision") != "ADR0238" or lock.get("cloud_execution_authorized") is not False:
        raise ValueError("Offline promotion contract required")
    for relative, expected in lock["files"].items():
        parts = Path(relative)
        if parts.is_absolute() or ".." in parts.parts or "\\" in relative:
            raise ValueError("Canonical promotion path required")
        path = ROOT / relative
        if path.is_symlink() or not path.resolve().is_relative_to(ROOT.resolve()):
            raise ValueError("Promotion input escapes checkout")
        raw = path.read_bytes().replace(b"\r\n", b"\n")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("Promotion identity drift: " + relative)
    contract = adapter.contract()
    historical_plan = historical.plan()
    runtime_contract = historical.GeneratorCompletionProbeContract(
        historical_plan["artifact_receipt"], "candidate", contract["api_sources"]
    )
    if runtime_contract.images != lock["historical_role_images"]:
        raise ValueError("Complete historical deployment contract differs")
    bundle = paid.generator_bundle()
    # Verify every parent file even though the qualified helper replaces one runtime member.
    frozen.frozen_bundle()
    hourly_plan = hourly.plan()
    expected = {"short": {"rate": 84, "seconds": SHORT.duration, "tickets": SHORT.expected},
                "hourly": {"rate": 84, "seconds": HOURLY.duration, "tickets": HOURLY.expected}}
    if lock["workload_profiles"] != expected or dependency.IMAGE != lock["cce_api_image"]:
        raise ValueError("Pinned image or workload profile differs")
    if "@sha256:" not in dependency.IMAGE or contract["backend_images"] != lock["historical_role_images"]:
        raise ValueError("Immutable historical role images required")
    if lock["historical_api_sources"] != contract["api_sources"]:
        raise ValueError("Historical runtime source identity differs")
    if hourly_plan["acquisition_budget"] != 20 or hourly_plan["pooler_server_connections"] != 24:
        raise ValueError("Historical hourly budget differs")
    return {"decision": "ADR0238", "pass": True, "checked_files": len(lock["files"]),
            "frozen_parent_files": 78, "runtime_generator_files": len(bundle),
            "historical_contract_constructor_verified": True, "historical_role_count": len(runtime_contract.images),
            "cce_api_image": dependency.IMAGE, "workload_profiles": expected,
            "historical_images_match_pr6_combined_source": False,
            "cloud_calls": 0, "customer_dispatches": 0,
            "capacity_improvement_measured": False, "production_qualified": False}


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
