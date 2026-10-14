"""ADR0307 renew admission only after immutable native/live background checks."""
import copy
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import work_envelope as policy


def contract():
    return {"decision": "ADR0307", "inventory_freshness_seconds": 300,
            "apis": 4, "workers": 14, "native_worker_scale_counts": {"ADR0315": 15, "ADR0319": 15},
            "additional_database_connections": 0,
            "helper_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def selected(goal):
    keys = {"inventory_renewal_decision", "inventory_renewal_contract_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    if (goal.get("inventory_renewal_decision") != "ADR0307"
            or goal.get("inventory_renewal_contract_sha256") != policy.digest(contract())
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("profile") not in {"cce_paid_comparison", "cce_hourly_qualification"}):
        raise ValueError("Exact live-admission renewal binding required")
    return True


def renew(inventory, *, run, binding, native_before, workers_before, observe_native,
          observe_workers, verify_background, now=None):
    if binding.get("cce_inventory_renewal_contract") != contract():
        raise ValueError("Reserved live-admission renewal required")
    if native_before.get("run") != run or workers_before.get("run") != run:
        raise ValueError("Same-run native admission evidence required")
    worker_count = 14
    if "cce_writer_scale_contract" in binding:
        from cce_recovery_headroom import writer_scale_contract
        from cce_worker_identity import validate_bundle
        if (binding["cce_writer_scale_contract"] != writer_scale_contract()
                or workers_before.get("decision") not in {"ADR0315", "ADR0319"}):
            raise ValueError("Exactly declared native writer scale required")
        if workers_before.get("decision") == "ADR0319":
            from cce_measured_payment_context_profile import contract as context_contract
            if binding.get("cce_measured_payment_context_contract") != context_contract():
                raise ValueError("Corrected image requires its reserved source-bound comparison")
        validate_bundle(workers_before)
        worker_count = 15
    if len(native_before.get("receipts", [])) != 4 or len(workers_before.get("receipts", [])) != worker_count:
        raise ValueError("Complete API and declared worker admission evidence required")
    native = observe_native()
    workers = observe_workers()
    verify_background()
    if native != native_before or workers != workers_before:
        raise ValueError("Live native admission identities changed")
    captured = datetime.fromisoformat(inventory["captured_at"])
    checked = now or datetime.now(UTC)
    if captured.tzinfo is None or checked.tzinfo is None or captured > checked:
        raise ValueError("Valid original and live-validation chronology required")
    value = copy.deepcopy(inventory)
    value["captured_at"] = checked.isoformat()
    evidence = {"decision": "ADR0307", "run": run,
                "binding_sha256": policy.digest(binding),
                "original_inventory_sha256": policy.digest(inventory),
                "original_captured_at_utc": inventory["captured_at"],
                "live_validated_at_utc": checked.isoformat(),
                "native_api_receipts_sha256": policy.digest(native),
                "native_worker_receipts_sha256": policy.digest(workers),
                "timestamp_scope": "joint live native admission revalidation; original ECS bootstrap retained",
                "background_unchanged": True, "native_background_workers": worker_count,
                "normal_ecs_background_workers": 14}
    value["native_admission_renewal"] = evidence
    return value, evidence
