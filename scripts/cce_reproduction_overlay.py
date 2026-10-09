"""ADR0242 verify archived historical inputs and explicit current orchestration overlay."""
import hashlib
import json
from pathlib import Path

OVERLAID = {
    "scripts/checkout_journey_probe.py", "scripts/paid_ticket_load_generator.py", "scripts/paid_ticket_sharded_generator.py",
    "scripts/cce_api_adapter.py", "scripts/cce_paid_stage.py", "scripts/cce_paid_observers.py",
    "scripts/cce_ecs_transition.py", "scripts/run_cce_paid_comparison.py", "scripts/check_cce_reproducibility.py",
    "scripts/run_cce_hourly_qualification.py"}
ADDITIONAL = {
    "scripts/paid_fixture_layout.py",
    "scripts/cce_customer_recovery_profile.py", "scripts/customer_recovery_bundle.py",
    "artifacts/customer-recovery/manifest.json", "artifacts/customer-recovery/coordinator.py",
    "docs/capacity/cce/customer-recovery-image-2026-10-10.json",
    'scripts/customer_recovery_client.py', 'scripts/prepare_customer_recovery_image.py', 'artifacts/slot-comparison-baseline/manifest.json', 'artifacts/slot-comparison-baseline/src/ticketing/api.py', 'artifacts/slot-comparison-baseline/src/ticketing/config.py', 'artifacts/slot-comparison-baseline/src/ticketing/http.py', 'artifacts/slot-comparison-baseline/src/ticketing/infrastructure/postgres.py', 'artifacts/slot-comparison-baseline/src/ticketing/infrastructure/slot_diagnostics.py',
    "scripts/cce_transaction_profile.py", "scripts/cce_slot_trace_transport.py",
    "scripts/cce_reproduction_overlay.py", "scripts/prepare_slot_comparison_sources.py",
    "scripts/observe_slot_paid_pipeline.py", "scripts/slot_failure_evidence.py",
    "scripts/bounded_trace_transport.py", "scripts/prepare_payment_context_sources.py", "scripts/cce_payment_context_images.py",
        "scripts/cce_simulator_dispatch_profile.py", "scripts/prepare_simulator_dispatch_sources.py",
        "docs/capacity/cce/simulator-dispatch-image-2026-10-09.json",
    "docs/capacity/cce/simulator-dispatch-comparison-2026-10-10.json",
    "docs/capacity/cce/payment-context-images-2026-10-09.json",
    "docs/capacity/cce/slot-comparison-images-2026-10-09.json"}
MANIFEST = "artifacts/cce-transaction-runner/manifest.json"


def raw(root, relative):
    parts = Path(relative)
    if parts.is_absolute() or ".." in parts.parts or "\\" in relative:
        raise ValueError("Canonical overlay input required")
    path = root / parts
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Overlay input escapes checkout")
    return path.read_bytes().replace(b"\r\n", b"\n")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def verify(root, lock):
    value = json.loads(raw(root, MANIFEST))
    if (set(value) != {"decision", "historical_lock_sha256", "overlays", "additional_inputs"}
            or value["decision"] != "ADR0242" or set(value["overlays"]) != OVERLAID
            or set(value["additional_inputs"]) != ADDITIONAL
            or value["historical_lock_sha256"] != sha(raw(root, "docs/capacity/cce/reproduction-lock-2026-10-09.json"))):
        raise ValueError("Exact declared historical/current overlay required")
    for name, item in value["overlays"].items():
        expected = lock["files"][name]
        if (set(item) != {"historical_sha256", "historical_blob", "current_sha256"}
                or item["historical_sha256"] != expected
                or item["historical_blob"] != "artifacts/cce-transaction-runner/blobs/" + expected
                or sha(raw(root, item["historical_blob"])) != expected
                or sha(raw(root, name)) != item["current_sha256"]):
            raise ValueError("Historical/current runner identity drift: " + name)
    for name, digest in value["additional_inputs"].items():
        if sha(raw(root, name)) != digest:
            raise ValueError("Transaction extension identity drift: " + name)
    return value
