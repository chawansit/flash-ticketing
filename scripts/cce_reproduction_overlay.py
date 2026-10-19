"""ADR0242 verify archived historical inputs and explicit current orchestration overlay."""
import hashlib
import json
from pathlib import Path

OVERLAID = {
    "scripts/probe_two_host_safety.py",
    'scripts/cce_paid_profiles.py', 'scripts/cce_paid_inputs.py', 'scripts/fixture_identity_evidence.py', 'scripts/run_two_host_paid_comparison.py',
    "scripts/work_envelope.py", "scripts/qualify_two_host_deployment.py", "scripts/prepare_two_host_scaling.py", "scripts/status_refresh_contract.py", "scripts/run_status_refresh_comparison.py",
    "scripts/collect_two_host_inventory.py", "scripts/runtime_source_identity.py",
    "scripts/cce_paid_resources.py", "scripts/cce_paid_lifecycle.py",
    "scripts/cce_dependency_probe.py", "scripts/observe_cce_paid_pipeline.py", "scripts/observe_two_host_cpu.py",
    "scripts/database_wait_evidence.py", "scripts/observe_two_host_pipeline.py",
    "scripts/checkout_journey_probe.py", "scripts/paid_ticket_load_generator.py", "scripts/paid_ticket_sharded_generator.py",
    "scripts/cce_api_adapter.py", "scripts/cce_paid_stage.py", "scripts/cce_paid_observers.py",
    "scripts/cce_ecs_transition.py", "scripts/run_cce_paid_comparison.py", "scripts/check_cce_reproducibility.py",
    "scripts/run_cce_hourly_qualification.py", "scripts/cce_hourly_paid_generator.py", "scripts/cce_hourly_paid_leaf.py"}
ADDITIONAL = {
    'scripts/cce_persistent_uuid.py','scripts/persistent_uuid_overlay.py','scripts/prepare_persistent_uuid_image.py','scripts/publish_persistent_uuid_image.py','docs/capacity/cce/persistent-uuid-local-2026-10-19.json','docs/capacity/cce/persistent-uuid-registry-2026-10-19.json',
          'scripts/cce_image_qualification.py','scripts/cce_projection_refresh_pipeline.py','scripts/projection_refresh_pipeline_overlay.py','scripts/prepare_projection_refresh_pipeline_image.py','scripts/publish_projection_refresh_pipeline_image.py','docs/capacity/cce/projection-refresh-pipeline-local-2026-10-19.json','docs/capacity/cce/projection-refresh-pipeline-registry-2026-10-19.json',
          "scripts/cce_reservation_claim_lookup.py", "scripts/reservation_claim_lookup_overlay.py",
    "scripts/prepare_reservation_claim_lookup_image.py", "scripts/publish_reservation_claim_lookup_image.py",
    "docs/capacity/cce/reservation-claim-lookup-local-2026-10-19.json",
    "docs/capacity/cce/reservation-claim-lookup-registry-2026-10-19.json",
    "scripts/cce_combined_worker_identity.py",
    "scripts/prepare_callback_write_pipeline_image.py",
    "docs/capacity/cce/callback-write-pipeline-local-2026-10-17.json",
    "docs/capacity/cce/callback-write-pipeline-registry-2026-10-17.json",
    "scripts/cce_fixed_worker_allocation.py", "scripts/cce_rate_qualification.py", "scripts/prepare_rate_qualification.py",
        "scripts/cce_replacement_rds_binding.py",
    "docs/capacity/cce/rds-endpoint-switch-2026-10-18.json",
    "scripts/cce_simulator_roundtrip_profile.py", "scripts/prepare_simulator_roundtrip_image.py",
    "scripts/prepare_simulator_roundtrip_comparison.py",
    "docs/capacity/cce/simulator-roundtrip-local-2026-10-17.json",
    "docs/capacity/cce/simulator-roundtrip-registry-2026-10-17.json",
    "scripts/cce_api_client_headroom.py",
    "docs/capacity/cce/api-client-pool-headroom-local-2026-10-17.json",
    "scripts/cce_payment_delivery_headroom.py",
    "docs/capacity/cce/payment-delivery-headroom-local-2026-10-17.json",
    "scripts/cce_auth_dispatch_profile.py",
    "docs/capacity/cce/authentication-dispatch-local-2026-10-17.json",
    "docs/capacity/cce/authentication-dispatch-image-2026-10-17.json",
    "scripts/database_query_cost.py",
    "scripts/cce_fixture_clock.py",
    "scripts/cce_generator_process_scaling.py",
    "docs/capacity/cce/generator-process-calibration-2026-10-17.json",
    "scripts/cce_primary_bootstrap.py", "scripts/qualify_cce_primary_bootstrap.py",
    "scripts/cce_bounded_financial_audit.py", "scripts/independent_database_sampling.py",
    "scripts/cce_committed_status_identity.py", "scripts/cce_committed_status_profile.py",
    "scripts/cce_committed_status_contract.py",
    "docs/capacity/cce/committed-fulfillment-status-image-2026-10-13.json",
    "scripts/cce_verification_contract.py", "scripts/cce_payment_pipeline_identity.py",
    "scripts/cce_payment_pipeline_profile.py", "scripts/cce_payment_pipeline_contract.py",
    "docs/capacity/cce/payment-write-pipeline-image-2026-10-12.json",
    "scripts/customer_timeout_bundle.py", "scripts/cce_native_database_bridge.py", "scripts/cce_status_refresh_reuse.py", "scripts/cce_status_reuse_hourly.py",
    "docs/capacity/cce/committed-status-reuse-result-2026-10-11.json",
    "artifacts/customer-timeout/manifest.json", "artifacts/customer-timeout/coordinator.py",
    "artifacts/customer-timeout/paid_ticket_load_generator.py",
    "artifacts/customer-timeout/paid_ticket_sharded_generator.py",
    "scripts/cce_ticket_target_profile.py",
    "scripts/cce_live_admission.py", "scripts/cce_worker_rebalance_profile.py",
    "scripts/cce_event_lane_identity.py", "scripts/cce_event_lane_profile.py",
        "scripts/cce_event_lane_contract.py", "scripts/observe_projection_kafka.py",
        "docs/capacity/cce/event-lane-fetch-image-2026-10-10.json",
        "scripts/cce_shared_worker_comparison.py", "scripts/cce_shared_worker_profile.py",
    "scripts/cce_shared_worker_contract.py", "scripts/cce_shared_application.py",
    "scripts/cce_shared_worker_image_identity.py", "scripts/cce_payment_dispatch_profile.py",
    "scripts/cce_native_workers.py", "scripts/cce_worker_transition.py", "scripts/cce_worker_identity.py", "scripts/observe_cce_workers.py",
    "docs/capacity/cce/shared-application-image-2026-10-10.json",
    "docs/capacity/cce/shared-application-image-consumer-restored-2026-10-10.json",
    "scripts/cce_recovery_hourly_profile.py", "artifacts/hourly-customer-recovery/manifest.json",
    "docs/capacity/cce/customer-recovery-comparison-2026-10-10.json",
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
