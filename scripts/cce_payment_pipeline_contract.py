"""ADR0285 image-only extension of the qualified event-lane lifecycle."""
import cce_event_lane_identity as legacy_pins
import cce_payment_pipeline_identity as pins
import cce_payment_pipeline_profile as profile
from cce_event_lane_contract import EventLaneContract


class PaymentPipelineContract(EventLaneContract):
    def __init__(self, *, arm, status_refresh_reuse=True):
        if arm not in {"control", "candidate"}:
            raise ValueError("Explicit payment pipeline arm required")
        super().__init__(status_refresh_reuse=status_refresh_reuse)
        self.api_settings = {**self.api_settings, "PAYMENT_WRITE_PIPELINE": "0"}
        self.pipeline_arm = arm
        self.shared = profile.image()
        for role in self.background:
            self.images[role] = pins.IMAGE_ID

    def settings(self, role):
        return {**super().settings(role), "PAYMENT_WRITE_PIPELINE": "0"}

    def image_program(self, roles):
        return super().image_program(roles).replace(repr(legacy_pins.SOURCE), repr(pins.SOURCE))

    def inventory_marker(self):
        return {**super().inventory_marker(), "shared_image_decision": pins.DECISION,
                "shared_image_source_sha256": pins.SOURCE, "shared_image_configuration_digest": pins.CONFIG,
                "shared_image_receipt_sha256": pins.RECEIPT_SHA256, "payment_pipeline_decision": pins.DECISION,
                "payment_pipeline_arm": self.pipeline_arm, "bootstrap_payment_write_pipeline": "0",
                "payment_write_pipeline": "1" if self.pipeline_arm == "candidate" else "0"}

    def verify_inventory(self, data):
        from observe_two_host_pipeline import verify_admission_factor_evidence
        from status_refresh_contract import StatusRefreshContract
        StatusRefreshContract.verify_inventory(self, data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Payment pipeline marker differs")
        legacy, _ = legacy_pins.historical_view(data, legacy_pins.digest(data), identity=pins)
        verify_admission_factor_evidence(legacy)
