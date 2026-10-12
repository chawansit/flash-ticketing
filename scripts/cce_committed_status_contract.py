"""ADR0298 preserve event-lane lifecycle with the new publication source."""
import cce_committed_status_identity as pins
import cce_committed_status_profile as profile
import cce_event_lane_identity as legacy_pins
from cce_event_lane_contract import EventLaneContract


class CommittedStatusContract(EventLaneContract):
    def __init__(self, *, arm):
        if arm not in {"control", "candidate"}:
            raise ValueError("Explicit committed-status arm required")
        super().__init__(status_refresh_reuse=True)
        self.status_arm = arm
        self.api_settings = {**self.api_settings, "ORDER_STATUS_COMMITTED_SNAPSHOT": "0"}
        self.shared = profile.image()
        for role in self.background:
            self.images[role] = pins.IMAGE_ID

    def settings(self, role):
        # ECS bootstrap stays off. Activation occurs only on native fulfillment pods.
        return {**super().settings(role), "ORDER_STATUS_COMMITTED_SNAPSHOT": "0"}

    def image_program(self, roles):
        return super().image_program(roles).replace(repr(legacy_pins.SOURCE), repr(pins.SOURCE))

    def inventory_marker(self):
        return {**super().inventory_marker(), "shared_image_decision": pins.DECISION,
                "shared_image_source_sha256": pins.SOURCE, "shared_image_configuration_digest": pins.CONFIG,
                "shared_image_receipt_sha256": pins.RECEIPT_SHA256, "committed_status_decision": "ADR0298",
                "committed_status_arm": self.status_arm,
                "committed_status_snapshot": "1" if self.status_arm == "candidate" else "0"}

    def verify_inventory(self, data):
        from observe_two_host_pipeline import verify_admission_factor_evidence
        from status_refresh_contract import StatusRefreshContract
        StatusRefreshContract.verify_inventory(self, data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Committed-status marker differs")
        legacy, _ = legacy_pins.historical_view(data, legacy_pins.digest(data), identity=pins)
        verify_admission_factor_evidence(legacy)
