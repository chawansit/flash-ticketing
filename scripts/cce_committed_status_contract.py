"""ADR0298 preserve event-lane lifecycle with the new publication source."""
import cce_committed_status_identity as pins
import cce_committed_status_profile as profile
import cce_event_lane_identity as legacy_pins
from cce_event_lane_contract import EventLaneContract


class CommittedStatusContract(EventLaneContract):
    def __init__(self, *, arm, identity=pins):
        if arm not in {"control", "candidate"}:
            raise ValueError("Explicit committed-status arm required")
        import cce_explicit_start_identity as explicit_pins
        import cce_measured_payment_context_identity as payment_pins
        import cce_notification_dedup_identity as notification_pins
        import cce_writer_sale_context_identity as writer_pins
        if identity not in (pins, payment_pins, writer_pins, notification_pins, explicit_pins):
            raise ValueError("Reviewed immutable shared-image identity required")
        self.identity = identity
        super().__init__(status_refresh_reuse=True)
        self.status_arm = arm
        self.api_settings = {**self.api_settings, "ORDER_STATUS_COMMITTED_SNAPSHOT": "0"}
        if identity is explicit_pins:
            from cce_explicit_start_profile import image
            self.shared = image()
            self.api_settings.update(ORDER_STATUS_NOTIFICATION_DEDUP="0", DB_EXPLICIT_TRANSACTION_START="1")
        elif identity is notification_pins:
            from cce_notification_dedup_profile import image
            self.shared = image()
            self.api_settings["ORDER_STATUS_NOTIFICATION_DEDUP"] = "0"
        elif identity is writer_pins:
            from cce_writer_sale_context_profile import image
            self.shared = image()
        elif identity is payment_pins:
            from cce_measured_payment_context_profile import image
            self.shared = image()
        else:
            self.shared = profile.image()
        for role in self.background:
            self.images[role] = identity.IMAGE_ID

    def settings(self, role):
        # ECS bootstrap stays off. Activation occurs only on native fulfillment pods.
        values = {**super().settings(role), "ORDER_STATUS_COMMITTED_SNAPSHOT": "0"}
        if self.identity.DECISION in {"ADR0322", "ADR0323"}:
            values["ORDER_STATUS_NOTIFICATION_DEDUP"] = "0"
        if self.identity.DECISION == "ADR0323":
            values["DB_EXPLICIT_TRANSACTION_START"] = "1" if role == "api" else "0"
        return values

    def image_program(self, roles):
        return super().image_program(roles).replace(repr(legacy_pins.SOURCE), repr(self.identity.SOURCE))

    def inventory_marker(self):
        additional = ({"notification_dedup_decision": "ADR0322",
                       "notification_dedup_enabled": "1" if self.status_arm == "candidate" else "0"}
                      if self.identity.DECISION in {"ADR0322", "ADR0323"} else {})
        if self.identity.DECISION == "ADR0323":
            additional.update(explicit_start_decision="ADR0323", explicit_start_enabled="1")
        return {**super().inventory_marker(), **additional, "shared_image_decision": self.identity.DECISION,
                "shared_image_source_sha256": self.identity.SOURCE, "shared_image_configuration_digest": self.identity.CONFIG,
                "shared_image_receipt_sha256": self.identity.RECEIPT_SHA256, "committed_status_decision": "ADR0298",
                "committed_status_arm": self.status_arm,
                "committed_status_snapshot": "1" if self.status_arm == "candidate" else "0"}

    def verify_inventory(self, data):
        from observe_two_host_pipeline import verify_admission_factor_evidence
        from status_refresh_contract import StatusRefreshContract
        StatusRefreshContract.verify_inventory(self, data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Committed-status marker differs")
        legacy, _ = legacy_pins.historical_view(data, legacy_pins.digest(data), identity=self.identity)
        verify_admission_factor_evidence(legacy)
