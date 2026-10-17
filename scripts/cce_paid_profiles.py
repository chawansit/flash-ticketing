"""ADR0232 exact immutable short and hourly qualification bounds."""

from dataclasses import dataclass


@dataclass(frozen=True)
class StageProfile:
    name: str
    ledger_prefix: str
    duration: int
    shows: int
    expected: int
    completion_deadline: int
    experiment_limit: int
    sale_hours: int
    token_lifetime: int

    @property
    def observer_seconds(self):
        return self.duration + 180

    @property
    def job_timeout(self):
        return self.duration + 180


SHORT = StageProfile("cce_paid_comparison", "bounded_cce_paid_comparison", 300, 84, 25200, 420, 3600, 1, 3600)
HOURLY = StageProfile(
    "cce_hourly_qualification", "bounded_cce_hourly_qualification", 3600, 1008, 302400, 3720, 5400, 2, 7200
)


TICKET_TARGET = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 168, 50400, 420, 3600, 1, 3600)


HEADROOM = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 102, 30600, 420, 3600, 1, 3600)


RATE_PUSH = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 120, 36000, 420, 3600, 1, 3600)


HIGHER_RATE_PUSH = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 140, 42000, 420, 3600, 1, 3600)


RATE_PUSH_160 = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 160, 48000, 420, 3600, 1, 3600)


RATE_PUSH_180 = StageProfile("cce_ticket_target", "bounded_cce_ticket_target", 300, 180, 54000, 420, 3600, 1, 3600)


def for_guard(guard):
    prefix = guard.key.split("__")[0]
    if prefix == HEADROOM.ledger_prefix and getattr(guard, "binding", {}).get("cce_headroom_contract"):
        from cce_recovery_headroom import contract
        goal = __import__("work_envelope").envelope()["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
        if guard.binding["cce_headroom_contract"] != contract(goal):
            raise ValueError("Exact declared headroom binding required")
        from cce_throughput_push import contract as push_contract
        from cce_throughput_push import selected as push_selected
        if push_selected(goal):
            if guard.binding.get("cce_throughput_push_contract") != push_contract(goal):
                raise ValueError("Exact declared throughput stage binding required")
            return {120: RATE_PUSH, 140: HIGHER_RATE_PUSH, 160: RATE_PUSH_160, 180: RATE_PUSH_180}[push_contract(goal)["offered_rate"]]
        return HEADROOM
    for profile in (SHORT, HOURLY, TICKET_TARGET):
        if prefix == profile.ledger_prefix:
            return profile
    raise ValueError("Exact registered CCE qualification profile required")
