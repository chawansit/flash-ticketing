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


def for_guard(guard):
    prefix = guard.key.split("__")[0]
    for profile in (SHORT, HOURLY):
        if prefix == profile.ledger_prefix:
            return profile
    raise ValueError("Exact registered CCE qualification profile required")
