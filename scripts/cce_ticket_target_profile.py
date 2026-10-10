"""ADR0277 one higher-load ticket target; historical workloads remain sealed."""
import hashlib

import cce_worker_rebalance_profile as placement
import work_envelope as policy

BASELINE = "docs/capacity/cce/worker-placement-paid-result-2026-10-10.json"
RATE, SECONDS, EXPECTED, SHOWS, CONCURRENCY, TARGET = 168, 300, 50400, 168, 1000, 50000


def active(goal):
    expected = {"extension_decision": "ADR0277", "decision": "ADR0277",
                "profile": "cce_ticket_target", "comparison_arm": "correction",
                "offered_journeys_per_second": RATE, "offered_seconds": SECONDS,
                "acquisition_budget": 20, "generator_concurrency": CONCURRENCY,
                "target_unique_tickets": TARGET, "candidate_factor": placement.factor()}
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact registered fifty-thousand-ticket profile required")
    placement.active({**goal, "extension_decision": "ADR0271", "decision": "ADR0228",
                      "profile": "cce_paid_comparison"})
    return goal


def plan(goal):
    active(goal)
    result = placement.plan({**goal, "extension_decision": "ADR0271", "decision": "ADR0228",
                             "profile": "cce_paid_comparison"})
    return {**result, "decision": "ADR0277", "extension_decision": "ADR0277",
            "common": {"buyer_journeys_per_second": RATE, "duration_seconds": SECONDS},
            "kind": "five_minute_ticket_capacity_target", "single_changed_factor": "offered_workload",
            "generator_concurrency": CONCURRENCY, "expected_journeys": EXPECTED,
            "target_unique_tickets": TARGET, "fixture_shows": SHOWS,
            "baseline_evidence": BASELINE, "baseline_sha256": policy.digest(policy.read(policy.ROOT / BASELINE)),
            "baseline_is_passing_control": False, "matched_performance_comparison": False,
            "duration_reason": "One user-requested five-minute 50,000-ticket test; audit and restoration remain mandatory."}


def qualify_workload(parent):
    from customer_recovery_bundle import qualify
    bundle, coordinator = qualify(parent)
    name = "scripts/paid_ticket_sharded_generator.py"
    old = b"if not 2 <= args.rate <= 100 or not 1 <= args.seconds <= 300:"
    if bundle[name].count(old) != 1:
        raise ValueError("Sealed sharded rate ceiling differs")
    source = bundle[name].replace(old, b"if not 2 <= args.rate <= 168 or not 1 <= args.seconds <= 300:")
    previous = hashlib.sha256(bundle[name]).hexdigest().encode()
    current = hashlib.sha256(source).hexdigest().encode()
    if coordinator.count(previous) != 1:
        raise ValueError("Sealed coordinator hash differs")
    return {**bundle, name: source}, coordinator.replace(previous, current)


def issuance_program(events, start):
    import math
    from uuid import UUID
    if (len(events) != SHOWS or len(set(events)) != SHOWS
            or any(str(UUID(v)) != v for v in events)
            or type(start) not in (int, float) or not math.isfinite(start) or start <= 0):
        raise ValueError("Exact owned ticket-target events and start required")
    from cce_hourly_window import QUERY
    query = QUERY.replace("3598", "298").replace("3600", "300").replace("300000", "50000").replace("302400", "50400")
    return "events=" + repr(events) + "\nstart=" + repr(start) + "\n" + query
