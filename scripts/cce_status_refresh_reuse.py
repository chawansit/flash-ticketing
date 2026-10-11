"""ADR0280 one explicit, bounded committed-snapshot reuse factor."""
import hashlib
import json

BASELINE = "docs/capacity/cce/native-database-bridge-result-2026-10-11.json"


def contract():
    return {"decision": "ADR0280", "factor": "consumer_ORDER_STATUS_EVENT_REFRESH_DEDUP",
            "baseline": "0", "candidate": "1", "freshness_ms": 1000,
            "application_image_unchanged": True, "physical_database_connections": 24,
            "consumer_pool_budget": 48, "offered_journeys_per_second": 84, "offered_seconds": 300}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def selected(goal):
    value = goal.get("status_refresh_reuse_decision") if goal else None
    if value is None:
        if goal and "status_refresh_reuse_sha256" in goal:
            raise ValueError("Orphan status reuse identity")
        return False
    from cce_native_database_bridge import selected as native_selected
    if (value != "ADR0280" or goal.get("status_refresh_reuse_sha256") != digest(contract())
            or not native_selected(goal) or goal.get("offered_journeys_per_second") != 84
            or goal.get("offered_seconds") != 300):
        raise ValueError("Exact explicitly selected committed status reuse required")
    return True


def inventory_selected(marker):
    value = marker.get("status_refresh_reuse_decision")
    if value is None:
        if "status_refresh_reuse_sha256" in marker:
            raise ValueError("Orphan inventory status reuse identity")
        return False
    if value != "ADR0280" or marker.get("status_refresh_reuse_sha256") != digest(contract()):
        raise ValueError("Status reuse inventory identity differs")
    return True
