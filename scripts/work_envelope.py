"""ADR0172 standing boundaries, append-only reservations and runtime stop checks."""
import hashlib
import json
import math
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
ENVELOPE = ROOT / "docs/capacity/work-envelope.json"
JOURNAL = ROOT / "docs/capacity/work-envelope-ledger.json"
STATE = ROOT / "docs/capacity/CURRENT_STATE.json"
LOCK = ROOT / "tmp/adr0151-run.lock"
PAUSE = ROOT / "tmp/work-envelope.pause"
PROFILE = "slow_database_control"
BASE_LEDGER = "bounded_slow_database_diagnostics"
PROFILES = {PROFILE: (BASE_LEDGER, "ADR0171"),
            "database_wait_control": ("bounded_database_wait_diagnostics", "ADR0173"),
            "api_placement_rebalance": ("bounded_api_placement_rebalance", "ADR0174"),
            "application_role_rebalance": ("bounded_application_role_rebalance", "ADR0177"),
            "diagnostic_placement": ("bounded_diagnostic_placement", "ADR0181"),
            "atomic_payment_claim": ("bounded_atomic_payment_claim", "ADR0193")}
SCOPE = re.compile("(?:" + "|".join(v[0] for v in PROFILES.values()) + r")__(?:[0-9a-f]{12})$")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write(path, value):
    if path.is_symlink():
        raise ValueError("Symlink state is forbidden")
    temporary = path.with_name(path.name + "." + uuid4().hex + ".pending")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def envelope():
    data = read(ENVELOPE)
    approved = data.get("permissions", {})
    limits = data.get("time", {})
    cap = limits.get("cumulative_experiment_seconds_limit")
    if (data.get("schema_version") != 1 or data.get("decision") != "ADR0172"
            or not re.fullmatch(r"standing-work-envelope-\d{4}-\d{2}-\d{2}", data.get("envelope_id", ""))
            or type(data.get("spending", {}).get("maximum_new_infrastructure_spend")) is not int
            or data["spending"]["maximum_new_infrastructure_spend"] != 0
            or not re.fullmatch(r"[0-9a-f]{64}", data.get("existing_resource_configuration_sha256", ""))
            or data.get("spending", {}).get("allow_new_resources_or_resize") is not False
            or approved.get("bounded_test_deployment") is not True
            or approved.get("branch_prefix") != "codex/" or approved.get("merge_main") is not False
            or approved.get("unattended_schedule") is not False
            or limits.get("experiment_seconds_limit") != 3600
            or limits.get("cleanup_may_exceed_deadline") is not True
            or (cap is None and limits.get("unlimited_cumulative_time_explicitly_authorized") is not True)
            or (cap is not None and (type(cap) is not int or cap <= 0))
            or data.get("customer_gates") != {
                "mode": "exact_current_profile", "zero_double_booking": True, "zero_payment_loss": True,
                "complete_queue_drain": True, "no_slo_relaxation": True}
            or data.get("qualified_profiles") not in [list(PROFILES)[:count] for count in range(1, len(PROFILES) + 1)]):
        raise ValueError("Standing boundaries changed; locally qualify and record the decision first")
    return data


def journal(data):
    result = read(JOURNAL)
    if result.get("schema_version") != 1 or result.get("envelope_id") != data["envelope_id"]:
        raise ValueError("Envelope journal identity differs")
    if type(result.get("human_pause")) is not bool or not isinstance(result.get("experiments"), list):
        raise ValueError("Explicit pause and experiment accounting required")
    seen = set()
    for item in result["experiments"]:
        if (not isinstance(item, dict) or not SCOPE.fullmatch(item.get("ledger", ""))
                or item["ledger"] in seen or item.get("reserved_seconds") != 3600
                or item.get("status") not in {"ACTIVE", "PASSED_RESTORED", "FAILED_RESTORED", "RECOVERY_REQUIRED"}
                or type(item.get("actual_elapsed_seconds")) not in {int, float}
                or not math.isfinite(item["actual_elapsed_seconds"]) or item["actual_elapsed_seconds"] < 0):
            raise ValueError("Journal accounting invalid")
        seen.add(item["ledger"])
    return result


def check_available(data, records, state):
    from historical_recovery_exception import accepted
    from pre_dispatch_abort_recovery import resolved

    if (PAUSE.exists() or records["human_pause"] or state.get("human_pause") is True or state.get("current_run")
            or any(r["status"] == "ACTIVE" or (r["status"] == "RECOVERY_REQUIRED" and not resolved(records, r, ROOT) and not accepted(records, r, ROOT))
                   for r in records["experiments"])):
        raise ValueError("Human pause, active scope or unresolved recovery blocks new experiments")
    # A legacy flag may mean an actual pause. Only the explicit migration marker permits exhaustion.
    if state.get("cloud_load_requires_resume") and state.get("cloud_load_stop_kind") != "scope_exhausted":
        raise ValueError("Unclassified legacy pause requires explicit resolution")
    if any(isinstance(v, dict) and (v.get("active_run") or v.get("recovery_required"))
           for k, v in state.items() if k.startswith("bounded_")):
        raise ValueError("Legacy experiment recovery remains unresolved")
    cap = data["time"]["cumulative_experiment_seconds_limit"]
    charged = sum(max(r["reserved_seconds"], math.ceil(r["actual_elapsed_seconds"]))
                  for r in records["experiments"])
    if cap is not None and charged + 3600 > cap:
        raise ValueError("Cumulative experiment budget exhausted")
    return charged


def reserve(binding, plan, *, profile=PROFILE):
    """Caller holds the existing exclusive run lock; reserve before any cloud access."""
    if (not LOCK.exists() or LOCK.is_symlink() or read(LOCK).get("pid") != os.getpid()
            or not re.fullmatch(r"adr0151-[0-9a-f]{12}", read(LOCK).get("run", ""))):
        raise ValueError("Exclusive owned experiment lock required")
    data = envelope()
    records = journal(data)
    state = read(STATE)
    check_available(data, records, state)
    if profile not in PROFILES or profile not in data["qualified_profiles"]:
        raise ValueError("Unknown diagnostic profile")
    base, decision = PROFILES[profile]
    expected_arms = ["control", "candidate"] if profile in {"api_placement_rebalance", "application_role_rebalance", "diagnostic_placement", "atomic_payment_claim"} else ["control"]
    if (plan.get("decision") != decision or plan.get("arms") != expected_arms
            or plan.get("common", {}).get("buyer_journeys_per_second") != 60
            or plan["common"].get("duration_seconds") != 300):
        raise ValueError("Only locally qualified unchanged diagnostic control permitted")
    if profile in {"api_placement_rebalance", "application_role_rebalance", "diagnostic_placement"} and (
            digest(plan.get("placements")) != digest({"control": {"primary": 2, "secondary": 2},
                                                      "candidate": {"primary": 1, "secondary": 3}})
            or digest(plan.get("allowance")) != digest({"qualification_runs_authorized": 1,
                                                        "paid_runs_authorized": 2, "safety_tickets_authorized": 4})):
        raise ValueError("Exact registered placement pair budget required")
    if profile == "application_role_rebalance" and (
            plan.get("diagnostic_scope") != "application_role" or plan.get("diagnostic_decision") != "ADR0176"):
        raise ValueError("Exact registered application diagnostic scope required")
    if profile == "diagnostic_placement" and (
            plan.get("diagnostic_connection_decision") != "ADR0180"
            or not re.fullmatch(r"[0-9a-f]{64}", binding.get("diagnostic_target_sha256", ""))):
        raise ValueError("Exact diagnostic target binding required")
    if profile == "atomic_payment_claim":
        import atomic_payment_claim_contract as claim

        if plan != claim.plan():
            raise ValueError("Exact registered atomic claim pair required")
    if binding.get("configuration_sha256") != data["existing_resource_configuration_sha256"]:
        raise ValueError("Existing resource configuration changed; infrastructure exception")
    identity = uuid4().hex[:12]
    key = base + "__" + identity
    stamp = datetime.now(UTC)
    entry = {"ledger": key, "authorization_id": "work-envelope-" + identity,
             "envelope_sha256": digest(data), "binding_sha256": digest(binding),
             "plan_sha256": digest(plan), "profile": profile, "reserved_seconds": 3600,
             "actual_elapsed_seconds": 0, "started_at_utc": stamp.isoformat(),
             "status": "ACTIVE", "reports": []}
    records["experiments"].append(entry)
    write(JOURNAL, records)  # Crash between writes remains ACTIVE and fails closed.
    state[key] = {
        "authorization_id": entry["authorization_id"], "binding": binding,
        "standing_envelope": {"ledger": key, "envelope_sha256": entry["envelope_sha256"]},
        "qualification_runs_authorized": 1, "paid_runs_authorized": len(expected_arms), "safety_tickets_authorized": 2 * len(expected_arms),
        "qualification_protocols_started": 0, "paid_runs_started": 0,
        "paid_protocols_started": 0, "safety_protocols_started": 0, "active_run": None,
        "scope": "Fresh " + decision + " experiment under ADR0172 standing boundaries; no replay or higher load"}
    write(STATE, state)
    return entry


def scope_authorized(state, key, binding):
    data = envelope()
    records = journal(data)
    item = next((r for r in records["experiments"] if r["ledger"] == key), None)
    if (PAUSE.exists() or not SCOPE.fullmatch(key) or records["human_pause"] or state.get("human_pause") is True
            or (state.get("cloud_load_requires_resume") and state.get("cloud_load_stop_kind") != "scope_exhausted")
            or item is None or item["status"] != "ACTIVE" or item["envelope_sha256"] != digest(data)
            or item["binding_sha256"] != digest(binding)
            or state.get(key, {}).get("standing_envelope") != {
                "ledger": key, "envelope_sha256": digest(data)}):
        raise ValueError("Fresh active standing reservation with exact binding required")
    owned_run = state.get(key, {}).get("active_run")
    if (state.get("current_run") or owned_run) and state.get("current_run") != owned_run:
        raise ValueError("Active experiment ownership differs")
    started = datetime.fromisoformat(item["started_at_utc"])
    if started.tzinfo is None:
        raise ValueError("Aware experiment start time required")
    elapsed = (datetime.now(UTC) - started).total_seconds()
    if not 0 <= elapsed < item["reserved_seconds"]:
        raise ValueError("Standing experiment deadline expired")
    return item


def base_ledger(key):
    """Only the qualified control can get fresh identities; other names stay exact."""
    return key.rsplit("__", 1)[0] if SCOPE.fullmatch(key) else key


class ActionGuard:
    def __init__(self, key, binding):
        self.key, self.binding = key, binding
        self.deadline = time.monotonic() + 3600

    def check(self, timeout=0):
        scope_authorized(read(STATE), self.key, self.binding)
        remaining = self.deadline - time.monotonic()
        if remaining <= max(0, timeout):
            raise TimeoutError("Not enough experiment time for another remote action")

    def finish(self, reports, elapsed):
        data = envelope()
        records = journal(data)
        entry = next(r for r in records["experiments"] if r["ledger"] == self.key)
        if entry["status"] != "ACTIVE":
            raise ValueError("Finished reservations cannot be rewritten")
        if type(elapsed) not in {int, float} or not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("Valid actual elapsed time required")
        state = read(STATE)
        restored = bool(reports) and all(
            isinstance(r.get("arms"), dict) and bool(r["arms"])
            and all(a.get("restoration_complete") is True for a in r["arms"].values()) for r in reports)
        integrity = bool(reports) and all(
            r.get("pass") is True if r.get("capacity_stages_started") == 0 else
            bool(r.get("arms")) and all(
                all(a.get("gates", {}).get("paid", {}).get(g) is True for g in (
                    "post_ttl_financial", "zero_double_booking", "full_keyspace_queue_drain", "kafka_drain"))
                for a in r["arms"].values())
            for r in reports)
        ambiguous = state.get("current_run") or state[self.key].get("active_run")
        passed = len(reports) == 2 and all(r.get("pass") is True for r in reports)
        if passed and (elapsed >= 3600 or records["human_pause"] or PAUSE.exists()):
            passed = False
        entry.update(actual_elapsed_seconds=elapsed, finished_at_utc=datetime.now(UTC).isoformat(),
                     status=("PASSED_RESTORED" if passed else "FAILED_RESTORED")
                     if restored and integrity and not ambiguous else "RECOVERY_REQUIRED",
                     reports=[{"run": r["run"], "pass": r.get("pass") is True} for r in reports])
        write(JOURNAL, records)
        return entry


def publication_allowed(branch, *, reviewed, sanitized):
    permissions = envelope()["permissions"]
    return (reviewed is True and sanitized is True
            and permissions.get("push_reviewed_code_and_sanitized_evidence") is True
            and isinstance(branch, str) and branch.startswith(permissions["branch_prefix"])
            and re.fullmatch(r"codex/[a-z0-9]+(?:[-/][a-z0-9]+)*", branch) is not None)
