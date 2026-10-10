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
            "atomic_payment_claim": ("bounded_atomic_payment_claim", "ADR0193"),
            "worker_separation": ("bounded_worker_separation", "ADR0201"),
            "callback_routing": ("bounded_callback_routing", "ADR0216"),
            "shared_callback_placement": ("bounded_shared_callback_placement", "ADR0217"),
            "shared_callback_rate_probe": ("bounded_shared_callback_rate_probe", "ADR0219"),
            "interleaved_refresh_probe": ("bounded_interleaved_refresh_probe", "ADR0222"),
            "orders_event_index_probe": ("bounded_orders_event_index_probe", "ADR0224"),
            "writer_write_pipeline_probe": ("bounded_writer_write_pipeline_probe", "ADR0225"),
            "generator_completion_probe": ("bounded_generator_completion_probe", "ADR0226"),
            "cce_dependency_probe": ("bounded_cce_dependency_probe", "ADR0228"),
            "cce_paid_comparison": ("bounded_cce_paid_comparison", "ADR0228"),
            "cce_hourly_qualification": ("bounded_cce_hourly_qualification", "ADR0232"),
            "cce_ticket_target": ("bounded_cce_ticket_target", "ADR0277")}
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



def validate_hourly_allowance(data):
    allowance = data.get("time", {}).get("hourly_qualification_exception", {})
    if allowance.get("experiment_seconds_limit") != 5400 or allowance.get("authorization") != "Direct user reply: Allow 90 minutes for hourly qualification":
        raise ValueError("Explicit 90-minute hourly exception required")
    if data.get("spending", {}).get("temporary_cce_pilot_exception", {}).get("goal_bounded_authorization", {}).get("profile") != "cce_hourly_qualification":
        raise ValueError("A fresh hourly goal is required")
    from cce_dependency_probe import authorized_today
    authorized_today(data)

def journal(data):
    result = read(JOURNAL)
    if result.get("schema_version") != 1 or result.get("envelope_id") != data["envelope_id"]:
        raise ValueError("Envelope journal identity differs")
    if type(result.get("human_pause")) is not bool or not isinstance(result.get("experiments"), list):
        raise ValueError("Explicit pause and experiment accounting required")
    seen = set()
    for item in result["experiments"]:
        if (not isinstance(item, dict) or not SCOPE.fullmatch(item.get("ledger", ""))
                or item["ledger"] in seen or item.get("reserved_seconds") != (5400 if item.get("profile") == "cce_hourly_qualification" else 3600)
                or item.get("status") not in {"ACTIVE", "PASSED_RESTORED", "FAILED_RESTORED", "RECOVERY_REQUIRED"}
                or type(item.get("actual_elapsed_seconds")) not in {int, float}
                or not math.isfinite(item["actual_elapsed_seconds"]) or item["actual_elapsed_seconds"] < 0):
            raise ValueError("Journal accounting invalid")
        seen.add(item["ledger"])
    return result


def check_available(data, records, state):
    from dispatched_cohort_recovery import resolved as paid_resolved
    from historical_recovery_exception import accepted
    from pre_dispatch_abort_recovery import resolved
    from safety_diagnostic_abort_recovery import resolved as safety_resolved

    if (PAUSE.exists() or records["human_pause"] or state.get("human_pause") is True or state.get("current_run")
            or any(r["status"] == "ACTIVE" or (r["status"] == "RECOVERY_REQUIRED" and not resolved(records, r, ROOT) and not safety_resolved(records, r, ROOT) and not paid_resolved(records, r, ROOT) and not accepted(records, r, ROOT))
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
    expected_arms = ["control", "candidate"] if profile in {"api_placement_rebalance", "application_role_rebalance", "diagnostic_placement", "atomic_payment_claim", "worker_separation", "callback_routing", "shared_callback_placement"} else ["control"]
    if profile in {"shared_callback_rate_probe", "interleaved_refresh_probe", "orders_event_index_probe", 'writer_write_pipeline_probe', 'generator_completion_probe'}:
        expected_arms = ["candidate"]
    if profile in {'cce_paid_comparison', 'cce_hourly_qualification', 'cce_ticket_target'}:
        import run_cce_paid_comparison as cce
        if profile == "cce_hourly_qualification":
            import run_cce_hourly_qualification as cce
            validate_hourly_allowance(data)
        (cce.core if profile == "cce_hourly_qualification" else cce).native.authorized_creation(data)
        if (plan != cce.plan() or binding.get('cce_paid_entry_sources') != cce.identity()
                or binding.get('cce_paid_core_sources') != (cce.core if profile == 'cce_hourly_qualification' else cce).paid.identity()
                or not all(re.fullmatch(r'[0-9a-f]{64}', binding.get(k,'')) for k in (
                    'cce_manifest_sha256','diagnostic_target_sha256','saved_api_service_sha256',
                    'image_proof_sha256','cce_resource_source_sha256','cce_transition_source_sha256'))):
            raise ValueError('Exactly bound locally qualified native paid comparison required')
        expected_arms = ['candidate']
    if profile == "cce_dependency_probe":
        from cce_dependency_probe import authorized_today, identity
        from cce_dependency_probe import plan as cce_plan
        authorized_today(data)
        if plan != cce_plan(binding.get("dependency_transport", "pooler")) or binding.get("cce_sources") != identity():
            raise ValueError("Exact qualified CCE dependency probe required")
        expected_arms = []
    if (plan.get("decision") != decision or plan.get("arms") != expected_arms
            or plan.get("common", {}).get("buyer_journeys_per_second") != (0 if profile == "cce_dependency_probe" else 168 if profile == "cce_ticket_target" else 84 if profile in {"shared_callback_rate_probe", "interleaved_refresh_probe", "orders_event_index_probe", 'writer_write_pipeline_probe', 'generator_completion_probe', 'cce_paid_comparison', 'cce_hourly_qualification'} else 60)
            or plan["common"].get("duration_seconds") != (0 if profile == "cce_dependency_probe" else 3600 if profile == "cce_hourly_qualification" else 300)):
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
    if profile in {"diagnostic_placement", "callback_routing", "shared_callback_placement", "shared_callback_rate_probe", "interleaved_refresh_probe", "orders_event_index_probe", 'writer_write_pipeline_probe', 'generator_completion_probe'} and (
            plan.get("diagnostic_connection_decision") != "ADR0180"
            or not re.fullmatch(r"[0-9a-f]{64}", binding.get("diagnostic_target_sha256", ""))):
        raise ValueError("Exact diagnostic target binding required")
    if profile == "generator_completion_probe":
        import generator_completion_probe_contract as completion_probe

        if plan != completion_probe.plan():
            raise ValueError("Exact generator-only bookkeeping correction probe required")
    if profile == "writer_write_pipeline_probe":
        import writer_write_pipeline_probe_contract as writer_probe

        if plan != writer_probe.plan():
            raise ValueError("Exact writer-only transport correction probe required")
    if profile == "orders_event_index_probe":
        import orders_event_index_probe_contract as index_probe

        if plan != index_probe.plan():
            raise ValueError("Exact index-only correction probe required")
    if profile == "interleaved_refresh_probe":
        import interleaved_refresh_probe_contract as correction

        if plan != correction.plan():
            raise ValueError("Exact consumer-only correction probe required")
    if profile == "shared_callback_rate_probe":
        import shared_callback_rate_probe_contract as probe

        if plan != probe.plan():
            raise ValueError("Exact locally qualified 84/s shared-callback probe required")
    if profile == "shared_callback_placement":
        import shared_callback_placement_contract as placement

        if plan != placement.plan():
            raise ValueError("Exact locally qualified shared-callback placement pair required")
    if profile == "callback_routing":
        import callback_routing_contract as routing

        if plan != routing.plan():
            raise ValueError("Exact locally qualified callback routing pair required")
    if profile == "atomic_payment_claim":
        import atomic_payment_claim_contract as claim

        if plan != claim.plan():
            raise ValueError("Exact registered atomic claim pair required")
    if profile == "worker_separation":
        from worker_separation_profile import entry_identity
        from worker_separation_profile import plan as worker_plan
        if plan != worker_plan() or binding.get("worker_entrypoint_sources_sha256") != entry_identity():
            raise ValueError("Exact locally qualified worker entry-point plan required")
    if binding.get("configuration_sha256") != data["existing_resource_configuration_sha256"]:
        raise ValueError("Existing resource configuration changed; infrastructure exception")
    identity = uuid4().hex[:12]
    key = base + "__" + identity
    stamp = datetime.now(UTC)
    entry = {"ledger": key, "authorization_id": "work-envelope-" + identity,
             "envelope_sha256": digest(data), "binding_sha256": digest(binding),
             "plan_sha256": digest(plan), "profile": profile, "reserved_seconds": 5400 if profile == "cce_hourly_qualification" else 3600,
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
        "scope": "Fresh " + decision + " experiment under ADR0172 standing boundaries; exact registered rate only, no replay"}
    if profile == "worker_separation":
        state[key].update(qualification_runs_authorized=0, safety_tickets_authorized=0)
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
        self.deadline = time.monotonic() + (5400 if base_ledger(key) == "bounded_cce_hourly_qualification" else 3600)

    def check(self, timeout=0):
        entry = scope_authorized(read(STATE), self.key, self.binding)
        if entry["profile"] == "cce_dependency_probe":
            from cce_dependency_probe import authorized_today, identity
            authorized_today(envelope())
            if self.binding.get("cce_sources") != identity():
                raise ValueError("CCE probe sources changed")
        if entry['profile'] in {'cce_paid_comparison', 'cce_hourly_qualification', 'cce_ticket_target'}:
            import run_cce_paid_comparison as cce
            if entry["profile"] == "cce_hourly_qualification":
                import run_cce_hourly_qualification as cce
                validate_hourly_allowance(envelope())
            (cce.core if entry["profile"] == "cce_hourly_qualification" else cce).native.dependency.authorized_today(envelope())
            if (self.binding.get('cce_paid_entry_sources') != cce.identity()
                    or self.binding.get('cce_paid_core_sources') != (cce.core if entry['profile'] == 'cce_hourly_qualification' else cce).paid.identity()
                    or self.binding.get('baseline_sha256') != cce.plan()['baseline_sha256']):
                raise ValueError('Native paid source or baseline drift')
        if entry["profile"] == "worker_separation":
            from worker_separation_profile import entry_identity
            if self.binding.get("worker_entrypoint_sources_sha256") != entry_identity():
                raise ValueError("Worker entry-point sources changed")
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
        if entry['profile'] == 'cce_dependency_probe':
            from cce_dependency_probe import outcome
            restored, integrity, passed = outcome(reports[0], state[self.key], self.binding) if len(reports) == 1 else (False, False, False)
            if state.get('current_run') or state[self.key].get('active_run'):
                restored = False
            if elapsed >= entry['reserved_seconds'] or records['human_pause'] or PAUSE.exists():
                passed = False
            entry.update(actual_elapsed_seconds=elapsed, finished_at_utc=datetime.now(UTC).isoformat(),
                         status=('PASSED_RESTORED' if passed else 'FAILED_RESTORED')
                         if restored and integrity else 'RECOVERY_REQUIRED',
                         reports=[{'run': r.get('run'), 'pass': r.get('pass') is True} for r in reports],
                         result_sha256=digest(reports[0]) if len(reports) == 1 else None,
                         paid_runs_started=0)
            write(JOURNAL, records)
            return entry
        if entry['profile'] in {'cce_paid_comparison', 'cce_hourly_qualification', 'cce_ticket_target'}:
            from run_cce_paid_comparison import outcome
            if entry["profile"] == "cce_hourly_qualification":
                from run_cce_hourly_qualification import outcome
            restored, integrity, passed = outcome(reports[0], state[self.key], self.binding) if len(reports)==1 else (False, False, False)
            ambiguous = state.get('current_run') or state[self.key].get('active_run')
            if elapsed >= entry['reserved_seconds'] or records['human_pause'] or PAUSE.exists():
                passed = False
            entry.update(actual_elapsed_seconds=elapsed, finished_at_utc=datetime.now(UTC).isoformat(),
                         status=('PASSED_RESTORED' if passed else 'FAILED_RESTORED')
                         if restored and integrity and not ambiguous else 'RECOVERY_REQUIRED',
                         reports=[{'run':r.get('run'),'pass':r.get('pass') is True} for r in reports],
                         paid_runs_started=state[self.key].get('paid_runs_started'),
                         result_sha256=digest(reports[0]) if len(reports)==1 else None)
            write(JOURNAL, records)
            return entry
        if entry['profile'] == 'worker_separation':
            from worker_separation_profile import entry_identity, outcome
            scope = state[self.key]
            valid = (len(reports) == 1 and isinstance(reports[0],dict)
                     and self.binding.get('worker_entrypoint_sources_sha256') == entry_identity()
                     and entry['binding_sha256'] == digest(self.binding) and entry['envelope_sha256'] == digest(data)
                     and scope.get('binding') == self.binding
                     and scope.get('worker_result_sha256') == digest(reports[0]))
            restored, integrity, passed = outcome(reports[0], scope, self.binding) if valid else (False, False, False)
            ambiguous = state.get('current_run') or scope.get('active_run')
            if elapsed >= 3600 or records['human_pause'] or PAUSE.exists() or state.get('human_pause') is True:
                passed = False
            entry.update(actual_elapsed_seconds=elapsed, finished_at_utc=datetime.now(UTC).isoformat(),
                         status=('PASSED_RESTORED' if passed else 'FAILED_RESTORED')
                         if restored and integrity and not ambiguous else 'RECOVERY_REQUIRED',
                         reports=[{'run': r.get('run'), 'pass': r.get('pass') is True} for r in reports if isinstance(r,dict)],
                         paid_runs_started=scope.get('paid_runs_started'),
                         result_sha256=digest(reports[0]) if len(reports) == 1 else None)
            write(JOURNAL, records)
            return entry
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
