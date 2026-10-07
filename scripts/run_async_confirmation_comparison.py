"""ADR0161: separate exact-bound profile, default-local; cloud needs a new allowance."""

import importlib.util
import json
from uuid import uuid4

import async_confirmation_contract as default_policy
from qualify_two_host_deployment import ROOT

LEDGER = "bounded_async_confirmation"
AUTHORIZATION = "adr0161-async-confirmation-pair-2026-10-05"


def create_runner(*, policy_module=None, ledger=None, authorization=None, decision="ADR0161",
                  profile_name="async_confirmation", artifact_directory="async-payment-confirmation",
                  patch_name="adr0160.patch", arms=("control", "candidate"), runner_filename=None,
                  preparer_filename=None, extra_identity=()):
    if arms not in (("control", "candidate"), ("control",)):
        raise ValueError("Exact ordered pair or single control required")
    if arms == ("control",) and decision not in {"ADR0170", "ADR0171", "ADR0173"}:
        raise ValueError("Single-control scope is restricted to qualified diagnostic profiles")
    policy = policy_module or default_policy
    # Load a private namespace; never mutate the imported historical runner or copy its engine.
    spec = importlib.util.spec_from_file_location(
        "adr0161_private_engine", ROOT / "scripts/run_status_refresh_comparison.py"
    )
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    engine.PLAN, engine.LEDGER, engine.AUTHORIZATION = policy.PLAN, ledger or LEDGER, authorization or AUTHORIZATION
    engine.ARMS = arms
    engine.ROLES = policy.AsyncConfirmationContract.roles
    engine.StatusRefreshContract = policy.AsyncConfirmationContract
    engine.source_contract = policy.source_contract
    original_identity = engine.identity
    original_matches = engine.qualification_matches
    original_protocol, original_prepare = engine.protocol, engine.prepare
    original_gates = engine.stage_gates
    original_measurements = engine.measurements

    def identity():
        return {**original_identity(), **{
            "scripts/" + name: engine.comparison.source_sha256((ROOT / "scripts" / name).read_bytes())
            for name in (runner_filename or "run_" + profile_name + "_comparison.py", profile_name + "_contract.py",
                         preparer_filename or "prepare_" + ("partial_timeout_reclamation" if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"} else "async_confirmation") + ".py", "prepare_status_refresh_artifacts.py",
                         "status_refresh_image_install.py", "stage_status_refresh_images.py", "run_async_confirmation_comparison.py",
                         "checkout_journey_probe.py", "fetch_status_refresh_parents.py",
                         *( ("admission_failure_evidence.py",) if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"} else () ),
                         *( ("slow_database_evidence.py",) if decision in {"ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"} else () ),
                         *( ("database_wait_evidence.py",) if decision in {"ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0216"} else () ),
                         *( ("application_database_wait_evidence.py", "application_role_runner_diagnostics.py", "api_placement_contract.py") if decision == "ADR0177" else () ),
                         *( ("diagnostic_connection.py", "diagnostic_runner_connection.py", "api_placement_contract.py") if decision in {"ADR0181", "ADR0216"} else () ),
                         *( ("partial_timeout_profile.py",) if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"} else () ))},
            **{"scripts/" + name: engine.comparison.source_sha256((ROOT / "scripts" / name).read_bytes()) for name in extra_identity},
            **{"artifacts/" + artifact_directory + "/" + name: engine.comparison.source_sha256(
                (ROOT / "artifacts" / artifact_directory / name).read_bytes())
               for name in ("manifest.json", patch_name)}}

    def qualification_matches(report, binding, *, now=None):
        return (isinstance(report, dict) and report.get("experiment_decision") == decision
                and report.get("factor") == policy.FACTOR
                and original_matches(report, binding, now=now))

    def protocol(config, artifact, sources, bundle, binding, *, execute, qualification=None):
        # Exercise both real constructor boundaries before a lock, reservation or cloud call.
        for arm in engine.ARMS:
            contract = engine.StatusRefreshContract(artifact, arm, sources)
            engine.RefreshStages(execute, bundle, contract, "adr0151-" + uuid4().hex[:12])
        # Preserve frozen controls except this explicitly pinned polling helper in both arms.
        bundle = {**bundle, "scripts/checkout_journey_probe.py":
                  (ROOT / "scripts/checkout_journey_probe.py").read_bytes().replace(b"\r\n", b"\n")}
        pinned = (ROOT / json.loads(policy.PLAN.read_text())["isolated_source_directory"] / "scripts/checkout_journey_probe.py").read_bytes()
        if bundle["scripts/checkout_journey_probe.py"] != pinned:
            raise ValueError("Exact qualified polling helper required before cloud access")
        report = original_protocol(config, artifact, sources, bundle, binding,
                                   execute=execute, qualification=qualification)
        report.update(experiment_decision=decision, factor=policy.FACTOR,
                      scope=("Fixed2+2API, identical images/cache/refresh/dedup-off/500ms polling and aggregate budgets; "
                             + ("only partial timeout reclamation0->1;synchronous intake both arms." if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"}
                                else "only durable callback intake0->1.") + "60journeys/s300s each; no hourly capacity claim."))
        if decision in {"ADR0170", "ADR0171", "ADR0173"}:
            report.update(scope="Unchanged frozen ADR0163 synchronous control, reclamation off, fixed 2+2 APIs and budgets; one safety protocol then one 60 journeys/s 300s stage. Failure capture and all financial/drain/restoration gates retained. No capacity improvement or hourly claim.",
                          kind=profile_name + "_paid_control" if execute else "status_refresh_dry_control")
        if decision in {"ADR0174", "ADR0177", "ADR0181", "ADR0216"}:
            report.update(scope="Same frozen control images/settings/budgets and diagnostics; API placement 2+2 versus 1+3 only; 60 journeys/s for 300 seconds per arm. No hourly qualification or higher load.")
        if decision == "ADR0216":
            report.update(scope="Only simulator callback destination changes from local APIs to the shared load balancer. Fixed 2+2 APIs, frozen images, settings and budgets; 60 journeys/s for 300 seconds per arm. No hourly qualification or higher load.")
        if decision == "ADR0193":
            report.update(scope="Arm-specific pinned images and sources; only atomic callback claim changes. Fixed 2+2 APIs, concurrency, polling and aggregate budgets; 60 journeys/s for 300 seconds per arm. Full financial/drain/restoration gates; no hourly claim.")
        if decision in {"ADR0181", "ADR0216"}:
            report.update(diagnostic_connection_decision="ADR0180", scope=report["scope"] + " Separate read-only verified-TLS full-visibility observer in both arms; unchanged application transport and budgets.")
        if decision == "ADR0177":
            report.update(diagnostic_scope="application_role", diagnostic_decision="ADR0176",
                          scope=report["scope"] + " Application-role diagnostics in both arms; foreign invisibility explicit, no full-database claim.")
        path = engine.ROOT / "tmp" / report["run"] / "comparison-summary.json"
        if path.exists():
            path.write_text(json.dumps(report, indent=2) + "\n")
        return report

    def prepare(output):
        result = original_prepare(output)
        result.update(kind=profile_name + "_runner_preparation", experiment_decision=decision,
                      factor=policy.FACTOR, ledger=engine.LEDGER, authorization_id=engine.AUTHORIZATION,
                      status="LOCAL_IMAGES_VERIFIED_FRESH_APPROVAL_AND_DRY_PENDING")
        output.resolve().write_text(json.dumps(result, indent=2) + "\n")
        return result

    def measurements(record, trace_path, inventory):
        result = original_measurements(record, trace_path, inventory)
        try:
            import math
            from datetime import datetime
            rows = [json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()]
            if len(rows) < 2:
                raise ValueError("Confirmation coverage missing")
            previous, maximum = None, 0
            for row in rows:
                replicas = row.get("confirmation_db_replicas", {})
                if row.get("confirmation_replicas") != 1 or len(replicas) != 1 or row.get("confirmation_metrics_error"):
                    raise ValueError("Confirmation worker coverage missing")
                metric = next(iter(replicas.values()))
                stamp = datetime.fromisoformat(row["utc"])
                keys = ("pending", "review", "oldest_seconds")
                values = [metric.get("confirmation_metric:ticketing_payment_confirmation_" + k) for k in keys]
                start = metric.get("process_start_time_seconds")
                if stamp.tzinfo is None or any(type(v) not in {int, float} or not math.isfinite(v) or v < 0 for v in [*values, start]):
                    raise ValueError("Confirmation identity/queue metric invalid")
                if previous and (not 0 < (stamp - previous[0]).total_seconds() <= 2 or start != previous[1]):
                    raise ValueError("Confirmation restart or sampling gap")
                maximum = max(maximum, values[0])
                previous = (stamp, start)
            result.update(confirmation_observed=True, confirmation_backlog_peak=maximum)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result.update(confirmation_observed=False, confirmation_failure_type=type(exc).__name__)
        if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"}:
            try:
                from observe_two_host_pipeline import admission_failure_metrics
                keys = set(admission_failure_metrics("# TYPE ticketing_db_acquisition_failures_total counter"))
                previous = {}
                first = None
                for row in rows:
                    replicas = row.get("api_replicas", {})
                    if len(replicas) != 4 or (previous and set(replicas) != set(previous)):
                        raise ValueError("Four admission diagnostic replicas required")
                    current = {label: {k: metric.get(k) for k in keys} for label, metric in replicas.items()}
                    for label, metrics in current.items():
                        if any(type(v) not in {int, float} or not math.isfinite(v) or v < 0 for v in metrics.values()):
                            raise ValueError("Admission counters missing or invalid")
                        if previous and any(v < previous[label][k] for k, v in metrics.items()):
                            raise ValueError("Admission counter reset")
                    if first is None:
                        first = current
                    previous = current
                if first is None:
                    raise ValueError("Admission coverage missing")
                result.update(admission_diagnostics_observed=True,
                              admission_failure_deltas={k: sum(previous[label][k] - first[label][k] for label in previous)
                                                        for k in keys},
                              admission_counter_window="observer lifetime;not exact offered window")
            except (ValueError, KeyError, TypeError, UnboundLocalError) as exc:
                result.update(admission_diagnostics_observed=False, admission_failure_type=type(exc).__name__)
        if decision in {"ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"}:
            result["slow_database_capture"] = record.get("slow_database_capture")
        if decision in {"ADR0173", "ADR0174", "ADR0181", "ADR0216"}:
            result["database_wait_capture"] = record.get("database_wait_capture")
        if decision == "ADR0177":
            result["application_database_wait_capture"] = record.get("application_database_wait_capture")
        return result

    def stage_gates(record, inventory, restored, contract):
        result = original_gates(record, inventory, restored, contract)
        receipt = record.get("confirmation_receipts", {})
        queues = record.get("global_queues", {})
        required = {"durable_confirmation_receipts_complete": receipt.get("pass") is True,
                    "durable_confirmation_global_drain": policy.receipt_drained(queues)}
        if decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"}:
            required["admission_failure_evidence_complete"] = record.get("admission_failure_capture", {}).get("complete") is True
        if decision in {"ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"}:
            required["slow_database_evidence_complete"] = record.get("slow_database_capture", {}).get("complete") is True
        if decision in {"ADR0173", "ADR0174", "ADR0181", "ADR0216"}:
            required["database_wait_evidence_complete"] = record.get("database_wait_capture", {}).get("complete") is True
        if decision in {"ADR0181", "ADR0216"}:
            proof = record.get("diagnostic_connection_preflight", {})
            required["diagnostic_connection_verified"] = all(proof.get(k) is True for k in ("pass", "diagnostic_helpers_verified", "read_only_connection_verified", "verified_tls", "one_data_connection"))
            required["diagnostic_credentials_removed"] = record.get("diagnostic_credentials_removed") is True
        if decision == "ADR0177":
            from application_role_runner_diagnostics import complete
            required["application_database_wait_evidence_complete"] = complete(record, inventory)
        result["confirmation"] = required
        result["failed_gates"] += [k for k, passed in required.items() if not passed]
        result["all_required_gates_pass"] = result["all_required_gates_pass"] and all(required.values())
        return result

    original_arm = engine.run_arm

    def run_arm(*args, **kwargs):
        result = original_arm(*args, **kwargs)
        measured = result.get("measurements")
        if kwargs.get("execute") and (not measured or measured.get("confirmation_observed") is not True):
            result["pass"] = False
            result["gates"]["all_required_gates_pass"] = False
            result["gates"]["failed_gates"].append("confirmation_worker_metrics_coverage")
        if kwargs.get("execute") and decision in {"ADR0164", "ADR0165", "ADR0170", "ADR0171", "ADR0173", "ADR0174", "ADR0177", "ADR0181", "ADR0193", "ADR0216"} and (not measured or measured.get("admission_diagnostics_observed") is not True):
            result["pass"] = False
            result["gates"]["all_required_gates_pass"] = False
            result["gates"]["failed_gates"].append("admission_diagnostics_coverage")
        return result

    engine.stage_gates, engine.measurements, engine.run_arm = stage_gates, measurements, run_arm
    engine.identity, engine.qualification_matches = identity, qualification_matches
    engine.protocol, engine.prepare = protocol, prepare
    return engine


def create_stager(**options):
    policy = options.get("policy_module") or default_policy
    decision = options.get("decision", "ADR0161")
    engine = create_runner(**options)
    spec = importlib.util.spec_from_file_location(
        "adr0161_private_stager", ROOT / "scripts/stage_status_refresh_images.py"
    )
    stager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stager)
    stager.STATE = engine.STATE
    stager.ROLES = policy.AsyncConfirmationContract.roles
    stager.source_contract = policy.source_contract
    stager.StatusRefreshContract = policy.AsyncConfirmationContract
    stager.binding_for, stager.validate_release = engine.binding_for, engine.validate_release
    stager.owned_stage, stager.run_lock = stager.stage, engine.RunLock

    def stage(config, artifact, output, password):
        stager.validate_config(config)
        sources = stager.source_contract()
        stager.StatusRefreshContract(artifact, "control", sources)
        stager.validate_release(json.loads(stager.STATE.read_text()),
                                stager.binding_for(config, artifact, sources), execute=False)
        lock = stager.run_lock("adr0151-" + uuid4().hex[:12])
        # Retain the lock on any ambiguous staging failure; do not replay or guess remote cleanup.
        result = stager.owned_stage(config, artifact, output, password)
        if result.get("pass") is not True or result.get("runtime_identities_unchanged") is not True:
            raise ValueError("Staging success and unchanged runtime proof required")
        result.update(experiment_decision=decision, factor=policy.FACTOR)
        (output / "staging-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        lock.release()
        return result

    stager.stage = stage
    return stager


if __name__ == "__main__":
    create_runner().main()
