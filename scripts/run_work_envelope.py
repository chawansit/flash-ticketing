"""ADR0172: local status by default; one qualified fresh experiment when executed."""
import argparse
import getpass
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

import work_envelope as policy


def status():
    data = policy.envelope()
    records = policy.journal(data)
    return {"decision": "ADR0172", "implemented": True, "cloud_calls": 0,
            "production_qualified": False, "human_pause": records["human_pause"] or policy.PAUSE.exists(),
            "profile": policy.PROFILE, "qualified_profiles": data["qualified_profiles"],
            "reserved_seconds": sum(r["reserved_seconds"] for r in records["experiments"]),
            "actual_elapsed_seconds": sum(r["actual_elapsed_seconds"] for r in records["experiments"]),
            "cumulative_seconds_limit": data["time"]["cumulative_experiment_seconds_limit"],
            "maximum_new_infrastructure_spend": 0,
            "existing_service_charges_continue": True}


def execute(config_path, artifact_path, ssh_runtime, *, profile_name=policy.PROFILE, diagnostic_target=None, worker_inputs=None, worker_ca=None):
    if profile_name == 'worker_separation':
        import worker_separation_profile as worker
        prepared = worker.prepare_files(config_path, worker_inputs, artifact_path, worker_ca, diagnostic_target)
        if not sys.stdin.isatty():raise ValueError('Protected credential terminal required')
        import subprocess
        subprocess.run([sys.executable, str(policy.ROOT / 'scripts/check_repository_names.py')],
                       cwd=policy.ROOT, check=True, stdout=subprocess.DEVNULL)
        sys.path.insert(0, str(ssh_runtime.resolve()))
        return worker.execute(prepared, getpass.getpass('ECS password: '), getpass.getpass('Diagnostic administrator password: '))
    if worker_inputs is not None or worker_ca is not None:raise ValueError('Worker inputs are restricted to the worker profile')
    if profile_name == "atomic_payment_claim":
        import atomic_payment_claim_contract as contract_policy
        import run_atomic_payment_claim_comparison as profile
    elif profile_name == "diagnostic_placement":
        import diagnostic_placement_contract as contract_policy
        import run_diagnostic_placement_comparison as profile
    elif profile_name == "application_role_rebalance":
        import application_role_rebalance_contract as contract_policy
        import run_application_role_rebalance_comparison as profile
    elif profile_name == "api_placement_rebalance":
        import api_placement_contract as contract_policy
        import run_api_placement_comparison as profile
    elif profile_name == "database_wait_control":
        import database_wait_contract as contract_policy
        import run_database_wait_diagnostics as profile
    elif profile_name == policy.PROFILE:
        import run_slow_database_diagnostics as profile
        import slow_database_contract as contract_policy
    else:
        raise ValueError("Unknown qualified profile")
    from run_status_refresh_comparison import RunLock

    engine = profile.create_runner()
    if profile_name == "diagnostic_placement":
        from diagnostic_runner_connection import validate_target
        if diagnostic_target is None or not sys.stdin.isatty():
            raise ValueError("Protected target file and credential terminal required")
        target = validate_target(policy.read(diagnostic_target))
        engine.configure_diagnostic_target(target)
    elif diagnostic_target is not None:
        raise ValueError("Diagnostic target is restricted to its registered profile")
    sources = engine.source_contract()
    plan = contract_policy.plan()
    artifact = policy.read(artifact_path) if artifact_path is not None else plan["artifact_receipt"]
    config = policy.read(config_path)
    for arm in engine.ARMS:
        engine.StatusRefreshContract(artifact, arm, sources)
    engine.comparison.validate_config(config)
    binding = engine.binding_for(config, artifact, sources)
    # Constructors and canonical names qualify before reservation or cloud mutation.
    engine.RefreshStages(False, {}, engine.StatusRefreshContract(artifact, "control", sources),
                         "adr0151-" + uuid4().hex[:12])
    engine.comparison.subprocess.run(
        [sys.executable, str(policy.ROOT / "scripts/check_repository_names.py")],
        cwd=policy.ROOT, check=True, stdout=engine.comparison.subprocess.DEVNULL)
    policy.LOCK.parent.mkdir(exist_ok=True)
    allocation_lock = RunLock("adr0151-" + uuid4().hex[:12])
    try:
        entry = policy.reserve(binding, plan, profile=profile_name)
    finally:
        allocation_lock.release()
    engine.LEDGER, engine.AUTHORIZATION = entry["ledger"], entry["authorization_id"]
    guard = policy.ActionGuard(entry["ledger"], binding)
    engine.ENVELOPE_GUARD = guard
    sys.path.insert(0, str(ssh_runtime.resolve()))
    reports, started = [], time.monotonic()
    try:
        if profile_name == "diagnostic_placement":
            engine.configure_diagnostic(target, getpass.getpass("Diagnostic administrator password: "))
        if hasattr(engine, "stage_images"):
            engine.stage_images(config, artifact, getpass.getpass("Image staging SSH password: "), guard)
        bundle = engine.comparison.frozen_bundle()
        guard.check()
        qualification = engine.protocol(config, artifact, sources, bundle, binding, execute=False)
        reports.append(qualification)
        if qualification["pass"] is True:
            guard.check()
            reports.append(engine.protocol(config, artifact, sources, bundle, binding,
                                           execute=True, qualification=qualification))
    finally:
        if hasattr(engine, "clear_diagnostic"):
            engine.clear_diagnostic()
        receipt = guard.finish(reports, time.monotonic() - started)
        print(json.dumps({"phase": "standing-experiment-finished", **receipt}), flush=True)
    return receipt


def set_pause(paused):
    # The independent stop file can be updated during an active workflow without
    # rewriting its counters, ownership or CURRENT_STATE.
    policy.envelope()
    policy.PAUSE.parent.mkdir(exist_ok=True)
    if paused:
        policy.write(policy.PAUSE, {"human_pause": True, "decision": "ADR0172"})
    elif policy.PAUSE.exists():
        if policy.PAUSE.is_symlink() or policy.read(policy.PAUSE) != {
                "human_pause": True, "decision": "ADR0172"}:
            raise ValueError("Unknown pause marker; explicit recovery required")
        policy.PAUSE.unlink()
    # Journal/CURRENT_STATE flags record pre-existing explicit pauses; never silently
    # clear them during active work or ambiguous ownership.
    if not paused and (policy.journal(policy.envelope())["human_pause"]
                       or policy.read(policy.STATE).get("human_pause") is True):
        from run_status_refresh_comparison import RunLock

        lock = RunLock("adr0151-" + uuid4().hex[:12])
        try:
            records, state = policy.journal(policy.envelope()), policy.read(policy.STATE)
            records["human_pause"], state["human_pause"] = False, False
            policy.write(policy.JOURNAL, records)
            policy.write(policy.STATE, state)
        finally:
            lock.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--execute", action="store_true")
    modes.add_argument("--worker-preflight", action="store_true")
    modes.add_argument("--pause", action="store_true")
    modes.add_argument("--resume", action="store_true")
    modes.add_argument("--check-publication", metavar="BRANCH")
    parser.add_argument("--reviewed", action="store_true")
    parser.add_argument("--sanitized", action="store_true")
    parser.add_argument("--profile", choices=list(policy.PROFILES), default=policy.PROFILE)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--diagnostic-target", type=Path)
    parser.add_argument("--ssh-runtime", type=Path)
    parser.add_argument("--worker-inputs", type=Path)
    parser.add_argument("--worker-ca", type=Path)
    parser.add_argument("--worker-evidence", type=Path)
    parser.add_argument("--worker-scope")
    args = parser.parse_args()
    if args.worker_preflight:
        from worker_separation_preload import from_files
        report = from_files(args.worker_inputs, args.worker_evidence, scope_key=args.worker_scope)
        print(json.dumps(report, indent=2))
        if not report["ready_for_load"]:
            raise SystemExit(1)
        return
    worker_execution = args.execute and args.profile == 'worker_separation'
    if args.worker_ca is not None and not worker_execution:parser.error('Worker CA is restricted to worker execution')
    if (args.worker_evidence is not None or args.worker_scope is not None
            or (args.worker_inputs is not None and not worker_execution)):
        parser.error("Worker evidence is restricted to --worker-preflight; this does not enable execution")
    if args.check_publication:
        allowed = policy.publication_allowed(args.check_publication,
                                             reviewed=args.reviewed, sanitized=args.sanitized)
        print(json.dumps({"publication_allowed": allowed, "branch": args.check_publication}))
        if not allowed:
            raise SystemExit(1)
        return
    if args.pause or args.resume:
        set_pause(args.pause)
    elif args.execute:
        if any(v is None for v in (args.config, args.ssh_runtime)):
            parser.error("Execution needs protected configuration and SSH runtime; default artifact is the exact profile receipt")
        result = execute(args.config, args.artifact, args.ssh_runtime, profile_name=args.profile, diagnostic_target=args.diagnostic_target, worker_inputs=args.worker_inputs, worker_ca=args.worker_ca)
        if result["status"] != "PASSED_RESTORED":
            raise SystemExit(1)
        return
    print(json.dumps(status(), indent=2))


if __name__ == "__main__":
    main()
