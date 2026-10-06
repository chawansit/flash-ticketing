"""ADR0151: default-local preparation; separately approved fixed-placement off/on pair."""

import argparse
import json
import math
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import run_two_host_paid_comparison as comparison
from prepare_two_host_scaling import evaluate_gates
from qualify_two_host_deployment import ROOT, run
from status_refresh_contract import LABEL, PLAN, ROLES, StatusRefreshContract, digest, source_contract
from summarize_two_host_paid_comparison import compact

STATE = ROOT / "docs/capacity/CURRENT_STATE.json"
LOCK = ROOT / "tmp/adr0151-run.lock"
LEDGER = "bounded_status_refresh"
AUTHORIZATION = "adr0151-status-refresh-pair-2026-10-05"
ARMS = ("control", "candidate")
ENVELOPE_GUARD = None


def identity():
    return {
        **comparison.adapter_identity(),
        **{
            "scripts/" + name: comparison.source_sha256((ROOT / "scripts" / name).read_bytes())
            for name in (
                "run_status_refresh_comparison.py",
                "status_refresh_contract.py",
                "two_host_topology.py",
                "work_envelope.py",
                "pre_dispatch_abort_recovery.py",
                "run_work_envelope.py",
            )
        },
        "experiment_plan": digest(json.loads(PLAN.read_text())),
    }


def write_state(state, run_id):
    if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run_id):
        raise ValueError("Owned run identity required")
    temporary = STATE.with_name(STATE.name + "." + run_id + ".pending")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(state, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, STATE)


class RunLock:
    def __init__(self, run_id):
        self.receipt = {"run": run_id, "pid": os.getpid()}
        with LOCK.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(self.receipt))
            stream.flush()
            os.fsync(stream.fileno())

    def release(self):
        if LOCK.is_symlink() or json.loads(LOCK.read_text()) != self.receipt:
            raise ValueError("Owned run lock differs; retained for explicit recovery")
        LOCK.unlink()


def restoration_complete(result):
    return (
        all(
            result.get(key) is True
            for key in (
                "restore_pass",
                "primary_runtime_semantics_restored",
                "secondary_resources_removed",
                "generator_idle_after",
                "credential_snapshots_removed",
            )
        )
        and result.get("restored_global_queues", {}).get("pass") is True
    )


def qualification_matches(report, binding, *, now=None):
    if not isinstance(report, dict) or not isinstance(report.get("arms"), dict):
        return False
    if any(not isinstance(arm, dict) for arm in report["arms"].values()):
        return False
    now = now or datetime.now(UTC)
    try:
        timestamp = datetime.fromisoformat(report["finished_at_utc"])
        fresh = timestamp.tzinfo is not None and 0 <= (now - timestamp).total_seconds() <= 3600
    except (KeyError, TypeError, ValueError):
        return False
    return (
        fresh
        and report.get("kind") == ("status_refresh_dry_control" if len(ARMS) == 1 else "status_refresh_dry_pair")
        and report.get("pass") is True
        and report.get("binding") == binding
        and type(report.get("capacity_stages_started")) is int
        and report["capacity_stages_started"] == 0
        and type(report.get("safety_protocols_started")) is int
        and report["safety_protocols_started"] == len(ARMS)
        and set(report.get("arms", {})) == set(ARMS)
        and all(
            arm.get("pass") is True
            and arm.get("restoration_complete") is True
            and arm.get("pre_safety_source_pass") is True
            for arm in report["arms"].values()
        )
    )


def binding_for(config, artifact, sources):
    return {
        "adapter_identity": identity(),
        "configuration_sha256": digest(config),
        "artifact_sha256": digest(artifact),
        "source_manifest_sha256": digest(
            {"base_revision": comparison.REVISION, "runtime_source_sha256": sources}
        ),
    }


def validate_release(state, binding, *, execute, qualification=None):
    ledger = state.get(LEDGER, {})
    if state.get("standing_work_envelope") and not ledger.get("standing_envelope"):
        raise ValueError("Use a fresh standing-envelope scope; legacy allowances remain closed")
    if ledger.get("standing_envelope"):
        from work_envelope import scope_authorized
        scope_authorized(state, LEDGER, binding)
        if state.get("current_run"):
            raise ValueError("Active experiment")
    elif state.get("cloud_load_requires_resume") or state.get("current_run"):
        raise ValueError("Paused or active experiment")
    if ledger.get("authorization_id") != AUTHORIZATION or ledger.get("binding") != binding:
        raise ValueError("New exact artifact/configuration/adapter approval required")
    numbers = {"qualification_runs_authorized": 1}
    allowances = (ledger.get("paid_runs_authorized"), ledger.get("safety_tickets_authorized"))
    count = len(ARMS)
    allowed = {(count, 2 * count)} if execute else {(0, count), (count, 2 * count)}
    if (
        any(type(ledger.get(k)) is not int for k in ("paid_runs_authorized", "safety_tickets_authorized"))
        or allowances not in allowed
        or any(type(ledger.get(k)) is not int or ledger[k] != value for k, value in numbers.items())
    ):
        raise ValueError("Exact bounded dry pair/paid pair/safety allowance required")
    for name in (
        "paid_runs_started",
        "paid_protocols_started",
        "safety_protocols_started",
        "qualification_protocols_started",
    ):
        if type(ledger.get(name)) is not int or ledger[name] < 0:
            raise ValueError("Explicit integer consumption counters required")
    if ledger["paid_runs_started"] or ledger["paid_protocols_started"]:
        raise ValueError("Paid pair allowance already consumed")
    if execute:
        if (
            ledger["qualification_protocols_started"] != 1
            or ledger["safety_protocols_started"] != len(ARMS)
            or not qualification_matches(qualification or {}, binding)
        ):
            raise ValueError("Fresh matching restored dry pair required; no automatic qualification")
    elif ledger["qualification_protocols_started"] != 0 or ledger["safety_protocols_started"] != 0:
        raise ValueError("Dry pair allowance already consumed")


def reserve_arm(run_id, arm, *, execute):
    state = json.loads(STATE.read_text())
    ledger = state[LEDGER]
    if state.get("current_run") != run_id or ledger.get("active_run") != run_id:
        raise ValueError("Active owned pair required")
    if ledger["safety_protocols_started"] >= ledger["safety_tickets_authorized"]:
        raise ValueError("Safety allowance exhausted")
    attempted = ledger.setdefault("paid_protocol_arms" if execute else "qualification_arms", [])
    if len(attempted) >= len(ARMS) or arm != ARMS[len(attempted)]:
        raise ValueError("Ordered off/on pair only; no replay")
    if execute:
        if ledger["paid_protocols_started"] >= ledger["paid_runs_authorized"]:
            raise ValueError("Paid protocol allowance exhausted")
        ledger["paid_protocols_started"] += 1
    ledger["safety_protocols_started"] += 1
    attempted.append(arm)
    write_state(state, run_id)  # Conservative reservation precedes SSH/ambiguous fixture dispatch.


class RefreshStages(comparison.Stages):
    def __init__(self, execute, bundle, contract, run_id):
        super().__init__(execute, bundle, ledger_key=LEDGER, stage_limit=1, contract=contract)
        self.run_id = run_id

    def __call__(self, session, physical_arm, routes, saved, owner, output):
        if physical_arm == "control":
            return  # Original driver's four-primary setup is never a measured arm.
        if physical_arm != "candidate":
            raise ValueError("Two-plus-two placement required")
        super().__call__(
            session, self.contract.arm, routes, self.contract.stage_snapshot(saved), owner, output
        )

    def claim_paid_stage(self, session, arm):
        state = json.loads(STATE.read_text())
        ledger = state[LEDGER]
        claimed = ledger.setdefault("attempted_paid_arms", [])
        if (
            state.get("current_run") != self.run_id
            or ledger.get("active_run") != self.run_id
            or arm != self.contract.arm
            or arm in claimed
            or session.state["capacity_stages_started"] != 0
            or ledger["paid_runs_started"] >= len(ARMS)
            or arm not in ledger.get("paid_protocol_arms", [])
        ):
            raise ValueError("Fresh reserved paid launch required")
        ledger["paid_runs_started"] += 1
        claimed.append(arm)
        write_state(state, self.run_id)  # Persist before the possibly ambiguous generator call.
        session.state["capacity_stages_started"] = 1
        session.state["attempted_paid_arms"] = [arm]
        session.checkpoint()


def stage_gates(record, inventory, restored, contract):
    result = comparison.gates_for_stage(record, inventory, restored)
    valid = False
    try:
        valid = contract.validate_inventory_receipt(record, inventory, final=True)
    except (ValueError, KeyError, TypeError):
        pass
    result["paid"].pop("cache_disabled")
    result["paid"]["bounded_equal_cache_age"] = valid
    # Evaluate the exact same22+10 obligations under one explicitly renamed cache policy.
    evaluated = evaluate_gates(
        {
            **{k: v for k, v in result["paid"].items() if k != "bounded_equal_cache_age"},
            "cache_disabled": valid,
        },
        result["additional"],
    )
    result.update(evaluated)
    result["failed_gates"] = [
        "bounded_equal_cache_age" if k == "cache_disabled" else k for k in result["failed_gates"]
    ]
    return result


def measurements(record, trace_path, inventory):
    """No RPS extrapolation; cache totals cover the observed lifetime including tail."""
    customer = record.get("customer", {})
    dispatched = customer.get("dispatched", 0)
    attempts = customer.get("physical_http_attempts", {})
    output = {
        "status_checks": attempts.get("orders"),
        "status_checks_per_dispatched_journey": attempts.get("orders", 0) / dispatched
        if dispatched
        else None,
        "consumer_database": record.get("pipeline_summary", {}).get("role_database", {}).get("consumer"),
        "callback_backlog": record.get("pipeline_summary", {}).get("pending_callback_deliveries"),
        "cache_scope": "Full observer lifetime including completion tail; not offered-window HTTP RPS.",
    }
    try:
        rows = [json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()]
        if len(rows) < 2:
            raise ValueError("Insufficient cache samples")
        labels = {f"{a['host_role']}:{a['container_id']}" for a in inventory["apis"]}
        first, previous, previous_time = {}, {}, None
        for row in rows:
            timestamp = datetime.fromisoformat(row["utc"])
            if timestamp.tzinfo is None or (
                previous_time is not None and not 0 < (timestamp - previous_time).total_seconds() <= 2
            ):
                raise ValueError("Cache sampling gap")
            values = row.get("api_replicas", {})
            if row.get("api_metrics_error") or set(values) != labels:
                raise ValueError("Incomplete cache replica observations")
            current = {}
            for label, metrics in values.items():
                start = metrics.get("process_start_time_seconds")
                if type(start) not in {int, float} or not math.isfinite(start) or start <= 0:
                    raise ValueError("Cache process identity missing")
                current[label] = {k: v for k, v in metrics.items() if k.startswith("status_cache:")}
                current[label]["process_start_time_seconds"] = start
                if any(
                    type(v) not in {int, float} or not math.isfinite(v) or v < 0
                    for v in current[label].values()
                ):
                    raise ValueError("Invalid cache counters")
                if label in previous and (
                    start != previous[label]["process_start_time_seconds"]
                    or any(
                        current[label].get(k, 0) < v
                        for k, v in previous[label].items()
                        if k.startswith("status_cache:")
                    )
                ):
                    raise ValueError("Cache counter reset")
            if not first:
                first = {k: dict(v) for k, v in current.items()}
            previous, previous_time = current, timestamp
        deltas = {}
        for label, counters in previous.items():
            for name, value in counters.items():
                if name.startswith("status_cache:"):
                    outcome = name.removeprefix("status_cache:")
                    deltas[outcome] = deltas.get(outcome, 0) + value - first[label].get(name, 0)
        hits, misses = deltas.get("hit", 0), deltas.get("miss", 0)
        output.update(
            cache_observed=True,
            cache_series_present=bool(deltas),
            cache_outcome_deltas=deltas,
            cache_hit_share=hits / (hits + misses) if hits + misses else None,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        output.update(cache_observed=False, cache_failure_type=type(exc).__name__, cache_hit_share=None)
    return output


def run_arm(config, artifact, sources, bundle, arm, run_id, output, *, execute):
    reserve_arm(run_id, arm, execute=execute)
    contract = StatusRefreshContract(artifact, arm, sources)
    stages = RefreshStages(execute, bundle, contract, run_id)
    guarded = {"action_guard": ENVELOPE_GUARD} if ENVELOPE_GUARD is not None else {}
    restored = run(config, output, stage_hook=stages, runtime_policy=contract, **guarded)
    record = stages.results.get(arm, {})
    path = output / arm / "inventory.private.json"
    gates = (
        stage_gates(record, json.loads(path.read_text()), restored, contract)
        if execute and path.exists()
        else {}
    )
    complete = restoration_complete(restored)
    passed = (
        restored.get("pass") is True
        and complete
        and set(stages.results) == {arm}
        and record.get("pass") is True
        and record.get("pre_dispatch_qualified") is True
        and record.get("private_cleanup_pass") is True
        and restored.get("candidate_pre_safety_sources_verified") is True
        and restored.get("post_ttl_financial", {}).get("pass") is True
        and restored.get("post_ttl_financial", {}).get("hold_deadlines_elapsed") is True
        and restored.get("capacity_stages_started") == (1 if execute else 0)
        and (not execute or gates.get("all_required_gates_pass") is True)
    )
    return {
        "pass": passed,
        "restoration_complete": complete,
        "pre_safety_source_pass": restored.get("candidate_pre_safety_sources_verified") is True,
        "capacity_stages_started": restored.get("capacity_stages_started", 0),
        "gates": gates,
        "stage": compact(record) if execute else None,
        "measurements": measurements(record, output / arm / "pipeline.jsonl", json.loads(path.read_text()))
        if execute and path.exists()
        else None,
        "evidence_directory": output.relative_to(ROOT).as_posix(),
        "failure_type": restored.get("failure_type") or restored.get("restoration_error_type"),
    }


def protocol(config, artifact, sources, bundle, binding, *, execute, qualification=None):
    if type(execute) is not bool or binding != binding_for(config, artifact, sources):
        raise ValueError("Actual configuration/artifact/source binding differs before cloud access")
    validate_release(json.loads(STATE.read_text()), binding, execute=execute, qualification=qualification)
    if ENVELOPE_GUARD is not None:
        ENVELOPE_GUARD.check()
    run_id = "adr0151-" + uuid4().hex[:12]
    output = ROOT / "tmp" / run_id
    lock = RunLock(run_id)
    arms, original_error = {}, None
    initial = json.loads(STATE.read_text())[LEDGER]
    initial_paid, initial_safety = initial["paid_runs_started"], initial["safety_protocols_started"]
    try:
        # Recheck under the exclusive lock before reserving or touching any cloud surface.
        state = json.loads(STATE.read_text())
        validate_release(state, binding, execute=execute, qualification=qualification)
        output.mkdir(mode=0o700)
        state["current_run"] = run_id
        state[LEDGER]["active_run"] = run_id
        if not execute:
            state[LEDGER]["qualification_protocols_started"] += 1
        write_state(state, run_id)
        print(json.dumps({"phase": "bounded-" + ("control" if len(ARMS) == 1 else "pair") + "-started", "execute": execute, "run": run_id}), flush=True)
        for arm in ARMS:
            directory = output / ("adr0151-arm-" + uuid4().hex[:12])
            directory.mkdir(mode=0o700)
            arms[arm] = run_arm(config, artifact, sources, bundle, arm, run_id, directory, execute=execute)
            if arms[arm]["pass"] is not True:
                break
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001 - preserve ambiguous owned state/allowances
        original_error = type(exc).__name__
    restored = (
        original_error is None
        and bool(arms)
        and all(a["restoration_complete"] is True for a in arms.values())
    )
    passed = (
        original_error is None
        and restored
        and set(arms) == set(ARMS)
        and all(a["pass"] is True for a in arms.values())
    )
    final_ledger = json.loads(STATE.read_text())[LEDGER]
    performance_complete = (
        execute
        and passed
        and all(
            (arm.get("measurements") or {}).get("cache_observed") is True
            and (arm.get("measurements") or {}).get("cache_series_present") is True
            for arm in arms.values()
        )
    )
    report = {
        "performance_measurement_complete": performance_complete,
        "kind": ("status_refresh_paid_" if execute else "status_refresh_dry_") + ("control" if len(ARMS) == 1 else "pair"),
        "pass": passed,
        "run": run_id,
        "binding": binding,
        "arms": arms,
        "failure_type": original_error,
        "capacity_stages_started": final_ledger["paid_runs_started"] - initial_paid,
        "safety_protocols_started": final_ledger["safety_protocols_started"] - initial_safety,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "scope": "Fixed2+2API topology, same1000ms cache and budgets;60buyers/s300s each paid arm. No hourly capacity claim.",
    }
    if output.exists():
        report_path = output / "comparison-summary.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        state = json.loads(STATE.read_text())
        if state.get("current_run") == run_id:
            state[LEDGER]["last_runner_report"] = report_path.relative_to(ROOT).as_posix()
            if not execute and passed:
                state[LEDGER]["dry_qualification_report"] = report_path.relative_to(ROOT).as_posix()
            if restored and original_error is None:
                state["current_run"] = None
                state[LEDGER]["active_run"] = None
                write_state(state, run_id)
                lock.release()
            else:
                state[LEDGER]["recovery_required"] = True
                write_state(state, run_id)
    else:
        lock.release()  # No state reservation/remote work took place.
    print(
        json.dumps(
            {
                "phase": "bounded-" + ("control" if len(ARMS) == 1 else "pair") + "-finished",
                "pass": passed,
                "run": run_id,
                "recovery_required": not restored,
            }
        ),
        flush=True,
    )
    return report


def prepare(output):
    sources = source_contract()
    proposal = {
        "kind": "status_refresh_runner_preparation",
        "status": "LOCAL_ONLY_IMAGES_AND_APPROVAL_PENDING",
        "binding_template": {"adapter_identity": identity()},
        "runtime_source_sha256": sources,
        "artifact_receipt_required": {
            "images": {r: "immutable-image-id-required" for r in ROLES},
            "parent_images": {r: "frozen-role-parent-id-required" for r in ROLES},
            "source_manifest_sha256": digest(
                {"base_revision": comparison.REVISION, "runtime_source_sha256": sources}
            ),
        },
        "image_label_required": LABEL,
        "cloud_calls": 0,
        "paid_launches": 0,
        "qualification_max_age_seconds": 3600,
        "safety_tickets_per_protocol": 1,
        "approval_scope": {
            "qualification_runs_authorized": 1,
            "paid_runs_authorized": len(ARMS),
            "safety_tickets_authorized": 2 * len(ARMS),
        },
        "commands": "prepare(default) performs no SSH; qualify and execute are separate approved invocations",
    }
    output = output.resolve()
    if not output.is_relative_to((ROOT / "tmp").resolve()) or output.exists():
        raise ValueError("Fresh owned preparation file under tmp required")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(proposal, indent=2) + "\n")
    return proposal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--qualify", action="store_true")
    modes.add_argument("--execute", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--ssh-runtime", type=Path)
    args = parser.parse_args()
    if not args.qualify and not args.execute:
        if args.output is None:
            parser.error("Local preparation requires --output")
        prepare(args.output)
        print(json.dumps({"phase": "local-prepared", "cloud_calls": 0, "output": str(args.output)}))
        return
    if any(value is None for value in (args.config, args.artifact, args.ssh_runtime)):
        parser.error("Cloud mode requires config, artifact receipt and SSH runtime")
    sources = source_contract()
    artifact = json.loads(args.artifact.read_text())
    StatusRefreshContract(artifact, "control", sources)
    config = json.loads(args.config.read_text())
    comparison.validate_config(config)
    binding = binding_for(config, artifact, sources)
    state = json.loads(STATE.read_text())
    qualification_path = state.get(LEDGER, {}).get("dry_qualification_report")
    qualification = (
        json.loads((ROOT / qualification_path).read_text()) if args.execute and qualification_path else None
    )
    validate_release(state, binding, execute=args.execute, qualification=qualification)
    bundle = comparison.frozen_bundle()
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    report = protocol(
        config, artifact, sources, bundle, binding, execute=args.execute, qualification=qualification
    )
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
