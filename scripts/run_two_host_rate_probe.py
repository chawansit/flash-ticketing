"""ADR0148: one explicitly authorized 84-buyer/s probe with exact dry qualification."""

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import run_two_host_paid_comparison as comparison
from qualify_two_host_deployment import IMAGE, ROOT, run
from summarize_two_host_paid_comparison import compact

LEDGER = "bounded_rate_probe"
AUTHORIZATION_ID = "adr0148-84-buyers-300s"


def identity():
    return {**comparison.adapter_identity(), "scripts/run_two_host_rate_probe.py":
            comparison.source_sha256(Path(__file__).read_bytes())}


def validate_release(state, baseline, *, execute):
    if state.get("cloud_load_requires_resume") or state.get("current_run"):
        raise ValueError("Paused or active experiment")
    if baseline.get("pass") is not True or baseline.get("restore_pass") is not True or baseline.get("application_revision") != comparison.REVISION:
        raise ValueError("Passing frozen matched baseline required")
    from prepare_two_host_scaling import evaluate_gates

    for arm in ("control", "candidate"):
        gates = baseline.get("gates", {}).get(arm, {})
        if not evaluate_gates(gates.get("paid", {}), gates.get("additional", {}))["all_required_gates_pass"]:
            raise ValueError("Complete passing baseline gate evidence required")
    ledger = state.get(LEDGER, {})
    if ledger.get("authorization_id") != AUTHORIZATION_ID or (type(ledger.get("qualification_runs_authorized")) is not int or ledger["qualification_runs_authorized"] != 1):
        raise ValueError("Explicit bounded probe and safety qualification authorization required")
    if type(ledger.get("paid_runs_started")) is not int or ledger["paid_runs_started"] != 0:
        raise ValueError("Paid probe already consumed; no automatic replacement")
    if execute and (type(ledger.get("paid_runs_authorized")) is not int or ledger["paid_runs_authorized"] != 1):
        raise ValueError("Explicit one paid probe authorization required")


def worker_logs_program(spec_path, since):
    """Read only the exact simulator/consumer IDs from this run's owned CPU spec."""
    return r"""import json,re,subprocess
from pathlib import Path
spec=json.loads(Path(SPEC).read_text())
logs=[]
for row in spec['containers']:
 if row['role'] not in {'consumer','simulator'}:continue
 cid=row['id']
 if not re.fullmatch('[0-9a-f]{64}',cid):raise ValueError('Invalid diagnostic container identity')
 current=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=10))[0]
 if current['Id']!=cid or current['Image']!=IMAGE or current['Config']['Labels']['com.docker.compose.service']!=row['role']:raise ValueError('Diagnostic container/source changed')
 result=subprocess.run(['docker','logs','--since',SINCE,'--tail','200',cid],capture_output=True,timeout=12)
 if result.returncode:raise ValueError('Diagnostic log unavailable')
 raw=result.stdout+result.stderr
 logs.append({'role':row['role'],'container_id':cid,'tail_truncated':len(raw)>16384,'text':raw[-16384:].decode(errors='replace')})
if sum(row['role']=='consumer' for row in logs)!=6 or sum(row['role']=='simulator' for row in logs)!=1:raise ValueError('Diagnostic worker coverage differs')
print(json.dumps({'logs':logs}))
""".replace("SPEC", repr(spec_path)).replace("SINCE", repr(since)).replace("IMAGE", repr(IMAGE))


def diagnostic_summary(logs):
    """Literal bounded-log matches, not invented incident or customer-error counts."""
    statuses, exceptions = {}, {}
    for log in logs:
        for status in re.findall(r"HTTP Error (\d{3})", log["text"]):
            statuses[status] = statuses.get(status, 0) + 1
        for name in ("PoolTimeout", "LockNotAvailable", "RemoteDisconnected",
                     "ConnectionResetError", "TimeoutError", "HTTPError"):
            count = len(re.findall(r"\b" + name + r"\b", log["text"]))
            if count:
                exceptions[name] = exceptions.get(name, 0) + count
    return {"http_status_literal_occurrences": statuses, "exception_name_literal_occurrences": exceptions,
            "note": "Trailing bounded exception logs may omit earlier errors; occurrences are not unique incidents or customer error percentage."}


class ProbeStages(comparison.Stages):
    def __init__(self, execute, bundle):
        super().__init__(execute, bundle, rate=84, ledger_key=LEDGER, stage_limit=1)

    def __call__(self, session, arm, routes, saved, owner, output):
        if arm == "control":
            return  # Deployment prerequisite only; no control fixture/observers/buyers.
        if arm != "candidate":
            raise ValueError("Candidate-only probe required")
        since = datetime.now(UTC).isoformat()
        try:
            super().__call__(session, arm, routes, saved, owner, output)
        finally:
            record = self.results.get(arm, {})
            local = output / arm
            try:
                captured = session.call("primary", worker_logs_program(owner + "/candidate/cpu-spec.json", since), 130)
                metadata = []
                for item in captured["logs"]:
                    path = local / (item["role"] + "-" + item["container_id"][:12] + ".private.log")
                    with path.open("x", encoding="utf-8") as file:
                        path.chmod(0o600)
                        file.write(item["text"])
                    metadata.append({"role": item["role"], "file": path.name, "tail_truncated": item["tail_truncated"]})
                record["worker_error_evidence"] = {"pass": True, "files": metadata,
                                                   **diagnostic_summary(captured["logs"])}
            except Exception as exc:  # noqa: BLE001 - preserve original stage error and restoration
                record["worker_error_evidence"] = {"pass": False, "failure_type": type(exc).__name__}
            evidence_pass = record["worker_error_evidence"]["pass"]
            previously_passed = record.get("pass") is True
            if not evidence_pass:
                record["pass"] = False
            (local / "stage.private.json").write_text(json.dumps(record, indent=2) + "\n")
            session.state.setdefault("stages", {}).setdefault(arm, {})["worker_error_evidence_pass"] = evidence_pass
            session.checkpoint()
            if previously_passed and not evidence_pass:
                raise ValueError("Required bounded worker exception evidence missing")


def qualification_matches(report, expected_identity):
    return (report.get("pass") is True and report.get("kind") == "two_host_84_buyer_probe_dry"
            and report.get("adapter_identity") == expected_identity
            and type(report.get("capacity_stages_started")) is int and report["capacity_stages_started"] == 0
            and report.get("restore_pass") is True
            and report.get("worker_error_evidence_pass") is True)


def protocol(config, bundle, *, execute):
    path = ROOT / "docs/capacity/CURRENT_STATE.json"
    state = json.loads(path.read_text())
    output = ROOT / "tmp" / ("adr0148-rate-" + uuid4().hex[:12])
    output.mkdir(mode=0o700)
    state["current_run"] = output.name
    state[LEDGER]["active_run"] = output.name
    if not execute:
        state[LEDGER]["qualification_protocols_started"] = state[LEDGER].get("qualification_protocols_started", 0) + 1
    path.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps({"phase": "rate-probe-output-created", "execute": execute, "output": str(output)}), flush=True)
    stages = ProbeStages(execute, bundle)
    result = {"pass": False, "restore_pass": False, "capacity_stages_started": 0}
    failure = None
    try:
        result = run(config, output, stage_hook=stages)
    except Exception as exc:  # noqa: BLE001 - retain launch failure and consumed ledger
        failure = type(exc).__name__
    stage = stages.results.get("candidate", {})
    gates = {}
    if execute and (output / "candidate/inventory.private.json").exists():
        inventory = json.loads((output / "candidate/inventory.private.json").read_text())
        gates = comparison.gates_for_stage(stage, inventory, result)
    diagnostic_pass = stage.get("worker_error_evidence", {}).get("pass") is True
    passed = (result.get("pass") is True and result.get("restore_pass") is True
              and set(stages.results) == {"candidate"} and stage.get("pass") is True
              and stage.get("pre_dispatch_qualified") is True and stage.get("private_cleanup_pass") is True
              and diagnostic_pass and result.get("capacity_stages_started") == (1 if execute else 0)
              and (not execute or gates.get("all_required_gates_pass") is True))
    report = {"kind": "two_host_84_buyer_probe" if execute else "two_host_84_buyer_probe_dry",
              "run": output.name, "pass": passed, "restore_pass": result.get("restore_pass"),
              "capacity_stages_started": result.get("capacity_stages_started"),
              "adapter_identity": identity(), "gates": gates,
              "worker_error_evidence_pass": diagnostic_pass,
              "worker_error_summary": {k: v for k, v in stage.get("worker_error_evidence", {}).items() if k != "files"},
              "stage": compact(stage) if execute else None, "failure_type": failure,
              "application_revision": comparison.REVISION,
              "scope": "84 offered buyers/s300s,25200 unique-seat journeys; expanded84-show inventory. Not maximum capacity, synchronized flash opening or sustained-hour validation."}
    report_path = output / "rate-probe-summary.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    state = json.loads(path.read_text())
    state["current_run"] = None
    state[LEDGER]["active_run"] = None
    state[LEDGER]["last_runner_report"] = report_path.relative_to(ROOT).as_posix()
    if not execute and passed:
        state[LEDGER]["dry_qualification_report"] = report_path.relative_to(ROOT).as_posix()
    state["status"] = "adr0148_probe_passed_restored" if execute and passed else "adr0148_dry_qualified" if passed else "adr0148_probe_or_qualification_failed"
    path.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps({"phase": "rate-probe-finished", "pass": passed, "execute": execute,
                      "report": str(report_path), "restore_pass": result.get("restore_pass")}), flush=True)
    return report, report_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--ssh-runtime", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    state = json.loads((ROOT / "docs/capacity/CURRENT_STATE.json").read_text())
    baseline = json.loads((ROOT / state["bounded_control"]["comparison_report"]).read_text())
    validate_release(state, baseline, execute=args.execute)
    config = json.loads(args.config.read_text())
    comparison.validate_config(config)
    bundle = comparison.frozen_bundle()
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    expected_identity = identity()
    qualification_path = state[LEDGER].get("dry_qualification_report")
    qualified = qualification_matches(json.loads((ROOT / qualification_path).read_text()), expected_identity) if qualification_path else False
    if not args.execute or not qualified:
        if state[LEDGER].get("qualification_protocols_started", 0) != 0:
            raise ValueError("Qualification already consumed; no automatic repeat")
        qualification, _ = protocol(config, bundle, execute=False)
        if not qualification_matches(qualification, expected_identity):
            raise SystemExit(1)
    if args.execute:
        state = json.loads((ROOT / "docs/capacity/CURRENT_STATE.json").read_text())
        validate_release(state, baseline, execute=True)
        report, _ = protocol(config, bundle, execute=True)
        if not report["pass"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
