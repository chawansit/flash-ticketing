#!/usr/bin/env python3
"""Run one distributed Huawei capacity stage without interactive model polling."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

SAFE_HOST = re.compile(r"^[A-Za-z0-9_.@-]+$")
SECRET_PATTERNS = [
    (re.compile(r"(?i)(password|token|secret|authorization)(\s*[=:]\s*)\S+"), r"\1\2[REDACTED]"),
    (re.compile(r"(?i)(postgres(?:ql)?://[^:\s]+:)[^@\s]+@"), r"\1[REDACTED]@"),
    (re.compile(r"Bearer\s+\S+", re.IGNORECASE), "Bearer [REDACTED]"),
]


def redact(value: str) -> str:
    for pattern, replacement in SECRET_PATTERNS:
        value = pattern.sub(replacement, value)
    return value


class Transport:
    def __init__(self, ssh: str, scp: str, log_dir: Path):
        self.ssh = ssh
        self.scp = scp
        self.log_dir = log_dir

    def _record(self, phase: str, result: subprocess.CompletedProcess[str]) -> None:
        payload = redact((result.stdout or "") + (result.stderr or ""))
        (self.log_dir / f"{phase}.log").write_text(payload, encoding="utf-8")

    def remote(
        self,
        phase: str,
        host: str,
        argv: list[str],
        *,
        check: bool = True,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [self.ssh, "-o", "BatchMode=yes", host, shlex.join(argv)]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
        self._record(phase, result)
        if check and result.returncode:
            raise RuntimeError(f"{phase} failed with exit code {result.returncode}")
        return result

    def copy_from(self, phase: str, host: str, remote: str, local: Path, recursive: bool = False) -> None:
        command = [self.scp, "-q", "-o", "BatchMode=yes"]
        if recursive:
            command.append("-r")
        command.extend([f"{host}:{remote}", str(local)])
        result = subprocess.run(
            command, capture_output=True, text=True, stdin=subprocess.DEVNULL, check=False
        )
        self._record(phase, result)
        if result.returncode:
            raise RuntimeError(f"{phase} failed with exit code {result.returncode}")

    def copy_to(self, phase: str, local: Path, host: str, remote: str, recursive: bool = False) -> None:
        command = [self.scp, "-q", "-o", "BatchMode=yes"]
        if recursive:
            command.append("-r")
        command.extend([str(local), f"{host}:{remote}"])
        result = subprocess.run(
            command, capture_output=True, text=True, stdin=subprocess.DEVNULL, check=False
        )
        self._record(phase, result)
        if result.returncode:
            raise RuntimeError(f"{phase} failed with exit code {result.returncode}")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate(output: Path, load_exit: int | None, audit_exit: int | None, admission_exit: int | None) -> dict:
    generator = output / "generator" / "public"
    backend = output / "backend" / "public"
    summary = load_json(generator / "load" / "summary.json") if (generator / "load" / "summary.json").exists() else {}
    durability = load_json(backend / "durability.json") if (backend / "durability.json").exists() else {}
    admission = load_json(backend / "admission.json") if (backend / "admission.json").exists() else {}
    rollback = load_json(backend / "rollback.json") if (backend / "rollback.json").exists() else {}
    gates = {
        "load_exit_zero": load_exit == 0,
        "workload": summary.get("workload_gate_pass") is True,
        "generator_drops_zero": summary.get("generator_drops") == 0,
        "transport_errors_zero": not summary.get("transport_errors"),
        "read_p95": (summary.get("worst_worker_read_p95_ms") or float("inf")) < 150,
        "hold_p95": (summary.get("worst_worker_hold_p95_ms") or float("inf")) < 300,
        "admission_zero": admission_exit == 0 and admission.get("admission_rejections") == 0,
        "audit_exit_zero": audit_exit == 0,
        "durability": durability.get("durability_and_expiry_pass") is True,
        "overlap_zero": durability.get("overlapping_load_hold_intervals") == 0,
        "queues_drained": all(value == 0 for value in durability.get("queues_snapshot", {}).values()),
        "rollback": rollback.get("pass") is True,
    }
    return {
        "completed_at": datetime.now(UTC).isoformat(),
        "gates": gates,
        "pass": all(gates.values()),
        "request_summary": {
            key: summary.get(key)
            for key in (
                "rate",
                "seconds",
                "generator_drops",
                "worst_worker_read_p95_ms",
                "worst_worker_hold_p95_ms",
            )
        },
        "admission_rejections": admission.get("admission_rejections"),
        "audited_holds": durability.get("audited_holds"),
        "overlapping_load_hold_intervals": durability.get("overlapping_load_hold_intervals"),
    }


def validate_args(args: argparse.Namespace) -> None:
    for host in (args.backend_host, args.generator_host):
        if not SAFE_HOST.fullmatch(host):
            raise ValueError(f"Unsafe SSH host alias: {host}")
    if args.output.exists():
        raise ValueError("Use a fresh output directory")
    if min(
        args.rate,
        args.seconds,
        args.workers,
        args.shows,
        args.seats,
        args.viewers,
        args.sale_hours,
        args.hold_expiry_wait,
    ) < 1:
        raise ValueError("Stage limits must be positive")
    if args.seats < (args.rate * args.seconds * 5 // 100 + args.shows - 1) // args.shows:
        raise ValueError("Fresh fixture has insufficient unique seats for the requested stage")
    if not 1 <= args.admission_candidate <= 64 or not 1 <= args.admission_rollback <= 64:
        raise ValueError("Admission values must be between 1 and 64")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-host", required=True, help="OpenSSH host alias; keys must work in batch mode")
    parser.add_argument("--generator-host", required=True, help="OpenSSH host alias; keys must work in batch mode")
    parser.add_argument("--backend-dir", default="/root/flash-ticketing-rds")
    parser.add_argument("--generator-dir", default="/root/flash-generator")
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--shows", type=int, default=800)
    parser.add_argument("--seats", type=int, default=300)
    parser.add_argument("--viewers", type=int, default=8000)
    parser.add_argument("--sale-hours", type=int, default=6)
    parser.add_argument("--seat-offset", type=int, default=0)
    parser.add_argument("--admission-candidate", type=int, required=True)
    parser.add_argument("--admission-rollback", type=int, required=True)
    parser.add_argument("--hold-expiry-wait", type=int, default=180)
    parser.add_argument("--ssh", default="ssh")
    parser.add_argument("--scp", default="scp")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    validate_args(args)

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    phases = [
        "prepare fresh fixture and private manifest",
        "transfer manifest through a private temporary directory",
        "deploy bounded candidate and verify readiness",
        "preflight and start observers",
        "run no-retry load on separate generator",
        "collect compact results and admission gate",
        "wait for hold expiry and run exact durability audit",
        "restore admission, collect compact evidence and delete private manifests",
    ]
    if args.dry_run:
        print(json.dumps({"run_id": run_id, "phases": phases}, indent=2))
        return

    args.output.mkdir(parents=True)
    log_dir = args.output / "operator-logs"
    log_dir.mkdir()
    state_path = args.output / "state.json"
    transport = Transport(args.ssh, args.scp, log_dir)
    backend_helper = f"{args.backend_dir}/scripts/huawei_capacity_backend.sh"
    generator_helper = f"{args.generator_dir}/scripts/huawei_capacity_generator.sh"
    backend_prefix = ["env", f"FLASH_TICKETING_BACKEND_DIR={args.backend_dir}", "sh", backend_helper]
    generator_prefix = [
        "env",
        f"FLASH_TICKETING_GENERATOR_DIR={args.generator_dir}",
        "sh",
        generator_helper,
    ]

    completed: list[str] = []
    deployed = False
    observers = False
    load_exit = audit_exit = admission_exit = None
    error = None

    def checkpoint(phase: str) -> None:
        completed.append(phase)
        state_path.write_text(
            json.dumps({"run_id": run_id, "completed": completed, "error": error}, indent=2) + "\n",
            encoding="utf-8",
        )

    try:
        transport.remote(
            "prepare",
            args.backend_host,
            backend_prefix
            + [
                "prepare",
                run_id,
                str(args.shows),
                str(args.seats),
                str(args.sale_hours),
                args.origin,
                str(args.viewers),
            ],
            timeout=900,
        )
        checkpoint("prepared")

        with tempfile.TemporaryDirectory(prefix="flash-capacity-private-") as private_dir:
            private_manifest = Path(private_dir) / "manifest.json"
            transport.copy_from(
                "manifest-from-backend",
                args.backend_host,
                f"{args.backend_dir}/tmp/unattended-{run_id}/private/manifest.json",
                private_manifest,
            )
            private_manifest.chmod(0o600)
            generator_upload = f"/root/unattended-{run_id}-upload.json"
            transport.copy_to(
                "manifest-to-generator",
                private_manifest,
                args.generator_host,
                generator_upload,
            )
        checkpoint("manifest_transferred")

        deployed = True
        transport.remote(
            "deploy",
            args.backend_host,
            backend_prefix
            + ["deploy", run_id, str(args.admission_candidate), str(args.admission_rollback)],
            timeout=300,
        )
        checkpoint("deployed")

        transport.remote(
            "preflight",
            args.backend_host,
            backend_prefix + ["preflight", run_id, str(args.seconds)],
            timeout=180,
        )
        checkpoint("preflight_passed")

        transport.remote(
            "observe",
            args.backend_host,
            backend_prefix + ["observe", run_id, str(args.seconds + 60)],
            timeout=30,
        )
        observers = True
        checkpoint("observers_started")

        load = transport.remote(
            "load",
            args.generator_host,
            generator_prefix
            + [
                "run",
                run_id,
                f"/root/unattended-{run_id}-upload.json",
                str(args.rate),
                str(args.seconds),
                str(args.workers),
                str(args.seat_offset),
            ],
            check=False,
            timeout=args.seconds + 240,
        )
        load_exit = load.returncode
        checkpoint("load_finished")

        transport.remote(
            "stop-observers",
            args.backend_host,
            backend_prefix + ["stop-observers", run_id],
            check=False,
            timeout=60,
        )
        observers = False

        generator_parent = args.output / "generator"
        generator_parent.mkdir()
        transport.copy_from(
            "generator-evidence",
            args.generator_host,
            f"/root/unattended-{run_id}/public",
            generator_parent,
            recursive=True,
        )
        transport.remote(
            "backend-load-dir",
            args.backend_host,
            ["mkdir", "-p", f"{args.backend_dir}/tmp/unattended-{run_id}/public"],
        )
        transport.copy_to(
            "load-results-to-backend",
            generator_parent / "public" / "load",
            args.backend_host,
            f"{args.backend_dir}/tmp/unattended-{run_id}/public/",
            recursive=True,
        )
        admission = transport.remote(
            "admission-gate",
            args.backend_host,
            backend_prefix + ["gate", run_id],
            check=False,
        )
        admission_exit = admission.returncode
        checkpoint("compact_results_collected")

        time.sleep(args.hold_expiry_wait)
        audit = transport.remote(
            "durability-audit",
            args.backend_host,
            backend_prefix + ["audit", run_id],
            check=False,
            timeout=180,
        )
        audit_exit = audit.returncode
        checkpoint("audit_finished")
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError, KeyboardInterrupt) as exc:
        error = redact(repr(exc))
    finally:
        finalizer_errors: list[str] = []

        def finish(label: str, operation) -> None:
            try:
                operation()
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                finalizer_errors.append(f"{label}: {redact(repr(exc))}")

        if observers:
            finish(
                "stop observers",
                lambda: transport.remote(
                    "stop-observers-finally",
                    args.backend_host,
                    backend_prefix + ["stop-observers", run_id],
                    check=False,
                    timeout=60,
                ),
            )
        if deployed:
            finish(
                "rollback",
                lambda: transport.remote(
                    "rollback",
                    args.backend_host,
                    backend_prefix + ["rollback", run_id],
                    check=False,
                    timeout=300,
                ),
            )
        backend_parent = args.output / "backend"
        backend_parent.mkdir(exist_ok=True)
        finish(
            "collect backend evidence",
            lambda: transport.copy_from(
                "backend-evidence",
                args.backend_host,
                f"{args.backend_dir}/tmp/unattended-{run_id}/public",
                backend_parent,
                recursive=True,
            ),
        )
        finish(
            "generator cleanup",
            lambda: transport.remote(
                "generator-cleanup",
                args.generator_host,
                generator_prefix + ["cleanup", run_id],
                check=False,
                timeout=30,
            ),
        )
        finish(
            "backend cleanup",
            lambda: transport.remote(
                "backend-cleanup",
                args.backend_host,
                backend_prefix + ["cleanup", run_id],
                check=False,
                timeout=60,
            ),
        )
        if finalizer_errors:
            finalizer_error = "; ".join(finalizer_errors)
            error = f"{error}; {finalizer_error}" if error else finalizer_error

    result = evaluate(args.output, load_exit, audit_exit, admission_exit)
    result["gates"]["execution_and_cleanup"] = error is None
    result["pass"] = all(result["gates"].values())
    result.update({"run_id": run_id, "error": error, "completed_phases": completed})
    (args.output / "stage-result.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result), flush=True)
    if error or not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
