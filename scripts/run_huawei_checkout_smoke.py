"""One bounded Huawei paid-ticket smoke with rollback and private-manifest cleanup."""

import argparse
import ipaddress
import json
import re
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from unattended_capacity_stage import Transport

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--backend-host", required=True)
parser.add_argument("--generator-host", required=True)
parser.add_argument("--backend-dir", required=True)
parser.add_argument("--generator-dir", required=True)
parser.add_argument("--origin", required=True)
parser.add_argument("--identity-file", type=Path, required=True)
parser.add_argument("--admission-candidate", type=int, required=True)
parser.add_argument("--admission-rollback", type=int, required=True)
parser.add_argument("--paid-rate", type=int, default=0)
parser.add_argument("--paid-seconds", type=int, default=30)
parser.add_argument("--paid-concurrency", type=int, default=100)
parser.add_argument("--paid-poll-seconds", type=float, default=0.2)
parser.add_argument("--simulator-concurrency-candidate", type=int, choices=(4, 8), default=4)
parser.add_argument("--consumer-candidate", type=int, choices=(1, 2), default=1)
parser.add_argument("--api-pool-waiters-candidate", type=int, choices=(3, 12), default=3)
parser.add_argument("--shows", type=int, default=2)
parser.add_argument("--viewers", type=int, default=20)
args = parser.parse_args()
EXPECTED = args.paid_rate * args.paid_seconds if args.paid_rate else 10
if not 0 <= args.paid_rate <= 100 or not 1 <= args.paid_seconds <= 300:
    parser.error("Bounded paid-stage rate/duration required")
if not 1 <= args.shows <= 1000 or not 1 <= args.viewers <= 50000:
    parser.error("Invalid fixture size")
if not 1 <= args.paid_concurrency <= 1000 or not 0.05 <= args.paid_poll_seconds <= 2:
    parser.error("Invalid paid-stage concurrency")
if EXPECTED > args.shows * 300 or EXPECTED > 300000:
    parser.error("This smoke runner supports at most 300000 distinct tickets")
BACKEND = args.backend_dir
GENERATOR = args.generator_dir
API_HOST = args.backend_host
GEN_HOST = args.generator_host
ORIGIN = args.origin.rstrip("/")
KEY = args.identity_file
COMPOSE = (
    "docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml "
    "-f compose.horizontal.yaml -f compose.keepalive10.yaml"
)
BIND_IP = urlsplit(ORIGIN).hostname
if not BIND_IP or ipaddress.ip_address(BIND_IP).version != 4:
    parser.error("An explicit IPv4 origin is required")
if not 1 <= args.admission_candidate <= 64 or not 1 <= args.admission_rollback <= 64:
    parser.error("Admission values must be between 1 and 64")
if any(not re.fullmatch(r"[A-Za-z0-9_./@-]+", value) for value in (BACKEND, GENERATOR, API_HOST, GEN_HOST)):
    parser.error("Invalid host or remote directory")
RUN = "checkout-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
OUT = ROOT / "tmp" / RUN
OUT.mkdir()
logs = OUT / "logs"
logs.mkdir()
transport = Transport("ssh", "scp", logs, KEY)
prefix = [
    "env",
    f"FLASH_TICKETING_BACKEND_DIR={BACKEND}",
    f"API_POOL_MAX_WAITING={args.api_pool_waiters_candidate}",
    "sh",
    f"{BACKEND}/scripts/huawei_capacity_backend.sh",
]
rollback_prefix = [
    "env",
    f"FLASH_TICKETING_BACKEND_DIR={BACKEND}",
    "API_POOL_MAX_WAITING=3",
    "sh",
    f"{BACKEND}/scripts/huawei_capacity_backend.sh",
]
waiter_check = (
    f'cd {BACKEND}; for id in $({COMPOSE} ps -q api); do docker exec "$id" printenv DB_POOL_MAX_WAITING; done'
)
state = {"run": RUN, "phases": [], "pass": False, "error": None}
deployed = prepared = probe_attempted = observer_started = simulator_changed = False


def step(label, host, argv, timeout=120):
    response = transport.remote(label, host, argv, timeout=timeout)
    state["phases"].append(label)
    (OUT / "state.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    return response


def backend_exec(label, code, timeout=120, check=True):
    return transport.remote(label, API_HOST, ["sh", "-lc", code], timeout=timeout, check=check)


try:
    if not KEY.is_file():
        raise RuntimeError("SSH identity missing")
    backend_rev = step(
        "backend-revision", API_HOST, ["git", "-C", BACKEND, "rev-parse", "HEAD"], 30
    ).stdout.strip()
    generator_rev = step(
        "generator-revision", GEN_HOST, ["git", "-C", GENERATOR, "rev-parse", "HEAD"], 30
    ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", backend_rev) or backend_rev != generator_rev:
        raise RuntimeError("Backend/generator revisions differ")
    state["source_revision"] = backend_rev
    health = step("api-readiness", API_HOST, ["curl", "-fsS", ORIGIN + "/health/ready"], 30)
    if json.loads(health.stdout).get("status") != "ready":
        raise RuntimeError("API not ready")
    original = step(
        "original-mode",
        API_HOST,
        [
            "sh",
            "-lc",
            f'cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); docker exec "$api" printenv RESERVATION_MODE',
        ],
        30,
    ).stdout.strip()
    if original != "postgres":
        raise RuntimeError("Unexpected initial reservation mode")
    deployed = True
    step(
        "deploy",
        API_HOST,
        prefix
        + [
            "deploy",
            RUN,
            str(args.admission_candidate),
            str(args.admission_rollback),
            "1",
            "1",
            str(args.consumer_candidate),
            "1",
            "0",
            "redis-first",
            "3",
            "4",
            "60",
            BIND_IP,
        ],
        300,
    )
    waiting_values = step(
        "api-pool-waiters-candidate", API_HOST, ["sh", "-lc", waiter_check], 30
    ).stdout.splitlines()
    if waiting_values != [str(args.api_pool_waiters_candidate)] * 4:
        raise RuntimeError("Candidate API waiter cap not active on all four replicas")
    if args.simulator_concurrency_candidate != 4:
        simulator_config = (
            f'cd {BACKEND}; id=$({COMPOSE} ps -q simulator); test -n "$id"; '
            "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
            "| grep -E '^(SIMULATOR_CONCURRENCY|DB_POOL_MAX)='"
        )
        original_simulator = step("simulator-original", API_HOST, ["sh", "-lc", simulator_config], 30).stdout
        if "SIMULATOR_CONCURRENCY=4" not in original_simulator or "DB_POOL_MAX=12" not in original_simulator:
            raise RuntimeError("Unexpected simulator baseline configuration")
        simulator_changed = True
        simulator_apply = (
            f"cd {BACKEND}; SIMULATOR_CONCURRENCY={args.simulator_concurrency_candidate} "
            f"{COMPOSE} up -d --no-deps --force-recreate simulator; "
            f"id=$({COMPOSE} ps -q simulator); "
            'test -n "$id"; '
            "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
            f"| grep -qx 'SIMULATOR_CONCURRENCY={args.simulator_concurrency_candidate}'"
        )
        step("simulator-candidate", API_HOST, ["sh", "-lc", simulator_apply], 120)
    step(
        "prepare",
        API_HOST,
        prefix + ["prepare", RUN, str(args.shows), "300", "1", ORIGIN, str(args.viewers)],
        300,
    )
    prepared = True
    step("warm", API_HOST, prefix + ["warm", RUN], 180)
    step("preflight", API_HOST, prefix + ["preflight", RUN, str(max(90, args.paid_seconds + 120))], 120)
    transport.copy_from(
        "queue-before",
        API_HOST,
        f"{BACKEND}/tmp/unattended-{RUN}/public/queue-before.json",
        OUT / "queue-before.json",
    )
    if not json.loads((OUT / "queue-before.json").read_text())["pass"]:
        raise RuntimeError("Preflight queues are not drained")

    if args.paid_rate:
        transport.copy_to(
            "observer-upload",
            ROOT / "scripts/observe_paid_pipeline.py",
            API_HOST,
            f"/root/unattended-{RUN}-observer.py",
        )
        observer_shell = (
            f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
            f'docker cp /root/unattended-{RUN}-observer.py "$api":/tmp/paid-observer.py; '
            'docker exec "$api" sh -lc \''
            'TEST_DATABASE_URL="$DATABASE_URL" nohup python /tmp/paid-observer.py '
            "--manifest /tmp/private-load-manifest.json "
            f"--output /tmp/paid-observer.jsonl --seconds {args.paid_seconds + 120} "
            "> /tmp/paid-observer.log 2>&1 < /dev/null & echo $! > /tmp/paid-observer.pid'"
        )
        backend_exec("observer-start", observer_shell, 30)
        observer_started = True

    with tempfile.TemporaryDirectory(prefix="checkout-private-") as temp:
        manifest = Path(temp) / "manifest.json"
        transport.copy_from(
            "manifest-download", API_HOST, f"{BACKEND}/tmp/unattended-{RUN}/private/manifest.json", manifest
        )
        manifest.chmod(0o600)
        transport.copy_to("manifest-upload", manifest, GEN_HOST, f"/root/unattended-{RUN}-upload.json")
    transport.copy_to(
        "probe-upload",
        ROOT / "scripts/checkout_journey_probe.py",
        GEN_HOST,
        f"/root/unattended-{RUN}-probe.py",
    )
    if args.paid_rate:
        script_dir = f"/root/unattended-{RUN}-scripts"
        step("generator-script-directory", GEN_HOST, ["mkdir", "-m", "700", script_dir], 30)
        transport.copy_to(
            "journey-library-upload",
            ROOT / "scripts/checkout_journey_probe.py",
            GEN_HOST,
            f"{script_dir}/checkout_journey_probe.py",
        )
        transport.copy_to(
            "paid-generator-upload",
            ROOT / "scripts/paid_ticket_load_generator.py",
            GEN_HOST,
            f"{script_dir}/paid_ticket_load_generator.py",
        )
        generator_args = [
            "/root/http-load-venv/bin/python",
            f"{script_dir}/paid_ticket_load_generator.py",
            "--manifest",
            f"/root/unattended-{RUN}-upload.json",
            "--origin",
            ORIGIN,
            "--output",
            f"/root/unattended-{RUN}-probe-result.json",
            "--rate",
            str(args.paid_rate),
            "--seconds",
            str(args.paid_seconds),
            "--completion-deadline-seconds",
            str(args.paid_seconds + 120),
            "--concurrency",
            str(args.paid_concurrency),
            "--poll-seconds",
            str(args.paid_poll_seconds),
            "--duplicates",
            "3",
        ]
        probe_timeout = args.paid_seconds + 180
    else:
        generator_args = [
            "/root/http-load-venv/bin/python",
            f"/root/unattended-{RUN}-probe.py",
            "--manifest",
            f"/root/unattended-{RUN}-upload.json",
            "--origin",
            ORIGIN,
            "--output",
            f"/root/unattended-{RUN}-probe-result.json",
            "--journeys",
            "10",
            "--concurrency",
            "5",
            "--duplicates",
            "3",
        ]
        probe_timeout = 180
    probe_attempted = True
    step("probe", GEN_HOST, generator_args, probe_timeout)
    transport.copy_from(
        "probe-result", GEN_HOST, f"/root/unattended-{RUN}-probe-result.json", OUT / "probe.json"
    )

    transport.copy_to(
        "audit-upload", ROOT / "scripts/audit_checkout_smoke.py", API_HOST, f"/root/unattended-{RUN}-audit.py"
    )
    audit_shell = (
        f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
        f'test -n "$api"; docker cp /root/unattended-{RUN}-audit.py '
        f'"$api":/tmp/checkout-audit.py; '
        "for n in $(seq 1 30); do "
        f'if docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
        "python /tmp/checkout-audit.py --manifest /tmp/private-load-manifest.json "
        f"--expected {EXPECTED} --output /tmp/checkout-audit-'$n'.json' "
        f'>/dev/null; then docker cp "$api":/tmp/checkout-audit-$n.json '
        f"{BACKEND}/tmp/unattended-{RUN}/public/checkout-audit.json; exit 0; fi; "
        "sleep 2; done; exit 1"
    )
    backend_exec("audit", audit_shell, 120)
    transport.copy_from(
        "audit-result",
        API_HOST,
        f"{BACKEND}/tmp/unattended-{RUN}/public/checkout-audit.json",
        OUT / "audit.json",
    )

    queue_shell = (
        f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
        f"public={BACKEND}/tmp/unattended-{RUN}/public; "
        'test -n "$api" || exit 1; started=$(date +%s); n=0; '
        'while [ "$n" -le 60 ]; do '
        "n=$((n+1)); status=0; "
        'docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
        'TEST_REDIS_URL="$REDIS_URL" python /app/scripts/capacity_queue_state.py '
        "--manifest /tmp/private-load-manifest.json "
        "--output /tmp/checkout-queue-'\"$n\"'.json' >/dev/null || status=$?; "
        'docker cp "$api":/tmp/checkout-queue-"$n".json "$public/checkout-queue.json" || exit 1; '
        'if [ "$n" -eq 1 ]; then cp "$public/checkout-queue.json" '
        '"$public/checkout-queue-initial.json"; fi; '
        'if [ "$status" -eq 0 ]; then '
        "elapsed=$(($(date +%s)-started)); "
        'printf \'{"samples":%s,"drain_seconds":%s,"pass":true}\\n\' '
        '"$n" "$elapsed" > "$public/checkout-queue-drain.json"; '
        "exit 0; fi; "
        '[ "$n" -le 60 ] || break; sleep 2; '
        "done; exit 1"
    )
    queue_response = backend_exec("queue", queue_shell, 160, check=False)
    for label, filename in (
        ("queue-initial", "checkout-queue-initial.json"),
        ("queue-result", "checkout-queue.json"),
    ):
        transport.copy_from(
            label,
            API_HOST,
            f"{BACKEND}/tmp/unattended-{RUN}/public/{filename}",
            OUT / (filename.removeprefix("checkout-")),
        )
    if queue_response.returncode:
        raise RuntimeError("Queue did not drain within 120 seconds")
    transport.copy_from(
        "queue-drain",
        API_HOST,
        f"{BACKEND}/tmp/unattended-{RUN}/public/checkout-queue-drain.json",
        OUT / "queue-drain.json",
    )
    state["pass"] = all(
        json.loads((OUT / name).read_text())["pass"] for name in ("probe.json", "audit.json", "queue.json")
    )
except Exception as exc:  # noqa: BLE001 - always attempt teardown
    state["error"] = str(exc)
finally:
    if observer_started:
        try:
            observer_stop_shell = (
                f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
                'docker exec "$api" sh -lc \''
                "if test -f /tmp/paid-observer.pid; then "
                'kill "$(cat /tmp/paid-observer.pid)" 2>/dev/null || true; fi\'; '
                "sleep 1; "
                f'docker cp "$api":/tmp/paid-observer.jsonl '
                f"{BACKEND}/tmp/unattended-{RUN}/public/paid-pipeline.jsonl"
            )
            backend_exec("observer-stop", observer_stop_shell, 45)
            transport.copy_from(
                "observer-trace",
                API_HOST,
                f"{BACKEND}/tmp/unattended-{RUN}/public/paid-pipeline.jsonl",
                OUT / "paid-pipeline.jsonl",
            )
            summary = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/summarize_paid_pipeline.py"),
                    str(OUT / "paid-pipeline.jsonl"),
                    "--output",
                    str(OUT / "paid-pipeline-summary.json"),
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            state["observer_pass"] = summary.returncode == 0
            if state["observer_pass"]:
                observer_summary = json.loads(
                    (OUT / "paid-pipeline-summary.json").read_text(encoding="utf-8")
                )
                state["observer_pass"] = (
                    observer_summary["api"]["observed_replicas"] == 4
                    and observer_summary["consumer"]["max_replicas"] == args.consumer_candidate
                    and observer_summary["api_metrics_errors"] == 0
                    and observer_summary["database_errors"] == 0
                )
        except Exception:  # noqa: BLE001 - rollback must proceed
            state["observer_pass"] = False
        if not state["observer_pass"]:
            state["pass"] = False
            state["error"] = (state["error"] or "") + "; paid pipeline observer failed"
    if probe_attempted and not (OUT / "probe.json").exists():
        try:
            transport.copy_from(
                "probe-result-after-failure",
                GEN_HOST,
                f"/root/unattended-{RUN}-probe-result.json",
                OUT / "probe.json",
            )
        except Exception as exc:  # noqa: BLE001 - retain original stage failure
            state["probe_result_collection_error"] = type(exc).__name__
    if prepared:
        try:
            transport.copy_to(
                "retire-upload",
                ROOT / "scripts/retire_checkout_smoke_fixture.py",
                API_HOST,
                f"/root/unattended-{RUN}-retire.py",
            )
            retire_shell = (
                f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
                f'docker cp /root/unattended-{RUN}-retire.py "$api":/tmp/checkout-retire.py; '
                'docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
                "python /tmp/checkout-retire.py --manifest /tmp/private-load-manifest.json'"
            )
            result = backend_exec("retire", retire_shell, 90)
            state["retirement"] = json.loads(result.stdout)
        except Exception as exc:  # noqa: BLE001 - always attempt teardown
            state["error"] = (state["error"] or "") + "; retirement: " + str(exc)
            state["pass"] = False
    if deployed:
        try:
            step("rollback", API_HOST, rollback_prefix + ["rollback", RUN], 300)
            transport.copy_from(
                "rollback-result",
                API_HOST,
                f"{BACKEND}/tmp/unattended-{RUN}/public/rollback.json",
                OUT / "rollback.json",
            )
            state["pass"] = state["pass"] and json.loads((OUT / "rollback.json").read_text())["pass"]
            restored_waiters = backend_exec("api-pool-waiters-restored", waiter_check, 30).stdout.splitlines()
            state["api_pool_waiters_restored"] = restored_waiters == ["3"] * 4
            state["pass"] = state["pass"] and state["api_pool_waiters_restored"]
        except Exception as exc:  # noqa: BLE001 - always attempt teardown
            state["error"] = (state["error"] or "") + "; rollback: " + str(exc)
            state["pass"] = False
    if args.paid_rate and state["error"] and (OUT / "probe.json").exists():
        accepted = json.loads((OUT / "probe.json").read_text()).get("dispatched", 0)
        if accepted:
            try:
                audit_result = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "scripts/audit_checkout_fixture_after_failure.py"),
                        "--backend-host",
                        API_HOST,
                        "--backend-dir",
                        BACKEND,
                        "--identity-file",
                        str(KEY),
                        "--run-id",
                        RUN,
                        "--expected",
                        str(accepted),
                        "--output",
                        str(OUT / "post-failure-audit"),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=180,
                )
                state["post_failure_audit_pass"] = audit_result.returncode == 0
            except subprocess.TimeoutExpired:
                state["post_failure_audit_pass"] = False
    if simulator_changed:
        try:
            simulator_restore = (
                f"cd {BACKEND}; SIMULATOR_CONCURRENCY=4 {COMPOSE} "
                "up -d --no-deps --force-recreate simulator; "
                f"id=$({COMPOSE} ps -q simulator); "
                'test -n "$id"; '
                "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
                "| grep -qx 'SIMULATOR_CONCURRENCY=4'"
            )
            step("simulator-restore", API_HOST, ["sh", "-lc", simulator_restore], 120)
            state["simulator_restored"] = True
        except Exception as exc:  # noqa: BLE001 - continue scratch cleanup
            state["simulator_restored"] = False
            state["error"] = (state["error"] or "") + "; simulator restore: " + str(exc)
            state["pass"] = False
    transport.remote(
        "container-scratch-cleanup",
        API_HOST,
        [
            "sh",
            "-lc",
            (
                f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
                'docker exec -u 0 "$api" sh -lc '
                "'rm -f /tmp/checkout-audit.py /tmp/checkout-audit-*.json "
                "/tmp/checkout-queue-*.json /tmp/checkout-retire.py'"
            ),
        ],
        check=False,
        timeout=30,
    )
    transport.remote("backend-cleanup", API_HOST, prefix + ["cleanup", RUN], check=False, timeout=60)
    transport.remote(
        "generator-cleanup",
        GEN_HOST,
        [
            "rm",
            "-f",
            f"/root/unattended-{RUN}-upload.json",
            f"/root/unattended-{RUN}-probe.py",
            f"/root/unattended-{RUN}-probe-result.json",
        ],
        check=False,
        timeout=30,
    )
    if args.paid_rate:
        transport.remote(
            "generator-script-cleanup",
            GEN_HOST,
            [
                "sh",
                "-lc",
                (
                    f"rm -f /root/unattended-{RUN}-scripts/checkout_journey_probe.py "
                    f"/root/unattended-{RUN}-scripts/paid_ticket_load_generator.py; "
                    f"rmdir /root/unattended-{RUN}-scripts"
                ),
            ],
            check=False,
            timeout=30,
        )
    transport.remote(
        "observer-scratch-cleanup",
        API_HOST,
        [
            "sh",
            "-lc",
            (
                f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
                'docker exec -u 0 "$api" rm -f /tmp/paid-observer.py '
                "/tmp/paid-observer.jsonl /tmp/paid-observer.log /tmp/paid-observer.pid; "
                f"rm -f /root/unattended-{RUN}-observer.py"
            ),
        ],
        check=False,
        timeout=30,
    )
    transport.remote(
        "backend-scratch-cleanup",
        API_HOST,
        ["rm", "-f", f"/root/unattended-{RUN}-audit.py", f"/root/unattended-{RUN}-retire.py"],
        check=False,
        timeout=30,
    )
    state["pass"] = state["pass"] and state["error"] is None
    (OUT / "state.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "run": RUN,
                "pass": state["pass"],
                "phases": state["phases"],
                "error": state["error"],
                "output": str(OUT),
            }
        )
    )
    if not state["pass"]:
        raise SystemExit(1)
