"""One bounded Huawei paid-ticket smoke with rollback and private-manifest cleanup."""

import argparse
import ipaddress
import json
import re
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
args = parser.parse_args()
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
    "sh",
    f"{BACKEND}/scripts/huawei_capacity_backend.sh",
]
state = {"run": RUN, "phases": [], "pass": False, "error": None}
deployed = prepared = False


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
            "1",
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
    step("prepare", API_HOST, prefix + ["prepare", RUN, "2", "300", "1", ORIGIN, "20"], 300)
    prepared = True
    step("warm", API_HOST, prefix + ["warm", RUN], 180)
    step("preflight", API_HOST, prefix + ["preflight", RUN, "90"], 120)
    transport.copy_from(
        "queue-before",
        API_HOST,
        f"{BACKEND}/tmp/unattended-{RUN}/public/queue-before.json",
        OUT / "queue-before.json",
    )
    if not json.loads((OUT / "queue-before.json").read_text())["pass"]:
        raise RuntimeError("Preflight queues are not drained")

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
    step(
        "probe",
        GEN_HOST,
        [
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
        ],
        180,
    )
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
        f"--expected 10 --output /tmp/checkout-audit-'$n'.json' "
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
        'docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
        'TEST_REDIS_URL="$REDIS_URL" python /app/scripts/capacity_queue_state.py '
        "--manifest /tmp/private-load-manifest.json --output /tmp/checkout-queue.json' "
        f'&& docker cp "$api":/tmp/checkout-queue.json {BACKEND}/tmp/unattended-{RUN}/public/checkout-queue.json'
    )
    backend_exec("queue", queue_shell, 90)
    transport.copy_from(
        "queue-result",
        API_HOST,
        f"{BACKEND}/tmp/unattended-{RUN}/public/checkout-queue.json",
        OUT / "queue.json",
    )
    state["pass"] = all(
        json.loads((OUT / name).read_text())["pass"] for name in ("probe.json", "audit.json", "queue.json")
    )
except Exception as exc:  # noqa: BLE001 - always attempt teardown
    state["error"] = str(exc)
finally:
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
            step("rollback", API_HOST, prefix + ["rollback", RUN], 300)
            transport.copy_from(
                "rollback-result",
                API_HOST,
                f"{BACKEND}/tmp/unattended-{RUN}/public/rollback.json",
                OUT / "rollback.json",
            )
            state["pass"] = state["pass"] and json.loads((OUT / "rollback.json").read_text())["pass"]
        except Exception as exc:  # noqa: BLE001 - always attempt teardown
            state["error"] = (state["error"] or "") + "; rollback: " + str(exc)
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
                "/tmp/checkout-queue.json /tmp/checkout-retire.py'"
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
