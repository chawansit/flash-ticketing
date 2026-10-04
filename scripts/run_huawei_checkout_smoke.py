"""One bounded Huawei paid-ticket smoke with rollback and private-manifest cleanup."""

import argparse
import ipaddress
import json
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from kafka_lag_observe import source_sha256
from observe_paid_pipeline import kafka_startup_view, paid_observer_seconds, pipeline_startup_view
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
parser.add_argument("--paid-http-max-connections", type=int, default=0)
parser.add_argument("--paid-http-client-count", type=int, default=1)
parser.add_argument("--paid-generator-shards", type=int, choices=(1, 2), default=1)
parser.add_argument("--paid-poll-seconds", type=float, default=0.2)
parser.add_argument("--paid-lifecycle-diagnostics", action="store_true")
parser.add_argument("--simulator-dispatch-mode-candidate", choices=("batch", "refill"), default="batch")
parser.add_argument("--simulator-concurrency-candidate", type=int, choices=(4, 8, 12), default=4)
parser.add_argument("--consumer-candidate", type=int, choices=(1, 2, 4, 6), default=1)
parser.add_argument("--consumer-pool-per-instance", type=int, choices=(8, 12), default=12)
parser.add_argument("--api-pool-per-instance-candidate", type=int, choices=(3, 4), default=3)
parser.add_argument("--api-payment-pool-max-candidate", type=int, choices=(0, 2), default=0)
parser.add_argument("--api-pool-waiters-candidate", type=int, choices=(3, 12), default=3)
parser.add_argument("--order-status-cache-ms-candidate", type=int, choices=(0, 1000, 3000), default=0)
parser.add_argument("--callback-duplicates", type=int, choices=(1, 3), default=3)
parser.add_argument("--shows", type=int, default=2)
parser.add_argument("--viewers", type=int, default=20)
args = parser.parse_args()
if args.paid_lifecycle_diagnostics and not args.paid_rate:
    parser.error("Lifecycle diagnostics require a paid control")
if (args.consumer_candidate == 6) != (args.consumer_pool_per_instance == 8):
    parser.error("Six consumers require pool8; other approved layouts use pool12")
EXPECTED = args.paid_rate * args.paid_seconds if args.paid_rate else 10
if not 0 <= args.paid_rate <= 100 or not 1 <= args.paid_seconds <= 300:
    parser.error("Bounded paid-stage rate/duration required")
OBSERVER_SECONDS = paid_observer_seconds(args.paid_seconds)
if not 1 <= args.shows <= 1000 or not 1 <= args.viewers <= 50000:
    parser.error("Invalid fixture size")
if not 1 <= args.paid_concurrency <= 1000 or not 0.05 <= args.paid_poll_seconds <= 2:
    parser.error("Invalid paid-stage concurrency")
if args.paid_http_max_connections and not (
    args.paid_concurrency <= args.paid_http_max_connections <= 4000
):
    parser.error("HTTP connection limit must fit bounded journey concurrency")
if args.paid_generator_shards == 2 and (
    args.paid_rate % 2 or args.paid_concurrency % 2 or args.shows < 2 or args.viewers < 2
    or args.paid_http_max_connections
):
    parser.error("Two shards require even rate/concurrency and default per-shard HTTP pools")
pool_budget = (args.paid_concurrency // 2 if args.paid_generator_shards == 2
               else args.paid_http_max_connections or args.paid_concurrency)
if not 1 <= args.paid_http_client_count <= min(16, pool_budget):
    parser.error("Client partition must fit per-process HTTP connection budget")
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
    f"API_POOL_PER_INSTANCE={args.api_pool_per_instance_candidate}",
    f"API_POOL_MAX_WAITING={args.api_pool_waiters_candidate}",
    f"API_PAYMENT_POOL_MAX={args.api_payment_pool_max_candidate}",
    f"ORDER_STATUS_CACHE_MS={args.order_status_cache_ms_candidate}",
    f"CONSUMER_POOL_PER_INSTANCE={args.consumer_pool_per_instance}",
    "sh",
    f"{BACKEND}/scripts/huawei_capacity_backend.sh",
]
rollback_prefix = [
    "env",
    f"FLASH_TICKETING_BACKEND_DIR={BACKEND}",
    "API_POOL_PER_INSTANCE=3",
    "API_POOL_MAX_WAITING=3",
    "API_PAYMENT_POOL_MAX=0",
    "ORDER_STATUS_CACHE_MS=0",
    "CONSUMER_POOL_PER_INSTANCE=12",
    "sh",
    f"{BACKEND}/scripts/huawei_capacity_backend.sh",
]
waiter_check = (
    f'cd {BACKEND}; for id in $({COMPOSE} ps -q api); do docker exec "$id" printenv DB_POOL_MAX_WAITING; done'
)
cache_setting_check = (
    f'cd {BACKEND}; for id in $({COMPOSE} ps -q api); do '
    'value=$(docker exec "$id" printenv ORDER_STATUS_CACHE_MS || true); '
    'printf "%s\n" "${value:-0}"; done'
)
payment_setting_check = (
    f'cd {BACKEND}; for id in $({COMPOSE} ps -q api); do '
    'value=$(docker exec "$id" printenv API_PAYMENT_POOL_MAX || true); '
    'printf "%s\n" "' + '$' + '{value:-0}"; done'
)
partition_probe = """
import json, urllib.request
from ticketing.config import Settings
from ticketing.infrastructure.postgres import api_pool_budgets
s=Settings()
b=api_pool_budgets(s.pool_max,s.pool_max_waiting,s.api_payment_pool_max)
raw=urllib.request.urlopen('http://127.0.0.1:8000/metrics',timeout=10).read().decode()
states={}
prefix='ticketing_db_pool_state{state="'
for line in raw.splitlines():
 if line.startswith(prefix):
  key=line.split('"',2)[1]
  states[key]=float(line.rsplit(' ',1)[1])
expected={'pool_max':s.pool_max,'general_pool_max':b['general']['maximum'],
          'payment_pool_max':b['payment']['maximum'] if b['payment'] else 0,
          'pool_max_waiting':s.pool_max if s.pool_max_waiting is None else s.pool_max_waiting,
          'general_max_waiting':b['general']['maximum_waiting'],
          'payment_max_waiting':b['payment']['maximum_waiting'] if b['payment'] else 0}
result={'payment_maximum':s.api_payment_pool_max,'budgets':b,'metrics':{k:states.get(k) for k in expected},
        'pass':all(states.get(k)==v for k,v in expected.items())}
print(json.dumps(result))
raise SystemExit(0 if result['pass'] else 1)
"""
partition_check = (
    f'cd {BACKEND}; for id in $({COMPOSE} ps -q api); do '
    f'docker exec "$id" python -c {shlex.quote(partition_probe)} || exit 1; done'
)
state = {"run": RUN, "phases": [], "pass": False, "error": None,
         "order_status_cache_ms_candidate": args.order_status_cache_ms_candidate,
         "consumer_pool_per_instance_candidate": args.consumer_pool_per_instance,
         "api_pool_per_instance_candidate": args.api_pool_per_instance_candidate,
         "api_payment_pool_max_candidate": args.api_payment_pool_max_candidate,
         "paid_configuration": {"rate": args.paid_rate, "seconds": args.paid_seconds,
                                "concurrency": args.paid_concurrency,
                                "generator_shards": args.paid_generator_shards,
                                "http_clients_per_shard": args.paid_http_client_count,
                                "http_connections_per_shard": pool_budget,
                                "poll_seconds": args.paid_poll_seconds,
                                "callback_duplicates": args.callback_duplicates,
                                "simulator_dispatch_mode": args.simulator_dispatch_mode_candidate,
                                "lifecycle_diagnostics": args.paid_lifecycle_diagnostics,
                                "kafka_observer_backend": "python"}}
deployed = prepared = probe_attempted = observer_started = kafka_observer_started = simulator_changed = False


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
    original_cache = step("order-cache-original", API_HOST, ["sh", "-lc", cache_setting_check], 30)
    if original_cache.stdout.splitlines() != ["0"] * 4:
        raise RuntimeError("Expected disabled order cache on all baseline APIs")
    original_payment = step("payment-pool-original", API_HOST, ["sh", "-lc", payment_setting_check], 30)
    if original_payment.stdout.splitlines() != ["0"] * 4:
        raise RuntimeError("Expected shared baseline API pools")
    original_budget = step("consumer-budget-original", API_HOST,
        ["sh", "-lc", f"cd {BACKEND} && python3 scripts/verify_consumer_pool_budget.py --consumers 1 --pool-max 12 --api-pool-max 3"], 30)
    state["consumer_budget_original"] = json.loads(original_budget.stdout)
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
    candidate_budget = step("consumer-budget-candidate", API_HOST,
        ["sh", "-lc", f"cd {BACKEND} && python3 scripts/verify_consumer_pool_budget.py --consumers {args.consumer_candidate} --pool-max {args.consumer_pool_per_instance} --api-pool-max {args.api_pool_per_instance_candidate}"], 30)
    state["consumer_budget_candidate"] = json.loads(candidate_budget.stdout)
    waiting_values = step(
        "api-pool-waiters-candidate", API_HOST, ["sh", "-lc", waiter_check], 30
    ).stdout.splitlines()
    if waiting_values != [str(args.api_pool_waiters_candidate)] * 4:
        raise RuntimeError("Candidate API waiter cap not active on all four replicas")
    candidate_cache = step("order-cache-candidate", API_HOST, ["sh", "-lc", cache_setting_check], 30)
    if candidate_cache.stdout.splitlines() != [str(args.order_status_cache_ms_candidate)] * 4:
        raise RuntimeError("Candidate order cache setting not active on all APIs")
    candidate_payment = step("payment-pool-candidate", API_HOST, ["sh", "-lc", payment_setting_check], 30)
    if candidate_payment.stdout.splitlines() != [str(args.api_payment_pool_max_candidate)] * 4:
        raise RuntimeError("Candidate payment allocation not active on all APIs")
    if args.api_payment_pool_max_candidate:
        partition = step("api-pool-partition-candidate", API_HOST, ["sh", "-lc", partition_check], 45)
        state["api_pool_partition_candidate"] = [json.loads(line) for line in partition.stdout.splitlines()]
        if len(state["api_pool_partition_candidate"]) != 4 or not all(
            row["pass"] for row in state["api_pool_partition_candidate"]
        ):
            raise RuntimeError("Actual API pool partition does not match fixed budgets")
    if args.simulator_concurrency_candidate != 4 or args.simulator_dispatch_mode_candidate != "batch":
        simulator_config = (
            f'cd {BACKEND}; id=$({COMPOSE} ps -q simulator); test -n "$id"; '
            "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
            "| grep -E '^(SIMULATOR_CONCURRENCY|SIMULATOR_DISPATCH_MODE|DB_POOL_MAX)='"
        )
        original_simulator = step("simulator-original", API_HOST, ["sh", "-lc", simulator_config], 30).stdout
        if not {"SIMULATOR_CONCURRENCY=4", "SIMULATOR_DISPATCH_MODE=batch", "DB_POOL_MAX=12"}.issubset(original_simulator.splitlines()):
            raise RuntimeError("Unexpected simulator baseline configuration")
        simulator_changed = True
        simulator_apply = (
            f"cd {BACKEND}; SIMULATOR_CONCURRENCY={args.simulator_concurrency_candidate} "
            f"SIMULATOR_DISPATCH_MODE={args.simulator_dispatch_mode_candidate} "
            f"{COMPOSE} up -d --no-deps --force-recreate simulator; "
            f"id=$({COMPOSE} ps -q simulator); "
            'test -n "$id"; '
            "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
            f"| grep -qx 'SIMULATOR_CONCURRENCY={args.simulator_concurrency_candidate}' && "
            f'docker exec "$id" printenv SIMULATOR_DISPATCH_MODE | grep -qx "{args.simulator_dispatch_mode_candidate}"'
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
            f"--output /tmp/paid-observer.jsonl --seconds {OBSERVER_SECONDS} "
            "> /tmp/paid-observer.log 2>&1 < /dev/null & echo $! > /tmp/paid-observer.pid'"
        )
        backend_exec("observer-start", observer_shell, 30)
        observer_started = True
        observer_ready_shell = (
            f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
            'docker exec "$api" sh -lc \''
            'n=0; while [ "$n" -lt 20 ]; do '
            'kill -0 "$(cat /tmp/paid-observer.pid)" 2>/dev/null || exit 1; '
            'if test -s /tmp/paid-observer.jsonl; then head -n 1 /tmp/paid-observer.jsonl; exit 0; fi; '
            'n=$((n+1)); sleep 1; done; exit 1\''
        )
        first_sample = step("observer-ready", API_HOST, ["sh", "-lc", observer_ready_shell], 30)
        state["pipeline_observer_startup"] = pipeline_startup_view(json.loads(first_sample.stdout), args.consumer_candidate)
        if not state["pipeline_observer_startup"]["pass"]:
            raise RuntimeError("Pipeline observer first sample failed pre-dispatch gate")
        expected_observer_sha = source_sha256((ROOT / "scripts/kafka_lag_observe.py").read_bytes())
        actual_observer_sha = backend_exec(
            "kafka-observer-source",
            f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
            'docker exec "$api" python -c '
            + shlex.quote("import hashlib;from pathlib import Path;print(hashlib.sha256("
                          "Path('/app/scripts/kafka_lag_observe.py').read_bytes()"
                          ".replace(bytes([13,10]),bytes([10]))).hexdigest())"),
            30,
        ).stdout.split()[0]
        if actual_observer_sha != expected_observer_sha:
            raise RuntimeError("Kafka observer image source differs from checkout")
        state["kafka_observer_source_sha256"] = actual_observer_sha
        kafka_shell = (
            f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
            'test -n "$api" || exit 1; '
            'docker exec "$api" sh -lc \''
            'nohup python /app/scripts/kafka_lag_observe.py --backend python '
            f"--seconds {OBSERVER_SECONDS} --interval 2 "
            "--output /tmp/paid-kafka-lag.ndjson "
            "> /tmp/paid-kafka-lag.log 2>&1 < /dev/null & "
            "echo $! > /tmp/paid-kafka-lag.pid\'"
        )
        backend_exec("kafka-observer-start", kafka_shell, 30)
        kafka_observer_started = True
        kafka_ready_shell = (
            f"cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
            'docker exec "$api" sh -lc \''
            'n=0; while [ "$n" -lt 20 ]; do '
            'kill -0 "$(cat /tmp/paid-kafka-lag.pid)" 2>/dev/null || exit 1; '
            'if test -s /tmp/paid-kafka-lag.ndjson; then '
            'head -n 1 /tmp/paid-kafka-lag.ndjson; exit 0; fi; '
            'n=$((n+1)); sleep 1; done; exit 1\''
        )
        first_kafka = step("kafka-observer-ready", API_HOST, ["sh", "-lc", kafka_ready_shell], 30)
        state["kafka_observer_startup"] = kafka_startup_view(json.loads(first_kafka.stdout), args.consumer_candidate)
        if not state["kafka_observer_startup"]["pass"]:
            raise RuntimeError("Kafka observer first sample failed pre-dispatch gate")

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
        if args.paid_generator_shards == 2:
            transport.copy_to(
                "sharded-generator-upload",
                ROOT / "scripts/paid_ticket_sharded_generator.py",
                GEN_HOST,
                f"{script_dir}/paid_ticket_sharded_generator.py",
            )
        generator_args = [
            "/root/http-load-venv/bin/python",
            f"{script_dir}/paid_ticket_sharded_generator.py"
            if args.paid_generator_shards == 2 else f"{script_dir}/paid_ticket_load_generator.py",
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
            "--http-client-count", str(args.paid_http_client_count),
            "--poll-seconds",
            str(args.paid_poll_seconds),
            "--duplicates",
            str(args.callback_duplicates),
        ]
        if args.paid_lifecycle_diagnostics:
            generator_args.append("--lifecycle-diagnostics")
        if args.paid_generator_shards == 1:
            generator_args.extend([
                "--http-max-connections", str(args.paid_http_max_connections or args.paid_concurrency)
            ])
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
            str(args.callback_duplicates),
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
        f"--expected {EXPECTED} --callback-duplicates {args.callback_duplicates} "
        "--output /tmp/checkout-audit-'$n'.json' "
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
                    and observer_summary["resources"]["pass"]
                    and observer_summary["role_database"]["writer"]["observed_replicas"] == 3
                    and all(not view["counter_reset_detected"]
                            for view in observer_summary["role_database"].values())
                    and not observer_summary["writer_phases"]["counter_reset_detected"]
                )
        except Exception:  # noqa: BLE001 - rollback must proceed
            state["observer_pass"] = False
        if not state["observer_pass"]:
            state["pass"] = False
            state["error"] = (state["error"] or "") + "; paid pipeline observer failed"
    if kafka_observer_started:
        try:
            kafka_stop_shell = (
                f"set -e; cd {BACKEND}; api=$({COMPOSE} ps -q api | head -n 1); "
                'docker exec "$api" sh -lc \''
                'pid=$(cat /tmp/paid-kafka-lag.pid); kill "$pid" 2>/dev/null || true; '
                'n=0; while kill -0 "$pid" 2>/dev/null && [ "$n" -lt 35 ] && test "$(cut -d" " -f3 /proc/$pid/stat)" != Z; do '
                'sleep 1; n=$((n+1)); done; '
                'if kill -0 "$pid" 2>/dev/null; then '
                'test "$(cut -d" " -f3 /proc/$pid/stat)" = Z || exit 1; fi; '
                'test -s /tmp/paid-kafka-lag.ndjson\'; '
                f'docker cp "$api":/tmp/paid-kafka-lag.ndjson '
                f"{BACKEND}/tmp/unattended-{RUN}/raw/paid-kafka-lag.ndjson; "
                'docker exec "$api" rm -f /tmp/paid-kafka-lag.pid /tmp/paid-kafka-lag.log '
                '/tmp/paid-kafka-lag.ndjson'
            )
            backend_exec("kafka-observer-stop", kafka_stop_shell, 50)
            transport.copy_from(
                "kafka-lag-trace", API_HOST,
                f"{BACKEND}/tmp/unattended-{RUN}/raw/paid-kafka-lag.ndjson",
                OUT / "paid-kafka-lag.ndjson",
            )
            summary = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/summarize_paid_kafka_lag.py"),
                    str(OUT / "paid-kafka-lag.ndjson"),
                    "--output",
                    str(OUT / "paid-kafka-lag-summary.json"),
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            state["kafka_observer_pass"] = summary.returncode == 0
            if state["kafka_observer_pass"]:
                lag = json.loads((OUT / "paid-kafka-lag-summary.json").read_text(encoding="utf-8"))
                state["kafka_observer_pass"] = (
                    lag["samples"] >= 5
                    and lag["sample_errors"] == 0
                    and lag["members_max"] == args.consumer_candidate
                    and (args.consumer_candidate != 6 or (
                        lag["ownership_observed"] and lag["members_min"] == 6
                        and lag["max_partitions_per_member"] == 1
                        and lag["last_member_partition_groups"] == [[i] for i in range(6)]
                    ))
                )
        except Exception:  # noqa: BLE001 - rollback must proceed
            state["kafka_observer_pass"] = False
        if not state["kafka_observer_pass"]:
            state["pass"] = False
            state["error"] = (state["error"] or "") + "; Kafka lag observer failed"
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
            restored_cache = backend_exec("order-cache-restored", cache_setting_check, 30).stdout.splitlines()
            state["order_status_cache_restored"] = restored_cache == ["0"] * 4
            state["pass"] = state["pass"] and state["order_status_cache_restored"]
            restored_payment = backend_exec("payment-pool-restored", payment_setting_check, 30).stdout.splitlines()
            state["api_payment_pool_restored"] = restored_payment == ["0"] * 4
            state["pass"] = state["pass"] and state["api_payment_pool_restored"]
            if args.api_payment_pool_max_candidate:
                restored_partition = backend_exec("api-pool-partition-restored", partition_check, 45)
                state["api_pool_partition_restored"] = [json.loads(line) for line in restored_partition.stdout.splitlines()]
                state["pass"] = state["pass"] and len(state["api_pool_partition_restored"]) == 4 and all(
                    row["pass"] and row["payment_maximum"] == 0 for row in state["api_pool_partition_restored"]
                )
        except Exception as exc:  # noqa: BLE001 - always attempt teardown
            state["error"] = (state["error"] or "") + "; rollback: " + str(exc)
            state["pass"] = False
    if deployed:
        try:
            restored_budget = backend_exec("consumer-budget-restored",
                f"cd {BACKEND} && python3 scripts/verify_consumer_pool_budget.py --consumers 1 --pool-max 12 --api-pool-max 3", 30)
            state["consumer_budget_restored"] = json.loads(restored_budget.stdout)
        except Exception as exc:  # noqa: BLE001 - preserve remaining cleanup after verification failure
            state["error"] = (state["error"] or "") + "; consumer budget rollback: " + str(exc)
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
                        "--callback-duplicates",
                        str(args.callback_duplicates),
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
                f"cd {BACKEND}; SIMULATOR_CONCURRENCY=4 SIMULATOR_DISPATCH_MODE=batch {COMPOSE} "
                "up -d --no-deps --force-recreate simulator; "
                f"id=$({COMPOSE} ps -q simulator); "
                'test -n "$id"; '
                "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \"$id\" "
                "| grep -qx 'SIMULATOR_CONCURRENCY=4' && "
                'docker exec "$id" printenv SIMULATOR_DISPATCH_MODE | grep -qx batch'
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
                    f"/root/unattended-{RUN}-scripts/paid_ticket_load_generator.py "
                    f"/root/unattended-{RUN}-scripts/paid_ticket_sharded_generator.py; "
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
