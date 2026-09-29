"""Read-only audit of accepted journeys after a failed paid-ticket stage."""

import argparse
import json
import re
from pathlib import Path

from unattended_capacity_stage import Transport

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--backend-host", required=True)
p.add_argument("--backend-dir", required=True)
p.add_argument("--identity-file", required=True, type=Path)
p.add_argument("--run-id", required=True)
p.add_argument("--expected", required=True, type=int)
p.add_argument("--expected-paid", type=int)
p.add_argument("--callback-duplicates", type=int, default=3)
p.add_argument("--output", required=True, type=Path)
a = p.parse_args()
if not re.fullmatch(r"checkout-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{6}", a.run_id):
    p.error("Invalid checkout run ID")
if (
    not 1 <= a.expected <= 300000
    or a.expected_paid is not None
    and not 0 <= a.expected_paid <= a.expected
    or not 1 <= a.callback_duplicates <= 10
    or a.output.exists()
):
    p.error("Expected count or fresh output path required")
if not re.fullmatch(r"[A-Za-z0-9_./@-]+", a.backend_host + a.backend_dir):
    p.error("Invalid host/directory")
a.output.mkdir(parents=True)
logs = a.output / "logs"
logs.mkdir()
t = Transport("ssh", "scp", logs, a.identity_file)
audit_file = f"/root/{a.run_id}-failed-audit.py"
fixture = f"{a.backend_dir}/tmp/unattended-{a.run_id}/public/fixture.json"
compose = (
    "docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml "
    "-f compose.horizontal.yaml -f compose.keepalive10.yaml"
)
try:
    t.copy_to("audit-upload", Path(__file__).with_name("audit_checkout_smoke.py"), a.backend_host, audit_file)
    paid_option = f"--expected-paid {a.expected_paid}" if a.expected_paid is not None else ""
    shell = (
        f"cd {a.backend_dir}; api=$({compose} ps -q api | head -n 1); "
        f'test -n "$api" && docker cp {audit_file} "$api":/tmp/failed-audit.py '
        f'&& docker cp {fixture} "$api":/tmp/failed-fixture.json '
        f'&& docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
        f"python /tmp/failed-audit.py --manifest /tmp/failed-fixture.json "
        f"--expected {a.expected} {paid_option} --callback-duplicates {a.callback_duplicates} "
        "--output /tmp/failed-audit.json' "
        f'&& docker exec "$api" sh -lc \'TEST_DATABASE_URL="$DATABASE_URL" '
        'TEST_REDIS_URL="$REDIS_URL" python /app/scripts/capacity_queue_state.py '
        "--manifest /tmp/failed-fixture.json --output /tmp/failed-queue.json' "
        f'&& docker cp "$api":/tmp/failed-audit.json {a.backend_dir}/tmp/unattended-{a.run_id}/public/failed-audit.json '
        f'&& docker cp "$api":/tmp/failed-queue.json {a.backend_dir}/tmp/unattended-{a.run_id}/public/failed-queue.json'
    )
    t.remote("audit-and-queue", a.backend_host, ["sh", "-lc", shell], timeout=120)
    t.copy_from(
        "audit-download",
        a.backend_host,
        f"{a.backend_dir}/tmp/unattended-{a.run_id}/public/failed-audit.json",
        a.output / "audit.json",
    )
    t.copy_from(
        "queue-download",
        a.backend_host,
        f"{a.backend_dir}/tmp/unattended-{a.run_id}/public/failed-queue.json",
        a.output / "queue.json",
    )
    print(
        json.dumps(
            {
                "audit": json.loads((a.output / "audit.json").read_text()),
                "queue": json.loads((a.output / "queue.json").read_text()),
            }
        )
    )
finally:
    cleanup = (
        f"cd {a.backend_dir}; api=$({compose} ps -q api | head -n 1); "
        'docker exec -u 0 "$api" rm -f /tmp/failed-audit.py /tmp/failed-fixture.json '
        f"/tmp/failed-audit.json /tmp/failed-queue.json; rm -f {audit_file}"
    )
    t.remote("scratch-cleanup", a.backend_host, ["sh", "-lc", cleanup], check=False, timeout=30)
