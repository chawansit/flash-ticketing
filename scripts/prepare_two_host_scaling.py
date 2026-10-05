"""Prepare ADR0147 configs and validate a private observed topology; never deploy or load."""

import argparse
import ipaddress
import json
import re
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-2026-10-05.json"
OVERHEAD = ROOT / "docs/capacity/flash-sale-opening/observation-overhead-2026-10-05.json"
REVISION = "deb330ec91e553640d1d0ba10e92aa8f29cd86dc"
API_SETTINGS = {
    "DB_POOL_MAX": "4",
    "DB_POOL_MAX_WAITING": "12",
    "DB_POOL_WAIT_MS": "500",
    "API_PAYMENT_POOL_MAX": "2",
    "API_POOL_SHARED_WAITING": "1",
    "API_CALLBACK_ACQUISITION_RESERVE": "0",
    "RESERVE_CONCURRENCY": "4",
    "ORDER_STATUS_CACHE_MS": "0",
    "SIMULATOR_CONCURRENCY": "2",
    "RESERVATION_MODE": "redis-first",
    "REDIS_RESERVATION_MAX_COMMAND_AGE_SECONDS": "60",
}
BACKGROUND = {
    "consumer": {"replicas": 6, "pool_per_replica": 8},
    "reservation-writer": {"replicas": 3, "batch_size": 4},
    "maintenance": {"replicas": 1},
    "publisher": {"replicas": 1},
    "reconciler": {"replicas": 1},
    "simulator": {"replicas": 1, "concurrency": 8, "dispatch_mode": "refill"},
}
EXTRA_GATES = [
    "distinct_api_and_generator_machines",
    "fixed_aggregate_budgets",
    "all_four_replicas_observed",
    "both_hosts_cpu_observed",
    "same_offered_measurement_window",
    "per_replica_traffic_distribution",
    "cross_host_one_seat_one_owner",
    "cross_host_idempotent_replay",
    "secondary_resources_removed",
    "primary_lb_and_four_apis_restored",
]


def private_ipv4(value):
    address = ipaddress.ip_address(value)
    if (
        address.version != 4
        or not address.is_private
        or address.is_loopback
        or address.is_unspecified
        or address.is_link_local
        or address.is_multicast
    ):
        raise ValueError("A routable private IPv4 address is required")
    # Restrict to RFC1918; ipaddress also marks documentation/reserved ranges private.
    if not any(address in ipaddress.ip_network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):
        raise ValueError("Backend address must be RFC1918")
    return str(address)


def digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid identity fingerprint")
    return value


def validate_inventory(data, *, image_id, now=None, contract=None):
    """Validate supplied observations, not their authenticity or live qualification."""
    settings = API_SETTINGS
    if contract is not None:
        from status_refresh_contract import StatusRefreshContract

        if not isinstance(contract, StatusRefreshContract) or image_id != contract.images["api"]:
            raise ValueError("Explicit isolated status refresh contract required")
        settings = contract.api_settings
        contract.verify_inventory(data)
    now = now or datetime.now(UTC)
    if data.get("schema") != 1 or data.get("arm") not in {"control", "candidate"}:
        raise ValueError("Unknown inventory schema or arm")
    captured = datetime.fromisoformat(data["captured_at"])
    if captured.tzinfo is None or not 0 <= (now - captured).total_seconds() <= 300:
        raise ValueError("Observation is stale or future dated")
    hosts = data["hosts"]
    if set(hosts) != {"primary", "secondary", "generator"}:
        raise ValueError("All three host roles are required")
    ids = [digest(h["machine_id_sha256"]) for h in hosts.values()]
    addresses = [private_ipv4(h["private_ipv4"]) for h in hosts.values()]
    if len(set(ids)) != 3 or len(set(addresses)) != 3:
        raise ValueError("API machines and generator must be independent")
    for role in ("primary", "secondary"):
        h = hosts[role]
        if (
            type(h["vcpus"]) is not int
            or h["vcpus"] != 4
            or type(h["memory_bytes"]) is not int
            or not 7 * 2**30 <= h["memory_bytes"] <= 9 * 2**30
        ):
            raise ValueError("Matching 4-vCPU/8-GiB API hosts required")
    memory = [hosts[r]["memory_bytes"] for r in ("primary", "secondary")]
    if abs(memory[0] - memory[1]) > min(memory) * 0.05:
        raise ValueError("API host memory must match within five percent")
    if hosts["generator"]["api_processes"] != 0:
        raise ValueError("Generator must remain dedicated")
    if hosts["secondary"]["non_api_services"]:
        raise ValueError("Secondary must not add a pooler, broker or background services")
    if data["background"] != (BACKGROUND if contract is None else contract.background):
        raise ValueError("Background placement/settings changed")
    if data["pgbouncer"] != {
        "host_role": "primary",
        "server_pool": 24,
        "reserve_pool": 0,
        "max_clients": 160,
    }:
        raise ValueError("One shared PgBouncer budget required")
    if data["generator_idle"] is not True or data["global_queues_zero"] is not True:
        raise ValueError("Idle generator and drained queues required before preparation")
    apis = data["apis"]
    if len(apis) != 4:
        raise ValueError("Exactly four API replicas required")
    counts = {"primary": 0, "secondary": 0}
    routes, containers, fingerprints = set(), set(), []
    for api in apis:
        role = api["host_role"]
        if role not in counts or api["private_ipv4"] != hosts[role]["private_ipv4"]:
            raise ValueError("Endpoint does not belong to its API host")
        address = private_ipv4(api["private_ipv4"])
        port = api["port"]
        if type(port) is not int or not 8101 <= port <= 8104:
            raise ValueError("Unexpected private API publication")
        route = (address, port)
        container = (role, api["container_id"])
        if not re.fullmatch(r"[0-9a-f]{64}", api["container_id"]):
            raise ValueError("Full container identity required")
        if route in routes or container in containers:
            raise ValueError("Duplicate endpoint/container")
        routes.add(route)
        containers.add(container)
        counts[role] += 1
        if (
            api["image_id"] != image_id
            or api["revision"] != REVISION
            or api["source_hashes_match"] is not True
        ):
            raise ValueError("Unpinned runtime image/source")
        if api["ready"] is not True or api["settings"] != settings:
            raise ValueError("Replica not ready or API budget changed")
        if api["pgbouncer_host_role"] != "primary" or api["pgbouncer_port"] != 5432:
            raise ValueError("Direct-to-RDS or duplicate-pooler route rejected")
        if role == "primary" and api["database_url_host"] != "pgbouncer":
            raise ValueError("Primary API must retain its baseline pooler route")
        if role == "secondary" and api["database_url_host"] != hosts["primary"]["private_ipv4"]:
            raise ValueError("Remote API must address primary PgBouncer privately")
        fingerprints.append(
            tuple(
                digest(api[k])
                for k in (
                    "other_settings_sha256",
                    "database_credentials_and_name_sha256",
                    "redis_authority_sha256",
                    "auth_secrets_sha256",
                )
            )
        )
    expected = ({"primary": 4, "secondary": 0} if data["arm"] == "control" else {"primary": 2, "secondary": 2}) if contract is None else {"primary": 2, "secondary": 2}
    reference = tuple(
        digest(data["baseline_authorities"][k])
        for k in (
            "other_settings_sha256",
            "database_credentials_and_name_sha256",
            "redis_authority_sha256",
            "auth_secrets_sha256",
        )
    )
    if counts != expected or any(f != reference for f in fingerprints):
        raise ValueError("Placement or shared settings/authorities differ")
    return {
        "arm": data["arm"],
        "api_counts": counts,
        "inventory_contract_pass": True,
        "live_qualification": "NOT_EXECUTED",
        "api_connections_total": 16,
        "payment_connections_included": 8,
        "pgbouncer_server_connections": 24,
    }


def nginx_config(apis):
    if len(apis) != 4 or any(type(a["port"]) is not int or not 8101 <= a["port"] <= 8104 for a in apis):
        raise ValueError("Four validated private endpoints required")
    servers = "\n".join(f"        server {private_ipv4(a['private_ipv4'])}:{a['port']};" for a in apis)
    # Identical policy in both arms; only the explicit four endpoints differ.
    return """events { worker_connections 4096; }
http {
    upstream ticketing_api {
        zone ticketing_api 64k;
        least_conn;
SERVERS
        keepalive 128;
        keepalive_timeout 5s;
    }
    server {
        listen 8000;
        access_log off;
        location = /lb-health { return 200 "ok\n"; add_header Content-Type text/plain; }
        location / {
            proxy_pass http://ticketing_api;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 1s;
            proxy_send_timeout 10s;
            proxy_read_timeout 10s;
            proxy_next_upstream off;
        }
    }
}
""".replace("SERVERS", servers)


def secondary_compose():
    # JSON is valid YAML; there is deliberately no build, migration or dependency service.
    return (
        json.dumps(
            {
                "name": "flash-ticketing-api-secondary",
                "services": {
                    "api": {
                        "image": "${BASELINE_API_IMAGE:?Load the exact frozen API image}",
                        "pull_policy": "never",
                        "env_file": [
                            {
                                "path": "${SECONDARY_API_ENV_FILE:?Set private baseline API environment file}",
                                "required": True,
                                "format": "raw",
                            }
                        ],
                        "environment": API_SETTINGS,
                        "ports": [
                            "${API_PRIVATE_BIND_IP:?Set verified secondary private IPv4}:8101-8104:8000"
                        ],
                        "command": [
                            "uvicorn",
                            "ticketing.api:app",
                            "--host",
                            "0.0.0.0",
                            "--port",
                            "8000",
                            "--limit-concurrency",
                            "256",
                            "--timeout-keep-alive",
                            "10",
                        ],
                        "restart": "unless-stopped",
                        "healthcheck": {
                            "test": [
                                "CMD",
                                "python",
                                "-c",
                                "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')",
                            ],
                            "interval": "5s",
                            "timeout": "3s",
                            "retries": 20,
                        },
                    }
                },
            },
            indent=2,
        )
        + "\n"
    )


def prepare(output, inventory=None, *, now=None):
    baseline = json.loads(BASELINE.read_text())
    image = json.loads(OVERHEAD.read_text())["measurement"]["images"]["api"][0]
    gates = list(baseline["required_gates"])
    if len(gates) != 22 or baseline["tested_revision"] != REVISION:
        raise ValueError("Frozen baseline gate/revision contract differs")
    view = validate_inventory(inventory, image_id=image, now=now) if inventory is not None else None
    output = output.resolve()
    if (
        not output.is_relative_to((ROOT / "tmp").resolve())
        or output == (ROOT / "tmp").resolve()
        or output.exists()
    ):
        raise ValueError("Fresh owned directory under repository tmp required")
    output.mkdir(parents=True, mode=0o700)
    plan = {
        "decision": "0147-fixed-budget-two-host-api-comparison",
        "status": "PREPARED_NOT_DEPLOYED",
        "runtime_revision": REVISION,
        "api_image_id": image,
        "paid_runs_started": 0,
        "paid_comparison_limit": 2,
        "automatic_escalation": False,
        "workload": {
            "buyers_per_second": 60,
            "seconds": 300,
            "shows": 60,
            "seats_per_show": 300,
            "journey_concurrency": 500,
            "generator_shards": 2,
            "http_clients_per_shard": 8,
            "poll_seconds": 1,
            "customer_retries": 0,
            "callback_deliveries": 1,
        },
        "required_paid_gates": gates,
        "additional_two_host_gates": EXTRA_GATES,
        "api_settings": API_SETTINGS,
        "background": BACKGROUND,
        "inventory_summary": view,
        "remaining_live_prerequisites": [
            "second API ECS access/verified inventory",
            "immutable image transfer",
            "private PgBouncer reachability and baseline environment parity",
            "primary LB/config backup",
            "both-host CPU and all-four-replica pipeline collectors",
            "cross-host contention/replay",
            "bounded two-host paid runner and verified rollback",
        ],
        "limitations": "Preparation only. Supplied inventory validation is not remote attestation, live execution or paid-capacity proof.",
    }
    files = {
        "plan.json": json.dumps(plan, indent=2) + "\n",
        "compose.secondary.yaml": secondary_compose(),
        "compose.primary.yaml": """services:
  api:
    image: ${BASELINE_API_IMAGE:?Load the exact frozen API image}
    pull_policy: never
    build: !reset null
    ports: !override ["${API_PRIVATE_BIND_IP:?Set verified primary private IPv4}:8101-8104:8000"]
    environment:
"""
        + "".join(f"      {k}: {json.dumps(v)}\n" for k, v in API_SETTINGS.items())
        + """  pgbouncer:
    ports: !override ["${API_PRIVATE_BIND_IP:?Set verified primary private IPv4}:5432:5432"]
    environment:
      DEFAULT_POOL_SIZE: "24"
      RESERVE_POOL_SIZE: "0"
      MAX_CLIENT_CONN: "160"
  load-balancer:
    volumes: !override ["${TWO_HOST_NGINX_CONFIG:?Set owned four-endpoint config}:/etc/nginx/nginx.conf:ro"]
""",
    }
    if inventory is not None:
        files["nginx." + inventory["arm"] + ".conf"] = nginx_config(inventory["apis"])
    for name, content in files.items():
        with (output / name).open("x", encoding="utf-8", newline="\n") as f:
            f.write(content)
    return plan


def evaluate_gates(paid, additional):
    """Keep missing, false and nonboolean gates failed; no vacuous qualification."""
    required = set(json.loads(BASELINE.read_text())["required_gates"])
    if not isinstance(paid, dict) or not isinstance(additional, dict):
        raise TypeError("Gate maps required")
    if set(paid) != required or set(additional) != set(EXTRA_GATES):
        raise ValueError("All original and cross-host gates required exactly once")
    failed = sorted(k for values in (paid, additional) for k, value in values.items() if value is not True)
    return {
        "all_required_gates_pass": not failed,
        "failed_gates": failed,
        "scope": "Gate-map evaluation only; live evidence must be collected by a qualified runner.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--inventory", type=Path, help="Fresh private observed inventory; never credential-bearing env"
    )
    args = parser.parse_args()
    try:
        inventory = json.loads(args.inventory.read_text()) if args.inventory else None
        plan = prepare(args.output, inventory)
    except (OSError, KeyError, TypeError, ValueError):
        parser.exit(1, "Preparation rejected: invalid/stale inventory, baseline or nonfresh output.\n")
    print(
        json.dumps(
            {
                "status": plan["status"],
                "paid_runs_started": 0,
                "inventory_contract_checked": inventory is not None,
                "live_qualification": "NOT_EXECUTED",
            }
        )
    )


if __name__ == "__main__":
    main()
