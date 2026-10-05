"""Collect ADR0147 fresh observations without publishing credentials."""

import hashlib
import json
from datetime import UTC, datetime
from urllib.parse import urlsplit

from prepare_two_host_scaling import API_SETTINGS, BACKGROUND, REVISION, validate_inventory
from qualify_two_host_deployment import GLOBAL_AUDIT, IMAGE, INSPECT, ROOT
from two_host_topology import environment, grouped


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def authorities(env):
    url = urlsplit(env["DATABASE_URL"])
    if url.scheme not in {"postgresql", "postgres"} or not url.hostname or not url.path:
        raise ValueError("Observed PostgreSQL URL required")
    auth = {k: env.get(k) for k in ("JWT_SECRET", "PAYMENT_CALLBACK_SECRET")}
    return {
        "other_settings_sha256": digest(
            {k: v for k, v in env.items() if k not in {*API_SETTINGS, "DATABASE_URL", "REDIS_URL", *auth}}
        ),
        "database_credentials_and_name_sha256": digest(
            [url.scheme, url.username, url.password, url.path, url.query]
        ),
        "redis_authority_sha256": digest(env["REDIS_URL"]),
        "auth_secrets_sha256": digest(auth),
    }


def container_call(session, role, cid, program, timeout=60):
    code = (
        "import json,subprocess,sys;r=subprocess.run("
        + repr(["docker", "exec", cid, "python", "-c", program])
        + ",capture_output=True,text=True,timeout="
        + str(timeout - 10)
        + ");sys.stderr.write(r.stderr);r.check_returncode();print(r.stdout)"
    )
    return session.call(role, code, timeout)


def host_program():
    return r"""import hashlib,json,os,subprocess
from pathlib import Path
uuid=Path('/sys/class/dmi/id/product_uuid').read_text().strip().lower()
memory=int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemTotal:')))*1024
addresses=[a['local'] for interface in json.loads(subprocess.check_output(['ip','-j','addr'],text=True)) for a in interface['addr_info'] if a['family']=='inet']
api_processes=0
load_processes=0
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:cmd=(p/'cmdline').read_bytes().split(b'\0')
 except OSError:continue
 if any(arg==b'ticketing.api:app' for arg in cmd):api_processes+=1
 if any(Path(arg.decode(errors='replace')).name in {'paid_ticket_sharded_generator.py','paid_ticket_load_generator.py','run_synchronized_paid_generator.py'} for arg in cmd):load_processes+=1
print(json.dumps({'machine_id_sha256':hashlib.sha256(uuid.encode()).hexdigest(),'vcpus':os.cpu_count(),'memory_bytes':memory,'addresses':addresses,'api_processes':api_processes,'load_processes':load_processes}))
"""


def observe(session, arm, routes, baseline_env):
    primary = session.call("primary", INSPECT)
    # Include all secondary resources, not just the expected project's running APIs.
    secondary = session.call(
        "secondary",
        "import json,subprocess;ids=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True).split();print(json.dumps(json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []))",
    )
    host_rows = {role: session.call(role, host_program()) for role in ("primary", "secondary", "generator")}
    hosts = {}
    for role, row in host_rows.items():
        address = session.config[role]["private_ipv4"]
        if address not in row.pop("addresses"):
            raise ValueError("Configured address is not bound to its observed host")
        hosts[role] = {**row, "private_ipv4": address}
    hosts["secondary"]["non_api_services"] = [
        r["Id"]
        for r in secondary
        if r["Config"]["Labels"].get("com.docker.compose.project") != "flash-ticketing-api-secondary"
        or r["Config"]["Labels"].get("com.docker.compose.service") != "api"
        or not r["State"]["Running"]
    ]
    by_id = {r["Id"]: r for r in [*primary, *secondary]}
    groups = grouped(primary)
    expected_counts = {
        "api": 4 if arm == "control" else 2,
        "consumer": 6,
        "reservation-writer": 3,
        "publisher": 1,
        "maintenance": 1,
        "reconciler": 1,
        "simulator": 1,
        "kafka": 1,
        "load-balancer": 1,
        "pgbouncer": 1,
    }
    if {k: len(v) for k, v in groups.items()} != expected_counts:
        raise ValueError("Observed primary placement differs")
    background = {k: {"replicas": len(groups[k])} for k in BACKGROUND}
    background["consumer"]["pool_per_replica"] = int(environment(groups["consumer"][0])["DB_POOL_MAX"])
    if any(environment(r)["DB_POOL_MAX"] != "8" for r in groups["consumer"]):
        raise ValueError("Consumer pool differs")
    background["reservation-writer"]["batch_size"] = int(
        environment(groups["reservation-writer"][0])["RESERVATION_WRITER_BATCH_SIZE"]
    )
    if any(environment(r)["RESERVATION_WRITER_BATCH_SIZE"] != "4" for r in groups["reservation-writer"]):
        raise ValueError("Writer batch differs")
    sim = environment(groups["simulator"][0])
    background["simulator"].update(
        concurrency=int(sim["SIMULATOR_CONCURRENCY"]), dispatch_mode=sim["SIMULATOR_DISPATCH_MODE"]
    )
    pool = environment(groups["pgbouncer"][0])
    queue = session.api(groups["api"][0]["Id"], GLOBAL_AUDIT, 60)
    if queue["kafka_members"] != 6:
        raise ValueError("Six Kafka members required before dispatch")
    expected = json.loads(
        (
            ROOT / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json"
        ).read_text()
    )["expected_runtime_source_sha256"]
    observed_apis = []
    for route in routes:
        row = by_id[route["container_id"]]
        env = environment(row)
        source_check = (
            "import hashlib,json,urllib.request;from pathlib import Path;expected="
            + repr(expected)
            + ";actual={k:hashlib.sha256((Path('/app')/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected};ready=json.loads(urllib.request.urlopen('http://127.0.0.1:8000/health/ready',timeout=3).read());print(json.dumps({'source_hashes_match':actual==expected,'ready':ready['status']=='ready'}))"
        )
        proof = container_call(session, route["host_role"], row["Id"], source_check)
        url = urlsplit(env["DATABASE_URL"])
        observed_apis.append(
            {
                **route,
                "image_id": row["Image"],
                "revision": REVISION,
                **proof,
                "settings": {k: env.get(k) for k in API_SETTINGS},
                "pgbouncer_host_role": "primary",
                "pgbouncer_port": url.port or 5432,
                "database_url_host": url.hostname,
                **authorities(env),
            }
        )
    inventory = {
        "schema": 1,
        "arm": arm,
        "captured_at": datetime.now(UTC).isoformat(),
        "hosts": hosts,
        "apis": observed_apis,
        "baseline_authorities": authorities(baseline_env),
        "background": background,
        "pgbouncer": {
            "host_role": "primary",
            "server_pool": int(pool["DEFAULT_POOL_SIZE"]),
            "reserve_pool": int(pool["RESERVE_POOL_SIZE"]),
            "max_clients": int(pool["MAX_CLIENT_CONN"]),
        },
        "generator_idle": hosts["generator"]["load_processes"] == 0,
        "global_queues_zero": queue["pass"],
        "identity_provenance": "Live DMI instance UUID, actual bound addresses and container inspection; authority values are fingerprinted.",
    }
    view = validate_inventory(inventory, image_id=IMAGE)
    return inventory, view, primary, secondary
