"""Pure ADR0147 deployment models and restore comparisons; never performs I/O."""

import copy
import hashlib
import json

from prepare_two_host_scaling import API_SETTINGS, private_ipv4

NORMAL_COUNTS = {
    "api": 4,
    "consumer": 1,
    "publisher": 1,
    "maintenance": 1,
    "reconciler": 1,
    "simulator": 1,
    "pgbouncer": 1,
    "kafka": 1,
    "load-balancer": 1,
}
CHANGED = (
    "api",
    "consumer",
    "reservation-writer",
    "publisher",
    "maintenance",
    "reconciler",
    "simulator",
    "pgbouncer",
    "load-balancer",
)
CANDIDATE_COUNTS = {**NORMAL_COUNTS, "consumer": 6, "reservation-writer": 3}


def environment(row):
    entries = row["Config"]["Env"]
    values = dict(entry.split("=", 1) for entry in entries)
    if len(values) != len(entries):
        raise ValueError("Duplicate runtime environment keys")
    return values


def bindings(row):
    result=[]
    for port,published in (row['HostConfig'].get('PortBindings') or {}).items():
        target,protocol=port.split('/')
        for declared in published or []:
            value=declared['HostPort'];interface=declared['HostIp']
            if '-' in value:
                lower,upper=map(int,value.split('-'))
                if not interface or not 1<=lower<=upper<=65535:raise ValueError('Exact explicit bounded port range required')
                matches=[item for item in (row.get('NetworkSettings',{}).get('Ports',{}).get(port) or [])
                         if item['HostIp']==interface]
                if len(matches)!=1:raise ValueError('One exact resolved interface binding required')
                assigned=int(matches[0]['HostPort'])
                if not lower<=assigned<=upper:raise ValueError('Resolved port outside declared range')
            else:assigned=int(value)
            if not 1<=assigned<=65535:raise ValueError('Bounded assigned port required')
            result.append((int(target),protocol,interface,assigned))
    if len(set(result))!=len(result):raise ValueError('Duplicate resolved port identity')
    return sorted(result)


def semantic(row):
    return {
        "image": row["Image"],
        "environment": environment(row),
        "command": row["Config"]["Cmd"],
        "entrypoint": row["Config"].get("Entrypoint"),
        "healthcheck": row["Config"].get("Healthcheck"),
        "ports": bindings(row),
        "mounts": sorted((m["Type"], m["Source"], m["Destination"], m["RW"]) for m in row.get("Mounts", [])),
        "restart_policy": row["HostConfig"]["RestartPolicy"],
    }


def fingerprint(row):
    return hashlib.sha256(json.dumps(semantic(row), sort_keys=True).encode()).hexdigest()


def grouped(rows):
    result = {}
    for row in rows:
        if not row["State"]["Running"]:
            continue
        labels = row["Config"]["Labels"]
        if labels.get("com.docker.compose.project") != "flash-ticketing":
            raise ValueError("Unexpected primary project")
        role = labels.get("com.docker.compose.service")
        result.setdefault(role, []).append(row)
    return result


def snapshot(config, rows, *, image_id):
    groups = grouped(rows)
    if {k: len(v) for k, v in groups.items()} != NORMAL_COUNTS:
        raise ValueError("Normal topology differs")
    if config.get("name") != "flash-ticketing":
        raise ValueError("Unexpected Compose project")
    original = copy.deepcopy(config)
    for role in CHANGED:
        if role == "reservation-writer":
            original["services"][role]["image"] = image_id
            original["services"][role].pop("build", None)
            continue
        examples = groups[role]
        if len({fingerprint(row) for row in examples}) != 1:
            raise ValueError("Different runtime semantics within one role")
        row, service = examples[0], original["services"][role]
        if role == "api" and row["Image"] != image_id:
            raise ValueError("Primary frozen API image differs")
        service.pop("build", None)
        service.pop("env_file", None)
        service["image"] = row["Image"]
        service["pull_policy"] = "never"
        service["environment"] = environment(row)
        service["command"] = row["Config"]["Cmd"]
        if row["Config"].get("Entrypoint") is not None:
            service["entrypoint"] = row["Config"]["Entrypoint"]
        service["ports"] = [
            {"target": port, "protocol": protocol, "host_ip": address, "published": str(published)}
            for port, protocol, address, published in bindings(row)
        ]
        # Actual bind sources include shell-only LB configuration changes.
        if any(m["Type"] != "bind" for m in row.get("Mounts", [])):
            raise ValueError("Changed service has unsupported non-bind mount")
        service["volumes"] = [
            {"type": "bind", "source": m["Source"], "target": m["Destination"], "read_only": not m["RW"]}
            for m in row.get("Mounts", [])
        ]
    apis = groups["api"]
    if any(
        environment(a).get("RESERVATION_MODE") != "postgres"
        or environment(a).get("DB_POOL_MAX") != "3"
        or environment(a).get("API_PAYMENT_POOL_MAX", "0") != "0"
        for a in apis
    ):
        raise ValueError("Normal API settings differ")
    return {
        "schema": 1,
        "model": original,
        "counts": NORMAL_COUNTS.copy(),
        "semantics": {r: fingerprint(v[0]) for r, v in groups.items()},
        "image_id": image_id,
    }


def literal_model(value):
    """Resolved snapshot values are literals, including credential dollar signs."""
    if isinstance(value, str):
        return value.replace("$", "$$")
    if isinstance(value, list):
        return [literal_model(v) for v in value]
    if isinstance(value, dict):
        return {k: literal_model(v) for k, v in value.items()}
    return value


def deployment_model(saved, *, primary_ip, nginx_path):
    address = private_ipv4(primary_ip)
    if not nginx_path.startswith("/") or ".." in nginx_path.split("/"):
        raise ValueError("Owned absolute route config required")
    model = copy.deepcopy(saved["model"])
    services = model["services"]
    api = services["api"]
    api["environment"].update(API_SETTINGS)
    api["ports"] = [{"target": 8000, "published": "8101-8104", "host_ip": address, "protocol": "tcp"}]
    for role in ("consumer", "reservation-writer", "publisher", "maintenance", "reconciler", "simulator"):
        service = services[role]
        service.pop("build", None)
        service["pull_policy"] = "never"
        if role == "reservation-writer":
            service["image"] = saved["image_id"]
        service["environment"].update(
            RESERVATION_MODE="redis-first",
            REDIS_RESERVATION_MAX_COMMAND_AGE_SECONDS="60",
            RESERVATION_WRITER_BATCH_SIZE="4",
        )
    services["consumer"]["environment"]["DB_POOL_MAX"] = "8"
    services["simulator"]["environment"].update(SIMULATOR_CONCURRENCY="8", SIMULATOR_DISPATCH_MODE="refill")
    services["pgbouncer"]["ports"] = [
        {"target": 5432, "published": "5432", "host_ip": address, "protocol": "tcp"}
    ]
    services["pgbouncer"]["environment"].update(
        DEFAULT_POOL_SIZE="24", RESERVE_POOL_SIZE="0", MAX_CLIENT_CONN="160"
    )
    services["load-balancer"]["volumes"] = [
        {"type": "bind", "source": nginx_path, "target": "/etc/nginx/nginx.conf", "read_only": True}
    ]
    return model


def restored(saved, rows):
    groups = grouped(rows)
    if {k: len(v) for k, v in groups.items()} != saved["counts"]:
        raise ValueError("Normal counts not restored")
    if any(
        fingerprint(row) != saved["semantics"][role] for role, entries in groups.items() for row in entries
    ):
        raise ValueError("Normal runtime semantics not restored")
    return {"primary_runtime_semantics_restored": True, "primary_api_count": 4}
