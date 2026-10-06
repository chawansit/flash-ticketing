"""ADR0184 pure worker-placement models. No cloud access, staging, or deployment."""
import copy
import re
from urllib.parse import urlsplit, urlunsplit

from prepare_two_host_scaling import private_ipv4

WORKERS = {
    "consumer": 6, "reservation-writer": 3, "publisher": 1, "maintenance": 1,
    "reconciler": 1, "simulator": 1, "confirmation": 1,
}
INFRA = {"api": 4, "pgbouncer": 1, "kafka": 1, "load-balancer": 1}
PROJECT = "flash-ticketing-background-secondary"
KAFKA_PORT = 19092
FORBIDDEN = {"build", "env_file", "network_mode", "privileged", "devices", "volumes_from", "container_name"}


def _environment(service):
    env = service.get("environment")
    if not isinstance(env, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in env.items()):
        raise ValueError("Resolved literal string environments required")
    return env


def _pinned(service):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", service.get("image", "")):
        raise ValueError("Immutable actual per-role image required")
    if set(service) & FORBIDDEN:
        raise ValueError("Unsupported service execution option")
    _environment(service)


def _integer(env, key):
    value = env.get(key, "")
    if not re.fullmatch(r"[1-9][0-9]{0,2}", value):
        raise ValueError("Explicit bounded " + key + " required")
    return int(value)


def _database_route(value, primary):
    parsed = urlsplit(value)
    if (parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname != "pgbouncer"
            or parsed.port not in {None, 5432} or not parsed.path or parsed.fragment):
        raise ValueError("Existing URI through primary PgBouncer required")
    credentials, separator, _ = parsed.netloc.rpartition("@")
    if not separator or not credentials:
        raise ValueError("Explicit existing database credentials required")
    return urlunsplit(parsed._replace(netloc=credentials + "@" + primary + ":5432"))


def budgets(model):
    services = model["services"]
    api = _environment(services["api"])
    if _integer(api, "DB_POOL_MAX") != 4 or _integer(api, "API_PAYMENT_POOL_MAX") != 2:
        raise ValueError("Frozen API pool budgets required")
    pool = _environment(services["pgbouncer"])
    if any(pool.get(k) != v for k, v in {"DEFAULT_POOL_SIZE": "24", "RESERVE_POOL_SIZE": "0", "MAX_CLIENT_CONN": "160"}.items()):
        raise ValueError("One unchanged PgBouncer server budget required")
    role_pools = {r: _integer(_environment(services[r]), "DB_POOL_MAX") for r in WORKERS}
    if role_pools["consumer"] != 8 or role_pools["simulator"] != 10 or role_pools["confirmation"] != 2:
        raise ValueError("Frozen payment/consumer pool budgets required")
    return {"api_connections": 24, "callback_pool_slots": 8,
            "worker_connections": sum(WORKERS[r] * role_pools[r] for r in WORKERS),
            "worker_pool_per_replica": role_pools,
            "pgbouncer_server_connections": 24, "pgbouncer_max_clients": 160}


def _broker(model, primary):
    broker = model["services"]["kafka"]
    env = _environment(broker)
    expected = {"KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
                "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"}
    if any(env.get(k) != v for k, v in expected.items()) or broker.get("ports"):
        raise ValueError("Exact existing internal broker listeners required")
    mounts = broker.get("volumes", [])
    if (len(mounts) != 1 or not isinstance(mounts[0], dict) or mounts[0].get("type") != "volume"
            or mounts[0].get("target") != env.get("KAFKA_LOG_DIRS") or mounts[0].get("read_only", False)):
        raise ValueError("Exact existing writable Kafka data volume required")
    definition = model.get("volumes", {}).get(mounts[0].get("source"))
    if not isinstance(definition, dict) or not definition.get("name"):
        raise ValueError("Actual Kafka volume name must be retained")
    env["KAFKA_LISTENERS"] += ",PRIVATE://:" + str(KAFKA_PORT)
    env["KAFKA_ADVERTISED_LISTENERS"] += ",PRIVATE://" + primary + ":" + str(KAFKA_PORT)
    env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] += ",PRIVATE:PLAINTEXT"
    broker["ports"] = [{"target": KAFKA_PORT, "published": str(KAFKA_PORT), "host_ip": primary, "protocol": "tcp"}]


def prepare_pair(model, *, primary_ip, secondary_ip):
    """Prepare secret-bearing in-memory models; callers must never publish them."""
    primary, secondary = private_ipv4(primary_ip), private_ipv4(secondary_ip)
    if primary == secondary:
        raise ValueError("Distinct existing ECS addresses required")
    if model.get("name") != "flash-ticketing" or set(model.get("services", {})) != set(INFRA) | set(WORKERS):
        raise ValueError("Exact prepared application and infrastructure roles required")
    for service in model["services"].values():
        _pinned(service)
    original_budget = budgets(model)
    common = copy.deepcopy(model)
    _broker(common, primary)
    # Readiness is external and must be proven by the qualified lifecycle, not local Compose dependencies.
    for service in common["services"].values():
        service.pop("depends_on", None)
        service.pop("profiles", None)
        service["pull_policy"] = "never"
    for index, role in enumerate(WORKERS):
        worker = common["services"][role]
        if worker.get("volumes") or worker.get("ports") or worker.get("networks") not in (None, {}, {"default": None}, {"default": {}}):
            raise ValueError("Worker mounts/network overrides need separate owned qualification")
        worker.pop("networks", None)
        env = _environment(worker)
        env["DATABASE_URL"] = _database_route(env.get("DATABASE_URL", ""), primary)
        redis = urlsplit(env.get("REDIS_URL", ""))
        if (redis.scheme not in {"redis", "rediss"} or not redis.hostname
                or redis.hostname in {"redis", "localhost", "127.0.0.1", "::1"}):
            raise ValueError("Existing managed Redis endpoint required")
        env["KAFKA_BOOTSTRAP"] = primary + ":" + str(KAFKA_PORT)
        env["API_URL"] = "http://" + primary + ":8000"
        metric = int(env.get("WORKER_METRICS_PORT", "9101"))
        if not 1024 <= metric <= 65535:
            raise ValueError("Valid worker metrics port required")
        first = 19101 + 100 * index
        last = first + WORKERS[role] - 1
        worker["ports"] = [{"target": metric, "published": str(first) if first == last else f"{first}-{last}",
                            "host_ip": primary, "protocol": "tcp"}]
    control = {"primary": common, "secondary": None,
               "counts": {"primary": {**INFRA, **WORKERS}, "secondary": {}}}
    candidate_primary = copy.deepcopy(common)
    remote = {"name": PROJECT, "services": {}}
    for role in WORKERS:
        service = candidate_primary["services"].pop(role)
        service["ports"][0]["host_ip"] = secondary
        remote["services"][role] = service
    candidate = {"primary": candidate_primary, "secondary": remote,
                 "counts": {"primary": dict(INFRA), "secondary": dict(WORKERS)}}
    result = {"decision": "ADR0184", "factor": "background_host_primary_to_secondary",
              "control": control, "candidate": candidate, "budgets": original_budget,
              "load_authorized": False, "live_qualified": False}
    validate_pair(result)
    return result


def validate_pair(pair):
    """Reject drift in every field apart from owned worker metric bind addresses."""
    if (pair.get("decision") != "ADR0184" or pair.get("factor") != "background_host_primary_to_secondary"
            or pair.get("load_authorized") is not False or pair.get("live_qualified") is not False):
        raise ValueError("Offline-only worker preparation required")
    control, candidate = pair["control"], pair["candidate"]
    if (control["secondary"] is not None or candidate["secondary"]["name"] != PROJECT
            or control["counts"] != {"primary": {**INFRA, **WORKERS}, "secondary": {}}
            or candidate["counts"] != {"primary": INFRA, "secondary": WORKERS}):
        raise ValueError("Exact disjoint replica placement required")
    if set(candidate["secondary"]["services"]) != set(WORKERS):
        raise ValueError("All and only background services required remotely")
    reconstructed = copy.deepcopy(candidate["primary"])
    worker_hosts = set()
    for role, worker in candidate["secondary"]["services"].items():
        if role in reconstructed["services"]:
            raise ValueError("Worker overlap rejected")
        service = copy.deepcopy(worker)
        if len(service.get("ports", [])) != 1:
            raise ValueError("Exact worker metric binding required")
        address = private_ipv4(service["ports"][0]["host_ip"])
        worker_hosts.add(address)
        original = control["primary"]["services"][role]
        primary = private_ipv4(original["ports"][0]["host_ip"])
        if address == primary:
            raise ValueError("Distinct worker host required")
        service["ports"][0]["host_ip"] = primary
        reconstructed["services"][role] = service
    if len(worker_hosts) != 1 or reconstructed != control["primary"] or budgets(reconstructed) != pair["budgets"]:
        raise ValueError("Fixed-factor images/settings/budgets changed")
    return True


def lifecycle_steps(arm):
    """Ordered requirements for a future qualified runner; these do not execute."""
    if arm not in {"control", "candidate"}:
        raise ValueError("Explicit comparison arm required")
    return (
        "verify_consumed_paid_failure_recovery", "bind_fresh_registered_scope",
        "snapshot_exact_runtime_broker_and_volume", "verify_generator_idle_and_all_queues_drained",
        "stage_owned_immutable_images", "stop_original_background_workers",
        "prove_original_workers_absent", "configure_private_broker_listener_and_common_callback_ingress",
        "verify_private_dependencies_and_broker_metadata", "start_exact_arm_workers",
        "verify_host_aware_sources_metrics_pools_and_callback_distribution",
        "retain_fixture_identity_before_dispatch", "run_qualified_safety_and_post_ttl_checks",
        "dispatch_bounded_paid_stage", "stop_dispatch_and_audit_post_ttl_entities_and_all_queues",
        "stop_arm_workers_and_prove_absence", "restore_original_broker_volume_and_runtime_semantics",
        "verify_restored_queues_identities_and_private_cleanup",
    )
