"""ADR0330 explicit primary-only CCE bootstrap; never emulate a retired host."""
import copy
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",",":"), allow_nan=False).encode()).hexdigest()

DECISION = "ADR0330"
MARKER = "cce_bootstrap_topology"
VALUE = "primary-only-ADR0330"
SOURCE = "scripts/cce_primary_bootstrap.py"
KEYS = {"primary_bootstrap_decision", "primary_bootstrap_contract_sha256", "primary_bootstrap_config_path", "primary_bootstrap_legacy_config_path"}
SOURCES = (SOURCE, "scripts/probe_two_host_safety.py", "scripts/qualify_cce_primary_bootstrap.py", "scripts/qualify_two_host_deployment.py", "scripts/collect_two_host_inventory.py",
           "scripts/prepare_two_host_scaling.py", "scripts/status_refresh_contract.py",
           "scripts/cce_committed_status_contract.py", "scripts/cce_ecs_transition.py",
           "scripts/cce_paid_lifecycle.py", "scripts/cce_paid_observers.py",
           "scripts/cce_recovery_headroom.py", "scripts/cce_throughput_push.py",
           "scripts/run_cce_paid_comparison.py", "scripts/cce_paid_stage.py",
           "scripts/run_status_refresh_comparison.py", "scripts/run_two_host_paid_comparison.py",
           "scripts/observe_two_host_pipeline.py", "scripts/observe_slot_paid_pipeline.py", "scripts/cce_verification_contract.py",
           "scripts/cce_reproduction_overlay.py", "artifacts/cce-transaction-runner/manifest.json")


def enabled(config):
    if MARKER not in config:
        return False
    if config[MARKER] != VALUE or "secondary" in config or config.get("secondary_ssh_private_fallback"):
        raise ValueError("Exact primary-only configuration without secondary required")
    if not all(role in config for role in ("primary", "generator")):
        raise ValueError("Primary and generator remain required")
    return True


def prepare_config(legacy):
    if "secondary" not in legacy or MARKER in legacy:
        raise ValueError("Unmodified legacy three-host configuration required")
    value=copy.deepcopy(legacy)
    value.pop("secondary")
    value.pop("secondary_ssh_private_fallback",None)
    value[MARKER]=VALUE
    enabled(value)
    return value


def ecs_roles(config):
    return ("primary",) if enabled(config) else ("primary", "secondary")


def connection_roles(config):
    return (*ecs_roles(config), "generator")


def configure(contract, config):
    if enabled(config):
        contract.primary_only_bootstrap = True
    return contract


def marker(settings=None):
    return {**({"api_settings_sha256":digest(settings)} if settings is not None else {}),"decision": DECISION, "bootstrap_api_counts": {"primary": 4},
            "observed_host_roles": ["primary", "generator"], "retired_host_observation": "not_applicable"}


def contract():
    return {**marker(), "application_changes": False, "physical_database_backends": 24,
            "sources": {name: hashlib.sha256((ROOT/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest() for name in SOURCES}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (goal.get("primary_bootstrap_decision") != DECISION
            or goal.get("primary_bootstrap_contract_sha256") != digest(contract())
            or not all(isinstance(goal.get(k), str) for k in ("primary_bootstrap_config_path", "primary_bootstrap_legacy_config_path"))):
        raise ValueError("Source-bound primary-only bootstrap authorization required")
    return True


def private_config(relative):
    path = ROOT / relative
    if not path.resolve().is_relative_to((ROOT/"tmp").resolve()) or path.is_symlink():
        raise ValueError("Owned private bootstrap configuration required")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_binding(goal, binding, original):
    if not selected(goal):
        raise ValueError("Explicit primary-only bootstrap required")
    old = private_config(goal["primary_bootstrap_legacy_config_path"])
    current = private_config(goal["primary_bootstrap_config_path"])
    expected = copy.deepcopy(old)
    expected.pop("secondary")
    expected.pop("secondary_ssh_private_fallback", None)
    expected[MARKER] = VALUE
    if (current != expected or not enabled(current)
            or original.get("configuration_sha256") != digest(old)
            or binding.get("configuration_sha256") != digest(current)
            or binding.get("cce_primary_bootstrap_contract") != contract()):
        raise ValueError("Only the declared secondary retirement may alter configuration")
    return True


def verify_inventory(contract, data):
    from cce_event_lane_identity import verify_inventory as verify_sources
    verify_sources(data, digest(data), identity=contract.identity)
    if (data.get("cce_primary_bootstrap_contract") != marker(contract.api_settings)
            or not getattr(contract,"primary_only_bootstrap",False)
            or set(data.get("hosts",{})) != {"primary","generator"}
            or any(api.get("host_role") != "primary" for api in data.get("apis",[]))):
        raise ValueError("Real primary-only inventory and unchanged application factors required")
    return True


def cpu_window(primary, *, offered_start_utc, offered_end_utc, hourly=False):
    from observe_two_host_cpu import placement, timestamp
    start,end=timestamp(offered_start_utc),timestamp(offered_end_utc)
    if (type(hourly) is not bool or primary.get("host_role") != "primary" or primary.get("pass") is not True
            or placement(primary) not in {"cce-api-isolation","cce-worker-isolation"}
            or not (0 < (end-start).total_seconds() <= (3600 if hourly else 300))
            or (hourly and (end-start).total_seconds()!=3600)
            or any(abs((timestamp(primary[k])-bound).total_seconds())>1 for k,bound in (("start_utc",start),("end_utc",end)))):
        raise ValueError("Actual primary CPU coverage of the exact offered window required")
    return {"decision":DECISION,"observed_host_roles":["primary"],"retired_secondary_cpu":"not_applicable",
            "same_offered_measurement_window":True,"host_cpu_percent":{"primary":primary["host_cpu_percent"]}}


def restoration_verified(value):
    return (value.get(MARKER)==VALUE and value.get("connected_host_roles")==["primary","generator"]
            and value.get("secondary_access_attempts")==0 and value.get("retired_secondary_cleanup")=="not_applicable")


def validate_observer_inventory(data, approved, *, image_id, now=None):
    from datetime import UTC, datetime

    import cce_fulfillment_batch_identity as identity
    from cce_event_lane_identity import verify_inventory as verify_sources
    from prepare_two_host_scaling import private_ipv4
    if digest(data) != approved:
        raise ValueError("Exact approved primary-only inventory digest required")
    settings=data.get("primary_bootstrap_api_settings",{})
    if data.get("cce_primary_bootstrap_contract") != marker(settings) or not settings:
        raise ValueError("Source-qualified primary-only settings required")
    verify_sources(data, approved, identity=identity)
    captured=datetime.fromisoformat(data["captured_at"])
    now=now or datetime.now(UTC)
    if captured.tzinfo is None or not 0 <= (now-captured).total_seconds() <= 300:
        raise ValueError("Fresh primary-only inventory required")
    hosts=data.get("hosts",{})
    if (set(hosts)!={"primary","generator"} or len({h["machine_id_sha256"] for h in hosts.values()})!=2
            or len({private_ipv4(h["private_ipv4"]) for h in hosts.values()})!=2
            or hosts["generator"]["api_processes"] != 0 or data.get("generator_idle") is not True
            or data.get("global_queues_zero") is not True
            or data.get("pgbouncer")!={"host_role":"primary","server_pool":24,"reserve_pool":0,"max_clients":160}):
        raise ValueError("Independent real hosts and unchanged database budget required")
    apis=data.get("apis",[])
    if (len(apis)!=4 or len({v["container_id"] for v in apis})!=4
            or len({v["port"] for v in apis})!=4):
        raise ValueError("Four distinct primary API containers required")
    for api in apis:
        if (api.get("host_role")!="primary" or api.get("private_ipv4")!=hosts["primary"]["private_ipv4"]
                or api.get("image_id")!=image_id or api.get("source_hashes_match") is not True
                or api.get("ready") is not True or api.get("settings")!=settings
                or api.get("pgbouncer_host_role")!="primary" or api.get("pgbouncer_port")!=5432
                or api.get("database_url_host")!="pgbouncer" or not 8101<=api["port"]<=8104
                or any(api.get(k)!=v for k,v in data["baseline_authorities"].items())):
            raise ValueError("Primary API image/source/authority/connection settings changed")
    return True
