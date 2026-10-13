"""ADR0317 fixed physical backend budget; pure routing, config and observations."""
import configparser
import hashlib
import io
import math
from urllib.parse import urlsplit, urlunsplit

BASELINE = "250aa2541308119f8da10a7ba3ebdf8024d99fb8a1fffbb796e8eead55fbfa2a"
ORIGINAL_CONFIG = "f77cde46c5f069a445030d6091495ba7777ade8e3da7a707d00be96fe263d859"
POOLER_IMAGE = "sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e"
ALIASES = {"ticketing_foreground":16, "ticketing_background":7, "ticketing":1}
DATA_USER = "ticketing"


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def contract():
    from pathlib import Path
    return {"decision":"ADR0317", "baseline_result_sha256":BASELINE,
            "original_pooler_config_sha256":ORIGINAL_CONFIG, "pooler_image":POOLER_IMAGE,
            "aliases":dict(ALIASES), "physical_backend_limit":24, "shared_data_user_limit":24,
            "data_user":DATA_USER, "backend_database":"ticketing", "transaction_pooling":True,
            "reserve_pool_size":0, "helper_sha256":sha(Path(__file__).read_bytes()),
            "runtime_sha256":sha(Path(__file__).with_name("cce_backend_partition_runtime.py").read_bytes()),
            "tls_and_authorization_unchanged":True, "same_four_writers_batch_four":True}


def selected(goal):
    keys = {"backend_partition_decision", "backend_partition_contract_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    import cce_recovery_headroom as h
    import work_envelope as p
    if (not h.selected(goal) or goal.get("backend_partition_decision") != "ADR0317"
            or goal.get("backend_partition_contract_sha256") != p.digest(contract())):
        raise ValueError("Exactly declared fixed backend partition required")
    return True


def route(url, alias):
    parsed = urlsplit(url)
    if (alias not in ALIASES or parsed.scheme not in {"postgres","postgresql"}
            or parsed.username != DATA_USER or parsed.path != "/ticketing"
            or not parsed.password or not parsed.hostname or parsed.port not in {5432,6432}):
        raise ValueError("Original single-user pooled route required")
    return urlunsplit(parsed._replace(path="/"+alias))


def render(raw, *, phase="active"):
    if hashlib.sha256(raw).hexdigest() != ORIGINAL_CONFIG or phase not in {"prepare","active"}:
        raise ValueError("Exact original pooler bytes and declared phase required")
    config = configparser.ConfigParser(interpolation=None)
    config.read_string(raw.decode())
    if set(config["databases"]) != {"ticketing"}:
        raise ValueError("One original database alias required")
    settings = config["pgbouncer"]
    if (settings.get("pool_mode") != "transaction" or settings.get("default_pool_size") != "24"
            or settings.get("reserve_pool_size") != "0"
            or settings.get("server_tls_sslmode") != "verify-full"):
        raise ValueError("Original transaction budget and verified TLS required")
    original = config["databases"]["ticketing"]
    authority = dict(k.split("=",1) for k in original.split())
    if (set(authority) != {"host","port","auth_user"} or authority["auth_user"] != DATA_USER):
        raise ValueError("Exact existing backend authority/authentication required")
    settings["max_user_connections"] = "24"
    for alias, limit in ALIASES.items():
        if phase == "prepare" and alias == "ticketing":
            limit = 24
        config["databases"][alias] = (original+" dbname=ticketing"
            +" pool_size="+str(limit)+" max_db_connections="+str(limit)
            +" reserve_pool_size=0 auth_dbname=ticketing")
    out = io.StringIO()
    config.write(out)
    return out.getvalue().encode()


def number(value):
    result = float(value or 0)
    if not math.isfinite(result) or result < 0:
        raise ValueError("Finite nonnegative pool counters required")
    return result


def view(conn, target_database, fields, stats_fields):
    if target_database != "ticketing":
        raise ValueError("Exact diagnostic alias required")
    raw_pools = conn.execute("SHOW POOLS").fetchall()
    raw_stats = conn.execute("SHOW STATS").fetchall()
    by_alias = {}
    for alias, cap in ALIASES.items():
        pools = [r for r in raw_pools if r["database"] == alias]
        stats = [r for r in raw_stats if r["database"] == alias]
        if not pools or not stats or any(r.get("user") != DATA_USER for r in pools):
            raise ValueError("Complete single-user foreground/background measurements required")
        physical = sum(sum(number(r.get(k)) for k in (
            "sv_active","sv_idle","sv_used","sv_tested","sv_login")) for r in pools)
        if physical > cap:
            raise ValueError("Alias physical backend ceiling exceeded")
        by_alias[alias] = {
            "pools":{k:sum(number(r.get(k)) for r in pools) for k in fields},
            "stats":{k:sum(number(r.get(k)) for r in stats) for k in stats_fields},
            "maxwait_seconds":max(number(r.get("maxwait"))+number(r.get("maxwait_us"))/1e6 for r in pools),
            "physical_backends":physical, "limit":cap}
    if any(r["database"] not in {*ALIASES,"pgbouncer"} for r in raw_pools):
        raise ValueError("Unknown data pool must not bypass total budget")
    total = sum(r["physical_backends"] for r in by_alias.values())
    if total > 24:
        raise ValueError("Aggregate physical backend ceiling exceeded")
    return {"pools":{k:sum(r["pools"][k] for r in by_alias.values()) for k in fields},
            "stats":{k:sum(r["stats"][k] for r in by_alias.values()) for k in stats_fields},
            "maxwait_seconds":max(r["maxwait_seconds"] for r in by_alias.values()),
            "aliases":by_alias, "physical_backends":total, "budget":24,
            "partition_decision":"ADR0317"}
