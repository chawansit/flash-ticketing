import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "two_host", Path(__file__).resolve().parents[2] / "scripts/prepare_two_host_scaling.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
NOW = datetime(2026, 10, 5, tzinfo=UTC)
IMAGE = "sha256:" + "a" * 64
AUTH = {
    k: "a" * 64
    for k in (
        "other_settings_sha256",
        "database_credentials_and_name_sha256",
        "redis_authority_sha256",
        "auth_secrets_sha256",
    )
}


def inventory(arm="candidate"):
    hosts = {
        r: {
            "machine_id_sha256": c * 64,
            "private_ipv4": f"10.0.0.{i}",
            "vcpus": 4,
            "memory_bytes": 8 * 2**30,
            "non_api_services": [],
            "api_processes": 0,
        }
        for r, c, i in [("primary", "a", 1), ("secondary", "b", 2), ("generator", "c", 3)]
    }
    roles = ["primary"] * 4 if arm == "control" else ["primary", "primary", "secondary", "secondary"]
    apis = [
        {
            "host_role": role,
            "private_ipv4": hosts[role]["private_ipv4"],
            "port": 8101 + i,
            "container_id": f"{i + 1:064x}",
            "image_id": IMAGE,
            "revision": module.REVISION,
            "source_hashes_match": True,
            "ready": True,
            "settings": module.API_SETTINGS.copy(),
            "pgbouncer_host_role": "primary",
            "pgbouncer_port": 5432,
            "database_url_host": "pgbouncer" if role == "primary" else "10.0.0.1",
            **AUTH,
        }
        for i, role in enumerate(roles)
    ]
    return {
        "schema": 1,
        "arm": arm,
        "captured_at": NOW.isoformat(),
        "hosts": hosts,
        "apis": apis,
        "baseline_authorities": AUTH.copy(),
        "background": json.loads(json.dumps(module.BACKGROUND)),
        "pgbouncer": {"host_role": "primary", "server_pool": 24, "reserve_pool": 0, "max_clients": 160},
        "generator_idle": True,
        "global_queues_zero": True,
    }


@pytest.mark.parametrize(
    "arm,counts", [("control", {"primary": 4, "secondary": 0}), ("candidate", {"primary": 2, "secondary": 2})]
)
def test_matching_placement_keeps_connections_and_unexecuted_status(arm, counts):
    result = module.validate_inventory(inventory(arm), image_id=IMAGE, now=NOW)
    assert result["api_counts"] == counts
    assert result["api_connections_total"] == 16
    assert result["payment_connections_included"] == 8
    assert result["live_qualification"] == "NOT_EXECUTED"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["hosts"]["secondary"].update(machine_id_sha256="a" * 64),
        lambda d: d["hosts"]["generator"].update(private_ipv4="10.0.0.1"),
        lambda d: d["hosts"]["generator"].update(api_processes=1),
        lambda d: d["hosts"]["secondary"].update(vcpus=8),
        lambda d: d["hosts"]["secondary"].update(memory_bytes=True),
        lambda d: d["hosts"]["secondary"].update(memory_bytes=7 * 2**30),
        lambda d: d["hosts"]["secondary"].update(non_api_services=["pgbouncer"]),
        lambda d: d["pgbouncer"].update(server_pool=48),
        lambda d: d["pgbouncer"].update(reserve_pool=4),
        lambda d: d["background"]["consumer"].update(replicas=12),
        lambda d: d["apis"][0].update(image_id="sha256:" + "b" * 64),
        lambda d: d["apis"][0].update(revision="a" * 40),
        lambda d: d["apis"][0].update(source_hashes_match=False),
        lambda d: d["apis"][0].update(ready=False),
        lambda d: d["apis"][0]["settings"].update(DB_POOL_MAX="8"),
        lambda d: d["apis"][0]["settings"].update(API_PAYMENT_POOL_MAX="3"),
        lambda d: d["apis"][0].update(database_url_host="rds"),
        lambda d: d["apis"][2].update(database_url_host="10.0.0.2"),
        lambda d: d["apis"][2].update(pgbouncer_host_role="secondary"),
        lambda d: d["apis"][2].update(auth_secrets_sha256="b" * 64),
        lambda d: d["baseline_authorities"].update(redis_authority_sha256="b" * 64),
        lambda d: d["apis"][1].update(port=8101),
        lambda d: d["apis"][1].update(container_id=f"{1:064x}"),
        lambda d: d.update(generator_idle=False),
        lambda d: d.update(global_queues_zero=False),
        lambda d: d.update(arm="control"),
        lambda d: d.update(captured_at=(NOW - timedelta(seconds=301)).isoformat()),
        lambda d: d.update(captured_at=(NOW + timedelta(seconds=1)).isoformat()),
        lambda d: d.update(captured_at="2026-10-05T00:00:00"),
        lambda d: d["apis"].pop(),
    ],
)
def test_unsafe_or_confounded_inventory_cannot_prepare(mutate):
    data = inventory()
    mutate(data)
    with pytest.raises(ValueError):
        module.validate_inventory(data, image_id=IMAGE, now=NOW)


@pytest.mark.parametrize(
    "address", ["0.0.0.0", "127.0.0.1", "169.254.1.1", "8.8.8.8", "192.0.2.1", "::1", "10.0.0.1;evil"]
)
def test_only_private_backend_addresses(address):
    with pytest.raises(ValueError):
        module.private_ipv4(address)


def test_no_retry_explicit_four_endpoint_routing():
    config = module.nginx_config(inventory()["apis"])
    assert config.count("        server 10.") == 4
    assert "proxy_next_upstream off;" in config
    assert "least_conn;" in config and "keepalive_timeout 5s;" in config
    data = inventory()["apis"]
    data[0]["port"] = "8101; evil"
    with pytest.raises(ValueError):
        module.nginx_config(data)


def test_secondary_never_adds_database_build_or_workers():
    data = json.loads(module.secondary_compose())
    assert set(data["services"]) == {"api"}
    api = data["services"]["api"]
    assert api["pull_policy"] == "never"
    assert "build" not in api and "depends_on" not in api
    assert api["environment"]["DB_POOL_MAX"] == "4"
    assert "${API_PRIVATE_BIND_IP:?" in api["ports"][0]


def test_fresh_private_bundle_does_not_claim_run_and_preserves_existing(monkeypatch, tmp_path):
    monkeypatch.setattr(module, "ROOT", tmp_path)
    output = tmp_path / "tmp" / "prepared"
    plan = module.prepare(output)
    assert plan["paid_runs_started"] == 0
    assert plan["inventory_summary"] is None
    assert len(plan["required_paid_gates"]) == 22
    assert not list(output.glob("nginx.*.conf"))
    before = (output / "plan.json").read_bytes()
    with pytest.raises(ValueError):
        module.prepare(output)
    assert (output / "plan.json").read_bytes() == before
    with pytest.raises(ValueError):
        module.prepare(tmp_path / "outside")


def test_missing_and_failed_gates_cannot_be_qualified():
    paid = dict.fromkeys(json.loads(module.BASELINE.read_text())["required_gates"], True)
    extra = dict.fromkeys(module.EXTRA_GATES, True)
    assert module.evaluate_gates(paid, extra)["all_required_gates_pass"]
    paid["customer_load"] = False
    assert module.evaluate_gates(paid, extra)["failed_gates"] == ["customer_load"]
    paid["customer_load"] = 1
    assert not module.evaluate_gates(paid, extra)["all_required_gates_pass"]
    paid.pop("zero_double_booking")
    with pytest.raises(ValueError):
        module.evaluate_gates(paid, extra)
    paid["zero_double_booking"] = True
    extra.pop("all_four_replicas_observed")
    with pytest.raises(ValueError):
        module.evaluate_gates(paid, extra)
