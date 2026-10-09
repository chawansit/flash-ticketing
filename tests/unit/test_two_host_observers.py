import copy
import importlib.util
import io
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from test_two_host_scaling_preparation import IMAGE, NOW, inventory

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cpu = load("observe_two_host_cpu")
pipeline = load("observe_two_host_pipeline")


def payload(count=10, start=1000, *, second_route=0):
    return (
        f"process_start_time_seconds {start}\n"
        f'ticketing_http_requests_total{{method="GET",route="/v1/orders/{{order_id}}",status="200"}} {count}\n'
        f'ticketing_http_requests_total{{method="GET",route="/v1/shows/{{id}}/seats",status="200"}} {second_route}\n'
        'ticketing_http_requests_total{method="GET",route="/health/ready",status="200"} 999\n'
    )


def test_wrapper_keeps_frozen_financial_and_worker_functions_and_one_scrape():
    frozen = pipeline.load_frozen(SCRIPTS / "observe_paid_pipeline.py")
    sample, workers = frozen.sample, frozen.worker_counters
    seen = []

    def original(host="api", port=8000):
        seen.append((host, port))
        return ["worker-only"]

    frozen.api_replicas = original
    urls = []

    def fetch(url, **kwargs):
        urls.append(url)
        return io.BytesIO(payload().encode())

    routes = pipeline.install_adapter(frozen, inventory(), image_id=IMAGE, now=NOW, fetch=fetch)
    assert frozen.sample is sample and frozen.worker_counters is workers
    assert frozen.api_replicas("consumer", 9101) == ["worker-only"]
    assert seen == [("consumer", 9101)]
    assert len(frozen.api_replicas()) == 4
    for label, entry in routes.items():
        metric = frozen.api_metrics(label)
        assert metric["business_http_requests_total"] == 10
        assert urls[-1] == f"http://{entry['private_ipv4']}:{entry['port']}/metrics"
    assert len(urls) == 4


@pytest.mark.parametrize(
    "candidate", [payload(start=1001), payload(count=9), payload(count=9, second_route=100)]
)
def test_restart_or_individual_counter_reset_remains_failed(candidate):
    frozen = pipeline.load_frozen(SCRIPTS / "observe_paid_pipeline.py")
    responses = iter([payload().encode(), candidate.encode(), candidate.encode()])

    def fetch(*args, **kwargs):
        return io.BytesIO(next(responses))

    pipeline.install_adapter(frozen, inventory(), image_id=IMAGE, now=NOW, fetch=fetch)
    label = frozen.api_replicas()[0]
    frozen.api_metrics(label)
    for _ in range(2):
        with pytest.raises(ValueError, match="restart or counter reset"):
            frozen.api_metrics(label)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "process_start_time_seconds 0",
        "process_start_time_seconds NaN",
        "process_start_time_seconds inf",
        payload(count=-1),
        payload() + "process_start_time_seconds 1000\n",
    ],
)
def test_missing_or_invalid_process_metrics_rejected(raw):
    with pytest.raises(ValueError):
        pipeline.extra_api_metrics(raw)


def test_idle_replica_has_zero_business_requests():
    assert pipeline.extra_api_metrics("process_start_time_seconds 1000")["business_http_requests_total"] == 0


def test_frozen_source_modification_rejected_before_import(tmp_path):
    changed = tmp_path / "observer.py"
    changed.write_text("raise AssertionError('must not import')")
    with pytest.raises(ValueError, match="source differs"):
        pipeline.load_frozen(changed)


def test_large_metric_payload_rejected():
    frozen = pipeline.load_frozen(SCRIPTS / "observe_paid_pipeline.py")
    pipeline.install_adapter(
        frozen,
        inventory(),
        image_id=IMAGE,
        now=NOW,
        fetch=lambda *args, **kwargs: io.BytesIO(b"a" * (pipeline.MAX_PAYLOAD + 1)),
    )
    with pytest.raises(ValueError, match="exceeds bound"):
        frozen.api_metrics(frozen.api_replicas()[0])


def test_stale_inventory_never_installs_adapter():
    frozen = pipeline.load_frozen(SCRIPTS / "observe_paid_pipeline.py")
    original = frozen.api_replicas
    with pytest.raises(ValueError):
        pipeline.install_adapter(frozen, inventory(), image_id=IMAGE, now=NOW + timedelta(seconds=301))
    assert frozen.api_replicas is original


def cpu_data(role="primary", arm="candidate"):
    count = 2 if arm == "candidate" else (4 if role == "primary" else 0)
    entries = [
        {"id": str(i + 1) * 64, "role": "api", "pid": 100 + i, "process_start_ticks": 42}
        for i in range(count)
    ]
    rows = [
        {
            "utc": (NOW + timedelta(seconds=i * 5)).isoformat(),
            "elapsed_seconds": i * 5,
            "host_ticks": [i * 100, 0, 0, i * 100, 0, 0, 0, 0],
            "container_cpu_usec": {e["id"]: i * 5_000_000 for e in entries},
        }
        for i in range(4)
    ]
    return {
        "schema": 1,
        "host_role": role,
        "arm": arm,
        "instance_uuid_sha256": ("a" if role == "primary" else "b") * 64,
        "requested_start_utc": NOW.isoformat(),
        "seconds": 15,
        "interval": 5,
        "containers": entries,
        "samples": rows,
    }


def test_cpu_comparison_preserves_host_percentages_sums_only_api_cores():
    primary, secondary = [cpu.summarize(cpu_data(role)) for role in ("primary", "secondary")]
    report = cpu.compare_windows(
        primary,
        secondary,
        offered_start_utc=NOW.isoformat(),
        offered_end_utc=(NOW + timedelta(seconds=15)).isoformat(),
    )
    assert report["aggregate_api_cpu_cores"] == 4
    assert report["host_cpu_percent"] == {"primary": 50, "secondary": 50}
    assert report["same_offered_measurement_window"]


def test_control_secondary_is_host_only():
    data = cpu_data("secondary", "control")
    assert cpu.validate_spec(data) == []
    assert cpu.summarize(data)["cpu_cores_by_role"] == {}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["samples"].pop(),
        lambda d: d["samples"][1].update(collection_error="restarted"),
        lambda d: d["samples"][1].update(elapsed_seconds=7),
        lambda d: d["samples"][1].update(utc=NOW.isoformat()),
        lambda d: d["samples"][1]["host_ticks"].__setitem__(0, True),
        lambda d: d["samples"][2]["host_ticks"].__setitem__(0, 0),
        lambda d: d["samples"][2]["container_cpu_usec"].update({"1" * 64: 0}),
        lambda d: d["samples"][2]["container_cpu_usec"].clear(),
        lambda d: d.update(seconds=301),
        lambda d: d.update(instance_uuid_sha256="unknown"),
        lambda d: d["containers"].append(copy.deepcopy(d["containers"][0])),
    ],
)
def test_incomplete_reset_clock_skewed_cpu_cannot_pass(mutate):
    data = cpu_data()
    mutate(data)
    with pytest.raises(ValueError):
        cpu.summarize(data)


def test_secondary_background_service_rejected():
    data = cpu_data("secondary")
    data["containers"].append({"id": "e" * 64, "role": "pgbouncer"})
    with pytest.raises(ValueError, match="API-only"):
        cpu.validate_spec(data)


@pytest.mark.parametrize("change", ["identity", "start", "offered", "failed"])
def test_cpu_comparison_rejects_colocation_mismatched_or_unoffered_window(change):
    primary, secondary = [cpu.summarize(cpu_data(role)) for role in ("primary", "secondary")]
    offered = NOW
    if change == "identity":
        secondary["instance_uuid_sha256"] = primary["instance_uuid_sha256"]
    elif change == "start":
        secondary["start_utc"] = (NOW + timedelta(seconds=3)).isoformat()
    elif change == "offered":
        offered += timedelta(seconds=3)
    else:
        secondary["pass"] = False
    with pytest.raises(ValueError):
        cpu.compare_windows(
            primary,
            secondary,
            offered_start_utc=offered.isoformat(),
            offered_end_utc=(NOW + timedelta(seconds=15)).isoformat(),
        )


def test_proc_start_tick_uses_final_parenthesis(monkeypatch):
    def read(path):
        return "999 (worker name (special)) " + " ".join(["S", *[str(i) for i in range(1, 20)]])

    monkeypatch.setattr(Path, "read_text", read)
    assert cpu.proc_identity(999) == 19


def distribution_rows():
    labels = [f"{a['host_role']}:{a['container_id']}" for a in inventory()["apis"]]
    return [
        {
            "utc": (NOW + timedelta(seconds=i)).isoformat(),
            "api_replicas": {
                label: {"process_start_time_seconds": 1000, "business_http_requests_total": i * (j + 1)}
                for j, label in enumerate(labels)
            },
        }
        for i in range(4)
    ]


def test_distribution_uses_same_offered_window_and_requires_all_replicas():
    result = pipeline.summarize_distribution(
        distribution_rows(),
        inventory(),
        offered_start_utc=NOW.isoformat(),
        offered_end_utc=(NOW + timedelta(seconds=3)).isoformat(),
    )
    assert sum(result["business_request_deltas"].values()) == 30
    assert sum(result["business_request_shares"].values()) == pytest.approx(1)


@pytest.mark.parametrize("problem", ["missing", "restart", "reset", "no_traffic", "gap", "late"])
def test_distribution_missing_restart_starvation_or_window_gap_cannot_pass(problem):
    rows = distribution_rows()
    label = next(iter(rows[1]["api_replicas"]))
    if problem == "missing":
        rows[1]["api_replicas"].pop(label)
    elif problem == "restart":
        rows[1]["api_replicas"][label]["process_start_time_seconds"] = 1001
    elif problem == "reset":
        rows[2]["api_replicas"][label]["business_http_requests_total"] = 0
    elif problem == "no_traffic":
        for row in rows:
            row["api_replicas"][label]["business_http_requests_total"] = 0
    elif problem == "gap":
        rows = [rows[0], rows[3]]
    else:
        rows.pop(0)
    with pytest.raises(ValueError):
        pipeline.summarize_distribution(
            rows,
            inventory(),
            offered_start_utc=NOW.isoformat(),
            offered_end_utc=(NOW + timedelta(seconds=3)).isoformat(),
        )


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_fixed_two_host_runner_emits_valid_specs_without_relabeling_factor(arm):
    from run_two_host_paid_comparison import cpu_spec

    summaries = []
    for role in ("primary", "secondary"):
        data = cpu_data(role, "candidate")
        rows = [
            {"Id": e["id"], "Config": {"Labels": {"com.docker.compose.service": e["role"]}}}
            for e in data["containers"]
        ]
        observed = {"hosts": {role: {"machine_id_sha256": data["instance_uuid_sha256"]}}}
        spec = cpu_spec(arm, role, rows, observed, fixed_two_host=True)
        assert spec["arm"] == arm and spec["placement"] == "two-plus-two"
        cpu.validate_spec(spec)
        data.update(arm=arm, placement=spec["placement"])
        summary = cpu.summarize(data)
        assert summary["placement"] == "two-plus-two" and summary["arm"] == arm
        summaries.append(summary)
    assert (
        cpu.compare_windows(
            *summaries,
            offered_start_utc=NOW.isoformat(),
            offered_end_utc=(NOW + timedelta(seconds=15)).isoformat(),
        )["aggregate_api_cpu_cores"]
        == 4
    )


@pytest.mark.parametrize("role", ["primary", "secondary"])
def test_legacy_control_spec_keeps_four_primary_and_zero_secondary(role):
    from run_two_host_paid_comparison import cpu_spec

    data = cpu_data(role, "control")
    rows = [
        {"Id": e["id"], "Config": {"Labels": {"com.docker.compose.service": e["role"]}}}
        for e in data["containers"]
    ]
    observed = {"hosts": {role: {"machine_id_sha256": data["instance_uuid_sha256"]}}}
    spec = cpu_spec("control", role, rows, observed)
    assert "placement" not in spec and cpu.placement(spec) == "four-primary"
    assert len(cpu.validate_spec(spec)) == (4 if role == "primary" else 0)


@pytest.mark.parametrize(
    "defect", ["missing", "count", "unknown", "four-primary-candidate", "secondary-worker"]
)
def test_fixed_two_host_spec_rejects_topology_drift_before_launch(defect):
    from run_two_host_paid_comparison import cpu_spec

    data = cpu_data("secondary", "candidate")
    rows = [
        {"Id": e["id"], "Config": {"Labels": {"com.docker.compose.service": e["role"]}}}
        for e in data["containers"]
    ]
    observed = {"hosts": {"secondary": {"machine_id_sha256": data["instance_uuid_sha256"]}}}
    if defect == "missing":
        rows = []
    if defect == "count":
        rows.pop()
    if defect == "secondary-worker":
        rows[0]["Config"]["Labels"]["com.docker.compose.service"] = "consumer"
    if defect in {"missing", "count", "secondary-worker"}:
        with pytest.raises(ValueError):
            cpu_spec("control", "secondary", rows, observed, fixed_two_host=True)
    else:
        data["placement"] = "unknown" if defect == "unknown" else "four-primary"
        with pytest.raises(ValueError):
            cpu.validate_spec(data)


def test_cpu_comparison_rejects_same_factor_with_different_physical_placement():
    primary = cpu.summarize(cpu_data("primary", "control"))
    data = cpu_data("secondary", "candidate")
    data.update(arm="control", placement="two-plus-two")
    secondary = cpu.summarize(data)
    with pytest.raises(ValueError, match="Distinct matched host roles"):
        cpu.compare_windows(
            primary,
            secondary,
            offered_start_utc=NOW.isoformat(),
            offered_end_utc=(NOW + timedelta(seconds=15)).isoformat(),
        )
