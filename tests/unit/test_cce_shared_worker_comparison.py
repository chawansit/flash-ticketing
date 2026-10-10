import copy

import pytest
from cce_shared_worker_comparison import (
    COUNTS,
    GATES,
    IMAGE,
    arm_binding,
    inactive_candidate,
    match_bindings,
    plan,
    require_passed_control,
)


def hashes():
    return {role: "1" * 64 for role in COUNTS}


def test_matched_budget_and_image():
    value = plan()
    assert value["maximum_pods"] == sum(COUNTS.values()) == 17
    assert value["pgbouncer_server_connections"] == 24
    assert value["requested_vcpu"] == 7.25 and value["requested_memory_gib"] == 14.5
    assert value["scheduled_journeys"] == value["offered_journeys_per_second"] * value["offered_seconds"]
    assert match_bindings(arm_binding("control", hashes()), arm_binding("candidate", hashes()))


def test_role_environment_drift_rejects_comparison():
    changed = hashes(); changed["simulator"] = "2" * 64
    with pytest.raises(ValueError, match="behavior changed"):
        match_bindings(arm_binding("control", hashes()), arm_binding("candidate", changed))


@pytest.mark.parametrize("change", ["unknown_role", "missing_writer", "bad_hash"])
def test_incomplete_or_extra_configuration_rejected(change):
    values = hashes()
    if change == "unknown_role": values["confirmaton"] = "1" * 64
    elif change == "missing_writer": values.pop("reservation-writer")
    else: values["consumer"] = "latest"
    with pytest.raises(ValueError, match="Complete sealed"):
        arm_binding("control", values)


def test_preview_stays_inactive_and_preserves_benchmark_routes_and_settings():
    value = inactive_candidate("ticketing-placement-preview", "10.1.137.69")
    deployments = [item for item in value["items"] if item["kind"] == "Deployment"]
    assert len(deployments) == 8 and all(item["spec"]["replicas"] == 0 for item in deployments)
    for item in deployments:
        pod = item["spec"]["template"]["spec"]
        container = pod["containers"][0]
        role = container["name"]
        assert container["image"] == IMAGE
        env = {item["name"]: item.get("value") for item in container["env"]}
        assert env["ORDER_STATUS_EVENT_REFRESH"] == ("1" if role == "consumer" else "0")
        if role == "api":
            assert container["command"][-2:] == ["--timeout-keep-alive", "10"]
            assert container["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
        else:
            assert pod["hostAliases"] == [{"ip": "10.1.137.69", "hostnames": ["kafka"]}]
        if role == "simulator":
            assert env["API_URL"] == "http://10.1.137.69:8000"
            assert env["SIMULATOR_CONCURRENCY"] == "12" and env["DB_POOL_MAX"] == "10"
        if role == "reservation-writer":
            assert env["RESERVATION_WRITE_PIPELINE"] == "1"
        if role == "confirmation":
            assert item["metadata"]["annotations"]["ticketing/suggested-test-replicas"] == "0"


@pytest.mark.parametrize("address", ["127.0.0.1", "8.8.8.8", "::1", "0.0.0.0", "169.254.1.1"])
def test_invalid_private_broker_route_rejected(address):
    with pytest.raises(ValueError, match="Private primary"):
        inactive_candidate("ticketing-placement-preview", address)


def passed_control():
    binding = arm_binding("control", hashes())
    return binding, {"comparison_binding": binding, "pass": True, "capacity_stages_started": 1,
                     "restoration_complete": True, "integrity_verified": True,
                     "transport_credentials_cleared": True,
                     "native": {"cleanup_complete": True, "measurement_gates": dict.fromkeys(GATES, True)}}


def test_complete_current_control_admits_candidate():
    binding, report = passed_control()
    assert set(require_passed_control(report, binding)) == {"control_result_sha256", "control_binding_sha256"}


@pytest.mark.parametrize("gate", GATES)
def test_any_failed_control_gate_blocks_candidate(gate):
    binding, report = passed_control()
    report["native"]["measurement_gates"][gate] = False
    with pytest.raises(ValueError, match="Fresh fully passing"):
        require_passed_control(report, binding)


def test_old_hourly_result_or_unrestored_control_cannot_be_used():
    binding, report = passed_control()
    stale = copy.deepcopy(report); stale.pop("comparison_binding")
    with pytest.raises(ValueError): require_passed_control(stale, binding)
    report["restoration_complete"] = False
    with pytest.raises(ValueError): require_passed_control(report, binding)


def test_public_plan_and_inactive_preview_match_executable_contract():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    assert json.loads((root / "docs/capacity/cce/shared-worker-placement-consumer-restored-plan-2026-10-10.json").read_text()) == plan()
    from cce_shared_worker_comparison import digest

    assert plan()["inactive_preview_sha256"] == digest(inactive_candidate("ticketing-placement-preview", "10.1.137.69"))


def test_corrected_control_binding_rejects_original_shared_image():
    import json
    from pathlib import Path
    binding, report = passed_control()
    old = json.loads((Path(__file__).resolve().parents[2] / "docs/capacity/cce/shared-worker-placement-plan-2026-10-10.json").read_text())
    binding["image"] = old["image"]
    with pytest.raises(ValueError, match="Current sealed"):
        require_passed_control(report, binding)


def test_corrected_receipt_is_explicit_and_only_changes_consumer_module():
    import json
    from pathlib import Path

    from cce_shared_worker_comparison import selected_receipt
    corrected = selected_receipt()
    old = json.loads((Path(__file__).resolve().parents[2] / "docs/capacity/cce/shared-application-image-2026-10-10.json").read_text())
    assert corrected["decision"] == "ADR0261"
    assert corrected["registry_image"] != old["registry_image"]
    assert {key for key in corrected["runtime_source_sha256"] if corrected["runtime_source_sha256"][key] != old["runtime_source_sha256"][key]} == {"src/ticketing/workers.py"}


def test_corrected_receipt_tampering_is_rejected(monkeypatch, tmp_path):
    import cce_shared_worker_comparison as comparison
    copy = tmp_path / "receipt.json"
    copy.write_bytes(comparison.RECEIPT.read_bytes() + b" ")
    monkeypatch.setattr(comparison, "RECEIPT", copy)
    with pytest.raises(ValueError, match="receipt"):
        comparison.selected_receipt()


def test_remote_worker_identity_imports_without_repository_artifacts(tmp_path):
    import json
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    import cce_shared_worker_image_identity as pins
    from cce_shared_worker_comparison import selected_receipt

    root = Path(__file__).resolve().parents[2]
    for name in ("cce_worker_identity.py", "cce_shared_worker_image_identity.py"):
        shutil.copyfile(root / "scripts" / name, tmp_path / name)
    program = "import json; import cce_worker_identity as i; print(json.dumps([i.SOURCE,i.CONFIG,i.ECS_IMAGE_ID,i.MANIFEST]))"
    result = subprocess.run([sys.executable, "-I", "-c", "import sys;sys.path.insert(0," + repr(str(tmp_path)) + ");" + program], cwd=tmp_path, capture_output=True, text=True, check=True)
    receipt = selected_receipt()
    assert json.loads(result.stdout) == [receipt["source_identity_sha256"], receipt["registry_configuration_digest"], receipt["local_image_id"], receipt["registry_manifest_digest"]]
    assert pins.IMAGE == receipt["registry_image"]
