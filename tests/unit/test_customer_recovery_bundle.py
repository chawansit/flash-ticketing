import importlib.util
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import cce_customer_recovery_profile as profile
import cce_paid_stage as stage
import customer_recovery_bundle as bundle
import pytest


def test_sealed_overlay_preserves_every_other_parent_file():
    parent = stage.generator_bundle()
    candidate, coordinator = bundle.qualify(parent)
    assert len(parent) == 78 and len(candidate) == 80
    assert candidate["scripts/checkout_journey_probe.py"] != parent["scripts/checkout_journey_probe.py"]
    assert all(candidate[n] == raw for n, raw in parent.items() if n not in bundle.OVERLAYS)
    spec = importlib.util.spec_from_file_location("recovery_coordinator", bundle.policy.ROOT / bundle.COORDINATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source = bundle.policy.ROOT / "scripts/paid_ticket_sharded_generator.py"
    now = time.time()
    loaded = module.load(source, now + 10, now=now)
    assert loaded.time() == now + 2
    assert b"--recovery-max-attempts" in candidate["scripts/paid_ticket_sharded_generator.py"]
    assert bundle.sha(coordinator) == "263dd15d47441c8db6a9eea7067b6a6c3512da75d1f1dce2ae09813b5afef275"


def test_overlay_rejects_parent_drift():
    parent = stage.generator_bundle()
    parent["scripts/prepare_capacity_fixture.py"] += b"# drift"
    with pytest.raises(ValueError, match="parent"):
        bundle.qualify(parent)


def test_overlay_rejects_changed_customer_source(monkeypatch):
    raw = bundle.raw
    monkeypatch.setattr(bundle, "raw", lambda name: raw(name) + (b"# drift" if name.endswith("client.py") else b""))
    with pytest.raises(ValueError, match="source drift"):
        bundle.qualify(stage.generator_bundle())


def test_unpublished_image_cannot_start_cloud_profile():
    assert profile.receipt(require_registry=False)["registry_published"] is False
    with pytest.raises(ValueError, match="Published and registry-pulled"):
        profile.receipt()


def test_same_budget_plan_and_single_candidate_factor(monkeypatch):
    local = profile.receipt(require_registry=False)
    monkeypatch.setattr(profile, "receipt", lambda **kw: local)
    goal = {"extension_decision": "ADR0255", "comparison_arm": "control", "decision": "ADR0228",
            "profile": "cce_paid_comparison", "acquisition_budget": 20,
            "candidate_factor": {"name": profile.FACTOR, "comparison_arm": "control", "baseline": "0", "candidate": "1",
                "image_pair_receipt_sha256": profile.PROOF_SHA256, "recovery_bundle_sha256": bundle.MANIFEST_SHA256,
                "recovery_max_attempts": 3, "database_connections_unchanged": True}}
    plan = profile.plan(goal)
    assert plan["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert (plan["replicas"], plan["connections_per_api"], plan["pooler_server_connections"],
            plan["acquisition_budget"], plan["simulator_concurrency"], plan["simulator_database_pool_max"]) == (4, 4, 24, 20, 12, 10)
    assert plan["single_changed_factor"] == "order_status_read_pipeline"
    with pytest.raises(ValueError, match="Exact recovery"):
        profile.active({**goal, "acquisition_budget": 21})


def test_recovery_cli_is_explicit_and_never_hourly():
    from cce_paid_profiles import HOURLY
    path = "/root/flash-ticketing/tmp/adr0151-aaaaaaaaaaaa-cce-candidate"
    origin = "http://10.1.137.69:8000"
    assert "--recovery-max-attempts" not in stage.generator_arguments(path, origin, 0)
    assert stage.generator_arguments(path, origin, 0, recovery_max_attempts=3)[-2:] == ["--recovery-max-attempts", "3"]
    with pytest.raises(ValueError, match="short comparison"):
        stage.generator_arguments(path, origin, 0, profile=HOURLY, recovery_max_attempts=3)



def test_remote_generator_imports_from_only_transferred_files(tmp_path):
    import os
    import subprocess
    import sys
    candidate, coordinator = bundle.qualify(stage.generator_bundle())
    names = ("checkout_journey_probe.py", "paid_ticket_load_generator.py", "paid_ticket_sharded_generator.py",
             "customer_recovery_client.py", "paid_fixture_layout.py")
    for name in names:
        (tmp_path / name).write_bytes(candidate["scripts/" + name])
    (tmp_path / "run_synchronized_paid_generator.py").write_bytes(coordinator)
    clean = dict(os.environ)
    clean.pop("PYTHONPATH", None)
    for name in ("paid_ticket_load_generator.py", "paid_ticket_sharded_generator.py"):
        result = subprocess.run([sys.executable, "-E", str(tmp_path / name), "--help"],
                                cwd=tmp_path, env=clean, capture_output=True, text=True, timeout=10, check=False)
        assert result.returncode == 0, result.stderr
        assert "--recovery-max-attempts" in result.stdout
