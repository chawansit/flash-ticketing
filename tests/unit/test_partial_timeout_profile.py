"""Frozen-source and sole-factor safeguards; no cloud or customer requests."""
import copy
import json
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import partial_timeout_profile as policy
import prepare_partial_timeout_reclamation as export
from test_two_host_deployment import fixture
from two_host_topology import deployment_model, snapshot

ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "docs/capacity/flash-sale-opening/partial-timeout-comparison-plan-2026-10-06.json"


def profile(arm="control"):
    plan = json.loads(PLAN.read_text())
    return policy.PartialTimeoutProfile(plan["artifact_receipt"], arm, plan["expected_runtime_source_sha256"])


def test_only_api_reclamation_differs_in_primary_and_secondary_models():
    config, rows = fixture()
    saved = snapshot(config, rows, image_id=export.base.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    off, on = [profile(arm).primary_model(model) for arm in ("control", "candidate")]
    assert off["services"]["api"]["environment"]["API_PARTIAL_TIMEOUT_RECLAIM"] == "0"
    off["services"]["api"]["environment"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    assert off == on
    assert on["services"]["api"]["environment"]["PAYMENT_CONFIRMATION_ASYNC"] == "0"
    assert on["services"]["api"]["environment"]["API_CALLBACK_ACQUISITION_RESERVE"] == "0"
    assert on["services"]["simulator"]["environment"]["DB_POOL_MAX"] == "10"
    assert on["services"]["confirmation"]["environment"]["DB_POOL_MAX"] == "2"
    assert on["services"]["confirmation"]["command"] == ["python", "-m", "ticketing.workers", "confirmation"]
    secondary = {"services": {"api": copy.deepcopy(model["services"]["api"])}}
    left, right = [profile(arm).secondary_model(secondary) for arm in ("control", "candidate")]
    left["services"]["api"]["environment"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    assert left == right
    for c in (profile(), profile("candidate")):
        assert {k: c.api_settings[k] for k in ("DB_POOL_MAX", "DB_POOL_MAX_WAITING", "DB_POOL_WAIT_MS", "API_PAYMENT_POOL_MAX")} == {
            "DB_POOL_MAX": "4", "DB_POOL_MAX_WAITING": "12", "DB_POOL_WAIT_MS": "500", "API_PAYMENT_POOL_MAX": "2"}


@pytest.mark.parametrize("role", list(policy.PartialTimeoutProfile.background))
def test_background_settings_identical_explicit_and_reclamation_disabled(role):
    off, on = profile(), profile("candidate")
    assert off.settings(role) == on.settings(role)
    assert on.settings(role)["API_PARTIAL_TIMEOUT_RECLAIM"] == "0"
    assert on.settings(role)["PAYMENT_CONFIRMATION_ASYNC"] == ("1" if role == "confirmation" else "0")
    settings = on.settings(role)
    on.verify_worker_settings(role, settings)
    settings.pop("API_PARTIAL_TIMEOUT_RECLAIM")
    with pytest.raises(ValueError, match="Explicit"):
        on.verify_worker_settings(role, settings)


@pytest.mark.parametrize("change", ["async", "missing_flag", "pool", "waiters", "reserve", "missing_api", "marker"])
def test_inventory_rejects_missing_factor_and_budget_drift(monkeypatch, change):
    c = profile("candidate")
    # Structural worker checks have their existing tests; isolate the new exact API factor check.
    monkeypatch.setattr(policy.StatusRefreshContract, "verify_inventory", lambda *_: None)
    data = {"apis": [{"settings": dict(c.api_settings)} for _ in range(4)], "status_refresh_contract": c.inventory_marker()}
    c.verify_inventory(data)
    keys = {"async": ("PAYMENT_CONFIRMATION_ASYNC", "1"), "pool": ("DB_POOL_MAX", "5"),
            "waiters": ("DB_POOL_MAX_WAITING", "13"), "reserve": ("API_CALLBACK_ACQUISITION_RESERVE", "1")}
    if change in keys:
        key, value = keys[change]
        data["apis"][0]["settings"][key] = value
    elif change == "missing_flag":
        data["apis"][0]["settings"].pop("API_PARTIAL_TIMEOUT_RECLAIM")
    elif change == "missing_api":
        data["apis"].pop()
    else:
        data["status_refresh_contract"]["partial_timeout_reclaim"] = "0"
    with pytest.raises(ValueError):
        c.verify_inventory(data)


@pytest.mark.parametrize("change", ["runtime", "source_manifest", "parent", "confirmation_image"])
def test_constructor_rejects_unpinned_source_images_and_parent(change):
    plan = json.loads(PLAN.read_text()); artifact = copy.deepcopy(plan["artifact_receipt"])
    sources = dict(plan["expected_runtime_source_sha256"])
    if change == "runtime":
        sources["src/ticketing/config.py"] = "a" * 64
    elif change == "source_manifest":
        artifact["source_manifest_sha256"] = "a" * 64
    elif change == "parent":
        artifact["parent_images"]["api"] = "sha256:" + "a" * 64
    else:
        artifact["images"]["confirmation"] = "sha256:" + "a" * 64
    with pytest.raises(ValueError):
        policy.PartialTimeoutProfile(artifact, "control", sources)


@pytest.fixture
def owned_source(tmp_path, monkeypatch):
    expected = export.expected_source_map()
    plan = json.loads(PLAN.read_text()); original = ROOT / plan["source_directory"]
    folder = tmp_path / "tmp" / "owned"
    shutil.copytree(original.parent, folder, ignore=shutil.ignore_patterns("build-*", "preflight-*", "image-context"))
    monkeypatch.setattr(export.base, "ROOT", tmp_path)
    monkeypatch.setattr(export, "expected_source_map", lambda: expected)
    return folder / "source"


@pytest.mark.parametrize("change", ["runtime", "extra", "dependency_resealed", "proof", "runtime_digest"])
def test_full_source_and_receipt_tampering_rejected(owned_source, change):
    policy.source_contract(owned_source)
    receipt_path = owned_source.parent / "admission-source-receipt.json"
    receipt = json.loads(receipt_path.read_text())
    if change == "runtime":
        (owned_source / "src/ticketing/config.py").write_text("# drift")
    elif change == "extra":
        (owned_source / "unqualified.py").write_text("# extra")
    elif change == "dependency_resealed":
        p = owned_source / "pyproject.toml"; p.write_text("# changed dependencies")
        receipt["source_file_sha256"]["pyproject.toml"] = export.base.sha(p.read_bytes())
    elif change == "proof":
        receipt["qualified_method_ast_parity"] = False
    else:
        receipt["source_manifest_sha256"] = "a" * 64
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        policy.source_contract(owned_source)


@pytest.mark.parametrize("change", ["schema_bool", "parent", "protected", "extra_overlay", "escaped_patch"])
def test_manifest_and_overlay_target_drift_rejected(tmp_path, monkeypatch, change):
    folder = tmp_path / "artifact"; shutil.copytree(export.ARTIFACTS, folder)
    monkeypatch.setattr(export, "ARTIFACTS", folder)
    p = folder / "manifest.json"; m = json.loads(p.read_text())
    if change == "schema_bool":
        m["schema"] = True
    elif change == "parent":
        m["parent_manifest_sha256"] = "a" * 64
    elif change == "protected":
        m["protected_source_sha256"]["src/ticketing/infrastructure/cache.py"] = "a" * 64
    elif change == "extra_overlay":
        m["overlay_sha256"]["../extra.py"] = "a" * 64
    else:
        patch = folder / "adr0163.patch"
        patch.write_text(patch.read_text().replace("+++ b/src/ticketing/api.py", "+++ b/../../escape.py"))
        m["patch_sha256"] = export.base.sha(patch.read_bytes())
    p.write_text(json.dumps(m))
    with pytest.raises(ValueError):
        export.manifest()


def test_profile_is_offline_and_does_not_reopen_consumed_scope():
    plan = json.loads(PLAN.read_text())
    assert plan["cloud_execution_implemented"] is False
    assert plan["cloud_calls"] == 0 and plan["prior_scope_reopened"] is False
    assert plan["fresh_scope_required"] is True
    assert "complete_global_queues_and_Kafka_drain" in plan["mandatory_future_gates"]


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_complete_inventory_uses_existing_full_placement_and_identity_validation(arm):
    from prepare_two_host_scaling import validate_inventory
    from test_status_refresh_comparison import observed

    c = profile(arm)
    data = observed(c)
    data["background"] = copy.deepcopy(c.background)
    for api in data["apis"]:
        api["settings"] = c.api_settings.copy()
    data["worker_sources"].append({"container_id": "f" * 64, "role": "confirmation",
                                  "image_id": c.images["confirmation"],
                                  "source_identity": {"source_hashes_match": True},
                                  "settings": c.settings("confirmation")})
    data["status_refresh_contract"] = c.inventory_marker()
    assert validate_inventory(data, image_id=c.images["api"], contract=c)["inventory_contract_pass"]
    data["apis"][0]["settings"]["PAYMENT_CONFIRMATION_ASYNC"] = "1"
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)
