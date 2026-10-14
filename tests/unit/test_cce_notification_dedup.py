"""Offline candidate activation and fixed-budget admission; no cloud publication mocked as real."""
import base64
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_native_workers as native
import cce_notification_dedup_profile as profile
import cce_recovery_headroom as headroom
import cce_transaction_profile as transaction
import work_envelope as policy
from cce_committed_status_contract import CommittedStatusContract
from cce_paid_profiles import HEADROOM
from test_cce_writer_sale_context import goal as parent_goal


def prepared():
    value = policy.read(policy.ROOT / profile.pins.RECEIPT)
    return {**value, "registry_image": "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + profile.pins.IMAGE_ID,
            "runtime_sources_sha256": value["runtime_source_sha256"]}


def goal():
    value = parent_goal()
    for key in ("writer_sale_context_decision", "writer_sale_context_receipt_sha256",
                "writer_sale_context_contract_sha256"):
        value.pop(key)
    value.update(notification_dedup_decision="ADR0322", notification_dedup_receipt_sha256=profile.pins.RECEIPT_SHA256,
                 notification_dedup_contract_sha256=policy.digest(profile.contract()), baseline_sha256=profile.BASELINE,
                 headroom_contract_sha256=policy.digest(headroom.contract()))
    return value


def test_unpublished_receipt_is_rejected_before_cloud_admission(monkeypatch, tmp_path):
    value = policy.read(policy.ROOT / profile.pins.RECEIPT)
    value["registry_published"] = False
    path = tmp_path / "unpublished.json"
    raw = (json.dumps(value, indent=2) + "\n").encode()
    path.write_bytes(raw)
    monkeypatch.setattr(profile.pins, "RECEIPT", str(path))
    monkeypatch.setattr(profile.pins, "RECEIPT_SHA256", hashlib.sha256(raw).hexdigest())
    with pytest.raises(ValueError, match="immutable"):
        profile.image()


def test_only_native_consumers_enable_notification_suppression():
    g = goal()
    with patch.object(profile, "image", return_value=prepared()):
        contract = CommittedStatusContract(arm="candidate", identity=profile.pins)
        assert contract.api_settings["ORDER_STATUS_NOTIFICATION_DEDUP"] == "0"
        with patch.object(transaction, "active", return_value=headroom.runtime_view(g)):
            environments = {}
            for role in native.identity.specification("ADR0322")["counts"]:
                settings = contract.settings(role)
                assert settings["ORDER_STATUS_NOTIFICATION_DEDUP"] == "0"
                settings["DATABASE_URL"] = "postgresql://ticketing:test@pgbouncer:5432/ticketing"
                if role == "simulator":
                    settings["API_URL"] = "http://load-balancer:8000"
                environments[role] = dict(settings)
                actual = native.environment(role, settings, "10.1.1.1", "ADR0322")
                assert actual["ORDER_STATUS_NOTIFICATION_DEDUP"] == ("1" if role == "consumer" else "0")
                assert actual["ORDER_STATUS_COMMITTED_SNAPSHOT"] == ("1" if role == "consumer" else "0")
            objects = native.objects("adr0151-123456abcdef", environments, "10.1.1.1", "ADR0322", profile=HEADROOM)
            pods = [item for item in objects if item["kind"] == "Pod"]
            assert len(pods) == 15
            for pod in pods:
                container = pod["spec"]["containers"][0]
                secret_name = container["envFrom"][0]["secretRef"]["name"]
                secret = next(item for item in objects if item["kind"] == "Secret"
                              and item["metadata"]["name"] == secret_name)
                env = {key: base64.b64decode(value).decode() for key, value in secret["data"].items()}
                role = pod["metadata"]["labels"]["codex-worker-role"]
                assert env["ORDER_STATUS_NOTIFICATION_DEDUP"] == ("1" if role == "consumer" else "0")
                assert container["image"] == prepared()["registry_image"]



@pytest.mark.parametrize("change", ["rate", "connections", "mixed_factor", "baseline", "receipt"])
def test_candidate_rejects_load_budget_baseline_and_image_drift(change):
    g = goal()
    if change == "rate":
        g["offered_journeys_per_second"] = 103
    elif change == "connections":
        g["acquisition_budget"] = 21
    elif change == "mixed_factor":
        g["writer_sale_context_decision"] = "ADR0321"
    elif change == "baseline":
        g["baseline_sha256"] = "0" * 64
    else:
        g["notification_dedup_receipt_sha256"] = "0" * 64
    with patch.object(profile, "image", return_value=prepared()), pytest.raises(ValueError):
        profile.selected(g)
