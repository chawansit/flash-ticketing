"""Writer transport comparison registration and immutable-factor regressions."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_writer_write_pipeline_probe as runner
import work_envelope as envelope
import writer_write_pipeline_probe_contract as policy
from observe_two_host_pipeline import verify_admission_factor_evidence
from test_diagnostic_runner_connection import TARGET
from test_interleaved_refresh_probe import probe_inventory
from test_work_envelope import area  # noqa: F401


def contract():
    data = policy.plan()
    return policy.WriterWritePipelineProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])


def inventory(c):
    data = probe_inventory()
    data["status_refresh_contract"] = c.inventory_marker()
    for row in data["worker_sources"]:
        row["image_id"] = c.images[row["role"]]
        row["settings"].update(c.settings(row["role"]))
        row["source_identity"]["resolved_imports"] = {p: {"sha256": h} for p, h in c.source_map(row["role"]).items()}
    return data


def test_only_writer_changes_and_existing_index_is_read_only():
    c = contract()
    old = policy.parent.OrdersEventIndexProbeContract(policy.parent.plan()["artifact_receipt"], "candidate", c.sources)
    assert {r for r in c.roles if c.images[r] != old.images[r]} == {"reservation-writer"}
    assert c.index_verify_only is True and old.index_verify_only is False
    assert c.sources == old.sources and c.parents == old.parents
    assert {p for p in c.source_map("reservation-writer") if c.source_map("reservation-writer")[p] != old.sources[p]} == {"src/ticketing/config.py", "src/ticketing/infrastructure/reservations.py", "src/ticketing/workers.py"}
    for role in c.roles:
        if role != "reservation-writer":
            assert c.source_map(role) == old.source_map(role)
            assert c.settings(role) == old.settings(role)
    assert c.settings("reservation-writer") == {**old.settings("reservation-writer"), "RESERVATION_WRITE_PIPELINE": "1"}
    assert policy.plan()["common"] == policy.parent.plan()["common"]


def test_actual_runner_stage_identity_and_fresh_scope(area):  # noqa: F811
    engine = runner.create_runner()
    engine.configure_diagnostic_target(TARGET)
    c = contract()
    data = inventory(c)
    c.verify_inventory(data)
    verify_admission_factor_evidence(data)
    from admission_failure_evidence import expected_allocation
    assert expected_allocation(data) == {"primary": 1, "secondary": 3}
    stage = engine.RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
    assert stage.rate == 84 and stage.expected_tickets == 25200 and stage.stage_limit == 1
    identity = engine.identity()
    assert {policy.MIGRATION, "scripts/prepare_writer_write_pipeline.py", "scripts/writer_write_pipeline_probe_contract.py", "artifacts/writer-write-pipeline/manifest.json", "artifacts/writer-write-pipeline/adr0225.patch"} <= set(identity)
    local_envelope = envelope.read(envelope.ENVELOPE)
    local_envelope["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, local_envelope)
    binding, _, _ = area
    binding["diagnostic_target_sha256"] = "f" * 64
    entry = envelope.reserve(binding, policy.plan(), profile="writer_write_pipeline_probe")
    assert entry["status"] == "ACTIVE" and entry["profile"] == "writer_write_pipeline_probe"


@pytest.mark.parametrize("drift", ["writer_flag", "wrong_image", "wrong_import", "flag_other_role"])
def test_factor_drift_blocks_dispatch(drift):
    c = contract()
    data = inventory(c)
    row = next(w for w in data["worker_sources"] if w["role"] == "reservation-writer")
    if drift == "writer_flag": row["settings"].pop("RESERVATION_WRITE_PIPELINE")
    elif drift == "wrong_image": row["image_id"] = c.images["api"]
    elif drift == "wrong_import": row["source_identity"]["resolved_imports"]["src/ticketing/workers.py"]["sha256"] = c.sources["src/ticketing/workers.py"]
    else: next(w for w in data["worker_sources"] if w["role"] == "consumer")["settings"]["RESERVATION_WRITE_PIPELINE"] = "1"
    with pytest.raises(ValueError):
        c.verify_inventory(data)
        verify_admission_factor_evidence(data)


def test_plan_drift_is_rejected(monkeypatch):
    data = copy.deepcopy(policy.plan())
    data["artifact_receipt"]["images"]["consumer"] = data["artifact_receipt"]["images"]["reservation-writer"]
    monkeypatch.setattr(policy, "PLAN", type("Fake", (), {"read_text": lambda self: __import__("json").dumps(data)})())
    with pytest.raises(ValueError): policy.plan()
