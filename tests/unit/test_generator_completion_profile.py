"""Exercise actual registry, constructor, diagnostic and transferred-bundle seams."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import generator_completion_probe_contract as policy
import run_generator_completion_probe as runner
import work_envelope as envelope
from admission_failure_evidence import expected_allocation
from observe_two_host_pipeline import verify_admission_factor_evidence
from prepare_generator_completion import PATH, corrected_bundle
from test_diagnostic_runner_connection import TARGET
from test_work_envelope import area  # noqa: F401
from test_writer_write_pipeline_probe import inventory


@pytest.fixture(scope="module", autouse=True)
def verify_unchanged_frozen_tree_once():
    # Only test memoization: verify the real exact frozen tree once. Production does not cache.
    original = policy.parent.verify_source
    data = policy.parent.plan()
    source = (policy.ROOT / data["writer_isolated_source_directory"]).resolve()
    verified = original(source)
    def cached(path):
        if Path(path).resolve() != source:
            raise ValueError("Unknown frozen source directory")
        return verified
    policy.parent.verify_source = cached
    try:
        yield
    finally:
        policy.parent.verify_source = original


def contract():
    data = policy.plan()
    return policy.GeneratorCompletionProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])


def test_backend_and_every_budget_unchanged():
    c = contract()
    parent_data = policy.parent.plan()
    old = policy.parent.WriterWritePipelineProbeContract(parent_data["artifact_receipt"], "candidate", c.sources)
    assert policy.plan()["common"] == parent_data["common"]
    assert c.images == old.images and c.parents == old.parents and c.sources == old.sources
    for role in c.roles:
        assert c.settings(role) == old.settings(role)
        assert c.source_map(role) == old.source_map(role)
    assert c.index_verify_only is True


def test_actual_stage_diagnostics_identity_and_fresh_scope(area):  # noqa: F811
    engine = runner.create_runner()
    engine.configure_diagnostic_target(TARGET)
    c = contract()
    data = inventory(c)
    c.verify_inventory(data)
    verify_admission_factor_evidence(data)
    assert expected_allocation(data) == {"primary": 1, "secondary": 3}
    stages = engine.RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
    assert stages.rate == 84 and stages.expected_tickets == 25200 and stages.stage_limit == 1
    identity = engine.identity()
    assert {"scripts/run_generator_completion_probe.py", "scripts/prepare_generator_completion.py",
        "artifacts/generator-completion/manifest.json", "artifacts/generator-completion/adr0226.patch"} <= set(identity)
    data = envelope.read(envelope.ENVELOPE)
    data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    binding, _, _ = area
    binding["diagnostic_target_sha256"] = "f" * 64
    entry = envelope.reserve(binding, policy.plan(), profile="generator_completion_probe")
    assert entry["profile"] == "generator_completion_probe" and entry["status"] == "ACTIVE"


def test_exact_bundle_reaches_actual_protocol_seam(monkeypatch):
    from types import SimpleNamespace

    import run_two_host_paid_comparison as comparison
    seen = []
    fake = SimpleNamespace(identity=dict, comparison=comparison,
        protocol=lambda *args, **kwargs: seen.append(args[3]) or {"run": "adr0151-" + "f" * 12})
    monkeypatch.setattr(runner, "parent_runner", lambda **kwargs: fake)
    engine = runner.create_runner()
    original = comparison.frozen_bundle()
    engine.protocol({}, {}, {}, original, {}, execute=False)
    assert seen == [corrected_bundle(original)]
    assert seen[0][PATH] != original[PATH]
    assert {p for p in seen[0] if seen[0][p] != original[p]} == {PATH}
    with pytest.raises(ValueError):
        engine.protocol({}, {}, {}, {**original, PATH: original[PATH] + b"\n"}, {}, execute=False)
    assert len(seen) == 1


@pytest.mark.parametrize("drift", ["backend_image", "writer_flag", "budget", "rate", "generator_hash"])
def test_factor_drift_blocks_before_dispatch(monkeypatch, drift):
    if drift == "writer_flag":
        c = contract()
        data = inventory(c)
        next(w for w in data["worker_sources"] if w["role"] == "reservation-writer")["settings"]["RESERVATION_WRITE_PIPELINE"] = "0"
        with pytest.raises(ValueError): c.verify_inventory(data)
        return
    data = copy.deepcopy(policy.plan())
    if drift == "backend_image": data["artifact_receipt"]["images"]["api"] = data["artifact_receipt"]["images"]["reservation-writer"]
    elif drift == "budget": data["common"]["generator_concurrency"] = 1000
    elif drift == "rate": data["common"]["buyer_journeys_per_second"] = 100
    else: data["generator_manifest"]["candidate_sha256"] = "f" * 64
    monkeypatch.setattr(policy, "PLAN", type("Fake", (), {"read_text": lambda self: __import__("json").dumps(data)})())
    with pytest.raises(ValueError): policy.plan()
