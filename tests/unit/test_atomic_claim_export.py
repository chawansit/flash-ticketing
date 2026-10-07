"""Immutable source-port safeguards; no cloud calls or test dispatch."""
import ast
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_atomic_payment_claim as export
from stage_status_refresh_images import new_stage_output


@pytest.fixture(scope="module")
def candidate():
    output = new_stage_output()
    output, _, receipt = export.prepare(output)
    try:
        yield output, receipt
    finally:
        assert output.resolve().parent == (export.ROOT / "tmp").resolve()
        shutil.rmtree(output)


def test_export_changes_only_claim_and_preserves_complete_tree(candidate):
    output, receipt = candidate
    runtime = export.verify(output / "source")
    original = (output / "control-workers.py").read_text()
    supplied = (output / "source" / export.WORKERS).read_text()
    export.verify_port(original, supplied, export.approved_workers())
    assert len(runtime) == 21
    assert receipt["changed_runtime_files"] == [export.WORKERS]
    baseline = export.parent.manifest()["runtime_source_sha256"]
    assert {p for p in runtime if runtime[p] != baseline[p]} == {export.WORKERS}
    old, new = [export.node(raw, export.FUNCTION) for raw in (original, supplied)]
    def claim_statements(function):
        return sum(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "execute"
                   for n in ast.walk(function.body[1]))
    assert claim_statements(old) == 2 and claim_statements(new) == 1


@pytest.mark.parametrize("before,after", [
    ("lease_token=%s", "lease_token=NULL"),
    ("interval '15 seconds'", "interval '5 seconds'"),
    ("response.read()", "pass"),
    ("deliveries=deliveries+1", "deliveries=deliveries+2"),
])
def test_unapproved_claim_or_delivery_drift_is_rejected(candidate, before, after):
    output, _ = candidate
    original = (output / "control-workers.py").read_text()
    supplied = (output / "source" / export.WORKERS).read_text()
    assert before in supplied
    with pytest.raises(ValueError):
        export.verify_port(original, supplied.replace(before, after), export.approved_workers())


def test_unrelated_worker_change_is_rejected(candidate):
    output, _ = candidate
    original = (output / "control-workers.py").read_text()
    supplied = (output / "source" / export.WORKERS).read_text()
    with pytest.raises(ValueError, match="Unrelated"):
        export.verify_port(original, supplied + "\nUNRELATED_CHANGE = True\n", export.approved_workers())


def test_missing_receipt_or_dependency_change_is_rejected(candidate):
    output, _ = candidate
    target = output / "source" / "requirements.lock"
    before = target.read_bytes()
    try:
        target.write_bytes(before + b"\n# unqualified dependency\n")
        with pytest.raises(ValueError, match="drift"):
            export.verify(output / "source")
    finally:
        target.write_bytes(before)
    target = output / "atomic-claim-source-receipt.json"
    before = target.read_bytes()
    try:
        target.write_bytes(before.replace(b'"ADR0192"', b'"ADR0189"'))
        with pytest.raises(ValueError, match="receipt"):
            export.verify(output / "source")
    finally:
        target.write_bytes(before)


def test_baseline_fingerprint_and_fresh_output_are_required(candidate):
    output, _ = candidate
    with pytest.raises(ValueError, match="Fresh"):
        export.prepare(output)
    original = output / "control-workers.py"
    before = original.read_bytes()
    try:
        original.write_bytes(before + b"\n# unpinned original\n")
        with pytest.raises(ValueError, match="fingerprint"):
            export.verify(output / "source")
    finally:
        original.write_bytes(before)
