import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import ast

import prepare_measured_payment_context_image as candidate
import pytest
from prepare_payment_context_sources import pairs, payment_method


@pytest.fixture
def parent(monkeypatch):
    values, _ = pairs()
    original = values["control"]
    expected = {name: candidate.sha(raw) for name, raw in original.items()}
    monkeypatch.setattr(candidate.parent_profile, "image", lambda: {"runtime_source_sha256": expected})
    return original


def test_only_reviewed_payment_method_changes(parent):
    result = candidate.overlay(parent)
    assert [k for k in parent if parent[k] != result[k]] == [candidate.MODULE]
    reviewed, _ = pairs()
    assert ast.dump(payment_method(result[candidate.MODULE])) == ast.dump(
        payment_method(reviewed["candidate"][candidate.MODULE]))
    assert b'conn.execute("BEGIN")' in result["src/ticketing/infrastructure/postgres.py"]
    assert b'FROM holds h LEFT JOIN payment_attempts' in result[candidate.MODULE]


def test_complete_source_identity_is_required(parent):
    with pytest.raises(ValueError, match="Complete measured parent"):
        candidate.overlay({k: v for k, v in parent.items() if k != "src/ticketing/config.py"})
    changed = {**parent, "src/ticketing/config.py": parent["src/ticketing/config.py"] + b"\n# drift\n"}
    with pytest.raises(ValueError, match="Complete measured parent"):
        candidate.overlay(changed)


def test_different_payment_parent_cannot_be_silently_overlaid(parent, monkeypatch):
    modified = {**parent, candidate.MODULE: parent[candidate.MODULE].replace(
        b'SELECT clock_timestamp() AS now', b'SELECT transaction_timestamp() AS now')}
    monkeypatch.setattr(candidate.parent_profile, "image", lambda: {
        "runtime_source_sha256": {k: candidate.sha(v) for k, v in modified.items()}})
    with pytest.raises(ValueError, match="differs from reviewed control"):
        candidate.overlay(modified)
