"""Guard the single writer-method image delta against source drift."""

import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import prepare_writer_sale_context_image as image
import pytest
from prepare_payment_context_sources import pairs


@pytest.fixture
def parent(monkeypatch):
    values, _ = pairs()
    source = values["control"]
    monkeypatch.setattr(
        image.parent_profile,
        "image",
        lambda: {"runtime_source_sha256": {name: image.sha(raw) for name, raw in source.items()}},
    )
    return source


def definitions_without_writer(source):
    tree = ast.parse(source)
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PostgresReservations")
    owner.body = [n for n in owner.body if getattr(n, "name", None) != "_persist_reservation_command"]
    return ast.dump(tree)


def test_only_writer_method_changes_and_payment_remains_parent(parent):
    candidate = image.overlay(parent)
    assert [name for name in parent if candidate[name] != parent[name]] == [image.MODULE]
    assert definitions_without_writer(parent[image.MODULE]) == definitions_without_writer(
        candidate[image.MODULE]
    )
    assert b"SELECT *, clock_timestamp() AS now FROM events" in candidate[image.MODULE]
    # Includes persistence/lock, expiry, idempotency, payment and messaging boundaries.
    assert image.method(candidate[image.MODULE]).name == "_persist_reservation_command"


def test_partial_or_modified_parent_map_cannot_build(parent):
    with pytest.raises(ValueError, match="Complete measured parent"):
        image.overlay({k: v for k, v in parent.items() if k != "src/ticketing/config.py"})
    with pytest.raises(ValueError, match="Complete measured parent"):
        image.overlay({**parent, image.MODULE: parent[image.MODULE] + b"\n# drift\n"})


def test_missing_reviewed_anchor_fails_even_with_a_matching_manifest(parent, monkeypatch):
    changed = {
        **parent,
        image.MODULE: parent[image.MODULE].replace(
            b"SELECT clock_timestamp() AS now", b"SELECT transaction_timestamp() AS now"
        ),
    }
    monkeypatch.setattr(
        image.parent_profile,
        "image",
        lambda: {"runtime_source_sha256": {name: image.sha(raw) for name, raw in changed.items()}},
    )
    with pytest.raises(ValueError, match="reviewed writer"):
        image.overlay(changed)
