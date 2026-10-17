import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import prepare_payment_claim_cutoff_image as recipe


@pytest.fixture
def parent(tmp_path, monkeypatch):
    # Isolated parent for wrapper guards; the separate real build verifies all22 measured modules.
    files = {recipe.MODULE: b'p.due_at<=clock_timestamp() and p.lease_until<clock_timestamp()',
             'src/ticketing/api.py': b'unchanged API'}
    expected = {name: recipe.builder.sha(raw) for name, raw in files.items()}
    monkeypatch.setattr(recipe.measured, 'image', lambda: {'runtime_source_sha256': expected})
    path = tmp_path / recipe.MODULE
    path.parent.mkdir(parents=True)
    path.write_bytes(files[recipe.MODULE].replace(recipe.OLD, recipe.NEW))
    monkeypatch.setattr(recipe, 'ROOT', tmp_path)
    return files


def test_only_exact_predicate_changes_from_complete_measured_source(parent):
    files = parent
    actual = recipe.overlay(files)
    assert {name for name in files if files[name] != actual[name]} == {recipe.MODULE}
    assert actual[recipe.MODULE] == (recipe.ROOT / recipe.MODULE).read_bytes().replace(b'\r\n', b'\n')
    assert b'p.lease_until<clock_timestamp()' in actual[recipe.MODULE]


def test_unrelated_or_incomplete_parent_is_rejected(parent):
    files = parent
    for other in ({**files, 'src/ticketing/api.py': b'changed'}, {k:v for k,v in files.items() if k != 'src/ticketing/api.py'}):
        with pytest.raises(ValueError, match='Complete measured'):
            recipe.overlay(other)


def test_checkout_drift_is_rejected(tmp_path, monkeypatch, parent):
    files = parent
    path = tmp_path / recipe.MODULE
    path.write_bytes(files[recipe.MODULE].replace(recipe.OLD, recipe.NEW) + b'\n# unrelated edit')
    monkeypatch.setattr(recipe, 'ROOT', tmp_path)
    with pytest.raises(ValueError, match='Checkout differs'):
        recipe.overlay(files)


def test_existing_builder_configuration_is_restored_on_failure(tmp_path, monkeypatch):
    before = recipe.builder.parent_profile, recipe.builder.overlay
    def fail(output):
        assert recipe.builder.parent_profile is recipe.measured
        raise ValueError('synthetic failure')
    monkeypatch.setattr(recipe.builder, 'prepare', fail)
    with pytest.raises(ValueError, match='synthetic'):
        recipe.prepare(tmp_path)
    assert (recipe.builder.parent_profile, recipe.builder.overlay) == before
