import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import prepare_simulator_roundtrip_image as recipe


@pytest.fixture
def parent(tmp_path, monkeypatch):
    before = b'unchanged = 1\n\n'
    after = b'\n\ndef unchanged_function():\n    return unchanged\n'
    old = b'def simulate_one(db, settings, transport=None):\n    return False\n'
    new = b'def simulate_one(db, settings, transport=None):\n    return True\n'
    files = {recipe.MODULE: before + old + after, 'src/ticketing/api.py': b'unchanged API'}
    expected = {name: recipe.builder.sha(raw) for name, raw in files.items()}
    monkeypatch.setattr(recipe.measured, 'image', lambda: {'runtime_source_sha256': expected})
    monkeypatch.setattr(recipe, 'REVIEWED_FUNCTION_SHA256', recipe.builder.sha(new))
    path = tmp_path / recipe.MODULE
    path.parent.mkdir(parents=True)
    path.write_bytes(before + new + after)
    monkeypatch.setattr(recipe, 'ROOT', tmp_path)
    return files


def test_only_reviewed_simulator_function_changes(parent):
    actual = recipe.overlay(parent)
    assert {name for name in actual if actual[name] != parent[name]} == {recipe.MODULE}
    assert recipe.function_parts(actual[recipe.MODULE])[::2] == recipe.function_parts(parent[recipe.MODULE])[::2]


def test_incomplete_or_changed_parent_is_rejected(parent):
    for files in ({recipe.MODULE: parent[recipe.MODULE]}, {**parent, 'src/ticketing/api.py': b'wrong'}):
        with pytest.raises(ValueError, match='Complete measured'):
            recipe.overlay(files)


@pytest.mark.parametrize('edit,match', [
    (lambda raw: raw.replace(b'unchanged = 1', b'unchanged = 2'), 'Only simulate_one'),
    (lambda raw: raw.replace(b'return True', b'return 42'), 'Unreviewed'),
    (lambda raw: raw + b'\ndef simulate_one():\n    pass\n', 'Exactly one'),
])
def test_unreviewed_source_cannot_be_packaged(parent, edit, match):
    path = recipe.ROOT / recipe.MODULE
    path.write_bytes(edit(path.read_bytes()))
    with pytest.raises(ValueError, match=match):
        recipe.overlay(parent)


def test_shared_builder_hooks_are_restored_on_failure(tmp_path, monkeypatch):
    before = recipe.builder.parent_profile, recipe.builder.overlay
    def fail(output):
        assert recipe.builder.parent_profile is recipe.measured
        raise ValueError('synthetic failure')
    monkeypatch.setattr(recipe.builder, 'prepare', fail)
    with pytest.raises(ValueError, match='synthetic failure'):
        recipe.prepare(tmp_path)
    assert (recipe.builder.parent_profile, recipe.builder.overlay) == before
