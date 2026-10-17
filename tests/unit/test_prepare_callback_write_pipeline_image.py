import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts'))
import prepare_callback_write_pipeline_image as recipe


@pytest.fixture
def parent(tmp_path, monkeypatch):
    files = {recipe.MODULE:b'old worker',
             recipe.API:b'    payment_db.payment_write_pipeline = settings.payment_write_pipeline\n\ndef actor():\n    pass\n',
             recipe.CONFIG:b'class Settings:\n    payment_write_pipeline: bool = False\n    def validate(self):\n        pass\n',
             recipe.CALLBACK:b'class Store:\n    def apply_callback(self, conn, payload):\n        return False\n\n    def initiate_payment(self):\n        return "parent"\n'}
    current = files[recipe.CALLBACK].replace(b'return False', b'return True').replace(b'"parent"',b'"unreviewed"')
    path = tmp_path/recipe.CALLBACK
    path.parent.mkdir(parents=True)
    path.write_bytes(current)
    config = files[recipe.CONFIG].replace(b'    payment_write_pipeline: bool =', b'    payment_callback_write_pipeline: bool = os.getenv("PAYMENT_CALLBACK_WRITE_PIPELINE", "0") == "1"\n    payment_write_pipeline: bool =')
    config = config.replace(b'    def validate(self):\n', b'    def validate(self):\n        if self.payment_callback_write_pipeline and self.payment_confirmation_async:\n            raise RuntimeError("PAYMENT_CALLBACK_WRITE_PIPELINE requires synchronous confirmation")\n')
    path = tmp_path/recipe.CONFIG
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(config)
    monkeypatch.setattr(recipe,'ROOT',tmp_path)
    monkeypatch.setattr(recipe,'REVIEWED_CALLBACK_SHA256',recipe.builder.sha(recipe.callback_parts(current)[1]))
    monkeypatch.setattr(recipe.simulator,'overlay',lambda value:{**value,recipe.MODULE:b'reviewed worker'})
    return files


def test_packages_only_reviewed_methods_and_keeps_parent_authentication(parent):
    result = recipe.overlay(parent)
    assert {k for k in result if result[k]!=parent[k]}==recipe.MODULES
    assert b'def actor()' in result[recipe.API] and b'async def actor' not in result[recipe.API]
    assert b'return True' in result[recipe.CALLBACK]
    assert b'return "parent"' in result[recipe.CALLBACK] and b'unreviewed' not in result[recipe.CALLBACK]


def test_unreviewed_callback_is_rejected(parent):
    path = recipe.ROOT/recipe.CALLBACK
    path.write_bytes(path.read_bytes().replace(b'return True',b'return 42'))
    with pytest.raises(ValueError,match='Unreviewed callback'):
        recipe.overlay(parent)


def test_unrelated_configuration_is_rejected(parent):
    path = recipe.ROOT/recipe.CONFIG
    path.write_bytes(path.read_bytes()+b'\nUNRELATED = True\n')
    with pytest.raises(ValueError,match='configuration differs'):
        recipe.overlay(parent)


def test_wrong_parent_anchor_is_rejected(parent):
    files = {**parent,recipe.API:b'not the measured API'}
    with pytest.raises(ValueError,match='immutable-parent'):
        recipe.overlay(files)


def test_existing_builder_hooks_are_restored_after_failure(tmp_path, monkeypatch):
    previous = recipe.builder.parent_profile,recipe.builder.overlay
    def fail(output):
        raise ValueError('local failure')
    monkeypatch.setattr(recipe.builder,'prepare',fail)
    with pytest.raises(ValueError,match='local failure'):
        recipe.prepare(tmp_path)
    assert (recipe.builder.parent_profile,recipe.builder.overlay)==previous
