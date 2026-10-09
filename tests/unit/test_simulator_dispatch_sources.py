import ast
import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_simulator_dispatch_sources as export
from prepare_slot_comparison_sources import pairs


def test_only_simulator_validation_changes_and_api_pair_remains_pinned():
    files, plan = export.source_plan()
    assert len(files) == 21
    assert plan["changed_paths"] == [export.CONFIG]
    for name, raw in files.items():
        assert hashlib.sha256(raw).hexdigest() == plan["runtime_source_sha256"][name]
        original = export.historical.blob(plan["parent_runtime_source_sha256"][name])
        if name == export.CONFIG:
            assert raw.replace(export.NEW, export.OLD) == original
        else:
            assert raw == original
    assert plan["simulator_concurrency"] == 12 and plan["database_pool_max"] == 10
    assert not plan["cloud_load_started"]
    import json
    receipt = json.loads((export.ROOT / "docs/capacity/cce/slot-comparison-images-2026-10-09.json").read_text())
    assert pairs()[1]["runtime_sources"]["control"] == receipt["images"]["control"]["runtime_sources_sha256"]
    node = ast.parse(files[export.CONFIG])
    assert any(isinstance(v, ast.Constant) and v.value == 32 for v in ast.walk(node))


def test_unreviewed_validation_cannot_prepare_an_image(monkeypatch):
    monkeypatch.setattr(export, "NEW", export.NEW.replace(b"<= 32", b"<= 64"))
    with pytest.raises(ValueError, match="reviewed"):
        export.source_plan()
