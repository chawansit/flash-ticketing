import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_slot_comparison_sources as source


def test_exact_one_factor_and_unchanged_historical_module_set():
    pairs, plan = source.pairs()
    control, candidate = pairs["control"], pairs["candidate"]
    assert len(control) == len(candidate) == 22
    assert [p for p in candidate if candidate[p] != control[p]] == [source.POSTGRES]
    assert control[source.POSTGRES].replace(source.CONTROL_BEGIN.encode(), source.CANDIDATE_BEGIN.encode(), 1
        ) == candidate[source.POSTGRES]
    assert len(plan["unchanged_legacy_modules"]) == 17
    for name in plan["unchanged_legacy_modules"]:
        assert hashlib.sha256(candidate[name]).hexdigest() == plan["parent_runtime_source_sha256"][name]
    assert plan["common_environment_changes"] == {"DB_FAILURE_DIAGNOSTICS": "1"}
    assert plan["cloud_execution_authorized_by_this_plan"] is False
    assert b"reservation_write_pipeline:" not in candidate["src/ticketing/config.py"]


def test_changed_transaction_factor_fails_before_output_creation(monkeypatch):
    original = Path.read_bytes

    def drift(path):
        raw = original(path)
        return raw.replace(source.CANDIDATE_BEGIN.encode(), b"unexpected") if path == source.BASELINE_SOURCE / source.POSTGRES else raw

    monkeypatch.setattr(Path, "read_bytes", drift)
    with pytest.raises(ValueError, match="frozen baseline source"):
        source.pairs()
