import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_slow_db_phases.py"
SPEC = importlib.util.spec_from_file_location("summarize_slow_db_phases", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_summarizes_valid_events_and_redacts_other_fields():
    rows = [
        json.dumps({"event": "slow_db_phase", "phase": "commit", "outcome": "ok",
                    "duration_ms": 151.2, "time": "2026-09-20T01:42:22Z",
                    "password": "secret"}),
        json.dumps({"event": "slow_db_phase", "phase": "commit", "outcome": "error",
                    "duration_ms": 601.0, "time": "2026-09-20T01:42:23Z"}),
        json.dumps({"event": "slow_db_phase", "phase": "pool_return", "outcome": "ok",
                    "duration_ms": 110.0, "time": "2026-09-20T01:42:23Z"}),
        json.dumps({"event": "slow_db_phase", "phase": "other", "outcome": "ok",
                    "duration_ms": 100.0}),
        "not json",
    ]
    result = module.summarize(rows, 2)
    assert result["counts"] == {"commit|ok": 1, "commit|error": 1, "pool_return|ok": 1}
    assert result["longest_by_phase"]["commit"]["duration_ms"] == 601.0
    assert len(result["last_events"]) == 2
    assert result["malformed_events"] == 1
    assert "secret" not in json.dumps(result)


def test_rejects_invalid_duration_and_outcome():
    rows = [
        json.dumps({"event": "slow_db_phase", "phase": "commit", "outcome": "ok",
                    "duration_ms": True}),
        json.dumps({"event": "slow_db_phase", "phase": "commit", "outcome": "ok",
                    "duration_ms": float("nan")}),
        json.dumps({"event": "slow_db_phase", "phase": "commit", "outcome": "private",
                    "duration_ms": 100}),
    ]
    result = module.summarize(rows, 5)
    assert result["counts"] == {}
    assert result["malformed_events"] == 3
