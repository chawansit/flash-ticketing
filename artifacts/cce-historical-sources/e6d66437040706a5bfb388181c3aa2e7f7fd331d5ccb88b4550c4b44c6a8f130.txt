import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_seat_delta_chain.py"
SPEC = importlib.util.spec_from_file_location("summarize_seat_delta_chain", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_summary_reports_server_identity_change_without_disclosing_ids(tmp_path):
    path = tmp_path / "samples.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in [
        {"counts": {"maps": 1}, "examples": [], "server_run_id": "private-a", "server_uptime_seconds": 100},
        {"counts": {"maps": 1, "tail_mismatch": 1}, "examples": [{"kind": "tail"}],
         "server_run_id": "private-b", "server_uptime_seconds": 12},
    ]), encoding="utf-8")
    result = module.summarize(path)
    assert result["server_identity_changes"] == 1
    assert result["min_server_uptime_seconds"] == 12
    assert result["counts"] == {"maps": 2, "tail_mismatch": 1}
    assert "private-a" not in json.dumps(result)
