import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))

from compare_postgres_baselines import compare, ratio


def test_ratio_handles_zero_and_values():
    assert ratio(4, 2) == 2
    assert ratio(4, 0) is None
    assert ratio(None, 2) is None


def test_compare_matches_plans_by_name():
    local = {
        "pass": True,
        "server": {"version": "local"},
        "plans": [{"name": "seat", "client_ms": 2, "plan": {"Execution Time": 1}}],
    }
    candidate = {
        "pass": True,
        "server": {"version": "rds"},
        "plans": [{"name": "seat", "client_ms": 3, "plan": {"Execution Time": 2}}],
    }
    result = compare(local, candidate)
    assert result["pass"] is True
    assert result["plans"] == [
        {
            "name": "seat",
            "local_execution_ms": 1,
            "candidate_execution_ms": 2,
            "execution_ratio": 2,
            "local_client_ms": 2,
            "candidate_client_ms": 3,
            "client_ratio": 1.5,
        }
    ]
