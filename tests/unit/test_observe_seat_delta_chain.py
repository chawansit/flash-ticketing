import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "observe_seat_delta_chain.py"
SPEC = importlib.util.spec_from_file_location("observe_seat_delta_chain", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def entry(start, end):
    return json.dumps({"from_version": start, "version": end, "seats": []})


def test_observer_classifies_live_chain_defects_without_show_ids():
    previous = {"private-show": ("same", 9)}
    counts, examples = module.inspect_sample(
        ["private-show"],
        [(["8", "same"], [entry(1, 3), entry(2, 5), entry(7, 8)])],
        previous,
    )
    assert counts["same_incarnation_regression"] == 1
    assert counts["internal_overlap"] == 1
    assert counts["internal_gap"] == 1
    assert counts["wide_range"] == 2
    assert counts.get("tail_mismatch", 0) == 0
    assert previous["private-show"] == ("same", 8)
    assert "private-show" not in json.dumps(examples)


def test_observer_detects_tail_and_incarnation_change():
    counts, _ = module.inspect_sample(
        ["show"], [(["7", "new"], [entry(4, 6)])], {"show": ("old", 20)}
    )
    assert counts["incarnation_change"] == 1
    assert counts.get("same_incarnation_regression", 0) == 0
    assert counts["tail_mismatch"] == 1


def test_observer_detects_plain_read_behind_atomic_lua():
    counts, examples = module.inspect_sample(
        ["show"], [(["11", "same"], [entry(10, 11)], ["9", "same"])], {}
    )
    assert counts["plain_read_behind_atomic"] == 1
    assert examples[0] == {"kind": "plain_read_behind_atomic", "atomic": 11, "plain": 9}
