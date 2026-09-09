from collections import Counter
from importlib.util import module_from_spec, spec_from_file_location
from itertools import pairwise
from pathlib import Path

spec = spec_from_file_location("generator", Path(__file__).resolve().parents[2] / "scripts/http_load_generator.py")
generator = module_from_spec(spec)
spec.loader.exec_module(generator)


def test_continuous_burst_preserves_arrivals_and_boundaries():
    plan = list(generator.arrival_plan(25, 240, True))
    assert Counter(phase for _, phase in plan) == {0: 1500, 1: 12000, 2: 1500}
    assert plan[1500] == (60, 1)
    assert plan[13500] == (180, 2)
    assert all(a[0] < b[0] for a, b in pairwise(plan))
    assert plan[-1][0] < 240


def test_uniform_schedule_unchanged():
    assert list(generator.arrival_plan(2, 2)) == [(0, 0), (0.5, 0), (1, 0), (1.5, 0)]


def test_burst_limit_counts_all_three_phases(tmp_path):
    import subprocess
    import sys
    result = subprocess.run([sys.executable, str(Path(generator.__file__)),
                             "--manifest", str(tmp_path / "absent.json"),
                             "--origin", "http://unused.invalid", "--output", str(tmp_path / "out.json"),
                             "--rate", "4000", "--burst"], capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 2
    assert "at most 2000000 requests" in result.stderr
