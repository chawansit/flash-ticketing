"""Exercise coordinator CLI allocation without making network requests."""
import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("rate,expected", [(400, [100, 100, 100, 100]), (650, [163, 163, 162, 162])])
def test_coordinator_preserves_total_and_disjoint_partitions(tmp_path, rate, expected):
    coordinator = Path(__file__).resolve().parents[2] / "scripts" / "parallel_cloud_load.py"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"origin": "http://unused.invalid", "show_ids": list(range(8)),
                                    "viewer_tokens": [f"test-viewer-{i}" for i in range(80)]}))
    (tmp_path / "http_load_generator.py").write_text('''
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--manifest');p.add_argument('--output');p.add_argument('--rate',type=int)
p.add_argument('--start-at');a,_=p.parse_known_args()
m=json.loads(Path(a.manifest).read_text())
Path(a.output).write_text(json.dumps({'measured_started_utc':a.start_at,
'generator_drops':0,'read_p95_ms':1,'hold_p95_ms':1,'offered_rps':a.rate,'workload_gate_pass':True,
'shows':m['show_ids'],'viewers':m['viewer_tokens']}))
''')
    output = tmp_path / "result"
    subprocess.run([sys.executable, str(coordinator), "--manifest", str(manifest),
                    "--rate", str(rate), "--seconds", "300", "--output", str(output)],
                   cwd=tmp_path, check=True, capture_output=True, timeout=20)
    summary = json.loads((output / "summary.json").read_text())
    assert summary["coordination_gate_pass"]
    assert summary["workload_gate_pass"]
    assert summary["gate_pass"]
    assert summary["worker_rates"] == expected
    workers = [json.loads((output / f"worker-{i}.json").read_text()) for i in range(4)]
    assert [w["offered_rps"] for w in workers] == expected
    assert sum(w["offered_rps"] for w in workers) == rate
    assert sorted(show for w in workers for show in w["shows"]) == list(range(8))
    assert sorted(viewer for w in workers for viewer in w["viewers"]) == sorted(
        f"test-viewer-{i}" for i in range(80))
