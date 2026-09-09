"""Run private cloud stages with separate seat allocations; stop on failure."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--start-rate", type=int, default=100)
args = parser.parse_args()
manifest = json.loads(Path("manifest.json").read_text())
for index, rate in enumerate((100, 200, 400, 800, 1200, 1600)):
    if rate < args.start_rate:
        continue
    manifest["seat_offset"] = 10 + index * 40
    Path("stage-manifest.json").write_text(json.dumps(manifest))
    with Path(f"stage-{rate}.log").open("w") as log:
        result = subprocess.run(
            [
                sys.executable,
                "http_load_generator.py",
                "--manifest",
                "stage-manifest.json",
                "--origin",
                manifest["origin"],
                "--rate",
                str(rate),
                "--seconds",
                "180",
                "--topology",
                "separate-host",
                "--output",
                f"stage-{rate}.json",
            ],
            check=False,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    print(f"Stage {rate} RPS exit={result.returncode}", flush=True)
    if result.returncode:
        print("Escalation stopped. Preserve failure and inspect before refinement.", flush=True)
        break
