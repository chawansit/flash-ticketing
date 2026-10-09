"""Close sale windows for a named isolated development fixture; keep rows for audit."""

import argparse
import json
import os
from pathlib import Path

import psycopg

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--manifest", required=True, type=Path)
a = p.parse_args()
manifest = json.loads(a.manifest.read_text(encoding="utf-8"))
if manifest.get("environment") != "development" or not manifest.get("show_ids"):
    p.error("An isolated development fixture is required")
url = os.getenv("TEST_DATABASE_URL")
if not url:
    raise RuntimeError("TEST_DATABASE_URL is required")
with psycopg.connect(url) as conn:
    rows = conn.execute(
        """UPDATE events SET sale_ends=clock_timestamp()
        WHERE id=ANY(%s::uuid[]) AND sale_ends>clock_timestamp()
        RETURNING id""",
        (manifest["show_ids"],),
    ).fetchall()
print(
    json.dumps(
        {
            "expected_shows": len(manifest["show_ids"]),
            "retired_shows": len(rows),
            "pass": len(rows) == len(manifest["show_ids"]),
        }
    )
)
if len(rows) != len(manifest["show_ids"]):
    raise SystemExit(1)
