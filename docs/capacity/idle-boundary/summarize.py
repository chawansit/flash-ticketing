import json
from collections import Counter
from pathlib import Path

root = Path(__file__).parent
out = []
for p in sorted((root / "generator/idle-boundary").glob("expiry-*.json")):
    d = json.loads(p.read_text())
    rows = d.pop("pairs")
    assert len(rows) == 240
    d["by_delay"] = []
    for delay in sorted({r["requested_idle_seconds"] for r in rows}):
        g = [r for r in rows if r["requested_idle_seconds"] == delay]
        d["by_delay"].append(
            {
                "idle_seconds": delay,
                "pairs": len(g),
                "probe_statuses": dict(Counter(str(r.get("probe", {}).get("status", "missing")) for r in g)),
                "reused": sum(r.get("same_local_port", False) for r in g),
                "new_connect": sum(
                    r.get("probe", {}).get("trace", {}).get("connect_attempted", False) for r in g
                ),
                "actual_idle_min": min(r["actual_client_idle_seconds"] for r in g),
                "actual_idle_max": max(r["actual_client_idle_seconds"] for r in g),
            }
        )
    errors = [r["probe"] for r in rows if r.get("probe", {}).get("status") == "transport_error"]
    d["error_classes"] = dict(Counter(e["trace"]["exception_chain"][0]["type"] for e in errors))
    d["errors_attempting_connect"] = sum(e["trace"]["connect_attempted"] for e in errors)
    out.append(d)
assert len(out) == 3
(root / "summary.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps([{k: v for k, v in d.items() if k != "by_delay"} for d in out], indent=2))
