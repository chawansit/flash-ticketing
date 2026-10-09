"""Compare credential-free PostgreSQL baseline reports."""

import argparse
import json
from pathlib import Path


def ratio(candidate, baseline):
    if candidate is None or baseline in (None, 0):
        return None
    return candidate / baseline


def compare(local: dict, candidate: dict) -> dict:
    local_plans = {row["name"]: row for row in local.get("plans", [])}
    candidate_plans = {row["name"]: row for row in candidate.get("plans", [])}
    plans = []
    for name in sorted(local_plans.keys() | candidate_plans.keys()):
        before = local_plans.get(name, {})
        after = candidate_plans.get(name, {})
        local_execution = before.get("plan", {}).get("Execution Time")
        candidate_execution = after.get("plan", {}).get("Execution Time")
        plans.append(
            {
                "name": name,
                "local_execution_ms": local_execution,
                "candidate_execution_ms": candidate_execution,
                "execution_ratio": ratio(candidate_execution, local_execution),
                "local_client_ms": before.get("client_ms"),
                "candidate_client_ms": after.get("client_ms"),
                "client_ratio": ratio(after.get("client_ms"), before.get("client_ms")),
            }
        )
    return {
        "local_pass": local.get("pass", False),
        "candidate_pass": candidate.get("pass", False),
        "local_server_version": local.get("server", {}).get("version"),
        "candidate_server_version": candidate.get("server", {}).get("version"),
        "plans": plans,
        "pass": bool(local.get("pass") and candidate.get("pass") and plans),
        "note": "Idle query-plan comparison only; sustained load evidence remains authoritative for capacity.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a fresh output path")
    result = compare(json.loads(args.local.read_text()), json.loads(args.candidate.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"pass": result["pass"], "plan_count": len(result["plans"])}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
