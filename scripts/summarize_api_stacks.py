"""Summarize sampled Python stacks without request data or private paths."""

import argparse
import json
import math
import re
from collections import Counter
from pathlib import Path

ROUTES = {
    "get_order": "order_status",
    "callback": "payment_callback",
    "payment": "payment_initiation",
    "initiate_payment": "payment_initiation",
    "reserve": "seat_hold",
    "reservation_command": "reservation_status",
    "metrics": "metrics",
    "ready": "readiness",
}


def public_file(value):
    parts = [part for part in str(value or "").replace("\\", "/").split("/") if part]
    for package in (
        "ticketing",
        "fastapi",
        "starlette",
        "anyio",
        "psycopg",
        "psycopg_pool",
        "redis",
        "jwt",
        "uvicorn",
        "prometheus_client",
        "json",
        "asyncio",
    ):
        if package in parts:
            return "/".join(parts[parts.index(package) :])
    return parts[-1] if parts else "<unknown>"


def frame_label(frame):
    name = re.sub(r"^thread \(\d+\)", "thread", frame["name"])
    return public_file(frame.get("file")) + ":" + name


def route_category(frames):
    for frame in reversed(frames):
        if (
            public_file(frame.get("file"))
            in {
                "ticketing/api.py",
                "ticketing/application/reservations.py",
                "ticketing/infrastructure/reservations.py",
            }
            and frame["name"] in ROUTES
        ):
            return ROUTES[frame["name"]]
    return "unattributed"


def summarize(documents):
    inclusive, exclusive, routes = Counter(), Counter(), Counter()
    units, observations, total_weight = set(), 0, 0.0
    for document in documents:
        frames = document["shared"]["frames"]
        if not isinstance(frames, list) or any(
            not isinstance(frame, dict) or not isinstance(frame.get("name"), str) for frame in frames
        ):
            raise ValueError("Invalid shared frames")
        for profile in document["profiles"]:
            if profile.get("type") != "sampled":
                raise ValueError("Only sampled speedscope profiles are supported")
            unit = profile["unit"]
            if unit not in {"none", "seconds", "milliseconds", "microseconds", "nanoseconds"}:
                raise ValueError("Unsupported sample unit")
            units.add(unit)
            samples = profile["samples"]
            weights = profile.get("weights", [1] * len(samples))
            if len(weights) != len(samples):
                raise ValueError("Weight/sample cardinality mismatch")
            for sample, weight in zip(samples, weights, strict=True):
                if (
                    not isinstance(weight, (int, float))
                    or isinstance(weight, bool)
                    or not math.isfinite(weight)
                    or weight <= 0
                ):
                    raise ValueError("Invalid sample weight")
                if not sample or any(
                    type(index) is not int or index < 0 or index >= len(frames) for index in sample
                ):
                    raise ValueError("Invalid stack frame index")
                stack = [frames[index] for index in sample]
                observations += 1
                total_weight += weight
                routes[route_category(stack)] += weight
                exclusive[frame_label(stack[-1])] += weight
                for label in {frame_label(frame) for frame in stack}:
                    inclusive[label] += weight
    if total_weight <= 0 or len(units) != 1:
        raise ValueError("Empty profiles or inconsistent sample units")

    def ranked(counter):
        return [
            {"symbol": symbol, "weight": weight, "sample_weight_percent": 100 * weight / total_weight}
            for symbol, weight in counter.most_common(30)
        ]

    return {
        "pass": True,
        "sample_observations": observations,
        "total_sample_weight": total_weight,
        "unit": next(iter(units)),
        "route_stack_presence": ranked(routes),
        "top_leaf_frames": ranked(exclusive),
        "top_inclusive_frames": ranked(inclusive),
        "limitations": "GIL-only nonblocking Python stack observations, not exact CPU or per-route latency. "
        "Native work and GIL-released waits are excluded. Inclusive shares overlap; "
        "generic framework/dependency stacks remain unattributed. No locals/request data.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = summarize([json.loads(path.read_text(encoding="utf-8")) for path in args.profiles])
    except (KeyError, TypeError, ValueError) as exc:
        result = {"pass": False, "error_type": type(exc).__name__}
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
