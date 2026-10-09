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
    "hold": "seat_hold",
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
        "logging",
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


def phase_category(frames):
    """Nearest specific owning frame; generic helpers remain contextual evidence."""
    fallback = None
    for frame in reversed(frames):
        file, name = public_file(frame.get("file")), frame["name"]
        if file.startswith("jwt/") or file == "ticketing/api.py" and name == "actor":
            return "authentication"
        if file.startswith(("psycopg/", "psycopg_pool/")) or file == "ticketing/infrastructure/postgres.py":
            return "database"
        if file.startswith("redis/") or file in {
            "ticketing/infrastructure/cache.py",
            "ticketing/infrastructure/redis_reservations.py",
            "ticketing/infrastructure/order_status_cache.py",
        }:
            return "redis"
        if file == "fastapi/dependencies/utils.py":
            return "dependency_resolution"
        if file == "fastapi/encoders.py" or (
            file == "fastapi/routing.py" and name == "serialize_response"
        ) or file == "starlette/responses.py" and name == "render":
            return "response_encoding"
        if file.startswith("logging/") or file == "ticketing/observability.py" and name == "format":
            return "logging"
        if file.startswith("prometheus_client/") or file == "ticketing/observability.py":
            return "instrumentation"
        if file.startswith("uvicorn/") or file == "starlette/responses.py" or (
            file == "ticketing/http.py" and name in {"observed_send", "observed_receive"}
        ):
            return "http_transport"
        if file == "starlette/concurrency.py" and name == "run_in_threadpool" or (
            file.startswith("anyio/") and name in {
                "run_sync_in_worker_thread", "run_sync_from_thread", "run_async_from_thread",
            }
        ) or file == "anyio/to_thread.py" and name == "run_sync":
            return "thread_dispatch"
        if file.startswith(("ticketing/application/", "ticketing/infrastructure/reservations.py")):
            return "application"
        if file == "ticketing/api.py" and name in ROUTES:
            return "application"
        if file.startswith(("fastapi/", "starlette/")) or file == "ticketing/http.py":
            return "request_dispatch"
        if fallback is None:
            if file.startswith(("asyncio/", "anyio/")):
                fallback = "async_runtime"
            elif file in {"threading.py", "queue.py"}:
                fallback = "synchronization"
            elif file.startswith("json/"):
                fallback = "json_helper"
    return fallback or "unclassified"


def summarize(documents):
    inclusive, exclusive, routes = Counter(), Counter(), Counter()
    phases, unattributed_phases = Counter(), Counter()
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
                route, phase = route_category(stack), phase_category(stack)
                routes[route] += weight
                phases[phase] += weight
                if route == "unattributed":
                    unattributed_phases[phase] += weight
                exclusive[frame_label(stack[-1])] += weight
                for label in {frame_label(frame) for frame in stack}:
                    inclusive[label] += weight
    if total_weight <= 0 or len(units) != 1:
        raise ValueError("Empty profiles or inconsistent sample units")

    if not math.isclose(sum(phases.values()), total_weight, rel_tol=1e-9) or not math.isclose(
        sum(unattributed_phases.values()), routes["unattributed"], rel_tol=1e-9, abs_tol=1e-9
    ):
        raise ValueError("Phase sample weight conservation failed")

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
        "exclusive_phase_stack_context": ranked(phases),
        "unattributed_route_phase_context": ranked(unattributed_phases),
        "phase_total_sample_weight": sum(phases.values()),
        "unattributed_route_total_weight": sum(unattributed_phases.values()),
        "phase_method": "One nearest recognized owning frame per stack; generic helpers fallback. "
        "Exclusive phase context weights partition samples, not charged CPU. Endpoints remain unknown without route frames.",
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
