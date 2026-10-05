"""Wrap the unchanged paid observer with four verified private API endpoints."""

import argparse
import hashlib
import importlib.util
import json
import math
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

from prepare_two_host_scaling import validate_inventory

FROZEN_NORMALIZED_SHA256 = "1890ac0302507fc81a3c451351521684aa92b00b47316ab320af44b592dacaf2"
FROZEN_IMAGE_ID = "sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e"
MAX_PAYLOAD = 4 * 2**20


def load_frozen(path):
    raw = path.read_bytes()
    if hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest() != FROZEN_NORMALIZED_SHA256:
        raise ValueError("Frozen pipeline source differs")
    spec = importlib.util.spec_from_file_location("frozen_paid_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extra_api_metrics(payload):
    """Read process identity and aggregate bounded static business routes only."""
    result = {}
    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(
            r"(process_start_time_seconds|ticketing_http_requests_total)(?:\{(.*)\})? ([^ ]+)", line
        )
        if not match:
            continue
        name, label_text, raw = match.groups()
        value = float(raw)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Invalid cumulative metric")
        if name == "process_start_time_seconds":
            if value <= 0 or name in result:
                raise ValueError("Invalid process identity")
            result[name] = value
            continue
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', label_text or ""))
        route, method, status = (labels.get(k, "") for k in ("route", "method", "status"))
        if (
            route.startswith("/v1/")
            and re.fullmatch(r"[A-Z]{3,7}", method)
            and re.fullmatch(r"[1-5][0-9]{2}", status)
        ):
            # Collapse routes/status codes to one business counter per replica;
            # frozen parsing still retains route-level errors and timing metrics.
            key = f"business_http:{route}:{method}:{status}"
            if key in result or len(result) >= 256:
                raise ValueError("Duplicate or excessive business series")
            result[key] = value
            result["business_http_requests_total"] = result.get("business_http_requests_total", 0) + value
    if "process_start_time_seconds" not in result:
        raise ValueError("Process-start metric missing")
    # The metric may have no business series before the first journey.
    result.setdefault("business_http_requests_total", 0)
    return result


def install_adapter(module, inventory, *, image_id, now=None, fetch=urlopen):
    validate_inventory(inventory, image_id=image_id, now=now)
    entries = sorted(inventory["apis"], key=lambda a: (a["host_role"], a["container_id"]))
    endpoints = {f"{a['host_role']}:{a['container_id']}": a for a in entries}
    original_discovery = module.api_replicas
    previous = {}

    def discover(host="api", port=8000):
        if host == "api" and port == 8000:
            return list(endpoints)
        return original_discovery(host, port)

    def metrics(label):
        entry = endpoints[label]
        with fetch(f"http://{entry['private_ipv4']}:{entry['port']}/metrics", timeout=2) as response:
            raw = response.read(MAX_PAYLOAD + 1)
        if len(raw) > MAX_PAYLOAD:
            raise ValueError("Metrics payload exceeds bound")
        payload = raw.decode("utf-8")
        extras = extra_api_metrics(payload)
        prior = previous.get(label)
        if prior and (
            extras["process_start_time_seconds"] != prior["process_start_time_seconds"]
            or any(extras.get(k, -1) < v for k, v in prior.items() if k.startswith("business_http"))
        ):
            raise ValueError("API restart or counter reset")
        previous[label] = extras
        return {**module.parse_api_metrics(payload), **extras}

    module.api_replicas = discover
    module.api_metrics = metrics
    return endpoints


def summarize_distribution(rows, inventory, *, offered_start_utc, offered_end_utc):
    """Require complete, restart-free replica counters bracketing actual dispatch."""
    start, end = [datetime.fromisoformat(v) for v in (offered_start_utc, offered_end_utc)]
    if start.tzinfo is None or end.tzinfo is None or not 0 < (end - start).total_seconds() <= 300:
        raise ValueError("Bounded aware offered window required")
    labels = {f"{a['host_role']}:{a['container_id']}" for a in inventory["apis"]}
    if len(labels) != 4:
        raise ValueError("Four distinct replicas required")
    parsed = []
    for row in rows:
        utc = datetime.fromisoformat(row["utc"])
        if utc.tzinfo is None or (parsed and utc <= parsed[-1][0]):
            raise ValueError("Nonmonotonic metric sampling")
        parsed.append((utc, row))
    before = [(t, r) for t, r in parsed if t <= start]
    after = [(t, r) for t, r in parsed if t >= end]
    if (
        not before
        or not after
        or (start - before[-1][0]).total_seconds() > 2
        or (after[0][0] - end).total_seconds() > 2
    ):
        raise ValueError("Metrics do not bracket offered window")
    first_at, first = before[-1]
    last_at, last = after[0]
    observed = [(t, r) for t, r in parsed if first_at <= t <= last_at]
    prior = None
    for utc, row in observed:
        apis = row.get("api_replicas", {})
        if row.get("api_metrics_error") or set(apis) != labels:
            raise ValueError("Missing replica observation")
        if prior and (utc - prior[0]).total_seconds() > 2:
            raise ValueError("Metric sampling gap")
        for label in labels:
            metrics = apis[label]
            for name in ("process_start_time_seconds", "business_http_requests_total"):
                value = metrics.get(name)
                if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                    raise ValueError("Invalid replica metric")
            if metrics["process_start_time_seconds"] <= 0:
                raise ValueError("Process identity missing")
            if prior:
                old = prior[1]["api_replicas"][label]
                if (
                    metrics["process_start_time_seconds"] != old["process_start_time_seconds"]
                    or metrics["business_http_requests_total"] < old["business_http_requests_total"]
                ):
                    raise ValueError("Replica restarted or counter reset")
        prior = (utc, row)
    counts = {
        label: last["api_replicas"][label]["business_http_requests_total"]
        - first["api_replicas"][label]["business_http_requests_total"]
        for label in sorted(labels)
    }
    if any(value <= 0 for value in counts.values()):
        raise ValueError("Every replica must receive business traffic")
    total = sum(counts.values())
    return {
        "all_four_replicas_observed": True,
        "per_replica_traffic_distribution": True,
        "business_request_deltas": counts,
        "business_request_shares": {label: value / total for label, value in counts.items()},
        "observed_start_utc": first_at.astimezone(UTC).isoformat(),
        "observed_end_utc": last_at.astimezone(UTC).isoformat(),
        "scope": "Bracketing business HTTP counters; not paid ticket throughput or proof of balanced routing.",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--frozen-observer", type=Path, required=True)
    args, remaining = parser.parse_known_args(argv)
    inventory = json.loads(args.inventory.read_text())
    frozen = load_frozen(args.frozen_observer)
    install_adapter(frozen, inventory, image_id=FROZEN_IMAGE_ID)
    sys.argv = [str(args.frozen_observer), *remaining]
    frozen.main()


if __name__ == "__main__":
    main()
