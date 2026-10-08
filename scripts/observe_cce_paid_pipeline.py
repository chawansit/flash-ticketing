"""Native CCE metric endpoints with the unchanged qualified background observer."""

import argparse
import ipaddress
import json
import math
from pathlib import Path
from urllib.request import urlopen

import observe_two_host_pipeline as observer


def validate_receipts(value):
    """These are native admission receipts, never Docker inventory entries."""
    if (
        set(value) != {"decision", "run", "api_sources", "resources", "manifest_digest", "receipts"}
        or value["decision"] != "ADR0228"
    ):
        raise ValueError("Exact native admission bundle required")
    entries = value["receipts"]
    if len(entries) != 4 or {r["pod_name"] for r in entries} != {f"api-{i}" for i in range(4)}:
        raise ValueError("Four native API identities required")
    for key in ("pod_uid", "private_ipv4"):
        if len({r[key] for r in entries}) != 4:
            raise ValueError("Duplicate native identity")
    for row in entries:
        address = ipaddress.IPv4Address(row["private_ipv4"])
        if not any(
            address in ipaddress.IPv4Network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        ):
            raise ValueError("Private native endpoint required")
        proof = row["startup_proof"]
        started = row["process_start_time_seconds"]
        if (
            row["image_id"].split("@")[-1] != value["manifest_digest"]
            or row["resources"] != value["resources"]
            or proof["sources"] != value["api_sources"]
            or proof["run"] != value["run"]
            or proof["pod_uid"] != row["pod_uid"]
            or type(started) not in (int, float)
            or not math.isfinite(started)
            or started <= 0
        ):
            raise ValueError("Native receipt differs from admitted identity")
    return {r["private_ipv4"]: r["process_start_time_seconds"] for r in entries}


def install(module, value, *, fetch=urlopen):
    starts = validate_receipts(value)
    discover = module.api_replicas
    previous = {}

    def replicas(host="api", port=8000):
        return sorted(starts) if host == "api" and port == 8000 else discover(host, port)

    def metrics(address):
        if address not in starts:
            raise ValueError("Unadmitted native endpoint")
        with fetch(f"http://{address}:8000/metrics", timeout=2) as response:
            raw = response.read(observer.MAX_PAYLOAD + 1)
        if len(raw) > observer.MAX_PAYLOAD:
            raise ValueError("Native metrics payload exceeds bound")
        payload = raw.decode("utf-8")
        extras = {**observer.extra_api_metrics(payload), **observer.admission_failure_metrics(payload)}
        if extras["process_start_time_seconds"] != starts[address]:
            raise ValueError("Native process replaced or restarted")
        prior = previous.get(address)
        if prior is not None and any(
            extras.get(k, -1) < v for k, v in prior.items() if k != "process_start_time_seconds"
        ):
            raise ValueError("Native API counter reset")
        previous[address] = extras
        return {**module.parse_api_metrics(payload), **extras}

    module.api_replicas, module.api_metrics = replicas, metrics
    return starts


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--native-receipts", type=Path, required=True)
    parser.add_argument("--native-receipts-sha256", required=True)
    args, remaining = parser.parse_known_args(argv)
    value = json.loads(args.native_receipts.read_text())
    import hashlib

    if (
        hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        != args.native_receipts_sha256
    ):
        raise ValueError("Native receipt transfer changed")
    validate_receipts(value)
    original = observer.install_adapter

    def adapter(module, *params, **kwargs):
        original(module, *params, **kwargs)  # Retain workers, diagnostics, budgets and metric parsing.
        return install(module, value)

    observer.install_adapter = adapter
    try:
        observer.main(remaining)
    finally:
        observer.install_adapter = original


if __name__ == "__main__":
    main()
