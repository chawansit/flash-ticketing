"""ADR0259 native worker metrics from admitted endpoints; one read per sample."""
import io
import math
from datetime import datetime
from itertools import pairwise
from urllib.request import urlopen

from cce_worker_identity import validate_bundle

ROLE_ALIAS = {"writer": "reservation-writer"}


def install(module, bundle, *, fetch=urlopen):
    starts = validate_bundle(bundle)
    by_role = {}
    for row in bundle["receipts"]:
        by_role.setdefault(row["role"], []).append(row["private_ipv4"])
    original_discover, original_counters = module.api_replicas, module.worker_counters
    previous = {}

    def discover(host="api", port=8000):
        if port == 9101 and host in by_role:
            return sorted(by_role[host])
        return original_discover(host, port)

    def counters(role, operation, url):
        actual = ROLE_ALIAS.get(role, role)
        if actual not in by_role:
            return original_counters(role, operation, url)
        captured = {}

        def measured_fetch(address, timeout):
            from urllib.parse import urlsplit
            parsed = urlsplit(address)
            if parsed.scheme != "http" or parsed.port != 9101 or parsed.path != "/metrics" or parsed.hostname not in by_role[actual]:
                raise ValueError("Unadmitted worker metric endpoint")
            with fetch(address, timeout=timeout) as response:
                raw = response.read(4194305)
            if len(raw) > 4194304:
                raise ValueError("Worker metrics payload exceeds bound")
            values = {}
            target = f'ticketing_worker_operations_total{{operation="{operation}",outcome="ok"}}'
            for line in raw.decode().splitlines():
                fields = line.split()
                if len(fields) == 2 and fields[0] in {"process_start_time_seconds", "process_cpu_seconds_total", target}:
                    key = "calls" if fields[0] == target else fields[0]
                    if key in values:
                        raise ValueError("Duplicate worker cumulative metric")
                    values[key] = float(fields[1])
            values.setdefault("calls", 0)
            if (values.get("process_start_time_seconds") != starts[parsed.hostname]
                    or any(type(values.get(k)) not in (int, float) or not math.isfinite(values[k]) or values[k] < 0 for k in ("calls", "process_cpu_seconds_total"))
                    or any(values[k] < v for k, v in previous.get(parsed.hostname, {}).items() if k != "process_start_time_seconds")):
                raise ValueError("Worker process identity/counter reset")
            previous[parsed.hostname] = dict(values)
            captured[parsed.hostname] = values
            return io.BytesIO(raw)

        prior_fetch = module.urlopen
        module.urlopen = measured_fetch
        try:
            result = original_counters(role, operation, url)
        finally:
            module.urlopen = prior_fetch
        if set(captured) != set(by_role[actual]):
            raise ValueError("Incomplete native worker metrics")
        result[role + "_native_processes"] = captured
        return result

    module.api_replicas, module.worker_counters = discover, counters
    return starts


def summarize(rows, bundle, start, end):
    starts = validate_bundle(bundle)
    left, right = datetime.fromisoformat(start), datetime.fromisoformat(end)
    if left.tzinfo is None or right.tzinfo is None or right <= left:
        raise ValueError("Aware positive offered window required")
    selected = sorted(rows, key=lambda r: datetime.fromisoformat(r["utc"]))
    first = max((r for r in selected if datetime.fromisoformat(r["utc"]) <= left), key=lambda r: r["utc"])
    last = min((r for r in selected if datetime.fromisoformat(r["utc"]) >= right), key=lambda r: r["utc"])
    window = [r for r in selected if first["utc"] <= r["utc"] <= last["utc"]]
    times = [datetime.fromisoformat(r["utc"]) for r in window]
    if ((left-times[0]).total_seconds() > 2 or (times[-1]-right).total_seconds() > 2
            or any(not 0 < (b-a).total_seconds() <= 2 for a, b in pairwise(times))):
        raise ValueError("Complete native worker offered-window coverage required")
    previous, initial = {}, {}
    for row in window:
        current = {}
        for role in ("consumer", "writer", "publisher", "maintenance", "reconciler", "simulator"):
            replicas = row.get(role + "_native_processes", {})
            if row.get(role + "_metrics_error"):
                raise ValueError("Native worker sampling failed")
            for address, values in replicas.items():
                if address in current:
                    raise ValueError("Duplicated worker sample endpoint")
                if (address not in starts or values.get("process_start_time_seconds") != starts[address]
                        or any(type(values.get(k)) not in (int, float) or not math.isfinite(values[k]) or values[k] < 0 or values[k] < previous.get(address, {}).get(k, 0) for k in ("calls", "process_cpu_seconds_total"))):
                    raise ValueError("Native worker sampling identity/counter drift")
                current[address] = values
        if set(current) != set(starts):
            raise ValueError("Complete thirteen-worker samples required")
        if not initial:
            initial = current
        previous = current
    elapsed = (times[-1]-times[0]).total_seconds()
    cpu = {addr: (previous[addr]["process_cpu_seconds_total"]-initial[addr]["process_cpu_seconds_total"])/elapsed for addr in starts}
    calls = {addr: previous[addr]["calls"]-initial[addr]["calls"] for addr in starts}
    if any(v <= 0 for v in calls.values()):
        raise ValueError("Every native worker must make measured progress")
    return {"pass": True, "counter_interval_seconds": elapsed, "worker_cpu_cores_by_endpoint": cpu,
            "aggregate_worker_cpu_cores": sum(cpu.values()), "successful_operations_by_endpoint": calls,
            "scope": "Worker process CPU and operations; CCE node CPU/throttling are not measured"}
