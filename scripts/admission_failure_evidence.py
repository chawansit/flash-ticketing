"""ADR0166 bounded, allowlisted failure snapshots from exact live API containers."""
import inspect
import json
import math
import re
from datetime import datetime
from pathlib import Path

ROLES = ("general", "payment")
REASONS = ("global_limit", "role_limit", "native_timeout", "native_limit")
MAX_BYTES = 64 * 2**20  # ADR0183: input scan only; line, records and deadline remain bounded.
MAX_LINE = 16384
MAX_RECORDS = 128


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 10**12:
        raise ValueError("Invalid bounded diagnostic number")
    return value


def sanitize(record):
    if record.get("event") != "db_acquisition_failure":
        return None
    role, reason, capture = (record.get(k) for k in ("role", "reason", "capture"))
    if role not in ROLES or reason not in REASONS or capture not in ("guard_rejection", "native_failure_before_release"):
        raise ValueError("Unknown failure classification")
    request = record.get("request_id")
    if request is not None and (not isinstance(request, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", request)):
        raise ValueError("Invalid request correlation")
    timestamp = record.get("time")
    if not isinstance(timestamp, str) or len(timestamp) > 64 or datetime.fromisoformat(timestamp).tzinfo is None:
        raise ValueError("Aware diagnostic timestamp required")
    native = record.get("native_pool")
    if not isinstance(native, dict):
        raise TypeError("Native failure snapshot missing")
    native = {k: number(native[k]) for k in ("pool_size", "pool_available", "requests_waiting", "pool_max")}
    source = record.get("guard", {})
    guard = {k: number(source[k]) for k in ("maximum", "used", "acquiring", "retained", "callback_reserved")}
    flag = source.get("partial_timeout_reclaim")
    if type(flag) is not bool:
        raise ValueError("Explicit reclaim flag required")
    guard["partial_timeout_reclaim"] = flag
    for key in ("counts", "retained_by_role", "limits"):
        values = source[key]
        if not isinstance(values, dict) or set(values) != set(ROLES):
            raise ValueError("Exact role counters required")
        guard[key] = {r: number(values[r]) for r in ROLES}
    return {"time": timestamp, "request_id": request, "event": "db_acquisition_failure", "role": role,
            "reason": reason, "capture": capture, "elapsed_ms": number(record["elapsed_ms"]),
            "native_pool": native, "guard": guard}


def remote_collect(apis, since, *, classifier=None, record_limits=None):
    import os
    import select
    import subprocess
    import time

    classifier = classifier or sanitize
    record_limits = record_limits or {"db_acquisition_failure": MAX_RECORDS}

    def identities():
        rows = json.loads(subprocess.check_output(["docker", "inspect", *[a["container_id"] for a in apis]], timeout=5))
        result = {}
        for row in rows:
            labels = row["Config"].get("Labels", {})
            if labels.get("com.docker.compose.service") != "api" or labels.get("com.docker.compose.project") not in (
                    "flash-ticketing", "flash-ticketing-api-secondary") or row["State"]["Running"] is not True:
                raise ValueError("Exact live API required")
            result[row["Id"]] = {"image_id": row["Image"], "started_at": row["State"]["StartedAt"]}
        return result

    expected = {a["container_id"]: {k: a[k] for k in ("image_id", "started_at")} for a in apis}
    if identities() != expected:
        raise ValueError("Container identity changed before log capture")
    results = []
    for api in apis:
        process = subprocess.Popen(["docker", "logs", "--timestamps", "--since", since, api["container_id"]],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        start, total, buffer, records, complete = time.monotonic(), 0, b"", [], True

        counts = {event: 0 for event in record_limits}

        def accept(line, records=records, counts=counts):
            if len(line) > MAX_LINE:
                raise ValueError("Diagnostic line bound exceeded")
            payload = line.partition(b" ")[2]
            if not payload.startswith(b"{"):
                if any(event.encode() in payload for event in record_limits):
                    raise ValueError("Malformed diagnostic object")
                return
            try:
                value = json.loads(payload)
            except (ValueError, UnicodeError):
                if any(event.encode() in payload for event in record_limits):
                    raise ValueError("Malformed failure snapshot") from None
                return
            if not isinstance(value, dict):
                if any(event.encode() in payload for event in record_limits):
                    raise ValueError("Malformed diagnostic object")
                return
            item = classifier(value)
            if item is not None:
                event = item["event"]
                if event not in counts or counts[event] >= record_limits[event]:
                    raise ValueError("Diagnostic record bound exceeded")
                counts[event] += 1
                records.append(item)

        try:
            while True:
                remaining = 10 - (time.monotonic() - start)
                if remaining <= 0:
                    raise ValueError("Diagnostic time bound exceeded")
                if not select.select([process.stdout], [], [], remaining)[0]:
                    raise ValueError("Diagnostic stream time bound exceeded")
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_BYTES:
                    raise ValueError("Diagnostic byte bound exceeded")
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    accept(line)
                if len(buffer) > MAX_LINE:
                    raise ValueError("Diagnostic line bound exceeded")
            if buffer:
                accept(buffer)
            if process.wait(timeout=1) != 0:
                complete = False
        except (ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
            complete = False
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=2)
            process.stdout.close()
        results.append({"container_id": api["container_id"], "complete": complete, "input_bytes": total,
                        "records": records})
    if identities() != expected:
        raise ValueError("Container identity changed during log capture")
    return {"complete": all(r["complete"] for r in results), "apis": results}


def expected_allocation(inventory):
    """Resolve exact coverage from the qualified placement policy, never a guessed count."""
    counts = {"primary": 2, "secondary": 2}
    marker = inventory.get("status_refresh_contract", {})
    placement = marker.get("decision") in ("ADR0174", "ADR0177", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225')
    if placement:
        if marker["decision"] == "ADR0174":
            import api_placement_contract as policy
            contract_type = policy.ApiPlacementContract
        elif marker["decision"] == "ADR0177":
            import application_role_rebalance_contract as policy
            contract_type = policy.ApplicationRoleRebalanceContract
        elif marker["decision"] == "ADR0216":
            import callback_routing_contract as policy
            contract_type = policy.CallbackRoutingContract
        elif marker["decision"] == "ADR0225":
            import writer_write_pipeline_probe_contract as policy
            contract_type = policy.WriterWritePipelineProbeContract
        elif marker["decision"] == "ADR0224":
            import orders_event_index_probe_contract as policy
            contract_type = policy.OrdersEventIndexProbeContract
        elif marker["decision"] == "ADR0222":
            import interleaved_refresh_probe_contract as policy
            contract_type = policy.InterleavedRefreshProbeContract
        elif marker["decision"] == "ADR0219":
            import shared_callback_rate_probe_contract as policy
            contract_type = policy.SharedCallbackRateProbeContract
        else:
            import shared_callback_placement_contract as policy
            contract_type = policy.SharedCallbackPlacementContract
        plan = policy.plan()
        contract = contract_type(plan["artifact_receipt"], marker.get("arm"), plan["expected_runtime_source_sha256"])
        if json.dumps(marker, sort_keys=True) != json.dumps(contract.inventory_marker(), sort_keys=True):
            raise ValueError("Exact qualified placement marker required")
        counts = contract.measured_api_counts
    rows = inventory["apis"]
    identities = [(a["host_role"], a["container_id"]) for a in rows]
    if len(rows) != 4 or len(set(identities)) != 4 or any(host not in counts for host, _ in identities):
        raise ValueError("Four distinct host-qualified API identities required")
    if placement and len({cid for _, cid in identities}) != 4:
        raise ValueError("Distinct immutable placement containers required")
    if any(sum(a["host_role"] == host for a in rows) != count for host, count in counts.items()):
        raise ValueError("Exact declared API allocation required")
    return counts


def program(apis, since, *, expected_api_count=None):
    if expected_api_count is not None:
        if type(expected_api_count) is not int or expected_api_count not in (1, 2, 3) or len(apis) != expected_api_count:
            raise ValueError("Exact bounded per-host API count required")
    elif not apis or len(apis) > 2:
        raise ValueError("One host's exact API replicas required")
    if not isinstance(since, str) or len(since) > 64 or datetime.fromisoformat(since).tzinfo is None:
        raise ValueError("Aware capture window required")
    if len({a["container_id"] for a in apis}) != len(apis):
        raise ValueError("Duplicate API identity")
    for api in apis:
        if not re.fullmatch(r"[0-9a-f]{64}", api["container_id"]) or not re.fullmatch(r"sha256:[0-9a-f]{64}", api["image_id"]):
            raise ValueError("Immutable container and image identity required")
        if not isinstance(api["started_at"], str) or len(api["started_at"]) > 64:
            raise ValueError("Exact start identity required")
    return ("import json,math,re\nfrom datetime import datetime\n"
            + "\n".join(f"{key}={value!r}" for key, value in {
                "ROLES": ROLES, "REASONS": REASONS, "MAX_BYTES": MAX_BYTES,
                "MAX_LINE": MAX_LINE, "MAX_RECORDS": MAX_RECORDS}.items()) + "\n"
            + inspect.getsource(number) + inspect.getsource(sanitize) + inspect.getsource(remote_collect)
            + "\nprint(json.dumps(remote_collect(" + repr(apis) + "," + repr(since) + ")))\n")


def counter_coverage(records_by_replica, trace):
    previous, deltas = {}, {}
    with Path(trace).open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            current = row["api_replicas"]
            if previous and set(current) != set(previous):
                raise ValueError("Failure replica coverage changed")
            for label, metric in current.items():
                for role in ROLES:
                    for reason in REASONS:
                        key = f"acquisition_failure:{role}:{reason}"
                        value = number(metric[key])
                        identity = (label, role, reason)
                        deltas.setdefault(identity, 0)
                        if label in previous:
                            delta = value - previous[label][key]
                            if delta < 0:
                                raise ValueError("Failure counter reset")
                            deltas[identity] += delta
            previous = current
    if not previous or set(records_by_replica) != set(previous):
        raise ValueError("Exact failure replica counter coverage required")
    counts = {key: 0 for key in deltas}
    for label, rows in records_by_replica.items():
        for row in rows:
            safe = sanitize(row)
            counts[label, safe["role"], safe["reason"]] += 1
    return all(counts[k] >= deltas[k] for k in counts)


def collect(session, inventory, local):
    collected, records, by_replica = [], [], {}
    counts = expected_allocation(inventory)
    for role in ("primary", "secondary"):
        apis = [{k: a[k] for k in ("container_id", "image_id", "started_at")}
                for a in inventory["apis"] if a["host_role"] == role]
        proof = session.call(role, program(apis, inventory["captured_at"], expected_api_count=counts[role]), 45)
        if {a["container_id"] for a in proof["apis"]} != {a["container_id"] for a in apis}:
            raise ValueError("Collected replica identity differs")
        safe_apis = []
        if len(proof["apis"]) != counts[role]:
            raise ValueError("Duplicate or extra diagnostic replicas")
        for api in proof["apis"]:
            if len(api["records"]) > MAX_RECORDS or number(api["input_bytes"]) > MAX_BYTES + 65536:
                raise ValueError("Returned evidence bound exceeded")
            safe_records = [sanitize(r) for r in api["records"]]
            if any(r is None for r in safe_records):
                raise ValueError("Unexpected returned log event")
            records += safe_records
            by_replica[role + ":" + api["container_id"]] = safe_records
            safe_apis.append({"container_id": api["container_id"], "complete": api.get("complete") is True,
                              "input_bytes": api["input_bytes"], "records": safe_records})
        collected.append({"host_role": role, "complete": proof.get("complete") is True
                          and all(a["complete"] for a in safe_apis), "apis": safe_apis})
    complete = all(h.get("complete") is True for h in collected)
    complete = complete and counter_coverage(by_replica, local / "pipeline.jsonl")
    result = {"decision": "ADR0166", "complete": complete, "hosts": collected,
              "record_count": len(records), "counter_coverage": complete,
              "scope": "Exact guard snapshot; native pool statistics sampled adjacent to the failure."}
    (local / "admission-failure-evidence.json").write_text(json.dumps(result, indent=2) + "\n")
    return {k: result[k] for k in ("decision", "complete", "record_count", "counter_coverage", "scope")}
