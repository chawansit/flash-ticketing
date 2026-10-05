"""ADR0166 bounded, allowlisted failure snapshots from exact live API containers."""
import inspect
import json
import math
import re
from datetime import datetime
from pathlib import Path

ROLES = ("general", "payment")
REASONS = ("global_limit", "role_limit", "native_timeout", "native_limit")
MAX_BYTES = 32 * 2**20
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


def remote_collect(apis, since):
    import os
    import select
    import subprocess
    import time

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

        def accept(line, records=records):
            if len(line) > MAX_LINE:
                raise ValueError("Diagnostic line bound exceeded")
            payload = line.partition(b" ")[2]
            if not payload.startswith(b"{"):
                return
            try:
                value = json.loads(payload)
            except (ValueError, UnicodeError):
                if b"db_acquisition_failure" in payload:
                    raise ValueError("Malformed failure snapshot") from None
                return
            item = sanitize(value)
            if item is not None:
                if len(records) >= MAX_RECORDS:
                    raise ValueError("Diagnostic record bound exceeded")
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


def program(apis, since):
    if not apis or len(apis) > 2:
        raise ValueError("One host's exact API replicas required")
    if not isinstance(since, str) or len(since) > 64 or datetime.fromisoformat(since).tzinfo is None:
        raise ValueError("Aware capture window required")
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
    for role in ("primary", "secondary"):
        apis = [{k: a[k] for k in ("container_id", "image_id", "started_at")}
                for a in inventory["apis"] if a["host_role"] == role]
        if len(apis) != 2:
            raise ValueError("Fixed two-host API allocation required")
        proof = session.call(role, program(apis, inventory["captured_at"]), 45)
        if {a["container_id"] for a in proof["apis"]} != {a["container_id"] for a in apis}:
            raise ValueError("Collected replica identity differs")
        safe_apis = []
        if len(proof["apis"]) != 2:
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
