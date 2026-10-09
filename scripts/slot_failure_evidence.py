"""ADR0241 strict collection of bounded metrics snapshots, without database queries."""
import hashlib
import json
import math

PREFIX = "# ticketing_db_failure_diagnostics "
MAX_PAYLOAD = 64 * 1024
OPERATIONS = {"payment_request", "payment_callback", "order_status", "order_create", "hold", "availability", "other"}
PHASES = {"connection_context", "setup", "body", "commit", "rollback", "context_exit", "pool_return"}
PHASES |= {"query_" + c for c in ("SELECT", "INSERT", "UPDATE", "DELETE", "SET", "BEGIN", "COMMIT", "ROLLBACK", "OTHER")}


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def finite(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def validate(value):
    required = {"schema_version", "failure_total", "overwritten_total", "diagnostic_errors", "complete", "records"}
    if (not isinstance(value, dict) or set(value) != required or value.get("schema_version") != 1
            or type(value.get("complete")) is not bool
            or any(not integer(value.get(k)) for k in ("failure_total", "overwritten_total", "diagnostic_errors"))
            or not isinstance(value.get("records"), list) or len(value["records"]) > 16):
        raise ValueError("Invalid bounded slot snapshot")
    if not value["complete"] or value["overwritten_total"] or value["diagnostic_errors"]:
        raise ValueError("Incomplete slot evidence")
    records = value["records"]
    if len(records) != value["failure_total"]:
        raise ValueError("Missing retained slot failures")
    for sequence, record in enumerate(records, 1):
        if (not isinstance(record, dict)
                or set(record) != {"sequence", "captured_at_unix_seconds", "role", "reason", "holders"}
                or record["sequence"] != sequence or not finite(record["captured_at_unix_seconds"])
                or record["role"] not in {"general", "payment", "callback"}
                or record["reason"] not in {"native_timeout", "native_limit", "global_limit", "role_limit"}
                or not isinstance(record["holders"], list) or len(record["holders"]) > 128):
            raise ValueError("Invalid slot failure event")
        seen = set()
        for holder in record["holders"]:
            if (not isinstance(holder, dict)
                    or set(holder) != {"lease", "role", "operation", "phase", "age_ms", "phase_age_ms"}
                    or not integer(holder["lease"], 1) or holder["lease"] in seen
                    or holder["role"] not in {"general", "payment"}
                    or holder["operation"] not in OPERATIONS or holder["phase"] not in PHASES
                    or not finite(holder["age_ms"]) or not finite(holder["phase_age_ms"])):
                raise ValueError("Invalid slot owner")
            seen.add(holder["lease"])
    return value


def parse(payload):
    lines = [line for line in payload.splitlines() if line.startswith(PREFIX)]
    if len(lines) != 1 or len(lines[0].encode()) + 1 > MAX_PAYLOAD:
        raise ValueError("Exactly one bounded slot metrics comment required")
    return validate(json.loads(lines[0][len(PREFIX):]))


def diagnostic_error(payload):
    """Retain safe bounded metadata only, never unknown fields or customer data."""
    lines = [line for line in payload.splitlines() if line.startswith(PREFIX)]
    result = {"kind": "invalid_or_incomplete_slot_comment", "comment_count": len(lines)}
    if len(lines) != 1:
        return result
    raw = lines[0].encode()
    result["comment_bytes"] = len(raw) + 1
    result["payload_prefix_sha256"] = hashlib.sha256(raw[:MAX_PAYLOAD]).hexdigest()
    if len(raw) + 1 > MAX_PAYLOAD:
        return result
    try:
        value = json.loads(lines[0][len(PREFIX):])
    except ValueError:
        return result
    if isinstance(value, dict):
        result["header"] = {k: value[k] for k in
            ("schema_version", "failure_total", "overwritten_total", "diagnostic_errors")
            if integer(value.get(k))}
        if type(value.get("complete")) is bool:
            result["header"]["complete"] = value["complete"]
    return result


def install(module):
    original = module.parse_api_metrics
    original_api = module.api_metrics

    def metrics(payload):
        # Legacy worker metrics share this parser but do not expose API slot state.
        result = original(payload)
        if any(line.startswith(PREFIX) for line in payload.splitlines()):
            try:
                result = {**result, "db_failure_diagnostics": parse(payload)}
            except ValueError:
                # Keep CPU/pool telemetry; completeness still fails in summarize().
                result = {**result, "db_failure_diagnostics_error": diagnostic_error(payload)}
        return result

    def api_metrics(address):
        result = original_api(address)
        if "db_failure_diagnostics" not in result and "db_failure_diagnostics_error" not in result:
            result = {**result, "db_failure_diagnostics_error": {"kind": "missing_slot_comment"}}
        return result

    module.parse_api_metrics = metrics
    module.api_metrics = api_metrics


def summarize(rows, expected_replicas):
    replicas = {key: {} for key in expected_replicas}
    totals = {key: 0 for key in replicas}
    last_counters = {key: 0 for key in replicas}
    for row in rows:
        values = row.get("api_replicas", {})
        if set(values) != set(replicas) or row.get("api_metrics_error"):
            raise ValueError("Complete admitted API evidence required")
        for name, metrics in values.items():
            if metrics.get("db_failure_diagnostics_error") or "db_failure_diagnostics" not in metrics:
                raise ValueError("Incomplete admitted API slot evidence")
            value = validate(metrics["db_failure_diagnostics"])
            if value["failure_total"] < totals[name]:
                raise ValueError("Failure counter reset")
            # Shared-admission failure counters must agree with the retained ring.
            counter = sum(v for k, v in metrics.items() if k.startswith("acquisition_failure:"))
            if not finite(counter) or counter < last_counters[name]:
                raise ValueError("Driver failure counter invalid or reset")
            last_counters[name] = counter
            for record in value["records"]:
                prior = replicas[name].get(record["sequence"])
                if prior is not None and prior != record:
                    raise ValueError("Retained failure identity changed")
                replicas[name][record["sequence"]] = record
            totals[name] = value["failure_total"]
    if not rows:
        raise ValueError("Slot evidence samples required")
    # Exposition is concurrent with failures. Earlier counter/ring reads may race;
    # require a settled final snapshot, never excuse missing terminal evidence.
    if any(last_counters[name] != totals[name] for name in replicas):
        raise ValueError("Terminal driver counter and slot evidence disagree")
    return {"complete": True, "failure_total": sum(totals.values()),
            "replicas": {k: list(v.values()) for k, v in replicas.items()},
            "historical_timeout_cause_established": False}
