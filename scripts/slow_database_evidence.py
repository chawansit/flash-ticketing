"""ADR0171 existing slow-phase logs with bounded temporal pool evidence."""
import bisect
import inspect
import json
from datetime import datetime
from pathlib import Path

import admission_failure_evidence as failure

number = failure.number
failure_sanitize = failure.sanitize
PHASES = ("commit", "pool_return")
MAX_SLOW_RECORDS = 512
MAX_TRACE_BYTES = 64 * 2**20
MAX_TRACE_ROWS = 2000
MAX_TRACE_LINE = 2 * 2**20
GAUGES = ("pool_acquiring", "pool_in_use", "pool_state:payment_pool_available",
          "pool_state:payment_requests_waiting", "pool_state:shared_acquisition_used",
          "pool_state:shared_acquisition_acquiring", "pool_state:shared_acquisition_retained")
DURATIONS = ("db_pool_acquire:ok", "db_commit:all", "db_connection_hold:all")


def sanitize(record):
    if record.get("event") != "slow_db_phase":
        return failure_sanitize(record)
    timestamp = record.get("time")
    if not isinstance(timestamp, str) or len(timestamp) > 64 or datetime.fromisoformat(timestamp).tzinfo is None:
        raise ValueError("Aware slow-phase timestamp required")
    if record.get("phase") not in PHASES or record.get("outcome") not in ("ok", "error"):
        raise ValueError("Unknown slow-phase classification")
    duration = number(record["duration_ms"])
    if duration < 100:
        raise ValueError("Frozen slow-phase threshold required")
    return {"event": "slow_db_phase", "time": timestamp, "phase": record["phase"],
            "outcome": record["outcome"], "duration_ms": duration}


def program(apis, since, *, expected_api_count=None):
    # Reuse the verified stream program, including exact identity validation.
    source = failure.program(apis, since, expected_api_count=expected_api_count).rsplit("\nprint(json.dumps(remote_collect(", 1)[0]
    source += "\nfailure_sanitize=sanitize\nPHASES=" + repr(PHASES) + "\n" + inspect.getsource(sanitize)
    limits = {"db_acquisition_failure": failure.MAX_RECORDS, "slow_db_phase": MAX_SLOW_RECORDS}
    return source + "\nprint(json.dumps(remote_collect(" + repr(apis) + "," + repr(since) + ",record_limits=" + repr(limits) + ")))\n"


def trace_context(path, expected_labels):
    path = Path(path)
    if path.stat().st_size > MAX_TRACE_BYTES:
        raise ValueError("Observer trace byte bound exceeded")
    samples, previous, starts, counters = [], None, {}, {}
    with path.open("rb") as stream:
        while line := stream.readline(MAX_TRACE_LINE + 1):
            if len(line) > MAX_TRACE_LINE or len(samples) >= MAX_TRACE_ROWS:
                raise ValueError("Observer trace row bound exceeded")
            row = json.loads(line)
            stamp = datetime.fromisoformat(row["utc"])
            if stamp.tzinfo is None or (previous and not 0 < (stamp - previous).total_seconds() <= 2):
                raise ValueError("Observer timestamp gap or reversal")
            replicas = row["api_replicas"]
            if set(replicas) != set(expected_labels) or row.get("api_metrics_error"):
                raise ValueError("Exact observer replica coverage required")
            context = {}
            for label, metric in replicas.items():
                start = number(metric["process_start_time_seconds"])
                if start <= 0 or (label in starts and start != starts[label]):
                    raise ValueError("Observer process identity changed")
                starts[label] = start
                values = {key: number(metric[key]) for key in GAUGES}
                durations = {}
                current = {}
                for phase in DURATIONS:
                    count_key, sum_key = "duration:" + phase + ":count", "duration:" + phase + ":sum"
                    count, total = number(metric[count_key]), number(metric[sum_key])
                    current[phase] = (count, total)
                    if label in counters:
                        old_count, old_total = counters[label][phase]
                        if count < old_count or total < old_total:
                            raise ValueError("Observer duration counter reset")
                        if count == old_count and total != old_total:
                            raise ValueError("Observer duration sum changed without count")
                        durations[phase] = 1000 * (total - old_total) / (count - old_count) if count > old_count else None
                    else:
                        durations[phase] = None
                counters[label] = current
                context[label] = {"pool": values, "interval_mean_ms": durations}
            samples.append({"utc": row["utc"], "timestamp": stamp.timestamp(),
                            "db_lock_waiters": number(row["db_lock_waiters"]), "replicas": context})
            previous = stamp
    if len(samples) < 2:
        raise ValueError("Observer context missing")
    return samples


def temporal_context(events, samples):
    times = [sample["timestamp"] for sample in samples]
    result, failures_complete = [], True
    for label, event in events:
        stamp = datetime.fromisoformat(event["time"]).timestamp()
        index = bisect.bisect_left(times, stamp)
        nearby = [s for s in samples[max(0, index - 2):index + 3] if abs(s["timestamp"] - stamp) <= 2]
        bracketed = any(s["timestamp"] <= stamp for s in nearby) and any(s["timestamp"] >= stamp for s in nearby)
        if event["event"] == "db_acquisition_failure" and not bracketed:
            failures_complete = False
        result.append({"replica": label, "record": event, "context_bracketed": bracketed,
                       "nearby_samples": [{"utc": s["utc"], "offset_seconds": s["timestamp"] - stamp,
                                            "db_lock_waiters": s["db_lock_waiters"], **s["replicas"][label]}
                                           for s in nearby]})
    return result, failures_complete


def collect(session, inventory, local):
    local = Path(local)
    hosts, failures, events = [], {}, []
    counts = failure.expected_allocation(inventory)
    for role in ("primary", "secondary"):
        apis = [{key: a[key] for key in ("container_id", "image_id", "started_at")}
                for a in inventory["apis"] if a["host_role"] == role]
        proof = session.call(role, program(apis, inventory["captured_at"], expected_api_count=counts[role]), 45)
        if len(proof["apis"]) != counts[role] or {a["container_id"] for a in proof["apis"]} != {a["container_id"] for a in apis}:
            raise ValueError("Collected replica identity differs")
        safe_apis = []
        for api in proof["apis"]:
            if number(api["input_bytes"]) > failure.MAX_BYTES + 65536:
                raise ValueError("Returned byte bound exceeded")
            rows = api["records"]
            if len(rows) > failure.MAX_RECORDS + MAX_SLOW_RECORDS:
                raise ValueError("Returned record bound exceeded")
            rows = [sanitize(row) for row in rows]
            if any(row is None for row in rows):
                raise ValueError("Unexpected returned diagnostic event")
            failed = [row for row in rows if row["event"] == "db_acquisition_failure"]
            slow = [row for row in rows if row["event"] == "slow_db_phase"]
            if len(failed) > failure.MAX_RECORDS or len(slow) > MAX_SLOW_RECORDS:
                raise ValueError("Per-class record bound exceeded")
            label = role + ":" + api["container_id"]
            failures[label] = failed
            events.extend((label, row) for row in rows)
            safe_apis.append({"container_id": api["container_id"], "complete": api.get("complete") is True,
                              "input_bytes": api["input_bytes"], "records": rows})
        hosts.append({"host_role": role, "complete": proof.get("complete") is True and all(a["complete"] for a in safe_apis),
                      "apis": safe_apis})
    (local / "slow-database-evidence.json").write_text(json.dumps({
        "decision": "ADR0171", "complete": False, "context_status": "pending", "hosts": hosts}, indent=2) + "\n")
    samples = trace_context(local / "pipeline.jsonl", failures)
    covered = failure.counter_coverage(failures, local / "pipeline.jsonl")
    contexts, context_complete = temporal_context(events, samples)
    complete = all(h["complete"] for h in hosts) and covered and context_complete
    result = {"decision": "ADR0171", "complete": complete, "counter_coverage": covered,
              "failure_context_complete": context_complete, "failure_count": sum(len(v) for v in failures.values()),
              "slow_phase_count": len(events) - sum(len(v) for v in failures.values()),
              "uncorrelated_slow_records": sum(not r["context_bracketed"] and r["record"]["event"] == "slow_db_phase" for r in contexts),
              "hosts": hosts, "temporal_context": contexts,
              "scope": "Existing API phase logs; replica/time adjacency and interval means, not request correlation or causal proof."}
    (local / "slow-database-evidence.json").write_text(json.dumps(result, indent=2) + "\n")
    legacy_hosts = [{**host, "apis": [{**api, "records": failures[host["host_role"] + ":" + api["container_id"]]}
                                     for api in host["apis"]]} for host in hosts]
    legacy = {"decision": "ADR0166", "complete": complete, "hosts": legacy_hosts,
              "record_count": result["failure_count"], "counter_coverage": covered,
              "scope": "Exact guard snapshot; native pool statistics sampled adjacent to the failure."}
    (local / "admission-failure-evidence.json").write_text(json.dumps(legacy, indent=2) + "\n")
    return {key: result[key] for key in ("decision", "complete", "counter_coverage", "failure_context_complete",
                                       "failure_count", "slow_phase_count", "uncorrelated_slow_records", "scope")}
