"""ADR0334 bounded creator-clock proof; production TTL and strict validator unchanged."""

import hashlib
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import work_envelope as policy

SOURCE = "scripts/cce_fixture_clock.py"
KEYS = {"fixture_clock_decision", "fixture_clock_contract_sha256"}


def contract():
    return {
        "decision": "ADR0334",
        "maximum_clock_offset_seconds": 2,
        "maximum_probe_round_trip_seconds": 5,
        "raw_receipt_before_validation": True,
        "observation_decision": "ADR0354",
        "maximum_observations": 3,
        "transport_timeout_seconds": 30,
        "raw_clock_attempt_before_validation": True,
        "strict_fixture_validator_unchanged": True,
        "application_ttl_unchanged": True,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
    }


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (
        goal.get("fixture_clock_decision") != "ADR0334"
        or goal.get("fixture_clock_contract_sha256") != policy.digest(contract())
        or goal.get("generator_process_decision") not in {"ADR0333", "ADR0335"}
    ):
        raise ValueError("Explicit source-bound creator-clock admission required")
    return True


def evidence(fixture, sample, *, nonce, wall_start, wall_end, mono_start, mono_end):
    rtt = mono_end - mono_start
    remote = datetime.fromisoformat(sample["utc"])
    if (
        sample.get("nonce") != nonce
        or remote.tzinfo is None
        or not all(math.isfinite(v) for v in (rtt, wall_start, wall_end, mono_end))
        or not 0 <= rtt <= 5
        or abs((wall_end - wall_start) - rtt) > 0.1
        or not -2 <= remote.timestamp() - wall_end <= 2
        or not -2 <= remote.timestamp() - wall_start <= 2
    ):
        raise ValueError("Creator clock uncertainty exceeds bounded admission")
    return {
        "decision": "ADR0334",
        "fixture_id": fixture["fixture_id"],
        "fixture_sha256": policy.digest(fixture),
        "controller_pid": os.getpid(),
        "creator_utc": remote.isoformat(),
        "probe_round_trip_seconds": rtt,
        "received_monotonic": mono_end,
        "offset_min_seconds": remote.timestamp() - wall_end,
        "offset_max_seconds": remote.timestamp() - wall_start,
    }


def reference_now(record, fixture, *, monotonic_now=None):
    proof = record.get("fixture_clock_evidence", {}).get(fixture["fixture_id"])
    current = time.monotonic() if monotonic_now is None else monotonic_now
    if (
        not isinstance(proof, dict)
        or proof.get("decision") != "ADR0334"
        or proof.get("fixture_id") != fixture["fixture_id"]
        or proof.get("fixture_sha256") != policy.digest(fixture)
        or proof.get("controller_pid") != os.getpid()
        or not math.isfinite(current)
        or current < proof["received_monotonic"]
    ):
        raise ValueError("Same fixture/process monotonic clock proof required")
    return datetime.fromtimestamp(
        datetime.fromisoformat(proof["creator_utc"]).timestamp()
        + proof["probe_round_trip_seconds"]
        + current
        - proof["received_monotonic"],
        UTC,
    )


def sample_creator(stage, fixture):
    attempts = stage.record.setdefault("fixture_clock_attempts", {})
    fixture_id = fixture["fixture_id"]
    if fixture_id in attempts:
        raise ValueError("Fixture clock already sampled; no replay")
    attempts[fixture_id] = []
    for attempt in range(1, 4):
        nonce = uuid4().hex
        program = (
            "import json;from datetime import UTC,datetime;print(json.dumps({'nonce':"
            + repr(nonce)
            + ",'utc':datetime.now(UTC).isoformat()}))"
        )
        row = {"attempt": attempt, "nonce": nonce, "status": "STARTED"}
        attempts[fixture_id].append(row)
        stage.checkpoint()
        wall_start, mono_start = time.time(), time.monotonic()
        try:
            sample = stage.session.api(stage.cid, program, 30)
        except Exception as error:
            row.update(status="TRANSPORT_FAILED", failure_type=type(error).__name__)
            stage.checkpoint()
            raise
        mono_end, wall_end = time.monotonic(), time.time()
        row.update(sample=sample, wall_start=wall_start, wall_end=wall_end,
                   mono_start=mono_start, mono_end=mono_end, status="OBSERVED")
        stage.checkpoint()  # Rejected observations must survive failed admission.
        if (not isinstance(sample, dict) or set(sample) != {"nonce", "utc"}
                or sample["nonce"] != nonce):
            row["status"] = "MALFORMED"
            stage.checkpoint()
            raise ValueError("Malformed creator clock response")
        try:
            remote = datetime.fromisoformat(sample["utc"])
            if remote.tzinfo is None:
                raise ValueError("Aware creator clock required")
        except (TypeError, ValueError):
            row["status"] = "MALFORMED"
            stage.checkpoint()
            raise
        try:
            proof = evidence(fixture, sample, nonce=nonce, wall_start=wall_start,
                             wall_end=wall_end, mono_start=mono_start, mono_end=mono_end)
        except ValueError:
            row["status"] = "REJECTED_UNCERTAINTY"
            stage.checkpoint()
            if attempt == 3:
                raise
            continue
        row["status"] = "ACCEPTED"
        stage.record.setdefault("fixture_clock_evidence", {})[fixture_id] = proof
        stage.checkpoint()
        return reference_now(stage.record, fixture)
    raise AssertionError("Bounded clock observations exhausted")
