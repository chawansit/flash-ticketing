"""Synchronized HTTP seat contention; no retries, separate correctness and availability gates."""
import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx
from http_load_generator import error_diagnostic, percentile


async def run(args):
    m = json.loads(args.manifest.read_text(encoding="utf-8"))
    if m.get("schema_version") != 1 or m.get("environment") != "development":
        raise ValueError("Development manifest required")
    if args.output.resolve() == args.manifest.resolve() or args.output.exists():
        raise ValueError("Use a fresh result path, never the credential path")
    if datetime.fromisoformat(m["expires_at"]) < datetime.now(UTC) + timedelta(minutes=5):
        raise ValueError("Fresh credentials required")
    if len(m["viewer_tokens"]) < args.contenders or not 0 <= args.seat < m["seats_per_show"]:
        raise ValueError("Insufficient viewers or invalid seat")
    show, seat, run_id = m["show_ids"][0], f"S{args.seat}", str(uuid4())
    gate = asyncio.Event()
    rows = []
    async with httpx.AsyncClient(base_url=m["origin"], timeout=20,
                                limits=httpx.Limits(max_connections=args.contenders,
                                                    max_keepalive_connections=args.contenders)) as client:
        (await client.get("/health/ready")).raise_for_status()
        async def attempt(index):
            await gate.wait()
            begin = perf_counter()
            try:
                r = await client.post("/v1/holds", json={"event_id": show, "seat_ids": [seat]},
                                      headers={"Authorization": "Bearer " + m["viewer_tokens"][index],
                                               "Idempotency-Key": f"{run_id}-{index}"})
                code, request_id = error_diagnostic(r) if r.status_code != 201 else (None, None)
                rows.append({"index": index, "status": str(r.status_code), "code": code,
                             "request_id": request_id, "latency_ms": (perf_counter()-begin)*1000,
                             "dispatch_offset_ms": (begin-release)*1000,
                             "hold_id": r.json().get("hold_id") if r.status_code == 201 else None})
            except httpx.HTTPError as exc:
                rows.append({"index": index, "status": "transport_error", "code": type(exc).__name__,
                             "latency_ms": (perf_counter()-begin)*1000,
                             "dispatch_offset_ms": (begin-release)*1000})
        tasks = [asyncio.create_task(attempt(i)) for i in range(args.contenders)]
        await asyncio.sleep(0)
        started = datetime.now(UTC).isoformat()
        release = perf_counter()
        gate.set()
        await asyncio.gather(*tasks)
    statuses = Counter(row["status"] for row in rows)
    expected = sum(row["status"] == "409" and row["code"] in {"SEAT_BUSY", "SEAT_UNAVAILABLE"} for row in rows)
    result = {"run_id": run_id, "measured_started_utc": started, "utc": datetime.now(UTC).isoformat(),
              "event_id": show, "seat_id": seat, "contenders": args.contenders,
              "statuses": {"hold": dict(statuses)}, "expected_conflicts": expected,
              "error_codes": dict(Counter(row["code"] for row in rows if row["code"])),
              "http_one_winner_pass": statuses["201"] == 1,
              "availability_gate_pass": statuses["201"] == 1 and expected == args.contenders-1,
              "failed_response_p95_ms": percentile([r["latency_ms"] for r in rows if r["status"] != "201"], .95),
              "dispatch_spread_ms": max(r["dispatch_offset_ms"] for r in rows), "requests": rows,
              "note": "One client barrier, not simultaneous server arrival. No retries. Durable ownership verified separately."}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({k:v for k,v in result.items() if k != "requests"}))
    if not result["http_one_winner_pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--contenders", type=int, choices=[100, 1000], required=True)
    p.add_argument("--seat", type=int, required=True)
    asyncio.run(run(p.parse_args()))
