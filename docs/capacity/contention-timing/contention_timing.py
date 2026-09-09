"""Controlled seat contention with explicit client queue and transport timing."""
import argparse
import asyncio
import json
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpcore
import httpx
from http_load_generator import error_diagnostic, percentile


def app_duration(value):
    match = re.search(r'(?:^|,)\s*app;dur=([0-9.]+)', value)
    return float(match[1]) if match else None


def distribution(values):
    values = [v for v in values if v is not None]
    return {"count": len(values), "p50": percentile(values, .5), "p95": percentile(values, .95),
            "p99": percentile(values, .99), "max": max(values, default=None)}


def port(response):
    stream = response.extensions.get("network_stream")
    address = stream.get_extra_info("client_addr") if stream else None
    return address[1] if address else None


async def measured_request(client, url, headers, body):
    events = {}
    async def trace(name, info):
        # Trace info contains request headers; deliberately retain only event names/times.
        events[name] = time.perf_counter()
    started = time.perf_counter()
    try:
        response = await client.post(url, headers=headers, json=body, extensions={"trace": trace})
        finished = time.perf_counter()
        code, request_id = error_diagnostic(response)
        app_ms = app_duration(response.headers.get("Server-Timing", ""))
        row = {"status": str(response.status_code), "code": code if response.status_code != 201 else None,
               "request_id": request_id, "app_ms": app_ms, "local_port": port(response),
               "hold_id": response.json().get("hold_id") if response.status_code == 201 else None}
    except httpx.HTTPError as exc:
        finished = time.perf_counter()
        row = {"status": "transport_error", "code": type(exc).__name__, "app_ms": None}
    def interval(start, end):
        return (events[end]-events[start])*1000 if start in events and end in events else None
    row.update({"client_ms": (finished-started)*1000,
                "connect_attempted": "connection.connect_tcp.started" in events,
                "tcp_ms": interval("connection.connect_tcp.started", "connection.connect_tcp.complete"),
                "before_io_ms": (min(events.values())-started)*1000 if events else None,
                "send_to_headers_ms": interval("http11.send_request_body.complete", "http11.receive_response_headers.complete")})
    row["outside_app_ms"] = row["client_ms"]-row["app_ms"] if row["app_ms"] is not None else None
    return row


async def worker(args):
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("Development manifest required")
    if datetime.fromisoformat(manifest["expires_at"]) < datetime.now(UTC)+timedelta(minutes=5):
        raise ValueError("Fresh credentials required")
    tokens = manifest["viewer_tokens"]
    if len(tokens) < args.contenders:
        raise ValueError("Insufficient distinct viewers")
    indices = list(range(args.worker_index, args.contenders, args.workers))
    lane_count = min(args.contenders, args.lanes)//args.workers
    clients = [httpx.AsyncClient(base_url=manifest["origin"], timeout=20, trust_env=False,
                               limits=httpx.Limits(max_connections=1, max_keepalive_connections=1,
                                                  keepalive_expiry=5)) for _ in range(lane_count)]
    locks = [asyncio.Lock() for _ in clients]
    warm_ports = [None]*lane_count
    rows, lags = [], []
    run_id = str(uuid4())
    finished = asyncio.Event()
    gate = asyncio.Event()
    try:
        if args.mode == "warm":
            responses = await asyncio.gather(*(c.get("/health/live") for c in clients))
            for i, r in enumerate(responses):
                r.raise_for_status()
                warm_ports[i] = port(r)
            if None in warm_ports or len(set(warm_ports)) != lane_count:
                raise ValueError("Warm connections were not independently verified")
        (args.output/f"ready-{args.worker_index}.json").write_text(json.dumps({"ready_at": time.time()}))
        deadline = time.monotonic()+40
        while not (args.output/"start.json").exists():
            if time.monotonic()>deadline:
                raise TimeoutError("Coordinator did not release the wave")
            await asyncio.sleep(.01)
        target = json.loads((args.output/"start.json").read_text())["epoch"]
        delay = target-time.time()
        if delay <= 0:
            raise ValueError("Missed coordinated start")
        async def attempt(position, index):
            await gate.wait()
            dispatched = time.perf_counter()
            lane = position % lane_count
            async with locks[lane]:
                acquired = time.perf_counter()
                row = await measured_request(clients[lane], "/v1/holds",
                                             {"Authorization": "Bearer "+tokens[index],
                                              "Idempotency-Key": f"{run_id}-{index}"},
                                             {"event_id": manifest["show_ids"][0], "seat_ids": [f"S{args.seat}"]})
                row.update({"index": index, "worker": args.worker_index, "lane": lane,
                            "dispatch_ms": (dispatched-release)*1000,
                            "lane_wait_ms": (acquired-dispatched)*1000,
                            "total_ms": (time.perf_counter()-release)*1000,
                            "warm_port_reused": row.get("local_port") == warm_ports[lane] if args.mode == "warm" else None})
                rows.append(row)
        async def lag_probe():
            expected = time.perf_counter()+.01
            while not finished.is_set():
                await asyncio.sleep(max(0, expected-time.perf_counter()))
                now = time.perf_counter()
                lags.append(max(0, now-expected)*1000)
                expected = now+.01
        tasks = [asyncio.create_task(attempt(i, index)) for i, index in enumerate(indices)]
        await asyncio.sleep(delay)
        started = datetime.now(UTC).isoformat()
        cpu_start, release = time.process_time(), time.perf_counter()
        probe = asyncio.create_task(lag_probe())
        gate.set()
        await asyncio.gather(*tasks)
        elapsed, cpu = time.perf_counter()-release, time.process_time()-cpu_start
        finished.set()
        await probe
        result = {"run_id": run_id, "measured_started_utc": started, "utc": datetime.now(UTC).isoformat(),
                  "mode": args.mode, "worker": args.worker_index, "lanes": lane_count,
                  "event_id": manifest["show_ids"][0], "seat_id": f"S{args.seat}",
                  "statuses": {"hold": dict(Counter(r["status"] for r in rows))}, "requests": rows,
                  "generator_cpu_seconds": cpu, "elapsed_seconds": elapsed,
                  "event_loop_lag_ms": distribution(lags), "httpx_version": httpx.__version__,
                  "httpcore_version": httpcore.__version__,
                  "warm_reuse_pass": all(r["warm_port_reused"] and not r["connect_attempted"] for r in rows) if args.mode == "warm" else None}
        (args.output/f"worker-{args.worker_index}.json").write_text(json.dumps(result, indent=2)+"\n")
    finally:
        finished.set()
        await asyncio.gather(*(c.aclose() for c in clients))


def coordinate(args):
    args.output.mkdir(parents=True, exist_ok=False)
    processes = []
    try:
        for i in range(args.workers):
            log = (args.output/f"worker-{i}.log").open("w", encoding="utf-8")
            command = [sys.executable, str(Path(__file__).resolve()), "--manifest", str(args.manifest.resolve()),
                       "--output", str(args.output.resolve()), "--contenders", str(args.contenders),
                       "--lanes", str(args.lanes), "--seat", str(args.seat), "--mode", args.mode,
                       "--workers", str(args.workers), "--worker-index", str(i)]
            processes.append((subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT), log))
        deadline = time.monotonic()+40
        while len(list(args.output.glob("ready-*.json"))) < args.workers:
            if any(proc.poll() is not None for proc, _ in processes) or time.monotonic()>deadline:
                raise RuntimeError("Worker bootstrap failed; inspect retained logs")
            time.sleep(.01)
        # All readers see a fully written file; it contains only a start timestamp.
        temporary = args.output/"start.tmp"
        temporary.write_text(json.dumps({"epoch": time.time()+.5}))
        temporary.replace(args.output/"start.json")
        codes = [proc.wait(timeout=60) for proc, _ in processes]
        if any(codes):
            raise RuntimeError("Worker failed; retain partial evidence")
        workers = [json.loads((args.output/f"worker-{i}.json").read_text()) for i in range(args.workers)]
        rows = [r for w in workers for r in w["requests"]]
        starts = [datetime.fromisoformat(w["measured_started_utc"]) for w in workers]
        winners = [{"run_id": w["run_id"], "hold_id": r["hold_id"]} for w in workers for r in w["requests"] if r["status"] == "201"]
        failed = [r for r in rows if r["status"] != "201"]
        expected = sum(r["status"] == "409" and r["code"] in {"SEAT_BUSY", "SEAT_UNAVAILABLE"} for r in rows)
        skew = (max(starts)-min(starts)).total_seconds()*1000
        result = {"contenders": args.contenders, "mode": args.mode, "workers": args.workers,
                  "total_lanes": min(args.contenders,args.lanes), "seat_id": workers[0]["seat_id"],
                  "event_id": workers[0]["event_id"], "winners": winners,
                  "start_utc": min(starts).isoformat(), "end_utc": max(w["utc"] for w in workers),
                  "start_skew_ms": skew, "statuses": dict(Counter(r["status"] for r in rows)),
                  "error_codes": dict(Counter(r["code"] for r in rows if r["code"])),
                  "expected_conflicts": expected, "one_http_winner_pass": len(winners)==1,
                  "accounting_pass": len(rows)==args.contenders and len({r["index"] for r in rows})==args.contenders,
                  "warm_reuse_pass": all(w["warm_reuse_pass"] for w in workers) if args.mode == "warm" else None,
                  "connect_attempts": sum(r["connect_attempted"] for r in rows),
                  "generator_cpu_seconds": sum(w["generator_cpu_seconds"] for w in workers),
                  "max_worker_loop_lag_ms": max(w["event_loop_lag_ms"]["max"] or 0 for w in workers),
                  "timing_ms": {key: distribution([r.get(key) for r in rows]) for key in
                                ["dispatch_ms","lane_wait_ms","tcp_ms","before_io_ms","send_to_headers_ms","app_ms","outside_app_ms","client_ms","total_ms"]},
                  "failed_total_ms": distribution([r["total_ms"] for r in failed]),
                  "failed_client_ms": distribution([r["client_ms"] for r in failed]),
                  "note": "Merged request percentiles. Total includes dispatch and explicit lane queue. Outside-app residual is not pure ingress wait."}
        result["measurement_valid"] = result["accounting_pass"] and skew <= 100 and result["warm_reuse_pass"] is not False
        result["availability_pass"] = len(winners)==1 and expected==args.contenders-1
        (args.output/"summary.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
        print(json.dumps({k:result[k] for k in ["mode","workers","contenders","statuses","one_http_winner_pass","measurement_valid","failed_total_ms","connect_attempts"]}),flush=True)
        if not result["measurement_valid"] or not result["one_http_winner_pass"]:
            raise RuntimeError("Measurement/correctness gate failed; stop matrix and retain evidence")
    finally:
        for proc, log in processes:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=10)
            log.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--contenders", type=int, choices=[100,1000], required=True)
    p.add_argument("--lanes", type=int, default=128)
    p.add_argument("--seat", type=int, required=True)
    p.add_argument("--mode", choices=["cold","warm"], default="cold")
    p.add_argument("--workers", type=int, choices=[1,4], default=1)
    p.add_argument("--worker-index", type=int)
    p.add_argument("--matrix", action="store_true")
    a = p.parse_args()
    if not 4 <= a.lanes <= 128 or min(a.lanes,a.contenders)%4 or not 0 <= a.seat <= (292 if a.matrix else 299):
        p.error("Use 4–128 total lanes divisible by four and fresh fixture seats within 0–299")
    if a.worker_index is not None:
        asyncio.run(worker(a))
    elif a.matrix:
        variants = [("cold",1),("warm",1),("cold",4),("warm",4)]
        for i,(mode,workers) in enumerate(variants+list(reversed(variants))):
            case = SimpleNamespace(**vars(a))
            case.mode,case.workers,case.seat = mode,workers,a.seat+i
            case.output = a.output/f"wave-{i}-{mode}-{workers}p"
            coordinate(case)
    else:
        coordinate(a)


if __name__ == "__main__":
    main()
