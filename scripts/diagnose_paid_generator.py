"""Isolate the paid generator against a loopback-only synthetic HTTP responder.

No cloud credentials or datastore connections are used. Results are generator
controls, never evidence of production ticket capacity or booking correctness.
"""

import argparse
import asyncio
import contextlib
import json
import math
import os
import platform
import pstats
import subprocess
from collections import Counter
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

from checkout_journey_probe import percentile
from paid_ticket_load_generator import endpoint
from paid_ticket_sharded_generator import run as run_shards


class Responder:
    def __init__(self, response_ms=30, command_seconds=0.5, ticket_seconds=5):
        self.response_ms = response_ms
        self.command_seconds = command_seconds
        self.ticket_seconds = ticket_seconds
        self.orders = {}
        self.hold_keys = {}
        self.commands = {}
        self.requests = Counter()
        self.connections = self.closed_connections = self.protocol_errors = 0
        self.active_handlers = self.peak_handlers = 0
        self.writers = set()
        self.tasks = set()
        self.handler_ms = []
        self.loop_lag_ms = []
        self.server = None
        self.monitor = None

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0, limit=16384)
        self.origin = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        self.monitor = asyncio.create_task(self.watch_loop())
        return self

    async def __aexit__(self, *_):
        self.server.close()
        await self.server.wait_closed()
        self.monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.monitor
        for writer in list(self.writers):
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def watch_loop(self):
        while True:
            due = perf_counter() + 0.1
            await asyncio.sleep(0.1)
            self.loop_lag_ms.append(max(0, (perf_counter() - due) * 1000))

    def route(self, method, path, headers, body):
        now = perf_counter()
        if method == "GET" and path == "/health/ready":
            return 200, {"status": "ready"}
        if method == "POST" and path == "/v1/holds":
            key = headers.get("idempotency-key")
            if not key or len(body.get("seat_ids", [])) != 1:
                return 400, {"error": "invalid synthetic hold"}
            if key not in self.hold_keys:
                order_id, command_id = str(uuid4()), str(uuid4())
                row = {"order_id": order_id, "command_id": command_id,
                       "show_id": body["event_id"], "durable_at": now + self.command_seconds,
                       "ticket_id": str(uuid4()), "payment_at": None}
                self.orders[order_id] = row
                self.commands[(row["show_id"], command_id)] = row
                self.hold_keys[key] = row
            row = self.hold_keys[key]
            return 202, {"command_id": row["command_id"], "order_id": row["order_id"]}
        parts = path.strip("/").split("/")
        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "reservation-commands"]:
            row = self.commands.get((parts[2], parts[3]))
            if row:
                return 200, {"persistence_status": "DURABLE" if now >= row["durable_at"] else "PENDING"}
        if len(parts) >= 3 and parts[:2] == ["v1", "orders"]:
            row = self.orders.get(parts[2])
            if row and method == "POST" and parts[3:] == ["payments"]:
                if now < row["durable_at"]:
                    return 409, {"error": "not durable"}
                if row["payment_at"] is None:
                    row["payment_at"] = now
                return 202, {"status": "ACCEPTED"}
            if row and method == "GET" and len(parts) == 3:
                paid = row["payment_at"] is not None
                fulfilled = paid and now >= row["payment_at"] + self.ticket_seconds
                return 200, {"status": "FULFILLED" if fulfilled else "PAID" if paid else "PENDING",
                             "tickets": [{"id": row["ticket_id"]}] if fulfilled else []}
        return 404, {"error": "unknown synthetic route"}

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.add(writer)
        self.connections += 1
        try:
            while True:
                try:
                    raw = await reader.readuntil(b"\r\n\r\n")
                except asyncio.IncompleteReadError as exc:
                    if exc.partial:
                        self.protocol_errors += 1
                    break
                lines = raw.decode("ascii").split("\r\n")
                method, path, version = lines[0].split()
                if version != "HTTP/1.1":
                    raise ValueError("HTTP/1.1 required")
                headers = dict(line.lower().split(":", 1) for line in lines[1:] if line)
                headers = {key: value.strip() for key, value in headers.items()}
                length = int(headers.get("content-length", "0"))
                if not 0 <= length <= 16384 or "transfer-encoding" in headers:
                    raise ValueError("Unsupported body framing")
                payload = await reader.readexactly(length)
                body = json.loads(payload) if payload else {}
                started = perf_counter()
                self.requests[endpoint(path)] += 1
                self.active_handlers += 1
                self.peak_handlers = max(self.peak_handlers, self.active_handlers)
                try:
                    await asyncio.sleep(self.response_ms / 1000)
                    code, response = self.route(method, path, headers, body)
                    data = json.dumps(response, separators=(",", ":")).encode()
                    elapsed = (perf_counter() - started) * 1000
                    self.handler_ms.append(elapsed)
                    reason = {200: "OK", 202: "Accepted", 400: "Bad Request",
                              404: "Not Found", 409: "Conflict"}[code]
                    writer.write((f"HTTP/1.1 {code} {reason}\r\n"
                                  f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n"
                                  f"Server-Timing: app;dur={elapsed:.3f}\r\n\r\n").encode() + data)
                    await writer.drain()
                finally:
                    self.active_handlers -= 1
                if headers.get("connection") == "close":
                    break
        except (ValueError, KeyError, UnicodeError, asyncio.LimitOverrunError,
                asyncio.IncompleteReadError):
            self.protocol_errors += 1
        except (ConnectionError, OSError):
            pass  # Client teardown is expected; outcome errors remain visible in generator results.
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
            self.writers.discard(writer)
            self.tasks.discard(task)
            self.closed_connections += 1

    def stats(self):
        return {"requests": dict(self.requests), "accepted_connections": self.connections,
                "closed_connections": self.closed_connections, "protocol_errors": self.protocol_errors,
                "peak_handlers": self.peak_handlers,
                "handler_p95_ms": percentile(self.handler_ms, 0.95),
                "handler_max_ms": max(self.handler_ms, default=0),
                "loop_lag_p95_ms": percentile(self.loop_lag_ms, 0.95),
                "loop_lag_max_ms": max(self.loop_lag_ms, default=0), "synthetic_orders": len(self.orders)}


def manifest(origin, scheduled):
    shows = max(2, math.ceil(scheduled / 600) * 2)
    return {"schema_version": 1, "environment": "development", "origin": origin,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            "show_ids": [str(uuid4()) for _ in range(shows)], "seat_offset": 0,
            "seats_per_show": 300, "viewer_tokens": ["synthetic-viewer-a", "synthetic-viewer-b"]}


def profile_summary(path):
    stats = pstats.Stats(str(path))
    def rows(position):
        values = sorted(stats.stats.items(), key=lambda item: item[1][position], reverse=True)[:12]
        return [{"function": f"{Path(key[0]).name}:{key[1]}:{key[2]}",
                 "calls": value[1], "self_seconds": round(value[2], 4),
                 "cumulative_seconds": round(value[3], 4)} for key, value in values]
    return {"total_profile_self_seconds": stats.total_tt, "total_calls": stats.total_calls,
            "top_self_time": rows(2), "top_cumulative_time": rows(3),
            "note": "Cumulative times overlap; profiling perturbs timing."}


def compact_summary(result):
    generator, responder = result["generator"], result["responder"]
    gates = {"responder_valid": result["responder_valid"],
             "generator_strict": generator["pass"],
             "source_clean": not result["source_dirty"]}
    return {"kind": result["kind"], "revision": result["source_revision"],
            "profiled": result.get("profiled", False), "configuration": result["configuration"],
            "pass": result["pass"], "failed_gates": [name for name, passed in gates.items() if not passed],
            "scheduled": generator["scheduled"], "completed": generator["completed"],
            "fulfilled": generator["fulfilled_by_deadline"], "drops": generator["generator_drops"],
            "outcomes": generator["outcomes"], "retries": generator["retry_attempts"],
            "hold_to_ticket_p95_ms": generator["hold_to_ticket_p95_ms"],
            "transport_p95_ms": generator["transport_phase_p95_ms"],
            "active_journeys_per_shard": [
                {key: row.get(key) for key in ("active_journeys_peak",
                 "active_journeys_time_weighted_mean", "drop_reasons", "lifecycle")}
                for row in generator["shards"]
            ],
            "responder_handler_p95_ms": responder["handler_p95_ms"],
            "responder_loop_lag_p95_ms": responder["loop_lag_p95_ms"],
            "durability_audit": "not_applicable_synthetic", "queue_drain": "not_applicable_synthetic",
            "production_capacity_evidence": False}


async def diagnose(args):
    profile_directory = args.output.with_suffix(".profiles") if getattr(args, "profile", False) else None
    async with Responder(args.response_ms, args.command_seconds, args.ticket_seconds) as server:
        generator_args = SimpleNamespace(
            origin=server.origin, rate=args.rate, seconds=args.seconds, concurrency=args.concurrency,
            completion_deadline_seconds=args.seconds + math.ceil(args.ticket_seconds + args.command_seconds) + 30,
            poll_seconds=0.2, duplicates=1, profile_directory=profile_directory,
            http_client_count=getattr(args, "http_client_count", 1),
            lifecycle_diagnostics=getattr(args, "lifecycle_diagnostics", False),
        )
        result = await run_shards(generator_args, manifest(server.origin, args.rate * args.seconds))
    revision = await asyncio.to_thread(
        subprocess.run, ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    )
    dirty = await asyncio.to_thread(
        subprocess.run, ["git", "status", "--porcelain", "--untracked-files=no"],
        capture_output=True, text=True, check=True,
    )
    stats = server.stats()
    responder_valid = (stats["protocol_errors"] == 0 and stats["loop_lag_p95_ms"] <= 10
                       and stats["handler_p95_ms"] <= args.response_ms + 20)
    report = {"kind": "synthetic_generator_control", "production_capacity_evidence": False,
            "created_at_utc": datetime.now(UTC).isoformat(), "source_revision": revision.stdout.strip(),
            "source_dirty": bool(dirty.stdout.strip()),
            "python_version": platform.python_version(), "platform": platform.system(),
            "logical_cpus": os.cpu_count(), "httpx_version": version("httpx"),
            "httpcore_version": version("httpcore"),
            "configuration": {"rate": args.rate, "seconds": args.seconds, "concurrency": args.concurrency,
                              "response_ms": args.response_ms, "command_seconds": args.command_seconds,
                              "ticket_seconds": args.ticket_seconds, "poll_seconds": 0.2, "shards": 2,
                              "http_clients_per_shard": getattr(args, "http_client_count", 1),
                              "lifecycle_diagnostics": getattr(args, "lifecycle_diagnostics", False)},
            "responder": stats, "responder_valid": responder_valid,
            "generator": result, "pass": responder_valid and result["pass"] and not dirty.stdout.strip()}
    report["profiled"] = profile_directory is not None
    if profile_directory is not None:
        report["cpu_profiles"] = [profile_summary(profile_directory / f"shard-{i}.prof") for i in range(2)]
    report["compact"] = compact_summary(report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=60)
    parser.add_argument("--http-client-count", type=int, default=1)
    parser.add_argument("--lifecycle-diagnostics", action="store_true")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=500)
    parser.add_argument("--response-ms", type=float, default=30)
    parser.add_argument("--command-seconds", type=float, default=0.5)
    parser.add_argument("--ticket-seconds", type=float, default=5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", action="store_true", help="Diagnostic only: CPU profiling perturbs timing")
    args = parser.parse_args()
    if (args.output.exists() or not 2 <= args.rate <= 100 or args.rate % 2
            or not 1 <= args.seconds <= 120 or not 2 <= args.concurrency <= 1000 or args.concurrency % 2
            or not 0 <= args.response_ms <= 500 or not 0 <= args.command_seconds <= 5
            or not 0 <= args.ticket_seconds <= 30
            or not 1 <= args.http_client_count <= min(16, args.concurrency // 2)):
        parser.error("Fresh output, even rate/concurrency and bounded synthetic delays required")
    result = asyncio.run(diagnose(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result["compact"]))
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
