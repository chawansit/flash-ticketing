"""Open-loop, bounded paid-ticket journey generator for development fixtures."""

import argparse
import asyncio
import json
from collections import Counter
from contextlib import AsyncExitStack, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter, process_time, time
from uuid import uuid4

import httpx
from checkout_journey_probe import journey, percentile


class ObservedResponseStream(httpx.AsyncByteStream):
    """Observe completion of the original stream, including its pool release."""

    def __init__(self, stream, observe):
        self.stream, self.observe = stream, observe
        self.closed = False
        self.body_complete = None
        self.body_bytes = 0

    async def __aiter__(self):
        async for chunk in self.stream:
            self.body_bytes += len(chunk)
            yield chunk
        self.body_complete = perf_counter()

    async def aclose(self):
        if self.closed:
            return
        self.closed = True
        started = perf_counter()
        closed_ok = False
        try:
            await self.stream.aclose()
            closed_ok = True
        finally:
            self.observe(self.body_complete, started, perf_counter(), self.body_bytes, closed_ok)


def endpoint(path):
    if path == "/v1/holds":
        return "holds"
    if path.startswith("/v1/reservation-commands/"):
        return "reservation_commands"
    if path.endswith("/payments"):
        return "payments"
    if path.startswith("/v1/orders/"):
        return "orders"
    return "other"


def validate(args, manifest):
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("A development-only private manifest is required")
    if args.origin.rstrip("/") != manifest.get("origin"):
        raise ValueError("Origin does not match private manifest")
    if not 1 <= args.rate <= 500 or not 1 <= args.seconds <= 3600:
        raise ValueError("Rate or duration outside bounded limits")
    scheduled = args.rate * args.seconds
    if scheduled > 300000:
        raise ValueError("Maximum 300000 scheduled journeys")
    if not 1 <= args.concurrency <= 1000 or not 1 <= args.duplicates <= 10:
        raise ValueError("Concurrency or callback count outside bounded limits")
    client_count = getattr(args, "http_client_count", 1)
    if not 1 <= client_count <= min(16, args.http_max_connections):
        raise ValueError("HTTP client count must fit bounded connection budget")
    if not args.concurrency <= args.http_max_connections <= 4000:
        raise ValueError("HTTP connection limit must fit bounded journey concurrency")
    if not 1 <= args.timeout_seconds <= 110 or not 0.05 <= args.poll_seconds <= 2:
        raise ValueError("Timeout or poll interval outside bounded limits")
    if args.completion_deadline_seconds < args.seconds:
        raise ValueError("Completion deadline precedes end of dispatch")
    if args.output.exists():
        raise ValueError("Use a fresh output path")
    shows = manifest.get("show_ids")
    tokens = manifest.get("viewer_tokens")
    if not shows or not tokens:
        raise ValueError("Manifest requires shows and viewer credentials")
    if manifest["seat_offset"] < 0 or (
        manifest["seat_offset"] + (scheduled - 1) // len(shows) >= manifest["seats_per_show"]
    ):
        raise ValueError("Fixture has insufficient distinct seats")
    if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
        seconds=args.completion_deadline_seconds + 60
    ):
        raise ValueError("Manifest expires before the measurement deadline")
    return scheduled


async def scheduled_journeys(args, manifest, journey_fn=journey):
    scheduled = validate(args, manifest)
    outcomes = Counter()
    attempts = Counter()
    latencies = {key: [] for key in (
        "hold_http_ms", "hold_app_ms", "hold_client_excess_ms",
        "command_durable_wait_ms", "durable_ms", "payment_http_ms",
        "payment_app_ms", "payment_client_excess_ms", "ticket_wait_ms",
        "payment_to_ticket_ms", "hold_to_ticket_ms",
    )}
    unique_orders = set()
    unique_tickets = set()
    active = set()
    dispatch_lags = []
    diagnostics = getattr(args, "lifecycle_diagnostics", False)
    phases = ["pre_send_ms", "response_wait_ms", "connect_ms"]
    if diagnostics:
        phases += ["response_body_ms", "stream_close_ms", "headers_to_pool_release_ms",
                   "request_to_pool_release_ms"]
    transport = {key: {phase: [] for phase in phases}
                 for key in ("holds", "reservation_commands", "payments", "orders")}
    dropped = dispatched = fulfilled_by_deadline = 0
    run_id = uuid4().hex
    close_errors, body_bytes = Counter(), Counter()
    loop_lags, drop_snapshots = [], []
    journey_durations = {}
    active_started = {}
    active_peak = 0
    occupancy_area = 0.0

    async def on_request(request):
        route = endpoint(request.url.path)
        attempts[route] += 1
        if route not in transport:
            return
        stamps = {"start": perf_counter()}
        request.extensions["checkout_trace_stamps"] = stamps

        async def trace(event, _info):
            if event in (
                "connection.connect_tcp.started",
                "connection.connect_tcp.complete",
                "http11.send_request_headers.started",
                "http11.receive_response_headers.started",
                "http11.receive_response_headers.complete",
            ):
                stamps.setdefault(event, perf_counter())

        request.extensions["trace"] = trace

    async def on_response(response):
        route = endpoint(response.request.url.path)
        if route not in transport:
            return
        stamps = response.request.extensions.get("checkout_trace_stamps", {})
        pairs = {
            "pre_send_ms": ("start", "http11.send_request_headers.started"),
            "response_wait_ms": (
                "http11.receive_response_headers.started",
                "http11.receive_response_headers.complete",
            ),
            "connect_ms": ("connection.connect_tcp.started", "connection.connect_tcp.complete"),
        }
        for phase, (start, end) in pairs.items():
            if start in stamps and end in stamps:
                transport[route][phase].append(max(0.0, (stamps[end] - stamps[start]) * 1000))

        if diagnostics:
            def observe(body_end, close_start, close_end, size, closed_ok):
                body_bytes[route] += size
                if not closed_ok:
                    close_errors[route] += 1
                    return
                if body_end is not None and "http11.receive_response_headers.complete" in stamps:
                    transport[route]["response_body_ms"].append(max(
                        0.0, (body_end - stamps["http11.receive_response_headers.complete"]) * 1000))
                transport[route]["stream_close_ms"].append((close_end - close_start) * 1000)
                for phase, start_key in [("headers_to_pool_release_ms",
                                           "http11.receive_response_headers.complete"),
                                          ("request_to_pool_release_ms", "start")]:
                    if start_key in stamps:
                        transport[route][phase].append(max(0.0, (close_end - stamps[start_key]) * 1000))
            response.stream = ObservedResponseStream(response.stream, observe)

    client_count = getattr(args, "http_client_count", 1)
    base, extra = divmod(args.http_max_connections, client_count)
    connection_budgets = [base + (index < extra) for index in range(client_count)]
    async with AsyncExitStack() as stack:
        clients = [await stack.enter_async_context(httpx.AsyncClient(
            base_url=args.origin, timeout=10,
            limits=httpx.Limits(max_connections=budget),
            event_hooks={"request": [on_request], "response": [on_response]},
        )) for budget in connection_budgets]
        (await clients[0].get("/health/ready")).raise_for_status()
        attempts.clear()
        if args.start_at_epoch is not None:
            delay = args.start_at_epoch - time()
            if delay < -0.5 or delay > 60:
                raise ValueError("Synchronized generator start missed or too far ahead")
            await asyncio.sleep(max(0.0, delay))
        started_utc = datetime.now(UTC).isoformat()
        started = perf_counter()
        cpu_started = process_time()
        occupancy_last = started

        def update_occupancy():
            nonlocal occupancy_area, occupancy_last
            now = perf_counter()
            occupancy_area += len(active) * (now - occupancy_last)
            occupancy_last = now

        def completed_task(task):
            update_occupancy()
            active.discard(task)
            active_started.pop(task, None)

        async def monitor_loop():
            while True:
                due = perf_counter() + 0.1
                await asyncio.sleep(0.1)
                loop_lags.append(max(0.0, (perf_counter() - due) * 1000))

        if diagnostics:
            monitor = asyncio.create_task(monitor_loop())

            async def stop_monitor():
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor

            stack.push_async_callback(stop_monitor)
        completion_deadline = started + args.completion_deadline_seconds

        async def one(index):
            nonlocal fulfilled_by_deadline
            journey_started = perf_counter()
            try:
                row = await journey_fn(
                    clients[index % client_count],
                    manifest,
                    index,
                    run_id,
                    args.timeout_seconds,
                    args.poll_seconds,
                    args.duplicates,
                )
            except Exception:  # noqa: BLE001 - account for every dispatched journey
                row = {"outcome": "generator_exception"}
            outcomes[row["outcome"]] += 1
            if diagnostics:
                journey_durations.setdefault(row["outcome"], []).append(
                    (perf_counter() - journey_started) * 1000)
            if row["outcome"] == "fulfilled":
                unique_orders.add(row["order_id"])
                unique_tickets.add(row["ticket_id"])
                if perf_counter() <= completion_deadline:
                    fulfilled_by_deadline += 1
                for key, values in latencies.items():
                    value = row.get(key)
                    if value is not None:
                        values.append(value)

        for index in range(scheduled):
            due = started + index / args.rate
            await asyncio.sleep(max(0.0, due - perf_counter()))
            dispatch_lags.append(max(0.0, (perf_counter() - due) * 1000))
            if len(active) >= args.concurrency:
                dropped += 1
                if diagnostics and len(drop_snapshots) < 8:
                    now = perf_counter()
                    drop_snapshots.append({
                        "elapsed_seconds": now - started, "active": len(active),
                        "completed_tasks_still_counted": sum(task.done() for task in active),
                        "oldest_active_ms": (now - min(active_started.values())) * 1000,
                    })
                continue
            update_occupancy()
            task = asyncio.create_task(one(index))
            active.add(task)
            active_peak = max(active_peak, len(active))
            if diagnostics:
                active_started[task] = perf_counter()
            task.add_done_callback(completed_task)
            dispatched += 1
        if active:
            await asyncio.gather(*active)
        update_occupancy()
        finished = perf_counter()
        cpu_seconds = process_time() - cpu_started

    fulfilled = outcomes["fulfilled"]
    result = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "started_at_utc": started_utc,
        "kind": "scheduled_paid_ticket_journeys",
        "http_client_count": client_count, "http_connection_budgets": connection_budgets,
        "rate_target_per_second": args.rate,
        "dispatch_seconds": args.seconds,
        "completion_deadline_seconds": args.completion_deadline_seconds,
        "scheduled": scheduled,
        "dispatched": dispatched,
        "generator_drops": dropped,
        "drop_reasons": {"active_journey_capacity": dropped} if dropped else {},
        "active_journeys_peak": active_peak,
        "active_journeys_time_weighted_mean": occupancy_area / (finished - started),
        "steady_state_mean_residence_budget_ms": args.concurrency / args.rate * 1000,
        "lifecycle_diagnostics": diagnostics,
        "completed": sum(outcomes.values()),
        "fulfilled": fulfilled,
        "fulfilled_by_deadline": fulfilled_by_deadline,
        "distinct_orders": len(unique_orders),
        "distinct_tickets": len(unique_tickets),
        "outcomes": dict(outcomes),
        "physical_http_attempts": dict(attempts),
        "retry_attempts": 0,
        "generator_max_in_flight": args.concurrency,
        "http_max_connections": args.http_max_connections,
        "transport_phase_p95_ms": {
            route: {phase: percentile(values, 0.95) for phase, values in phases.items()}
            for route, phases in transport.items()
        },
        "transport_phase_samples": {
            route: {phase: len(values) for phase, values in phases.items()}
            for route, phases in transport.items()
        },
        "dispatch_lag_p95_ms": percentile(dispatch_lags, 0.95),
        "dispatch_lag_max_ms": max(dispatch_lags, default=0),
        "hold_http_p95_ms": percentile(latencies["hold_http_ms"], 0.95),
        "hold_app_p95_ms": percentile(latencies["hold_app_ms"], 0.95),
        "hold_client_excess_p95_ms": percentile(latencies["hold_client_excess_ms"], 0.95),
        "hold_app_timing_samples": len(latencies["hold_app_ms"]),
        "command_durable_wait_p95_ms": percentile(latencies["command_durable_wait_ms"], 0.95),
        "durable_p95_ms": percentile(latencies["durable_ms"], 0.95),
        "payment_http_p95_ms": percentile(latencies["payment_http_ms"], 0.95),
        "payment_app_p95_ms": percentile(latencies["payment_app_ms"], 0.95),
        "payment_client_excess_p95_ms": percentile(latencies["payment_client_excess_ms"], 0.95),
        "payment_app_timing_samples": len(latencies["payment_app_ms"]),
        "ticket_wait_p95_ms": percentile(latencies["ticket_wait_ms"], 0.95),
        "payment_to_ticket_p95_ms": percentile(latencies["payment_to_ticket_ms"], 0.95),
        "hold_to_ticket_p95_ms": percentile(latencies["hold_to_ticket_ms"], 0.95),
        "elapsed_seconds": finished - started,
    }
    if diagnostics:
        result["lifecycle"] = {
            "process_cpu_seconds": cpu_seconds,
            "process_cpu_wall_ratio": cpu_seconds / (finished - started),
            "loop_lag_p95_ms": percentile(loop_lags, 0.95),
            "loop_lag_max_ms": max(loop_lags, default=0.0), "loop_samples": len(loop_lags),
            "body_bytes": dict(body_bytes), "stream_close_errors": dict(close_errors),
            "drop_snapshots": drop_snapshots,
            "journey_duration_by_outcome_ms": {
                outcome: {"count": len(values), "mean": sum(values) / len(values),
                          "p95": percentile(values, 0.95), "max": max(values)}
                for outcome, values in journey_durations.items()
            },
        }
    result["pass"] = (
        scheduled
        == dispatched
        == result["completed"]
        == fulfilled
        == fulfilled_by_deadline
        == len(unique_orders)
        == len(unique_tickets)
        and dropped == 0
        and result["dispatch_lag_p95_ms"] <= 100
        and result["dispatch_lag_max_ms"] <= 500
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rate", required=True, type=int)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--completion-deadline-seconds", required=True, type=int)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--http-max-connections", type=int)
    parser.add_argument("--http-client-count", type=int, default=1)
    parser.add_argument("--lifecycle-diagnostics", action="store_true")
    parser.add_argument("--start-at-epoch", type=float)
    parser.add_argument("--duplicates", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    args = parser.parse_args()
    if args.http_max_connections is None:
        args.http_max_connections = args.concurrency
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    result = asyncio.run(scheduled_journeys(args, manifest))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
