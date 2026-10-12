"""Native CCE metric endpoints with the unchanged qualified background observer."""

import argparse
import ipaddress
import json
import math
from pathlib import Path
from urllib.request import urlopen

import observe_two_host_pipeline as observer


def validate_receipts(value):
    """These are native admission receipts, never Docker inventory entries."""
    if (
        set(value) != {"decision", "run", "api_sources", "resources", "manifest_digest", "receipts"}
        or value["decision"] != "ADR0228"
    ):
        raise ValueError("Exact native admission bundle required")
    entries = value["receipts"]
    if len(entries) != 4 or {r["pod_name"] for r in entries} != {f"api-{i}" for i in range(4)}:
        raise ValueError("Four native API identities required")
    for key in ("pod_uid", "private_ipv4"):
        if len({r[key] for r in entries}) != 4:
            raise ValueError("Duplicate native identity")
    for row in entries:
        address = ipaddress.IPv4Address(row["private_ipv4"])
        if not any(
            address in ipaddress.IPv4Network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        ):
            raise ValueError("Private native endpoint required")
        proof = row["startup_proof"]
        started = row["process_start_time_seconds"]
        if (
            row["image_id"].split("@")[-1] != value["manifest_digest"]
            or row["resources"] != value["resources"]
            or proof["sources"] != value["api_sources"]
            or proof["run"] != value["run"]
            or proof["pod_uid"] != row["pod_uid"]
            or type(started) not in (int, float)
            or not math.isfinite(started)
            or started <= 0
        ):
            raise ValueError("Native receipt differs from admitted identity")
    return {r["private_ipv4"]: r["process_start_time_seconds"] for r in entries}


def install(module, value, *, fetch=urlopen, hourly=False, observer_seconds=3780):
    if type(hourly) is not bool:
        raise ValueError("Explicit hourly observer mode required")
    if hourly:
        install_hourly_sampling(module, observer_seconds=observer_seconds)
    else:
        install_indexed_cohort_query(module)
    starts = validate_receipts(value)
    discover = module.api_replicas
    previous = {}

    def replicas(host="api", port=8000):
        return sorted(starts) if host == "api" and port == 8000 else discover(host, port)

    def metrics(address):
        if address not in starts:
            raise ValueError("Unadmitted native endpoint")
        with fetch(f"http://{address}:8000/metrics", timeout=2) as response:
            raw = response.read(observer.MAX_PAYLOAD + 1)
        if len(raw) > observer.MAX_PAYLOAD:
            raise ValueError("Native metrics payload exceeds bound")
        payload = raw.decode("utf-8")
        extras = {**observer.extra_api_metrics(payload), **observer.admission_failure_metrics(payload)}
        if extras["process_start_time_seconds"] != starts[address]:
            raise ValueError("Native process replaced or restarted")
        prior = previous.get(address)
        if prior is not None and any(
            extras.get(k, -1) < v for k, v in prior.items() if k != "process_start_time_seconds"
        ):
            raise ValueError("Native API counter reset")
        previous[address] = extras
        return {**module.parse_api_metrics(payload), **extras}

    module.api_replicas, module.api_metrics = replicas, metrics
    return starts



def cohort_sampling_contract():
    return {"decision": "ADR0283", "short_cohort_interval_seconds": 1,
            "hourly_maximum_cohort_age_seconds": 10, "hourly_slices": 8,
            "data_connections": 1, "statement_timeout_seconds": 3,
            "query": "existing_unique_indexes_parameterized_lateral",
            "cpu_coverage_gates_unchanged": True, "application_runtime_unchanged": True}


def install_indexed_cohort_query(module):
    """Preserve all predicates and columns while using existing unique indexes."""
    old = "FROM payment_attempts p JOIN cohort o ON o.id=p.order_id"
    tickets = "FROM tickets t JOIN bookings b ON b.id=t.booking_id"
    query = module.PAID_COHORT_QUERY
    if query.count(old) != 1 or query.count(tickets) != 1:
        raise ValueError("Exact frozen cohort query required")
    module.PAID_COHORT_QUERY = query.replace(
        old, "FROM cohort o CROSS JOIN LATERAL (SELECT status,deliveries,target_deliveries "
             "FROM payment_attempts WHERE order_id=o.id OFFSET 0) p"
    ).replace(tickets, "FROM bookings b CROSS JOIN LATERAL "
                      "(SELECT id FROM tickets WHERE booking_id=b.id OFFSET 0) t")


def install_hourly_sampling(module, *, now=None, observer_seconds=3780):
    """ADR0283: bounded-age disjoint slices on the existing observation connection."""
    import time

    now = time.monotonic if now is None else now
    original = module.sample
    install_indexed_cohort_query(module)
    if observer_seconds not in {3780, 4200}:
        raise ValueError("Exact hourly sampling lifetime required")
    module.MAX_PAID_OBSERVER_SECONDS = observer_seconds
    names = ("pending_orders", "paid_unfulfilled", "fulfilled_orders",
             "pending_callback_deliveries", "pending_payment_attempts",
             "delivered_callbacks", "issued_tickets")
    cohort, parts, cache, refreshed, failures = None, [], {}, {}, {}
    cursor, attempted = 0, float("-inf")

    def sample(conn, shows):
        nonlocal cohort, parts, cursor, attempted
        identity = tuple(shows)
        if not identity or len(identity) > 1008 or len(set(identity)) != len(identity):
            raise ValueError("Distinct bounded cohort required")
        if cohort is None:
            cohort = identity
            count = min(8, math.ceil(len(identity) / 126))
            parts = [identity[i::count] for i in range(count)]
            selected = range(count)  # Prime every slice before publishing any count.
        elif identity != cohort:
            raise ValueError("Hourly observer cohort changed")
        elif now() - attempted >= (10 if len(parts) == 1 else 0.9):
            selected = [cursor]
        else:
            selected = []
        for index in selected:
            attempted = now()  # Schedule from query start; execution must not halve the cadence.
            try:
                value = dict(original(conn, list(parts[index])))
                if any(type(value.get(k)) is not int or value[k] < 0 for k in names):
                    raise ValueError("Invalid cohort counter")
                cache[index], refreshed[index] = value, now()
                failures.pop(index, None)
            except module.psycopg.Error as error:
                conn.rollback()
                failures[index] = type(error).__name__
            cursor = (index + 1) % len(parts)
        row = conn.execute("SELECT (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),"
                           "(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock')").fetchone()
        latest = {"unpublished_outbox": row[0], "db_lock_waiters": row[1]}
        at = now()
        complete = len(cache) == len(parts)
        age = max((at - stamp for stamp in refreshed.values()), default=None)
        result = {k: latest[k] for k in ("unpublished_outbox", "db_lock_waiters")}
        result.update(cohort_sample_age_seconds=age, cohort_sample_interval_seconds=10,
                      cohort_slice_count=len(parts), cohort_sample_complete=complete and not failures
                      and age is not None and 0 <= age <= 10)
        if complete:
            result.update({k: sum(value[k] for value in cache.values()) for k in names})
        if not result["cohort_sample_complete"]:
            result["database_error"] = (next(iter(failures.values())) if failures
                                        else "IncompleteOrStaleCohort")
        if failures:
            result["cohort_slice_errors"] = {str(k): v for k, v in failures.items()}
        return result

    module.sample = sample


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--hourly", action="store_true")
    parser.add_argument("--early-hourly-bootstrap", action="store_true")
    parser.add_argument("--native-workers", type=Path)
    parser.add_argument("--native-workers-sha256")
    parser.add_argument("--native-receipts", type=Path, required=True)
    parser.add_argument("--native-receipts-sha256", required=True)
    args, remaining = parser.parse_known_args(argv)
    if args.early_hourly_bootstrap and not args.hourly:
        raise ValueError("Early bootstrap requires hourly mode")
    value = json.loads(args.native_receipts.read_text())
    import hashlib

    if (
        hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        != args.native_receipts_sha256
    ):
        raise ValueError("Native receipt transfer changed")
    validate_receipts(value)
    workers = None
    if args.native_workers is not None or args.native_workers_sha256 is not None:
        from cce_worker_identity import digest, validate_bundle
        if args.native_workers is None or args.native_workers_sha256 is None:
            raise ValueError("Complete native worker transfer binding required")
        workers = json.loads(args.native_workers.read_text())
        validate_bundle(workers)
        if digest(workers) != args.native_workers_sha256 or workers["run"] != value["run"]:
            raise ValueError("Worker receipt transfer/run differs")
    original = observer.install_adapter

    def adapter(module, *params, **kwargs):
        original(module, *params, **kwargs)  # Retain workers, diagnostics, budgets and metric parsing.
        result = install(module, value, hourly=args.hourly,
                         observer_seconds=4200 if args.early_hourly_bootstrap else 3780)
        if workers is not None:
            from observe_cce_workers import install as worker_install
            worker_install(module, workers)
        return result

    if args.hourly:
        index = remaining.index("--seconds") if "--seconds" in remaining else -1
        expected_seconds = "4200" if args.early_hourly_bootstrap else "3780"
        if index < 0 or remaining[index + 1] != expected_seconds:
            raise ValueError("Exact hourly observer duration required")
    observer.install_adapter = adapter
    try:
        observer.main(remaining)
    finally:
        observer.install_adapter = original


if __name__ == "__main__":
    main()
