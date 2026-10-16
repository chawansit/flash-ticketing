"""ADR0333 explicit four-process coordinator over the unchanged sealed paid leaf."""

import hashlib

import work_envelope as policy

SOURCE = "scripts/cce_generator_process_scaling.py"
KEYS = {"generator_process_decision", "generator_process_contract_sha256"}
CHANGED_SOURCES = {
    SOURCE,
    "scripts/cce_paid_stage.py",
    "scripts/cce_api_adapter.py",
    "scripts/cce_customer_outcome_slo.py",
    "scripts/cce_recovery_headroom.py",
    "scripts/cce_throughput_push.py",
    "scripts/run_cce_paid_comparison.py",
}
CALIBRATION = "docs/capacity/cce/generator-process-calibration-2026-10-17.json"


def contract():
    return {
        "decision": "ADR0333",
        "generator_processes": 4,
        "rate": 160,
        "seconds": 300,
        "total_concurrency": 500,
        "total_http_connections": 500,
        "total_http_clients": 16,
        "per_process_concurrency": 125,
        "per_process_http_clients": 4,
        "leaf_unchanged": True,
        "application_and_database_budgets_unchanged": True,
        "calibration_sha256": hashlib.sha256((policy.ROOT / CALIBRATION).read_bytes()).hexdigest(),
        "sources": {
            p: hashlib.sha256((policy.ROOT / p).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for p in sorted(CHANGED_SOURCES)
        },
    }


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    runtime = goal.get("_headroom_runtime_view") is True
    if (
        goal.get("generator_process_decision") != "ADR0333"
        or goal.get("generator_process_contract_sha256") != policy.digest(contract())
        or goal.get("throughput_push_decision") != "ADR0329"
        or goal.get("offered_journeys_per_second") != (84 if runtime else 160)
        or goal.get("offered_seconds") != 300
        or goal.get("generator_concurrency") != 500
        or goal.get("maximum_paid_stages") != 1
        or goal.get("maximum_experiment_seconds") != 3600
    ):
        raise ValueError("Exact source-bound four-process160/s comparison required")
    return True


def active_goal():
    return (
        policy.envelope()
        .get("spending", {})
        .get("temporary_cce_pilot_exception", {})
        .get("goal_bounded_authorization")
    )


def validate_binding(goal, binding):
    if not selected(goal) or binding.get("cce_generator_process_contract") != contract():
        raise ValueError("Four-process source/budget binding required")
    return True


def qualify_workload(bundle, coordinator):
    name = "scripts/paid_ticket_sharded_generator.py"
    source = bundle[name]
    calibration = policy.read(policy.ROOT / CALIBRATION)
    for child in (
        "paid_ticket_load_generator.py",
        "checkout_journey_probe.py",
        "paid_fixture_layout.py",
        "customer_recovery_client.py",
    ):
        value = bundle["scripts/" + child].replace(b"\r\n", b"\n")
        if hashlib.sha256(value).hexdigest() != calibration["source_sha256"][child]:
            raise ValueError("Sealed calibrated leaf behavior changed")
    patches = {
        b"len(shard_results) == 2": b"len(shard_results) == 4",
        b"exit_codes == [0, 0]": b"exit_codes == [0, 0, 0, 0]",
        b"args.rate % 2": b"args.rate % 4",
        b"args.concurrency % 2": b"args.concurrency % 4",
        b"split_manifest(manifest, 2, args.rate // 2 * args.seconds)": b"split_manifest(manifest, 4, args.rate // 4 * args.seconds)",
        b"str(args.rate // 2)": b"str(args.rate // 4)",
        b'str(getattr(args, "http_client_count", 1))': b"str(args.http_client_count // 2)",
    }
    # The two separate flags must each preserve the aggregate500 budget.
    if source.count(b"str(args.concurrency // 2)") != 2:
        raise ValueError("Exact sealed concurrency and HTTP connection flags required")
    source = source.replace(b"str(args.concurrency // 2)", b"str(args.concurrency // 4)")
    for before, after in patches.items():
        if source.count(before) != 1:
            raise ValueError("Sealed coordinator patch differs: " + before.decode())
        source = source.replace(before, after)
    guard = b"async def run(args, manifest):\n"
    if source.count(guard) != 1:
        raise ValueError("Exact sealed entry point required")
    source = source.replace(
        guard,
        guard
        + b"    if (args.rate, args.seconds, args.concurrency, args.http_client_count, args.poll_seconds, args.duplicates, args.recovery_max_attempts, manifest.get('fixture_layout')) != (160, 300, 500, 8, 1, 1, 3, 'distributed'):\n        raise ValueError('Exact ADR0333 four-process profile required')\n",
    )
    before, after = (hashlib.sha256(value).hexdigest().encode() for value in (bundle[name], source))
    if coordinator.count(before) != 1:
        raise ValueError("Exact coordinator-to-source binding required")
    compile(source, name, "exec")
    return {**bundle, name: source}, coordinator.replace(before, after)
