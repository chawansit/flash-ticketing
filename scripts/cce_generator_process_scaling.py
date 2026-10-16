"""ADR0333 explicit four-process coordinator over the unchanged sealed paid leaf."""

import hashlib

import work_envelope as policy

SOURCE = "scripts/cce_generator_process_scaling.py"
KEYS = {"generator_process_decision", "generator_process_contract_sha256"}
CHANGED_SOURCES = {
    SOURCE,
    "scripts/cce_fixture_clock.py",
    "scripts/cce_committed_status_profile.py",
    "scripts/cce_fulfillment_batch_profile.py",
    "scripts/cce_paid_inputs.py",
    "scripts/cce_paid_stage.py",
    "scripts/cce_api_adapter.py",
    "scripts/cce_customer_outcome_slo.py",
    "scripts/cce_recovery_headroom.py",
    "scripts/cce_throughput_push.py",
    "scripts/run_cce_paid_comparison.py",
}
OCCUPANCY_REFERENCE_RUN = "adr0151-89d76200cc84"
OCCUPANCY_REFERENCE_LEDGER = "bounded_cce_ticket_target__41e1f8253bf4"
OCCUPANCY_REFERENCE_SHA256 = "075031a9e6e2072cdb340fd32875092fcdab8ecd431c56eaf4b6f001609e6624"
CALIBRATION = "docs/capacity/cce/generator-process-calibration-2026-10-17.json"


def active_concurrency(goal=None):
    return 800 if goal is not None and goal.get("generator_process_decision") == "ADR0335" else 500


def contract(goal=None):
    occupancy = active_concurrency(goal) == 800
    return {
        "decision": "ADR0335" if occupancy else "ADR0333",
        "generator_processes": 4,
        "rate": 160,
        "seconds": 300,
        "total_concurrency": active_concurrency(goal),
        "total_http_connections": 500,
        "total_http_clients": 16,
        "per_process_concurrency": active_concurrency(goal) // 4,
        "per_process_http_clients": 4,
        "leaf_unchanged": not occupancy,
        **({"leaf_change": "exact200-active/125-HTTP/4-client resource validation only",
            "reference_result_sha256": OCCUPANCY_REFERENCE_SHA256,
            "diagnostics_helper_sha256": hashlib.sha256((policy.ROOT / "scripts/database_query_cost.py").read_bytes().replace(b"\r\n", b"\n")).hexdigest()} if occupancy else {}),
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
        goal.get("generator_process_decision") not in {"ADR0333", "ADR0335"}
        or goal.get("generator_process_contract_sha256") != policy.digest(contract(goal))
        or goal.get("throughput_push_decision") != "ADR0329"
        or goal.get("offered_journeys_per_second") != (84 if runtime else 160)
        or goal.get("offered_seconds") != 300
        or goal.get("generator_concurrency") != active_concurrency(goal)
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
    if not selected(goal) or binding.get("cce_generator_process_contract") != contract(goal):
        raise ValueError("Four-process source/budget binding required")
    import cce_fixture_clock as clock
    if clock.selected(goal):
        if binding.get("cce_fixture_clock_contract") != clock.contract():
            raise ValueError("Exactly bound creator-clock proof required")
    elif "cce_fixture_clock_contract" in binding:
        raise ValueError("Unselected creator-clock proof")
    if active_concurrency(goal) == 800:
        validate_occupancy_reference(binding)
    return True


def validate_occupancy_reference(binding):
    """Retain the exact failed160/s control and its independently audited recovery."""
    run, ledger, expected = OCCUPANCY_REFERENCE_RUN, OCCUPANCY_REFERENCE_LEDGER, OCCUPANCY_REFERENCE_SHA256
    report = policy.read(policy.ROOT / "tmp" / run / "cce-comparison.private.json")
    recovery = policy.read(policy.ROOT / "docs/capacity/cce/four-generator-160-recovery-2026-10-17.json")
    scope = policy.read(policy.STATE)[ledger]
    entries = [v for v in policy.journal(policy.envelope())["experiments"] if v["ledger"] == ledger]
    if (policy.digest(report) != expected or len(entries) != 1 or entries[0].get("status") != "FAILED_RESTORED"
            or entries[0].get("result_sha256") != expected or scope.get("active_run")
            or report.get("binding_sha256") != policy.digest(scope["binding"])
            or report.get("pass") is not False or report.get("restoration_complete") is not True
            or report.get("integrity_verified") is not False or report.get("transport_credentials_cleared") is not True
            or entries[0].get("paid_runs_started") != 1 or scope.get("paid_runs_started") != 1
            or recovery.get("fresh_financial", {}).get("pass") is not True
            or recovery.get("fresh_financial", {}).get("hold_deadlines_elapsed") is not True
            or recovery.get("original_result_sha256") != expected or recovery.get("pass") is not True
            or not recovery.get("gates") or any(v is not True for v in recovery["gates"].values())
            or entries[0].get("recovery", {}).get("sha256") != policy.digest(recovery)
            or recovery.get("raw_shard_accounting", {}).get("dispatched") != 40095
            or recovery.get("raw_shard_accounting", {}).get("generator_drops") != 7905):
        raise ValueError("Exact failed160/s comparison and verified operational recovery required")
    changed = {"cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256",
               "cce_headroom_contract", "cce_throughput_push_contract", "cce_generator_process_contract",
               "cce_primary_bootstrap_contract", "cce_fixture_clock_contract",
               "cce_metric_admission_contract_sha256", "cce_inventory_renewal_contract",
               "cce_verification_contract_sha256", "cce_fulfillment_batch_contract"}
    prior = scope["binding"]
    if any(prior.get(k) != binding.get(k) for k in prior.keys() | binding.keys() if k not in changed):
        raise ValueError("Active-journey comparison changed application, resources or connection budgets")
    return True


def qualify_workload(bundle, coordinator, goal=None):
    if goal is not None:
        selected(goal)
    occupancy = active_concurrency(goal) == 800
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
        b'len(results) == 2': b'len(results) == 4',
        b'evidence_states == ["final", "final"]': b'evidence_states == ["final"] * 4',
        b'row.get("rate_target_per_second") != args.rate // 2': b'row.get("rate_target_per_second") != args.rate // 4',
        b'row.get("scheduled") != args.rate // 2 * args.seconds': b'row.get("scheduled") != args.rate // 4 * args.seconds',
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
    if occupancy:
        before = b'"--http-max-connections", str(args.concurrency // 2)'
        if source.count(before) != 1:
            raise ValueError("Exact independent HTTP connection flag required")
        source = source.replace(before, b'"--http-max-connections", "125"')
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
    changed = dict(bundle)
    if occupancy:
        before = b"(160, 300, 500, 8, 1, 1, 3, 'distributed')"
        if source.count(before) != 1:
            raise ValueError("Exact500-slot coordinator guard required")
        source = source.replace(before, b"(160, 300, 800, 8, 1, 1, 3, 'distributed')")
        leaf_name = "scripts/paid_ticket_load_generator.py"
        leaf = changed[leaf_name]
        before = b"if not args.concurrency <= args.http_max_connections <= 4000:"
        after = b"if (args.concurrency, args.http_max_connections, client_count) != (200, 125, 4):"
        if leaf.count(before) != 1:
            raise ValueError("Exact sealed leaf resource validator required")
        changed[leaf_name] = leaf.replace(before, after)
        compile(changed[leaf_name], leaf_name, "exec")
    before, after = (hashlib.sha256(value).hexdigest().encode() for value in (bundle[name], source))
    if coordinator.count(before) != 1:
        raise ValueError("Exact coordinator-to-source binding required")
    compile(source, name, "exec")
    return {**changed, name: source}, coordinator.replace(before, after)
