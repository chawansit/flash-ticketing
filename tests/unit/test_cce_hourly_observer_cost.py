"""Execute hourly sampling, failure cadence and accepted CPU placement without cloud."""
import copy
import hashlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import observe_cce_paid_pipeline as native
import observe_two_host_cpu as cpu

NAMES = ("pending_orders", "paid_unfulfilled", "fulfilled_orders", "pending_callback_deliveries",
         "pending_payment_attempts", "delivered_callbacks", "issued_tickets")
QUERY = ("FROM payment_attempts p JOIN cohort o ON o.id=p.order_id; "
         "FROM tickets t JOIN bookings b ON b.id=t.booking_id")


class DatabaseError(Exception):
    pass


def sampler():
    clock, calls, failures, rollbacks = [0.0], [], set(), []
    def original(conn, shows):
        calls.append((clock[0], tuple(shows)))
        clock[0] += 0.04
        if shows[0] in failures:
            raise DatabaseError("not exported")
        return {**{k: sum(int(s) + 1 for s in shows) for k in NAMES},
                "unpublished_outbox": 999, "db_lock_waiters": 999}
    module = SimpleNamespace(sample=original, PAID_COHORT_QUERY=QUERY,
                             psycopg=SimpleNamespace(Error=DatabaseError))
    reads = []
    conn = SimpleNamespace(execute=lambda query: reads.append(query) or SimpleNamespace(fetchone=lambda: (2, 3)),
                           rollback=lambda: rollbacks.append(True))
    native.install_hourly_sampling(module, now=lambda: clock[0])
    return module, conn, clock, calls, failures, rollbacks, reads


def test_every_show_counted_once_and_global_reads_always_fresh():
    module, conn, clock, calls, _, _, reads = sampler()
    shows = [str(i) for i in range(1008)]
    row = module.sample(conn, shows)
    assert len(calls) == 8
    assert sorted(int(s) for _, part in calls for s in part) == list(range(1008))
    assert all(len(part) == 126 for _, part in calls)
    assert all(row[k] == sum(range(1, 1009)) for k in NAMES)
    assert row["cohort_sample_complete"] is True
    assert row["cohort_sample_age_seconds"] == pytest.approx(0.28)
    assert row["unpublished_outbox"] == 2 and row["db_lock_waiters"] == 3
    for index in range(8):
        clock[0] += 1
        row = module.sample(conn, shows)
        assert calls[-1][1] == tuple(shows[index::8])
        assert row["cohort_sample_complete"] is True
        assert row["cohort_sample_age_seconds"] <= 10
    assert len(reads) == 9 and len(calls) == 16
    assert module.PAID_COHORT_QUERY.count("OFFSET 0") == 2
    assert module.MAX_PAID_OBSERVER_SECONDS == 3780


def test_one_second_observer_ticks_do_not_skip_alternate_refreshes():
    module, conn, clock, calls, _, _, _ = sampler()
    shows = [str(i) for i in range(1008)]
    module.sample(conn, shows)
    start = clock[0]
    for index in range(1, 21):
        clock[0] = start + index * 0.9999
        row = module.sample(conn, shows)
        assert row["cohort_sample_complete"] is True
        assert len(calls) == 8 + index
        assert row["cohort_sample_age_seconds"] <= 10


def test_timeout_is_retained_until_next_round_and_not_retried_each_tick():
    module, conn, clock, calls, failures, rollbacks, _ = sampler()
    shows = [str(i) for i in range(1008)]
    module.sample(conn, shows)
    failures.add("0");clock[0] += 1
    row = module.sample(conn, shows)
    assert row["database_error"] == "DatabaseError" and row["cohort_slice_errors"] == {"0": "DatabaseError"}
    assert row["cohort_sample_complete"] is False and rollbacks == [True]
    assert "not exported" not in str(row)
    failures.clear()
    for index in range(1, 8):
        clock[0] += 1;row = module.sample(conn, shows)
        assert calls[-1][1] == tuple(shows[index::8])
        assert row["database_error"] == "DatabaseError"
    clock[0] += 1;row = module.sample(conn, shows)
    assert row["cohort_sample_complete"] is True and "database_error" not in row
    assert len(calls) == 17


def test_failed_priming_never_synthesizes_full_cohort_counts():
    module, conn, _, _, failures, _, _ = sampler()
    failures.add("1")
    row = module.sample(conn, [str(i) for i in range(1008)])
    assert row["cohort_sample_complete"] is False
    assert row["database_error"] == "DatabaseError"
    assert all(k not in row for k in NAMES)
    assert row["unpublished_outbox"] == 2


def test_stale_observation_and_changed_or_duplicate_cohort_fail():
    module, conn, clock, _, _, _, _ = sampler()
    shows = [str(i) for i in range(1008)]
    module.sample(conn, shows);clock[0] += 12
    row = module.sample(conn, shows)
    assert row["cohort_sample_age_seconds"] > 10
    assert row["database_error"] == "IncompleteOrStaleCohort"
    with pytest.raises(ValueError, match="changed"):
        module.sample(conn, shows[::-1])
    with pytest.raises(ValueError, match="Distinct"):
        module.sample(conn, ["0", "0"])


def cpu_data(role):
    owner = "adr0151-123456789abc"
    roles = ["confirmation", "kafka", "pgbouncer", "load-balancer", "cce-audit", "cce-pooler", "cce-kafka"] if role == "primary" else []
    entries = [{"id": f"{i+1:064x}", "role": r, "image_id": "sha256:" + "a" * 64,
                "started_at": "2026-10-12T00:00:00+00:00", "pid": i+1, "process_start_ticks": 1,
                "project": "cce-" + owner if r.startswith("cce-") else "flash-ticketing",
                "owner": owner if r.startswith("cce-") else None} for i, r in enumerate(roles)]
    start = datetime(2026, 10, 12, tzinfo=UTC)
    return {"schema": 1, "host_role": role, "arm": "candidate", "placement": "cce-worker-isolation",
            "decision": "ADR0259", "inventory_sha256": "b" * 64,
            "instance_uuid_sha256": hashlib.sha256(role.encode()).hexdigest(), "containers": entries,
            "seconds": 3600, "hourly": True, "interval": 5, "requested_start_utc": start.isoformat(),
            "samples": [{"utc": (start + timedelta(seconds=i)).isoformat(), "elapsed_seconds": i,
                         "host_ticks": [i*100, 0, 0, i*100, 0, 0, 0, 0],
                         "container_cpu_usec": {e["id"]: i*1000 for e in entries}} for i in range(0, 3601, 5)]}


def test_hourly_worker_placement_collects_and_summarizes_exact_host_identities(monkeypatch):
    spec = cpu_data("primary")
    def read(path, *args, **kwargs):
        if path.as_posix() == "/sys/class/dmi/id/product_uuid": return "primary"
        if path.as_posix() == "/proc/stat": return "cpu 100 0 0 100 0 0 0 0"
        return "usage_usec 1000"
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(cpu, "inspect_containers", lambda entries: {e["id"]: {**e, "path": Path("fake")} for e in entries})
    monkeypatch.setattr(cpu, "proc_identity", lambda pid: 1)
    monkeypatch.setattr(cpu.time, "sleep", lambda seconds: None)
    collected = cpu.collect(spec, (datetime.now(UTC) + timedelta(seconds=1)).isoformat(), seconds=3600, hourly=True)
    assert len(collected["samples"]) == 721 and len(collected["containers"]) == 7
    primary, secondary = [cpu.summarize(cpu_data(role)) for role in ("primary", "secondary")]
    result = cpu.compare_windows(primary, secondary, offered_start_utc=primary["start_utc"],
                                 offered_end_utc=primary["end_utc"], hourly=True)
    assert result["both_hosts_cpu_observed"] is True
    secondary["inventory_sha256"] = "c" * 64
    with pytest.raises(ValueError, match="Distinct matched"):
        cpu.compare_windows(primary, secondary, offered_start_utc=primary["start_utc"],
                            offered_end_utc=primary["end_utc"], hourly=True)


@pytest.mark.parametrize("fault", ["gap", "reset", "wrong_role", "wrong_duration", "identity"])
def test_hourly_cpu_correction_keeps_existing_rejection_gates(fault):
    data = copy.deepcopy(cpu_data("primary"))
    if fault == "gap": data["samples"].pop(2)
    elif fault == "reset": data["samples"][2]["container_cpu_usec"][data["containers"][0]["id"]] = 0
    elif fault == "wrong_role": data["containers"][0]["role"] = "consumer"
    elif fault == "wrong_duration": data["seconds"] = 300
    else: data["decision"] = "ADR0228"
    with pytest.raises(ValueError): cpu.summarize(data)


@pytest.mark.parametrize("fault", [None, "wrong_decision", "wrong_digest", "orphan"])
def test_comparison_plan_records_only_the_bound_observer_correction(fault):
    import cce_worker_rebalance_profile as placement
    import work_envelope as policy
    from test_cce_status_refresh_reuse import goal
    value = goal()
    value.update(observer_cost_decision="ADR0283",
                 observer_cost_sha256=policy.digest(native.cohort_sampling_contract()))
    if fault == "wrong_decision": value["observer_cost_decision"] = "ADR0282"
    elif fault == "wrong_digest": value["observer_cost_sha256"] = "0" * 64
    elif fault == "orphan": value.pop("observer_cost_decision")
    if fault:
        with pytest.raises(ValueError): placement.plan(value)
    else:
        result = placement.plan(value)
        assert result["single_changed_factor"] == "observer_query_planning"
        assert result["baseline_is_passing_control"] is True
        assert result["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
        assert (result["pooler_server_connections"], result["acquisition_budget"], result["maximum_pods"]) == (24, 20, 18)
