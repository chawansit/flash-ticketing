"""A changed image-verification label must never omit an admitted projection worker."""
import copy
from functools import lru_cache

import cce_payment_pipeline_profile as pipeline
import cce_worker_identity as identity
import observe_cce_workers as observer
import pytest
from test_cce_worker_rebalance import bundle


@lru_cache(maxsize=2)
def admitted(decision):
    value = bundle()
    if decision == "ADR0285":
        value["decision"] = decision
        value["manifest_digest"] = identity.specification(decision)["manifest"]
        value["sources"] = pipeline.image()["runtime_source_sha256"]
        for row in value["receipts"]:
            row["image_id"] = pipeline.image()["registry_image"]
            row["startup_proof"]["sources"] = value["sources"]
    identity.validate_bundle(value)
    return value


def rows(value):
    aliases = {"reservation-writer": "writer", "projection-consumer": "projection"}
    result = []
    for index in range(3):
        row = {"utc": f"2026-10-12T00:00:0{index}+00:00"}
        for receipt in value["receipts"]:
            role = aliases.get(receipt["role"], receipt["role"])
            row.setdefault(role + "_native_processes", {})[receipt["private_ipv4"]] = {
                "process_start_time_seconds": receipt["process_start_time_seconds"],
                "process_cpu_seconds_total": index * 0.1, "calls": index + 1}
        result.append(row)
    return result


@pytest.mark.parametrize("decision", ["ADR0271", "ADR0285"])
def test_every_admitted_projection_process_counts_regardless_of_image_decision(decision):
    value = admitted(decision)
    result = observer.summarize(rows(value), value, "2026-10-12T00:00:00+00:00", "2026-10-12T00:00:02+00:00")
    assert result["pass"] and len(result["worker_cpu_cores_by_endpoint"]) == 14
    assert result["aggregate_worker_cpu_cores"] == pytest.approx(1.4)
    assert set(result["successful_operations_by_endpoint"]) == set(identity.validate_bundle(value))


@pytest.mark.parametrize("fault", ["missing", "no_progress", "error", "reset"])
def test_projection_integrity_and_progress_gates_are_not_relaxed(fault):
    value = admitted("ADR0285")
    samples = copy.deepcopy(rows(value))
    address = next(r["private_ipv4"] for r in value["receipts"] if r["role"] == "projection-consumer")
    if fault == "missing": samples[1].pop("projection_native_processes")
    elif fault == "no_progress":
        for sample in samples: sample["projection_native_processes"][address]["calls"] = 1
    elif fault == "error": samples[1]["projection_metrics_error"] = "TimeoutError"
    elif fault == "reset": samples[-1]["projection_native_processes"][address]["process_cpu_seconds_total"] = 0
    with pytest.raises(ValueError):
        observer.summarize(samples, value, "2026-10-12T00:00:00+00:00", "2026-10-12T00:00:02+00:00")
