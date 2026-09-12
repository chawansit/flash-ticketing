"""Summarize seat-map read probe with focused DB/workload metrics."""

import argparse
import gzip
import json
import math
from collections.abc import Iterable
from pathlib import Path

MetricRowMap = dict[tuple[tuple[str, str], ...], float]


def _read_payload(path: Path) -> dict:
    if path.suffix == ".gz":
        text = gzip.decompress(path.read_bytes()).decode("utf-8")
    else:
        text = path.read_text()
    return json.loads(text)


def _to_metric_map(rows: Iterable[dict]) -> MetricRowMap:
    out: MetricRowMap = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        metric = row.get("metric")
        value = row.get("value")
        if not isinstance(metric, dict) or not isinstance(value, list) or len(value) < 2:
            continue
        try:
            out[tuple(sorted(metric.items()))] = float(value[1])
        except (TypeError, ValueError):
            continue
    return out


def _labels_match(metrics: tuple[tuple[str, str], ...], name: str, **labels: str) -> bool:
    m = dict(metrics)
    if m.get("__name__") != name:
        return False
    for key, expected in labels.items():
        if m.get(key) != expected:
            return False
    return True


def _delta(start: MetricRowMap, end: MetricRowMap, name: str, labels: dict[str, str]) -> float | None:
    start_total = 0.0
    end_total = 0.0
    for key, value in end.items():
        metric = dict(key)
        if metric.get("__name__") != name:
            continue
        if any(metric.get(k) != v for k, v in labels.items()):
            continue
        end_total += value
        start_total += start.get(key, 0.0)

    value_delta = end_total - start_total
    if value_delta < 0:
        return None
    return value_delta


def _histogram_p95(
    start: MetricRowMap,
    end: MetricRowMap,
    prefix: str,
    labels: dict[str, str],
) -> float | None:
    bucket_deltas: dict[float, float] = {}
    for key, end_value in end.items():
        metric = dict(key)
        if metric.get("__name__") != f"{prefix}_bucket":
            continue
        if any(metric.get(k) != v for k, v in labels.items()):
            continue

        try:
            bucket_bound = float(metric.get("le"))
        except (TypeError, ValueError):
            continue
        start_value = start.get(key, 0.0)
        delta = end_value - start_value
        if delta <= 0:
            continue
        bucket_deltas[bucket_bound] = bucket_deltas.get(bucket_bound, 0.0) + delta

    total = sum(bucket_deltas.values())
    if not total:
        return None

    target = total * 0.95
    cumulative = 0.0
    for bound in sorted(bucket_deltas):
        cumulative += bucket_deltas[bound]
        if cumulative >= target:
            return bound * 1000.0 if math.isfinite(bound) else None
    return None


def _max_sample(start: list[dict], name: str, **labels: str) -> float | None:
    max_value: float | None = None
    for sample in start:
        if not isinstance(sample, dict):
            continue
        for row in sample.get("metrics", []):
            if not isinstance(row, dict):
                continue
            metric = row.get("metric")
            if not isinstance(metric, dict):
                continue
            if metric.get("__name__") != name:
                continue
            if any(metric.get(k) != v for k, v in labels.items()):
                continue
            value = row.get("value")
            if not isinstance(value, list) or not value:
                continue
            try:
                observed = float(value[1])
            except (TypeError, ValueError):
                continue
            if max_value is None or observed > max_value:
                max_value = observed
    return max_value


def _read_rows_as_map(samples: list[dict]) -> list[MetricRowMap]:
    metric_samples = []
    for row in samples:
        metrics = row.get("metrics")
        if not isinstance(metrics, list):
            continue
        metric_samples.append(_to_metric_map(metrics))
    return metric_samples


def _sum_worker_busy(start: MetricRowMap, end: MetricRowMap, operation: str) -> float | None:
    return _delta(start, end, "ticketing_worker_busy_seconds_total", {"operation": operation})


def _gather_metric_observations(stage: dict) -> list[dict]:
    observations = stage.get("observations")
    if isinstance(observations, list):
        return [row for row in observations if isinstance(row, dict)]
    return []

def _status_count(statements: dict, op: str, status: str) -> int:
    return int((statements.get(op, {}) or {}).get(status, 0))


def _summarize_stage(stage: dict) -> dict:
    statuses = stage.get("statuses", {})
    read_ok = _status_count(statuses, "read", "200")
    read_304 = _status_count(statuses, "read", "304")
    hold_ok = _status_count(statuses, "hold", "201")

    observations = _gather_metric_observations(stage)
    metric_samples = _read_rows_as_map(observations)

    if len(metric_samples) < 2:
        return {
            "stage_mode": stage.get("mode"),
            "offered_rps": stage.get("offered_rps"),
            "seconds": stage.get("seconds"),
            "observation_samples": len(observations),
            "read_200": read_ok,
            "read_304": read_304,
            "hold_201": hold_ok,
            "generator_drops": stage.get("generator_drops"),
            "metric_note": "Not enough metrics samples to calculate deltas.",
        }

    start = metric_samples[0]
    end = metric_samples[-1]

    db_query_count = _delta(start, end, "ticketing_db_query_seconds_count", {})
    db_tx_count = _delta(start, end, "ticketing_db_transaction_seconds_count", {})
    db_query_sum = _delta(start, end, "ticketing_db_query_seconds_sum", {})
    db_tx_sum = _delta(start, end, "ticketing_db_transaction_seconds_sum", {})

    db_query_mean_ms = (db_query_sum * 1000.0 / db_query_count) if db_query_count else None
    db_tx_mean_ms = (db_tx_sum * 1000.0 / db_tx_count) if db_tx_count else None

    db_query_p95 = _histogram_p95(start, end, "ticketing_db_query_seconds", {})
    db_tx_p95 = _histogram_p95(start, end, "ticketing_db_transaction_seconds", {})

    pool_acquire_wait_count = _delta(start, end, "ticketing_db_pool_acquire_seconds_count", {"outcome": "ok"})
    pool_acquire_error_count = _delta(start, end, "ticketing_db_pool_acquire_seconds_count", {"outcome": "error"})
    pool_wait_p95 = _histogram_p95(start, end, "ticketing_db_pool_acquire_seconds", {"outcome": "ok"})

    db_error_counts = {
        label: _delta(start, end, "ticketing_db_errors_total", {"type": label})
        for label in ["55P03", "40P01", "deadlock", "lock_not_available"]
    }

    worker_busy = {
        operation: _sum_worker_busy(start, end, operation)
        for operation in ["snapshot", "refresh_one", "publish_batch", "consume_event", "simulate_one"]
    }
    worker_active = {
        op: _max_sample(observations, "ticketing_worker_active", operation=op)
        for op in ["snapshot", "refresh_one", "publish_batch", "consume_event", "simulate_one"]
    }

    cache_rows_full = _delta(start, end, "ticketing_cache_rows_total", {"mode": "full"})
    cache_rows_patch = _delta(start, end, "ticketing_cache_rows_total", {"mode": "patch"})

    pool_state = {
        key: _max_sample(observations, "ticketing_db_pool_state", state=key)
        for key in ["pool_size", "pool_in_use", "pool_acquiring"]
    }

    return {
        "stage_mode": stage.get("mode"),
        "offered_rps": stage.get("offered_rps"),
        "seconds": stage.get("seconds"),
        "observation_samples": len(observations),
        "read_p95_ms": stage.get("read_p95_ms"),
        "write_p95_ms": stage.get("server_p95_ms", {}).get("hold:201"),
        "read_status_200": read_ok,
        "read_status_304": read_304,
        "hold_status_201": hold_ok,
        "generator_drops": stage.get("generator_drops"),
        "response_body_bytes": stage.get("response_body_bytes"),
        "db_query_count_delta": db_query_count,
        "db_query_mean_ms": db_query_mean_ms,
        "db_query_p95_ms": db_query_p95,
        "db_tx_count_delta": db_tx_count,
        "db_transaction_mean_ms": db_tx_mean_ms,
        "db_transaction_p95_ms": db_tx_p95,
        "pool_acquire_ok_count": pool_acquire_wait_count,
        "pool_acquire_error_count": pool_acquire_error_count,
        "pool_acquire_p95_ms": pool_wait_p95,
        "db_lock_errors": db_error_counts,
        "worker_busy_seconds": worker_busy,
        "worker_active_max": worker_active,
        "cache_rows_full": cache_rows_full,
        "cache_rows_patch": cache_rows_patch,
        "pool_state_max": pool_state,
        "local_read_gate_pass": stage.get("local_read_gate_pass"),
        "read_200_304_ratio": (read_ok / max(1, read_ok + read_304)) if (read_ok or read_304) else None,
    }


def _summarize_path(path: Path) -> list[dict]:
    run = _read_payload(path)
    out = []
    for stage in run.get("results", []):
        if not isinstance(stage, dict):
            continue
        out.append(_summarize_stage(stage) | {"source_file": str(path)})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()

    stages: list[dict] = []
    for path in args.paths:
        if path.is_dir():
            stages.extend(
                item
                for json_path in sorted(path.glob("*.json"))
                for item in _summarize_path(json_path)
            )
        else:
            stages.extend(_summarize_path(path))

    print(json.dumps({"stages": stages}, indent=2 if not args.compact else None))


if __name__ == "__main__":
    main()
