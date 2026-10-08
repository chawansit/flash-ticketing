"""Wrap the unchanged paid observer with four verified private API endpoints."""

import argparse
import copy
import hashlib
import importlib.util
import json
import math
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

from prepare_two_host_scaling import API_SETTINGS, BACKGROUND, validate_inventory

FROZEN_NORMALIZED_SHA256 = "1890ac0302507fc81a3c451351521684aa92b00b47316ab320af44b592dacaf2"
FROZEN_IMAGE_ID = "sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e"
MAX_PAYLOAD = 4 * 2**20


def load_frozen(path):
    raw = path.read_bytes()
    if hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest() != FROZEN_NORMALIZED_SHA256:
        raise ValueError("Frozen pipeline source differs")
    spec = importlib.util.spec_from_file_location("frozen_paid_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extra_api_metrics(payload):
    """Read process identity and aggregate bounded static business routes only."""
    result = {}
    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(
            r"(process_start_time_seconds|ticketing_http_requests_total|ticketing_order_status_cache_total)(?:\{(.*)\})? ([^ ]+)", line
        )
        if not match:
            continue
        name, label_text, raw = match.groups()
        value = float(raw)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Invalid cumulative metric")
        if name == "process_start_time_seconds":
            if value <= 0 or name in result:
                raise ValueError("Invalid process identity")
            result[name] = value
            continue
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', label_text or ""))
        if name == "ticketing_order_status_cache_total":
            outcome = labels.get("outcome", "")
            if not re.fullmatch(r"[a-z_]{1,40}", outcome):
                raise ValueError("Unbounded order cache outcome")
            key = "status_cache:" + outcome
            if key in result or len(result) >= 256:
                raise ValueError("Duplicate or excessive order cache series")
            result[key] = value
            continue
        route, method, status = (labels.get(k, "") for k in ("route", "method", "status"))
        if (
            route.startswith("/v1/")
            and re.fullmatch(r"[A-Z]{3,7}", method)
            and re.fullmatch(r"[1-5][0-9]{2}", status)
        ):
            # Collapse routes/status codes to one business counter per replica;
            # frozen parsing still retains route-level errors and timing metrics.
            key = f"business_http:{route}:{method}:{status}"
            if key in result or len(result) >= 256:
                raise ValueError("Duplicate or excessive business series")
            result[key] = value
            result["business_http_requests_total"] = result.get("business_http_requests_total", 0) + value
    if "process_start_time_seconds" not in result:
        raise ValueError("Process-start metric missing")
    # The metric may have no business series before the first journey.
    result.setdefault("business_http_requests_total", 0)
    return result


def verify_dedup_factor_evidence(inventory):
    arm = inventory.get("arm")
    marker = inventory.get("status_refresh_contract", {})
    expected = {"decision": "ADR0157", "cache_age_ms": 1000, "arm": arm,
                "api_image_id": marker.get("api_image_id"), "consumer_event_refresh": "1",
                "consumer_event_refresh_dedup": "1" if arm == "candidate" else "0"}
    if arm not in {"control", "candidate"} or marker != expected:
        raise ValueError("Exact deduplication inventory marker required")
    if any(a.get("ORDER_STATUS_EVENT_REFRESH_DEDUP") != "0" for a in inventory.get("apis", [])):
        raise ValueError("API deduplication must explicitly be disabled")
    workers = inventory.get("worker_sources", [])
    counts = {r: 0 for r in BACKGROUND}
    for row in workers:
        role = row.get("role")
        settings = row.get("settings", {})
        want = {"ORDER_STATUS_CACHE_MS": "1000" if role == "consumer" else "0",
                "ORDER_STATUS_EVENT_REFRESH": "1" if role == "consumer" else "0",
                "ORDER_STATUS_EVENT_REFRESH_DEDUP": expected["consumer_event_refresh_dedup"]
                if role == "consumer" else "0"}
        if role not in counts or any(settings.get(k) != v for k, v in want.items()):
            raise ValueError("Explicit worker deduplication factor evidence required")
        counts[role] += 1
    if counts != {r: c["replicas"] for r, c in BACKGROUND.items()}:
        raise ValueError("Complete factor evidence required")



def verify_confirmation_factor_evidence(inventory):
    marker = inventory.get("status_refresh_contract", {})
    arm = inventory.get("arm")
    flag = "1" if arm == "candidate" else "0"
    expected = {**BACKGROUND, "simulator": {**BACKGROUND["simulator"], "pool_per_replica": 10},
                "confirmation": {"replicas": 1, "pool_per_replica": 2, "concurrency": 2}}
    if (arm not in {"control", "candidate"} or marker.get("decision") != "ADR0161"
            or marker.get("async_confirmation") != flag or marker.get("poll_ms") != 500
            or inventory.get("background") != expected):
        raise ValueError("Exact confirmation placement/pool allocation required")
    counts = {r: 0 for r in expected}
    for row in inventory.get("worker_sources", []):
        role, settings = row.get("role"), row.get("settings", {})
        if role not in counts:
            raise ValueError("Unexpected confirmation background role")
        counts[role] += 1
        if (settings.get("ORDER_STATUS_EVENT_REFRESH") != ("1" if role == "consumer" else "0")
                or settings.get("ORDER_STATUS_EVENT_REFRESH_DEDUP") != "0"
                or settings.get("PAYMENT_CONFIRMATION_ASYNC") != ("1" if role == "confirmation" else "0")
                or settings.get("ORDER_STATUS_POLL_MS") != "500"):
            raise ValueError("Explicit common background feature settings required")
        if role in {"confirmation", "simulator"} and settings.get("DB_POOL_MAX") != ("2" if role == "confirmation" else "10"):
            raise ValueError("Simulator/confirmation aggregate connection budget differs")
        if role == "confirmation" and settings.get("CONFIRMATION_CONCURRENCY") != "2":
            raise ValueError("Confirmation concurrency differs")
    if counts != {r: c["replicas"] for r, c in expected.items()}:
        raise ValueError("Every confirmation worker must be observed")
    if any(a.get("settings", {}).get("PAYMENT_CONFIRMATION_ASYNC") != flag
           or a.get("settings", {}).get("ORDER_STATUS_POLL_MS") != "500" for a in inventory.get("apis", [])):
        raise ValueError("Explicit common polling and isolated API intake factor required")



def verify_admission_factor_evidence(inventory):
    marker = inventory.get("status_refresh_contract", {})
    arm = inventory.get("arm")
    if marker.get("decision") in {"ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
        decision = marker["decision"]
        shared_placement = decision in {"ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}
        url = "http://load-balancer:8000" if shared_placement else {"control": "http://api:8000", "candidate": "http://load-balancer:8000"}.get(arm)
        counts = {"primary": 1, "secondary": 3} if shared_placement and arm == "candidate" else {"primary": 2, "secondary": 2}
        factor = "api_placement_2_2_to_1_3_shared_callbacks" if shared_placement else "simulator_callback_destination_local_to_shared"
        if decision == "ADR0219":
            if arm != "candidate":
                raise ValueError("84/s probe requires the candidate-only measured placement")
            factor = "paid_offered_rate_60_to_84_shared_1_3"
        if decision == "ADR0222":
            if arm != "candidate":
                raise ValueError("Consumer correction is candidate-only")
            factor = "consumer_interleaved_refresh_at_84_shared_1_3"
        if decision == "ADR0224":
            if arm != "candidate":
                raise ValueError("Index correction is candidate-only")
            factor = "orders_event_id_index_at_84_shared_1_3"
        if decision == "ADR0225":
            if arm != "candidate":
                raise ValueError("Writer correction is candidate-only")
            factor = "writer_write_pipeline_at_84_shared_1_3"
            workers = inventory.get("worker_sources", [])
            writers = [w for w in workers if w.get("role") == "reservation-writer"]
            if len(writers) != 3 or any(w.get("settings", {}).get("RESERVATION_WRITE_PIPELINE") != "1" for w in writers) or any(w.get("settings", {}).get("RESERVATION_WRITE_PIPELINE", "0") != "0" for w in workers if w.get("role") != "reservation-writer"):
                raise ValueError("Writer-only pipeline activation must be explicitly observed")
        expected = {"decision": decision, "arm": arm, "factor": factor,
                    "api_counts": counts, "callback_url": url, "partial_timeout_reclaim": "0", "async_intake": "0",
                    "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": marker.get("api_image_id")}
        actual = {role: sum(a.get("host_role") == role for a in inventory.get("apis", [])) for role in counts}
        simulators = [w for w in inventory.get("worker_sources", []) if w.get("role") == "simulator"]
        if (arm not in {"control", "candidate"} or marker != expected or actual != counts
                or len(simulators) != 1 or simulators[0].get("settings", {}).get("API_URL") != url):
            raise ValueError("Exact inspected callback routing and fixed placement required")
        common = copy.deepcopy(inventory)
        common["arm"] = arm if shared_placement else "control"
        common["status_refresh_contract"] = {"decision": "ADR0174", "arm": common["arm"],
            "factor": "api_placement_2_2_to_1_3", "api_counts": counts, "partial_timeout_reclaim": "0",
            "async_intake": "0", "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": marker["api_image_id"]}
        verify_admission_factor_evidence(common)
        return
    if marker.get("decision") == "ADR0193":
        expected = {"decision": "ADR0193", "arm": arm, "factor": "payment_dispatch_claim_two_statements_to_one",
                    "claim_statements": 1 if arm == "candidate" else 2,
                    "worker_source_sha256": marker.get("worker_source_sha256"),
                    "partial_timeout_reclaim": "0", "async_intake": "0", "cache_age_ms": 1000,
                    "poll_ms": 500, "api_image_id": marker.get("api_image_id")}
        source_hash = marker.get("worker_source_sha256")
        if (arm not in {"control", "candidate"} or marker != expected
                or not isinstance(source_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", source_hash)):
            raise ValueError("Exact atomic claim factor evidence required")
        workers = inventory.get("worker_sources", [])
        if not workers or any(row.get("source_identity", {}).get("source_hashes_match") is not True
                or row.get("source_identity", {}).get("resolved_imports", {}).get("src/ticketing/workers.py", {}).get("sha256") != source_hash
                for row in workers):
            raise ValueError("Every worker must execute the bound claim source")
        common = copy.deepcopy(inventory)
        common["arm"] = "control"
        common["status_refresh_contract"] = {"decision": "ADR0163", "arm": "control",
            "factor": "api_API_PARTIAL_TIMEOUT_RECLAIM", "partial_timeout_reclaim": "0",
            "async_intake": "0", "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": marker["api_image_id"]}
        verify_admission_factor_evidence(common)
        return
    if marker.get("decision") == "ADR0177":
        from application_database_wait_evidence import binding_from_record

        binding_from_record(inventory.get("application_database_role_binding"))
        if marker.get("diagnostic_scope") != "application_role" or marker.get("diagnostic_decision") != "ADR0176":
            raise ValueError("Exact role-scoped diagnostic policy required")
        common = copy.deepcopy(inventory)
        common["status_refresh_contract"].update(decision="ADR0174")
        common["status_refresh_contract"].pop("diagnostic_scope")
        common["status_refresh_contract"].pop("diagnostic_decision")
        verify_admission_factor_evidence(common)
        return
    if marker.get("decision") == "ADR0174":
        counts = {"primary": 2, "secondary": 2} if arm == "control" else {"primary": 1, "secondary": 3}
        expected = {"decision": "ADR0174", "arm": arm, "factor": "api_placement_2_2_to_1_3",
                    "api_counts": counts, "partial_timeout_reclaim": "0", "async_intake": "0",
                    "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": marker.get("api_image_id")}
        actual = {r: sum(a.get("host_role") == r for a in inventory.get("apis", [])) for r in counts}
        if arm not in {"control", "candidate"} or marker != expected or actual != counts:
            raise ValueError("Exact placement evidence required")
        common = copy.deepcopy(inventory)
        common["arm"] = "control"
        common["status_refresh_contract"] = {"decision": "ADR0163", "arm": "control",
            "factor": "api_API_PARTIAL_TIMEOUT_RECLAIM", "partial_timeout_reclaim": "0",
            "async_intake": "0", "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": marker["api_image_id"]}
        verify_admission_factor_evidence(common)
        return
    flag = "1" if arm == "candidate" else "0"
    expected_marker = {"decision": "ADR0163", "arm": arm, "factor": "api_API_PARTIAL_TIMEOUT_RECLAIM",
                       "partial_timeout_reclaim": flag, "async_intake": "0", "cache_age_ms": 1000,
                       "poll_ms": 500, "api_image_id": marker.get("api_image_id")}
    if arm not in {"control", "candidate"} or marker != expected_marker:
        raise ValueError("Exact partial timeout factor evidence required")
    expected_settings = {**API_SETTINGS, "ORDER_STATUS_CACHE_MS": "1000",
                         "ORDER_STATUS_EVENT_REFRESH": "0", "ORDER_STATUS_EVENT_REFRESH_DEDUP": "0",
                         "ORDER_STATUS_POLL_MS": "500", "PAYMENT_CALLBACK_PROVIDER": "simulator",
                         "CONFIRMATION_MAX_PENDING": "10000", "CONFIRMATION_LEASE_SECONDS": "30",
                         "CONFIRMATION_MAX_ATTEMPTS": "8", "CONFIRMATION_RETRY_MS": "100",
                         "PAYMENT_CONFIRMATION_ASYNC": "0", "API_PARTIAL_TIMEOUT_RECLAIM": flag}
    if any(a.get("settings") != expected_settings for a in inventory.get("apis", [])):
        raise ValueError("Complete exact admission API settings required before compatibility projection")
    # Compatibility view only: reuse the exact common receipt-worker/budget checks.
    common = copy.deepcopy(inventory)
    common["arm"] = "control"
    common["status_refresh_contract"].update(decision="ADR0161", async_confirmation="0")
    verify_confirmation_factor_evidence(common)
    for api in inventory.get("apis", []):
        settings = api.get("settings", {})
        if any(settings.get(k) != v for k, v in {
                "API_PARTIAL_TIMEOUT_RECLAIM": flag, "API_CALLBACK_ACQUISITION_RESERVE": "0",
                "DB_POOL_MAX": "4", "DB_POOL_MAX_WAITING": "12", "DB_POOL_WAIT_MS": "500",
                "API_PAYMENT_POOL_MAX": "2", "API_POOL_SHARED_WAITING": "1"}.items()):
            raise ValueError("Exact admission API factor and budgets required")
    if any(row.get("settings", {}).get("API_PARTIAL_TIMEOUT_RECLAIM") != "0"
           for row in inventory.get("worker_sources", [])):
        raise ValueError("Background reclamation must be explicitly disabled")


def admission_failure_metrics(payload):
    if "# TYPE ticketing_db_acquisition_failures_total counter" not in payload:
        raise ValueError("Admission diagnostic metric family missing")
    roles = {"general", "payment"}
    reasons = {"global_limit", "role_limit", "native_timeout", "native_limit"}
    result = {"acquisition_failure:" + role + ":" + reason: 0.0 for role in roles for reason in reasons}
    seen = set()
    for line in payload.splitlines():
        if not line.startswith("ticketing_db_acquisition_failures_total{"):
            continue
        name, raw = line.rsplit(" ", 1)
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', name))
        if set(labels) != {"role", "reason"} or labels["role"] not in roles or labels["reason"] not in reasons:
            raise ValueError("Unknown admission metric labels")
        key = "acquisition_failure:" + labels["role"] + ":" + labels["reason"]
        value = float(raw)
        if key in seen or not math.isfinite(value) or value < 0:
            raise ValueError("Invalid or duplicate admission failure counter")
        seen.add(key); result[key] = value
    return result


def admission_startup(row):
    """Reject missing four-replica acquisition counters before customer dispatch."""
    keys = set(admission_failure_metrics("# TYPE ticketing_db_acquisition_failures_total counter"))
    replicas = row.get("api_replicas", {})
    if not isinstance(replicas, dict) or len(replicas) != 4 or row.get("api_metrics_error"):
        raise ValueError("Complete four-replica admission startup required")
    for values in replicas.values():
        if any(type(values.get(k)) not in {int, float} or not math.isfinite(values[k]) or values[k] < 0 for k in keys):
            raise ValueError("Complete admission counters required before dispatch")


def confirmation_startup(row):
    replicas = row.get("confirmation_db_replicas", {})
    if row.get("confirmation_replicas") != 1 or len(replicas) != 1:
        raise ValueError("Confirmation worker metrics missing")
    for metric in replicas.values():
        for key in ("process_cpu_seconds_total", "confirmation_metric:ticketing_payment_confirmation_pending",
                    "confirmation_metric:ticketing_payment_confirmation_review",
                    "confirmation_metric:ticketing_payment_confirmation_oldest_seconds"):
            value = metric.get(key)
            if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                raise ValueError("Confirmation queue/CPU metrics invalid or missing")
        if metric["confirmation_metric:ticketing_payment_confirmation_pending"] != 0:
            raise ValueError("Confirmation queue must start drained")
    if any(k.endswith("_error") for k in row):
        raise ValueError("Confirmation startup observer error")


def install_adapter(module, inventory, *, image_id, now=None, fetch=urlopen, approved_inventory_sha256=None):
    if approved_inventory_sha256 is None:
        validate_inventory(inventory, image_id=image_id, now=now)
    else:
        actual = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        marker = inventory.get("status_refresh_contract", {})
        if (not isinstance(approved_inventory_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", approved_inventory_sha256)
                or actual != approved_inventory_sha256 or marker.get("decision") not in {"ADR0151", "ADR0157", "ADR0161", "ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}
                or marker.get("cache_age_ms") != 1000 or marker.get("arm") != inventory.get("arm")
                or not isinstance(marker.get("api_image_id"), str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", marker["api_image_id"])):
            raise ValueError("Exact host-qualified inventory digest required")
        if marker["decision"] == "ADR0161":
            verify_confirmation_factor_evidence(inventory)
        if marker["decision"] in {"ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
            verify_admission_factor_evidence(inventory)
        if marker["decision"] == "ADR0157":
            verify_dedup_factor_evidence(inventory)
        if any(a.get("settings", {}).get("ORDER_STATUS_CACHE_MS") != "1000"
               or a.get("ORDER_STATUS_EVENT_REFRESH", "0") != "0" for a in inventory["apis"]):
            raise ValueError("Explicit equal cache age and consumer-only factor required")
        # Only adapt historical assertions for structural validation; retain original evidence.
        structural = copy.deepcopy(inventory)
        structural["arm"] = "candidate"
        for api in structural["apis"]:
            api["settings"]["ORDER_STATUS_CACHE_MS"] = "0"
        if marker["decision"] in {"ADR0161", "ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
            structural["background"] = copy.deepcopy(BACKGROUND)
            for api in structural["apis"]:
                api["settings"].pop("PAYMENT_CONFIRMATION_ASYNC")
                api["settings"].pop("ORDER_STATUS_POLL_MS")
        if marker["decision"] in {"ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
            # Full extended dictionaries were verified above; keep every original budget.
            for api in structural["apis"]:
                api["settings"] = {key: api["settings"][key] for key in API_SETTINGS}
        if marker["decision"] == "ADR0177":
            # Exact scoped evidence was validated above; only the legacy structural view changes.
            structural["status_refresh_contract"]["decision"] = "ADR0174"
            structural["status_refresh_contract"].pop("diagnostic_scope")
            structural["status_refresh_contract"].pop("diagnostic_decision")
        if marker["decision"] in {"ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
            structural["status_refresh_contract"].update(decision="ADR0174", factor="api_placement_2_2_to_1_3")
            structural["status_refresh_contract"].pop("callback_url")
        placement = "one-plus-three" if marker["decision"] in {"ADR0174", "ADR0177", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'} and inventory["arm"] == "candidate" else None
        validate_inventory(structural, image_id=marker["api_image_id"], now=now, placement=placement)
    if inventory.get("status_refresh_contract", {}).get("decision") in {"ADR0161", "ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
        module.METRICS["confirmation"] = ("confirm_one", "http://confirmation:9101/metrics")
        original_parser = module.parse_api_metrics

        def parse_confirmation(payload):
            result = original_parser(payload)
            for line in payload.splitlines():
                if line.startswith("process_start_time_seconds "):
                    result["process_start_time_seconds"] = float(line.rsplit(" ", 1)[1])
                if line.startswith(("ticketing_payment_confirmation_", "ticketing_payment_confirmations_total")) and " " in line:
                    name, value = line.rsplit(" ", 1)
                    numeric = float(value)
                    if not math.isfinite(numeric) or numeric < 0:
                        raise ValueError("Invalid confirmation metric")
                    result["confirmation_metric:" + name] = numeric
            return result

        module.parse_api_metrics = parse_confirmation
    entries = sorted(inventory["apis"], key=lambda a: (a["host_role"], a["container_id"]))
    endpoints = {f"{a['host_role']}:{a['container_id']}": a for a in entries}
    original_discovery = module.api_replicas
    previous = {}

    def discover(host="api", port=8000):
        if host == "api" and port == 8000:
            return list(endpoints)
        return original_discovery(host, port)

    def metrics(label):
        entry = endpoints[label]
        with fetch(f"http://{entry['private_ipv4']}:{entry['port']}/metrics", timeout=2) as response:
            raw = response.read(MAX_PAYLOAD + 1)
        if len(raw) > MAX_PAYLOAD:
            raise ValueError("Metrics payload exceeds bound")
        payload = raw.decode("utf-8")
        extras = extra_api_metrics(payload)
        if inventory.get("status_refresh_contract", {}).get("decision") in {"ADR0163", "ADR0174", "ADR0177", "ADR0193", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}:
            extras.update(admission_failure_metrics(payload))
        prior = previous.get(label)
        if prior and (
            extras["process_start_time_seconds"] != prior["process_start_time_seconds"]
            or any(extras.get(k, -1) < v for k, v in prior.items() if k.startswith(("business_http", "status_cache:", "acquisition_failure:")))
        ):
            raise ValueError("API restart or counter reset")
        previous[label] = extras
        return {**module.parse_api_metrics(payload), **extras}

    module.api_replicas = discover
    module.api_metrics = metrics
    return endpoints


def summarize_distribution(rows, inventory, *, offered_start_utc, offered_end_utc):
    """Require complete, restart-free replica counters bracketing actual dispatch."""
    start, end = [datetime.fromisoformat(v) for v in (offered_start_utc, offered_end_utc)]
    if start.tzinfo is None or end.tzinfo is None or not 0 < (end - start).total_seconds() <= 300:
        raise ValueError("Bounded aware offered window required")
    labels = {f"{a['host_role']}:{a['container_id']}" for a in inventory["apis"]}
    if len(labels) != 4:
        raise ValueError("Four distinct replicas required")
    parsed = []
    for row in rows:
        utc = datetime.fromisoformat(row["utc"])
        if utc.tzinfo is None or (parsed and utc <= parsed[-1][0]):
            raise ValueError("Nonmonotonic metric sampling")
        parsed.append((utc, row))
    before = [(t, r) for t, r in parsed if t <= start]
    after = [(t, r) for t, r in parsed if t >= end]
    if (
        not before
        or not after
        or (start - before[-1][0]).total_seconds() > 2
        or (after[0][0] - end).total_seconds() > 2
    ):
        raise ValueError("Metrics do not bracket offered window")
    first_at, first = before[-1]
    last_at, last = after[0]
    observed = [(t, r) for t, r in parsed if first_at <= t <= last_at]
    prior = None
    for utc, row in observed:
        apis = row.get("api_replicas", {})
        if row.get("api_metrics_error") or set(apis) != labels:
            raise ValueError("Missing replica observation")
        if prior and (utc - prior[0]).total_seconds() > 2:
            raise ValueError("Metric sampling gap")
        for label in labels:
            metrics = apis[label]
            for name in ("process_start_time_seconds", "business_http_requests_total"):
                value = metrics.get(name)
                if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                    raise ValueError("Invalid replica metric")
            if metrics["process_start_time_seconds"] <= 0:
                raise ValueError("Process identity missing")
            if prior:
                old = prior[1]["api_replicas"][label]
                if (
                    metrics["process_start_time_seconds"] != old["process_start_time_seconds"]
                    or metrics["business_http_requests_total"] < old["business_http_requests_total"]
                ):
                    raise ValueError("Replica restarted or counter reset")
        prior = (utc, row)
    counts = {
        label: last["api_replicas"][label]["business_http_requests_total"]
        - first["api_replicas"][label]["business_http_requests_total"]
        for label in sorted(labels)
    }
    if any(value <= 0 for value in counts.values()):
        raise ValueError("Every replica must receive business traffic")
    total = sum(counts.values())
    return {
        "all_four_replicas_observed": True,
        "per_replica_traffic_distribution": True,
        "business_request_deltas": counts,
        "business_request_shares": {label: value / total for label, value in counts.items()},
        "observed_start_utc": first_at.astimezone(UTC).isoformat(),
        "observed_end_utc": last_at.astimezone(UTC).isoformat(),
        "scope": "Bracketing business HTTP counters; not paid ticket throughput or proof of balanced routing.",
    }


def install_diagnostic_connection(module, inventory, args):
    """ADR0180 opt-in only: owned credentials and trust are inventory-bound."""
    binding = inventory.get("diagnostic_connection_binding")
    path = args.diagnostic_connection_bundle
    if binding is None and path is None:
        return None
    if (not args.database_wait_diagnostics or not args.approved_inventory_sha256
            or inventory.get("status_refresh_contract", {}).get("decision") not in {"ADR0174", "ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224", 'ADR0225'}
            or not isinstance(binding, dict) or path is None
            or set(binding) != {"decision", "database", "identity_sha256", "endpoint", "bundle_sha256", "ca_sha256"}
            or binding["decision"] != "ADR0180"
            or not isinstance(binding["database"], str) or not binding["database"]
            or not isinstance(binding["endpoint"], list) or len(binding["endpoint"]) != 2
            or any(not isinstance(binding[key], str) or not re.fullmatch(r"[0-9a-f]{64}", binding[key])
                   for key in ("identity_sha256", "bundle_sha256", "ca_sha256"))):
        raise ValueError("Exact full-visibility diagnostic connection binding required")
    if hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest() != args.approved_inventory_sha256:
        raise ValueError("Diagnostic inventory binding differs")
    from diagnostic_connection import install, load_bundle

    spec = load_bundle(path, owned_directory=args.inventory.parent,
                       expected_sha256=binding["bundle_sha256"], expected_database=binding["database"],
                       expected_identity=binding["identity_sha256"], expected_endpoint=tuple(binding["endpoint"]),
                       expected_ca_sha256=binding["ca_sha256"])
    source = os.environ.get("TEST_DATABASE_URL")
    if not source:
        raise ValueError("Existing application observer connection required")
    from psycopg.conninfo import conninfo_to_dict

    if conninfo_to_dict(source).get("dbname") != binding["database"]:
        raise ValueError("Diagnostic and application databases differ")
    return install(module, source, spec)


def install_phase_timings(module, *, clock=None):
    """ADR0223: time existing serial calls without changing their order or errors."""
    import time

    clock = time.monotonic if clock is None else clock
    phases = {}

    def wrap(name, key, *, reset=False, attach=False):
        original = getattr(module, name)

        def measured(*args, **kwargs):
            if reset:
                phases.clear()
            started = clock()
            try:
                result = original(*args, **kwargs)
                if attach:
                    # Retain the same mapping through later worker/API collection.
                    result["observer_phase_ms"] = phases
                return result
            finally:
                label = key(*args, **kwargs) if callable(key) else key
                phases[label] = (clock() - started) * 1000

        setattr(module, name, measured)

    wrap("host_cpu_ticks", "host_cpu", reset=True)
    wrap("pgbouncer_view", "pgbouncer")
    wrap("sample", "paid_cohort", attach=True)
    wrap("worker_counters", lambda role, *args, **kwargs: "worker:" + role)
    wrap("api_metrics", lambda address: "api:" + address)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--frozen-observer", type=Path, required=True)
    parser.add_argument("--approved-inventory-sha256")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--database-wait-diagnostics", action="store_true")
    modes.add_argument("--application-database-wait-diagnostics", action="store_true")
    parser.add_argument("--diagnostic-connection-bundle", type=Path)
    args, remaining = parser.parse_known_args(argv)
    inventory = json.loads(args.inventory.read_text())
    scoped_profile = inventory.get("status_refresh_contract", {}).get("decision") == "ADR0177"
    if scoped_profile != args.application_database_wait_diagnostics or (scoped_profile and not args.approved_inventory_sha256):
        raise ValueError("Diagnostic mode must match the exact qualified inventory")
    frozen = load_frozen(args.frozen_observer)
    install_adapter(frozen, inventory, image_id=FROZEN_IMAGE_ID, approved_inventory_sha256=args.approved_inventory_sha256)
    install_diagnostic_connection(frozen, inventory, args)
    if inventory.get("status_refresh_contract", {}).get("decision") in {"ADR0222", "ADR0224", 'ADR0225'}:
        # Wrap before wait diagnostics so paid_cohort excludes wait collection.
        install_phase_timings(frozen)
    if args.database_wait_diagnostics:
        from database_wait_evidence import install
        install(frozen)
    if args.application_database_wait_diagnostics:
        from application_database_wait_evidence import binding_from_record, install
        install(frozen, binding_from_record(inventory.get("application_database_role_binding")))
    sys.argv = [str(args.frozen_observer), *remaining]
    frozen.main()


if __name__ == "__main__":
    main()
