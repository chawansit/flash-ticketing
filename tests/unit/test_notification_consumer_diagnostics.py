"""Bind partition timing to an admitted endpoint without additional metric reads."""
import io
import sys
from pathlib import Path

from ticketing import observability

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import observe_cce_workers as workers
import observe_two_host_pipeline as frozen_loader


def test_partition_and_error_counters_are_retained_per_endpoint_with_one_scrape(monkeypatch):
    module = frozen_loader.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    bundle = {"receipts": [{"role": "consumer", "private_ipv4": "10.2.1.1"},
                            {"role": "consumer", "private_ipv4": "10.2.1.2"}]}
    monkeypatch.setattr(workers, "validate_bundle", lambda _: {"10.2.1.1": 10, "10.2.1.2": 10})
    calls = []
    def fetch(url, timeout):
        calls.append(url)
        part = 0 if "10.2.1.1" in url else 1
        raw = ("process_start_time_seconds 10\nprocess_cpu_seconds_total 1\n"
               'ticketing_worker_operations_total{operation="consume_event",outcome="ok"} 5\n'
               f'ticketing_consumer_phase_seconds_count{{phase="partition",partition="{part}",outcome="ok"}} 3\n'
               f'ticketing_consumer_phase_seconds_sum{{phase="partition",partition="{part}",outcome="ok"}} 0.2\n'
               'ticketing_consumer_batch_failures_total{sqlstate="other"} 1\n'
               'ticketing_consumer_failure_classes_total{category="ValueError"} 1\n')
        return io.BytesIO(raw.encode())
    workers.install(module, bundle, fetch=fetch)
    result = module.worker_counters("consumer", "consume_event", "http://consumer:9101/metrics")
    assert len(calls) == len(set(calls)) == 2
    details = result["consumer_details_by_endpoint"]
    assert details["10.2.1.1"]["phases"] == {"partition:0:ok:count": 3, "partition:0:ok:sum": .2}
    assert details["10.2.1.2"]["phases"] == {"partition:1:ok:count": 3, "partition:1:ok:sum": .2}
    assert details["10.2.1.2"]["exception_classes"] == {"ValueError": 1}
    assert result["consumer_phases"]["partition:1:ok:count"] == 3


def test_exception_labels_never_include_arbitrary_class_names_or_messages():
    PrivateCustomerIdentifier = type("PrivateCustomerIdentifier", (Exception,), {})
    observability.observe_consumer_batch_error(PrivateCustomerIdentifier("secret customer data"))
    metrics = list(observability.CONSUMER_FAILURE_CLASSES.collect())
    assert any(row.labels.get("category") == "other" for metric in metrics for row in metric.samples)
    assert all(row.labels.get("category") != "PrivateCustomerIdentifier"
               for metric in metrics for row in metric.samples)
