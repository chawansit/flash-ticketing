"""ADR0272 bounded redacted live failure capture; no database or customer reads."""
import copy
import threading
import time

from slot_failure_evidence import validate_record


class Capture:
    def __init__(self, deployment, *, now=time.monotonic):
        self.deployment, self.now = deployment, now
        self.identities = dict(deployment.pod_uids)
        if set(self.identities) != {f"api-{i}" for i in range(4)} or len(set(self.identities.values())) != 4:
            raise ValueError("Four unique admitted API identities required")
        self.records = {name: {} for name in self.identities}
        self.samples, self.errors = [], []
        self.stop_event = threading.Event()
        self.thread = None
        self.started = None

    def read(self):
        at = self.now()
        if len(self.samples) >= 64 or self.started is None or at - self.started > 600:
            raise ValueError("Live admission capture budget exhausted")
        if self.samples and at - self.samples[-1] > 20:
            raise ValueError("Live admission coverage gap exceeds overlap")
        value = self.deployment.admission_diagnostics(lookback_seconds=30)
        self.samples.append(at)
        if (value.get("all_pods_captured") is not True or value.get("errors")
                or set(value.get("pods", {})) != set(self.identities)):
            raise ValueError("Complete four-pod live capture required")
        for name, row in value["pods"].items():
            if (row.get("pod_uid") != self.identities[name]
                    or row.get("schema_version") != 1
                    or any(row.get(k) not in (False, 0) for k in
                           ("byte_limit_reached", "overflow_events", "malformed_events"))
                    or not isinstance(row.get("records"), list) or len(row["records"]) > 128
                    or row.get("failure_events") != len(row["records"])):
                raise ValueError("Live admission identity or truncation differs")
            for item in row["records"]:
                record = validate_record(item.get("slot_ownership"))
                if item.get("role") != record["role"] or item.get("reason") != record["reason"]:
                    raise ValueError("Live log and slot identity disagree")
                prior = self.records[name].get(record["sequence"])
                if prior is not None and prior != record:
                    raise ValueError("Conflicting live admission duplicate")
                if prior is None and len(self.records[name]) >= 4096:
                    raise ValueError("Bounded live admission history exhausted")
                self.records[name][record["sequence"]] = copy.deepcopy(record)

    def _loop(self):
        while not self.stop_event.wait(10):
            try:
                self.read()
            except Exception as error:  # noqa: BLE001 - Preserve failure without hiding customer outcome.
                self.errors.append(type(error).__name__)
                self.stop_event.set()

    def start(self):
        if self.started is not None:
            raise ValueError("Fresh live admission collector required")
        self.started = self.now()
        self.read()
        self.thread = threading.Thread(target=self._loop, name="bounded-admission-capture", daemon=False)
        self.thread.start()

    def finish(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=260)
            if self.thread.is_alive():
                raise TimeoutError("Live collector must stop before credential or pod removal")
        try:
            self.read()
        except Exception as error:  # noqa: BLE001 - Financial verification still follows incomplete diagnostics.
            self.errors.append(type(error).__name__)
        return {"decision": "ADR0272", "complete": not self.errors and bool(self.samples),
                "samples": len(self.samples), "errors": list(self.errors),
                "pod_uids": dict(self.identities), "maximum_capture_seconds": 600,
                "records": {name: [rows[k] for k in sorted(rows)] for name, rows in self.records.items()}}


def endpoints(evidence, bundle):
    if evidence.get("decision") != "ADR0272" or evidence.get("complete") is not True or evidence.get("errors"):
        raise ValueError("Complete bounded live admission capture required")
    rows = bundle["receipts"]
    identities = {row["pod_name"]: row["pod_uid"] for row in rows}
    if evidence.get("pod_uids") != identities or set(evidence.get("records", {})) != set(identities):
        raise ValueError("Live admission receipts changed")
    return {row["private_ipv4"]: evidence["records"][row["pod_name"]] for row in rows}
