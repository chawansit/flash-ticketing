"""ADR0285: one existing data connection, owned by one bounded SQL sampler."""
import copy
import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from diagnostic_connection import _Driver


class Sampler:
    def __init__(self, connection, sample, shows, output, *, interval=1, seconds=480, max_seconds=3780):
        if max_seconds not in (3780, 4200) or not 0.01 <= interval <= 1 or not 0 < seconds <= max_seconds:
            raise ValueError("Bounded sampler interval and duration required")
        self.connection, self.sample = connection, sample
        self.shows = tuple(shows)
        self.output, self.interval, self.seconds = Path(output), interval, seconds
        self.latest, self.collected_at, self.failure = None, None, None
        self.stop_event, self.ready = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, name="bounded-database-sampler", daemon=True)
        self.thread.start()

    def _run(self):
        end = time.monotonic() + self.seconds
        try:
            with self.output.open("x", encoding="utf-8") as stream:
                while not self.stop_event.is_set() and time.monotonic() < end:
                    started = time.monotonic()
                    try:
                        row = dict(self.sample(self.connection, list(self.shows)))
                    except Exception as error:  # noqa: BLE001 - Retain type only; no retry or replacement connection.
                        row = {"database_error": type(error).__name__, "database_wait_diagnostics": {
                            "complete": False, "error_type": type(error).__name__}}
                        try:
                            self.connection.rollback()
                        except Exception as rollback_error:  # noqa: BLE001 - Retain rollback failure too.
                            row["database_rollback_error"] = type(rollback_error).__name__
                    row.pop("observer_phase_ms", None)  # Foreground phase timings are not SQL timings.
                    row.update(utc=datetime.now(UTC).isoformat(),
                               database_sample_ms=(time.monotonic() - started) * 1000)
                    stream.write(json.dumps(row) + "\n")
                    stream.flush()
                    with self.lock:
                        self.latest, self.collected_at = copy.deepcopy(row), time.monotonic()
                    self.ready.set()
                    self.stop_event.wait(max(0, self.interval - (time.monotonic() - started)))
        except Exception as error:  # noqa: BLE001 - File/serialization failures must invalidate observation.
            self.failure = type(error).__name__
        finally:
            self.ready.set()

    def snapshot(self, shows):
        if tuple(shows) != self.shows:
            raise ValueError("Original diagnostic cohort required")
        if not self.ready.wait(10):
            raise TimeoutError("Diagnostic startup did not produce a bounded sample")
        if self.failure:
            raise RuntimeError("Independent diagnostic stream failed: " + self.failure)
        with self.lock:
            if self.latest is None:
                raise RuntimeError("Diagnostic sampler ended without evidence")
            row = copy.deepcopy(self.latest)
            age = time.monotonic() - self.collected_at
        row["database_sample_utc"] = row.pop("utc")
        row["database_sample_age_seconds"] = age
        if "cohort_sample_age_seconds" in row and row["cohort_sample_age_seconds"] is not None:
            row["cohort_sample_age_seconds"] += age
            if row["cohort_sample_age_seconds"] > 10:
                row.update(cohort_sample_complete=False, database_error="StaleCohortSnapshot")
        if age > 2:
            row["database_error"] = "StaleDatabaseSnapshot"
        if not self.thread.is_alive():
            row["database_error"] = "DatabaseSamplerStopped"
        return row

    def stop(self):
        self.stop_event.set()
        self.thread.join(5)
        if self.thread.is_alive():
            self.connection.cancel()
            self.thread.join(5)
            if self.thread.is_alive():
                raise TimeoutError("Diagnostic sampler did not stop before connection close")
        if self.failure:
            raise RuntimeError("Independent diagnostic stream failed: " + self.failure)


class OwnedConnection:
    def __init__(self, connection):
        self._connection, self.sampler = connection, None

    def __getattr__(self, name):
        if self.sampler is not None and name in {"execute", "cursor", "rollback", "commit", "transaction"}:
            raise ValueError("Diagnostic data connection belongs exclusively to the SQL sampler")
        return getattr(self._connection, name)

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, *args):
        try:
            if self.sampler is not None:
                self.sampler.stop()
        finally:
            if self.sampler is None or not self.sampler.thread.is_alive():
                self._connection.__exit__(*args)

    def close(self):
        try:
            if self.sampler is not None:
                self.sampler.stop()
        finally:
            if self.sampler is None or not self.sampler.thread.is_alive():
                self._connection.close()


def install(module, source_dsn, output, seconds, *, max_seconds=3780):
    """Intercept only the already-authorized data connection; leave admin untouched."""
    if max_seconds not in (3780, 4200) or not 0 < seconds <= max_seconds:
        raise ValueError("Bounded sampler interval and duration required")
    original_connect, original_sample = module.psycopg.connect, module.sample
    created = []

    def connect(conninfo="", **kwargs):
        if conninfo != source_dsn:
            return original_connect(conninfo, **kwargs)
        if kwargs != {"autocommit": True} or created:
            raise ValueError("Exactly one existing data connection required")
        result = OwnedConnection(original_connect(conninfo, **kwargs))
        created.append(result)
        return result

    def sample(conn, shows):
        if len(created) != 1 or conn is not created[0]:
            raise ValueError("Protected diagnostic connection required")
        if conn.sampler is None:
            conn.sampler = Sampler(conn._connection, original_sample, shows, output, seconds=seconds, max_seconds=max_seconds)
        return conn.sampler.snapshot(shows)

    module.psycopg = _Driver(module.psycopg, connect)
    module.sample = sample
    return created


def summarize(path, start, end, *, hourly=False):
    """Original diagnostic quality gates plus coverage of the actual offered window."""
    from database_wait_evidence import stamp
    from database_wait_evidence import summarize as summarize_waits
    result = summarize_waits(path, hourly=hourly)
    first, last = None, None
    with Path(path).open(encoding="utf-8") as stream:
        for line in stream:
            item = json.loads(line).get("database_wait_diagnostics", {})
            if item.get("complete") is True:
                at = stamp(item["statistics_utc"])
                first = at if first is None else first
                last = at
    left, right = stamp(start), stamp(end)
    covered = (first is not None and last is not None and left < right
               and first <= left and last >= right)
    result.update(independent_sampling=True, offered_window_covered=covered,
                  first_statistics_utc=None if first is None else first.isoformat(),
                  last_statistics_utc=None if last is None else last.isoformat(),
                  complete=result["complete"] and covered)
    return result
