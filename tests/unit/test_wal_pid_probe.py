import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "wal_pid_probe.py"
SPEC = importlib.util.spec_from_file_location("wal_pid_probe", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_same_backend_wait_is_attributed_only_during_slow_commit():
    rows = [
        {
            "success": True,
            "backend_pid": 123,
            "commit_ms": 150.0,
            "commit_started_utc": "2026-09-20T00:00:00+00:00",
            "commit_ended_utc": "2026-09-20T00:00:00.150000+00:00",
        }
    ]
    samples = [
        {
            "utc": "2026-09-20T00:00:00.050000+00:00",
            "states": [
                {"pid": 123, "wait_event": "WalSync"},
                {"pid": 456, "wait_event": "WALWrite"},
            ],
        },
        {
            "utc": "2026-09-20T00:00:00.200000+00:00",
            "states": [
                {"pid": 123, "wait_event": "ClientRead"},
            ],
        },
    ]
    assert module.correlate(rows, samples) == {"slow_commits": 1, "same_pid_wait_events": {"WalSync": 1}}


def test_unsampled_slow_commit_remains_inconclusive():
    rows = [
        {
            "success": True,
            "backend_pid": 123,
            "commit_ms": 120.0,
            "commit_started_utc": "2026-09-20T00:00:00+00:00",
            "commit_ended_utc": "2026-09-20T00:00:00.120000+00:00",
        }
    ]
    assert module.correlate(rows, [])["same_pid_wait_events"] == {"not_sampled_or_not_waiting": 1}
