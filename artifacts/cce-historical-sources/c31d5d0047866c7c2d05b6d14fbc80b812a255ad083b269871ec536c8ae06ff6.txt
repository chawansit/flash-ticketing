from __future__ import annotations

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "generator_observe", Path(__file__).resolve().parents[2] / "scripts/generator_observe.py"
)
assert spec and spec.loader
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


def test_percentile_uses_nearest_rank() -> None:
    assert observer.percentile([4.0, 1.0, 3.0, 2.0], 0.95) == 4.0
    assert observer.percentile([], 0.95) is None


def test_cpu_delta_excludes_guest_ticks_from_total() -> None:
    previous = observer.parse_cpu("cpu  100 5 20 800 10 0 5 1 4 2\n")
    current = observer.parse_cpu("cpu  120 5 30 860 20 0 5 1 14 2\n")

    result = observer.cpu_delta(previous, current)

    assert result == {"busy_pct": 30.0, "iowait_pct": 10.0, "steal_pct": 0.0}


def test_pressure_parser_preserves_averages_and_total() -> None:
    parsed = observer.parse_pressure(
        "some avg10=1.25 avg60=0.50 avg300=0.10 total=12345\n"
        "full avg10=0.20 avg60=0.10 avg300=0.01 total=98\n"
    )

    assert parsed["some"]["avg10"] == 1.25
    assert parsed["some"]["total"] == 12345
    assert parsed["full"]["total"] == 98


def test_net_parser_excludes_loopback_and_maps_drop_columns() -> None:
    parsed = observer.parse_net_dev(
        "Inter-| Receive | Transmit\n"
        " face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n"
        "    lo: 100 1 0 0 0 0 0 0 100 1 0 0 0 0 0 0\n"
        "  eth0: 1000 10 2 3 0 0 0 0 2000 20 4 5 0 0 0 0\n"
        "  eth1: 500 5 0 1 0 0 0 0 700 7 1 2 0 0 0 0\n"
    )

    assert parsed == {
        "rx_bytes": 1500,
        "rx_errors": 2,
        "rx_drops": 4,
        "tx_bytes": 2700,
        "tx_errors": 5,
        "tx_drops": 7,
    }


def test_process_delta_aggregates_only_persistent_processes(monkeypatch) -> None:
    monkeypatch.setattr(observer.os, "sysconf", lambda _: 100, raising=False)
    previous = {
        10: {
            "cpu_ticks": 100,
            "voluntary_context_switches": 20,
            "nonvoluntary_context_switches": 3,
        }
    }
    current = {
        "processes": [
            {
                "pid": 10,
                "cpu_ticks": 150,
                "voluntary_context_switches": 27,
                "nonvoluntary_context_switches": 5,
            },
            {
                "pid": 11,
                "cpu_ticks": 500,
                "voluntary_context_switches": 100,
                "nonvoluntary_context_switches": 100,
            },
        ]
    }

    assert observer.process_delta(previous, current, 2.0) == {
        "cpu_pct": 25.0,
        "voluntary_context_switches": 7,
        "nonvoluntary_context_switches": 2,
    }
