import gzip
import hashlib
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import bounded_trace_transport as trace


def fixture(tmp_path, raw=b"sample\n" * 100):
    source = tmp_path / "raw.jsonl"
    source.write_bytes(raw)
    packed = tmp_path / "trace.gz"
    receipt = trace.pack_trace(source, packed)
    return source, packed, receipt


@pytest.mark.parametrize("raw", [b"", b"sample\n" * 100, bytes(range(256)) * 10000],
                         ids=["empty", "jsonl", "multi-buffer"])
def test_roundtrip_and_deterministic_transport(tmp_path, raw):
    source, packed, receipt = fixture(tmp_path, raw)
    second = tmp_path / "second.gz"
    assert trace.pack_trace(source, second) == receipt
    assert packed.read_bytes() == second.read_bytes()
    target = tmp_path / "restored.jsonl"
    assert trace.unpack_trace(packed, target, receipt)["pass"]
    assert target.read_bytes() == raw == source.read_bytes()
    assert receipt["raw_sha256"] == hashlib.sha256(raw).hexdigest()
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o600
        assert packed.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("operation", ["pack", "unpack"])
def test_stale_destination_preserved(tmp_path, operation):
    source, packed, receipt = fixture(tmp_path)
    target = tmp_path / "existing"
    target.write_bytes(b"unrelated")
    with pytest.raises(FileExistsError):
        if operation == "pack":
            trace.pack_trace(source, target)
        else:
            trace.unpack_trace(packed, target, receipt)
    assert target.read_bytes() == b"unrelated"


@pytest.mark.parametrize("corruption", ["truncated", "trailing", "second-member", "crc", "sha", "short", "long"])
def test_bad_trace_rejected_and_owned_output_removed(tmp_path, corruption):
    source, packed, receipt = fixture(tmp_path)
    raw = packed.read_bytes()
    if corruption == "truncated":
        raw = raw[:-1]
    elif corruption == "trailing":
        raw += b"garbage"
    elif corruption == "second-member":
        raw += gzip.compress(b"second")
    elif corruption == "crc":
        raw = raw[:-8] + bytes([raw[-8] ^ 1]) + raw[-7:]
    elif corruption == "sha":
        receipt["raw_sha256"] = "0" * 64
    elif corruption == "short":
        receipt["raw_bytes"] -= 1
    elif corruption == "long":
        receipt["raw_bytes"] += 1
    packed.write_bytes(raw)
    receipt["compressed_bytes"] = len(raw)
    target = tmp_path / "restored"
    with pytest.raises((ValueError, trace.zlib.error)):
        trace.unpack_trace(packed, target, receipt)
    assert not target.exists()
    assert source.read_bytes() == b"sample\n" * 100


def test_ceiling_prevents_expansion(tmp_path):
    _, packed, receipt = fixture(tmp_path, b"A" * 10000)
    target = tmp_path / "restored"
    with pytest.raises(ValueError):
        trace.unpack_trace(packed, target, receipt, trace.Bounds(raw=100))
    assert not target.exists()


@pytest.mark.parametrize("bounds", [trace.Bounds(raw=1), trace.Bounds(compressed=1)])
def test_pack_bounds_keep_source_and_remove_output(tmp_path, bounds):
    source = tmp_path / "source"
    source.write_bytes(b"abcdefgh" * 100)
    target = tmp_path / "packed"
    with pytest.raises(ValueError):
        trace.pack_trace(source, target, bounds)
    assert not target.exists()
    assert source.read_bytes() == b"abcdefgh" * 100


@pytest.mark.parametrize("operation", ["pack", "unpack"])
def test_deadline_removes_owned_output(tmp_path, monkeypatch, operation):
    source, packed, receipt = fixture(tmp_path)
    times = iter([0, 26])
    monkeypatch.setattr(trace.time, "monotonic", lambda: next(times))
    target = tmp_path / "target"
    with pytest.raises(TimeoutError):
        if operation == "pack":
            trace.pack_trace(source, target)
        else:
            trace.unpack_trace(packed, target, receipt)
    assert not target.exists()


def test_modified_source_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"original")
    original = trace.unchanged

    def modified(path, stream, before):
        source.write_bytes(b"modified")
        original(path, stream, before)

    monkeypatch.setattr(trace, "unchanged", modified)
    target = tmp_path / "packed"
    with pytest.raises(ValueError, match="changed"):
        trace.pack_trace(source, target)
    assert not target.exists()
    assert source.read_bytes() == b"modified"


def test_nonregular_source_rejected(tmp_path):
    with pytest.raises(ValueError):
        trace.pack_trace(tmp_path, tmp_path / "target")


@pytest.mark.skipif(os.name == "nt", reason="Windows symlink privilege is not assumed")
def test_source_symlink_rejected(tmp_path):
    source, _, _ = fixture(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(source)
    with pytest.raises(ValueError):
        trace.pack_trace(link, tmp_path / "target")


@pytest.mark.parametrize("kwargs", [{"raw": 0}, {"chunk": 2**21}, {"seconds": 26},
                                     {"raw": 2**30}, {"compressed": 2**30}])
def test_policy_boundaries(kwargs):
    with pytest.raises(ValueError):
        trace.Bounds(**kwargs)
