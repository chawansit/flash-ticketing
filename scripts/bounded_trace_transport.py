"""ADR0240 byte-preserving local trace transport; does not authorize cloud actions."""
import gzip
import hashlib
import os
import stat
import time
import zlib
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Bounds:
    raw: int = 512 * 1024 * 1024
    compressed: int = 128 * 1024 * 1024
    seconds: float = 25.0
    chunk: int = 1024 * 1024

    def __post_init__(self):
        if any(type(x) is not int or x <= 0 for x in (self.raw, self.compressed, self.chunk)):
            raise ValueError("Positive byte limits required")
        if (not 0 < self.seconds <= 25 or self.chunk > 1024 * 1024
                or self.raw > 512 * 1024 * 1024 or self.compressed > 128 * 1024 * 1024):
            raise ValueError("Bounded transform deadline and buffer required")


DEFAULT_BOUNDS = Bounds()


def identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def open_source(path):
    path = Path(path)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("Regular non-symlink source required")
    stream = path.open("rb")
    if identity(os.fstat(stream.fileno())) != identity(before):
        stream.close()
        raise ValueError("Source changed before opening")
    return stream, before


class Output:
    def __init__(self, path):
        self.path = Path(path)
        self.stream = self.path.open("xb")
        self.owner = os.fstat(self.stream.fileno())
        try:
            os.chmod(self.path, 0o600)
        except BaseException:
            self.discard()
            raise

    def discard(self):
        self.stream.close()
        try:
            now = self.path.lstat()
            if (now.st_dev, now.st_ino) == (self.owner.st_dev, self.owner.st_ino):
                self.path.unlink()
        except FileNotFoundError:
            pass


def tick(deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError("Trace transform deadline exceeded")


def unchanged(path, stream, before):
    after = Path(path).lstat()
    if (identity(os.fstat(stream.fileno())) != identity(before) or identity(after) != identity(before)
            or after.st_ctime_ns != before.st_ctime_ns):
        raise ValueError("Source changed during transform")


def pack_trace(source, destination, bounds=DEFAULT_BOUNDS):
    deadline = time.monotonic() + bounds.seconds
    src, before = open_source(source)
    output = None
    try:
        if before.st_size > bounds.raw:
            raise ValueError("Raw trace exceeds ceiling")
        output = Output(destination)
        digest = hashlib.sha256()
        total = 0
        with gzip.GzipFile(filename="", mode="wb", fileobj=output.stream, mtime=0) as packed:
            while True:
                tick(deadline)
                data = src.read(bounds.chunk)
                if not data:
                    break
                total += len(data)
                if total > bounds.raw:
                    raise ValueError("Raw trace exceeds ceiling")
                digest.update(data)
                packed.write(data)
                if output.stream.tell() > bounds.compressed:
                    raise ValueError("Compressed trace exceeds ceiling")
        output.stream.flush()
        tick(deadline)
        unchanged(source, src, before)
        if total != before.st_size or output.stream.tell() > bounds.compressed:
            raise ValueError("Trace length or compressed ceiling mismatch")
        size = output.stream.tell()
        output.stream.close()
        return {"format": "gzip-single-member-v1", "raw_bytes": total,
                "compressed_bytes": size, "raw_sha256": digest.hexdigest(), "bounds": asdict(bounds)}
    except BaseException:
        if output is not None:
            output.discard()
        raise
    finally:
        src.close()


def unpack_trace(source, destination, receipt, bounds=DEFAULT_BOUNDS):
    expected = receipt.get("raw_bytes")
    packed_size = receipt.get("compressed_bytes")
    digest_text = receipt.get("raw_sha256", "")
    if (receipt.get("format") != "gzip-single-member-v1"
            or type(expected) is not int or not 0 <= expected <= bounds.raw
            or type(packed_size) is not int or not 0 < packed_size <= bounds.compressed
            or len(digest_text) != 64 or any(c not in "0123456789abcdef" for c in digest_text)):
        raise ValueError("Valid bounded trace receipt required")
    deadline = time.monotonic() + bounds.seconds
    src, before = open_source(source)
    output = None
    try:
        if before.st_size != packed_size:
            raise ValueError("Compressed length mismatch")
        output = Output(destination)
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        digest = hashlib.sha256()
        total = 0
        consumed = 0
        while True:
            tick(deadline)
            data = src.read(bounds.chunk)
            if not data:
                break
            consumed += len(data)
            if consumed > packed_size:
                raise ValueError("Compressed input grew beyond declared length")
            while data:
                tick(deadline)
                decoded = decoder.decompress(data, min(bounds.chunk, expected - total + 1))
                total += len(decoded)
                if total > expected:
                    raise ValueError("Expanded trace exceeds declared length")
                output.stream.write(decoded)
                digest.update(decoded)
                if decoder.unused_data:
                    raise ValueError("Trailing input or additional gzip member")
                data = decoder.unconsumed_tail
        if not decoder.eof or total != expected or digest.hexdigest() != digest_text:
            raise ValueError("Trace integrity mismatch")
        tick(deadline)
        unchanged(source, src, before)
        output.stream.close()
        return {"pass": True, "raw_bytes": total, "raw_sha256": digest.hexdigest()}
    except BaseException:
        if output is not None:
            output.discard()
        raise
    finally:
        src.close()
