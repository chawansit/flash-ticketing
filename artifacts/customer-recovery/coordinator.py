"""Explicit common-start adapter for the frozen ADR0147 sharded generator."""

import argparse
import hashlib
import importlib.util
import math
import sys
from pathlib import Path
from time import time

FROZEN_SHA256 = "5e9983cd535025344ce35c5354b4389715a0c97e0c28c19fd5e1132978fbb931"


def load(path, start_at_epoch, *, now=None):
    now = time() if now is None else now
    if not math.isfinite(start_at_epoch) or not 3 <= start_at_epoch - now <= 60:
        raise ValueError("Common start must be between3 and60seconds ahead")
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(raw).hexdigest() != FROZEN_SHA256 or raw.count(b"start_at = time() + 8") != 1:
        raise ValueError("Frozen coordinator source/scheduling contract differs")
    spec = importlib.util.spec_from_file_location("frozen_sharded_generator", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # The frozen coordinator uses its imported time function once, solely for this start.
    module.time = lambda: start_at_epoch - 8
    return module


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--start-at-epoch", type=float, required=True)
    parser.add_argument("--frozen-generator", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    frozen = load(args.frozen_generator, args.start_at_epoch)
    sys.argv = [str(args.frozen_generator), *remaining]
    frozen.main()


if __name__ == "__main__":
    main()
