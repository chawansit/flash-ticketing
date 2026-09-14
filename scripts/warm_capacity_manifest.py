#!/usr/bin/env python3
"""Warm every manifest show through HTTP before measured load."""

import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import httpx


async def run(args: argparse.Namespace) -> int:
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    unresolved = set(range(len(manifest["show_ids"])))
    history = []
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=manifest["origin"], limits=limits, timeout=args.timeout) as client:
        for attempt in range(1, args.attempts + 1):
            semaphore = asyncio.Semaphore(args.concurrency)

            async def one(index: int, limiter: asyncio.Semaphore = semaphore) -> tuple[int, str]:
                async with limiter:
                    try:
                        response = await client.get(
                            f"/v1/events/{manifest['show_ids'][index]}/availability"
                        )
                        if response.status_code in (200, 304):
                            return index, "ok"
                        payload = response.json()
                        return index, f"{response.status_code}:{payload.get('code', 'unknown')}"
                    except Exception as exc:  # noqa: BLE001
                        return index, type(exc).__name__

            rows = await asyncio.gather(*(one(index) for index in unresolved))
            counts = Counter(code for _, code in rows)
            unresolved = {index for index, code in rows if code != "ok"}
            history.append({"attempt": attempt, "counts": dict(counts), "remaining": len(unresolved)})
            if not unresolved:
                break
            await asyncio.sleep(1)

    result = {
        "utc": datetime.now(UTC).isoformat(),
        "show_count": len(manifest["show_ids"]),
        "remaining": len(unresolved),
        "pass": not unresolved and bool(manifest["show_ids"]),
        "history": history,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    return 0 if result["pass"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    if args.output.exists() or min(args.attempts, args.concurrency, args.timeout) <= 0:
        parser.error("Use a fresh output and positive limits")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
