"""Compare old/new browse representation cost using isolated temporary Redis data.

This is a sequential component benchmark, not HTTP or production capacity.
"""
import argparse
import json
import statistics
import time
from pathlib import Path
from uuid import uuid4

from starlette.responses import JSONResponse, Response

from ticketing.infrastructure.cache import RedisSeats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.requests < 20:
        parser.error("At least 20 requests required")
    cache = RedisSeats(args.redis_url)
    event = str(uuid4())
    seats = [{"seat_id": f"A{i:03}", "price": 100, "status": "AVAILABLE",
              "reserved_until": None, "source_version": 0} for i in range(300)]
    results = []
    try:
        cache.put(event, 0, {"seats": seats})
        version = 0
        for repeat in range(3):
            # Alternate order to reduce a systematic first-run advantage.
            modes = ["baseline", "encoded"] if repeat % 2 == 0 else ["encoded", "baseline"]
            for mode in modes:
                cache.redis.delete(cache.key(event))
                cache.put(event, 0, {"seats": seats})
                version = 0
                samples, cpu_seconds, response_bytes = [], 0.0, 0
                status_counts = {}
                tag = None
                for index in range(args.requests):
                    if index % 20 == 0:
                        version += 1
                        cache.patch(event, [{**seats[0], "source_version": version,
                                             "status": "HELD" if version % 2 else "AVAILABLE"}])
                    # Independent cold-viewer reads alternate with matching revalidation.
                    validator = tag if index % 2 else None
                    start, cpu = time.perf_counter(), time.process_time()
                    if mode == "baseline":
                        status, tag, body = cache.browse(event, "availability", validator)
                        response = Response(status_code=304) if status == 304 else JSONResponse(body)
                    else:
                        status, tag, body = cache.browse_encoded(event, "availability", validator)
                        response = Response(status_code=304) if status == 304 else Response(
                            body, media_type="application/json")
                    cpu_seconds += time.process_time() - cpu
                    samples.append((time.perf_counter() - start) * 1000)
                    response_bytes += len(response.body)
                    status_counts[status] = status_counts.get(status, 0) + 1
                results.append({"mode": mode, "repeat": repeat, "requests": args.requests,
                                "mean_ms": statistics.mean(samples),
                                "p95_ms": sorted(samples)[int(len(samples) * .95) - 1],
                                "process_cpu_seconds": cpu_seconds, "response_bytes": response_bytes,
                                "statuses": status_counts})
    finally:
        cache.redis.delete(cache.key(event))
        cache.redis.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"results": results,
        "note": "Sequential component benchmark: one 300-seat event, update every 20 reads, "
                "50% client revalidation; excludes HTTP/auth/workers/DB and mutation time. "
                "Not production RPS or an admission rejection test."}, indent=2) + "\n")
    print(json.dumps(results))


if __name__ == "__main__":
    main()
