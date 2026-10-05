"""ADR0167 single-command local projection attribution; never target cloud services."""
import argparse
import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import redis
from redis import Redis
from redis.exceptions import RedisError

from ticketing.domain import Failure
from ticketing.infrastructure.cache import PUT, RedisSeats

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "sha256:bb186d083732f669da90be8b0f975a37812b15e913465bb14d845db72a4e3e08"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, timeout=20).strip()


def seat(index, version=0, status="AVAILABLE"):
    return {"seat_id": f"S{index}", "source_version": version, "version": version,
            "price": 100, "status": status, "reserved_until": None}


def phase(url, readers):
    cache = RedisSeats(url);diagnostic = Redis.from_url(url, decode_responses=True, socket_timeout=1)
    connection = cache.redis.connection_pool.make_connection()
    if connection.retry._retries != 0:raise ValueError("Measured adapter must not retry")
    event = str(uuid4());data = {"seats": [seat(i) for i in range(18000)]}
    result = {"seats": 18000, "readers": readers, "patch_writers": int(readers > 0), "publications": [],
              "reader_calls": 0, "reader_errors": {}, "reader_elapsed_ms": [], "patch_calls": 0, "patch_errors": {},
              "partial_maps": 0, "source_regressions": 0}
    stop = threading.Event();lock = threading.Lock();expected_source = 0; acknowledged_source = 0
    cache.put(event, 0, data)
    incarnation = diagnostic.hget(cache.key(event), "incarnation")
    diagnostic.slowlog_reset()
    barrier = threading.Barrier(readers + int(readers > 0) + 1)

    def read_load():
        barrier.wait(timeout=5);previous = 0
        for _ in range(50):
            if stop.is_set():break
            try:
                started = time.perf_counter();value = cache.read(event)
                with lock:
                    result["reader_calls"] += 1
                    result["reader_elapsed_ms"].append(1000*(time.perf_counter()-started))
                    result["partial_maps"] += int(len(value["seats"]) != 18000)
                    result["source_regressions"] += int(value["version"] < previous)
                previous = value["version"]
            except (RedisError, Failure, ValueError, KeyError, TypeError) as exc:
                with lock:
                    name = type(exc).__name__;result["reader_errors"][name] = result["reader_errors"].get(name, 0) + 1

    def patch_load():
        nonlocal expected_source, acknowledged_source
        barrier.wait(timeout=5)
        for version in range(1, 51):
            if stop.is_set():break
            try:
                accepted = cache.patch(event, [seat(0, version, "SOLD")])
                if accepted:
                    with lock:
                        result["patch_calls"] += 1
                        acknowledged_source = version
            except (RedisError, Failure, ValueError, KeyError, TypeError) as exc:
                with lock:
                    name = type(exc).__name__;result["patch_errors"][name] = result["patch_errors"].get(name, 0) + 1
            # Include ambiguous acknowledgement: the server may have committed this source version.
            expected_source = version
            time.sleep(0.01)

    try:
        with ThreadPoolExecutor(max_workers=max(1, readers + 1)) as pool:
            futures = [pool.submit(read_load) for _ in range(readers)]
            if readers:futures.append(pool.submit(patch_load))
            barrier.wait(timeout=5)
            try:
                for _ in range(6):
                    started = time.perf_counter();args = cache._full_arguments(event, data)
                    encoded = time.perf_counter();row = {"encoding_ms": 1000 * (encoded-started),
                              "argument_bytes": sum(len(str(a).encode()) for a in args),
                              "metadata_bytes": len(args[3].encode()), "layout_bytes": len(args[4].encode()),
                              "payload_bytes": len(args[-2].encode())}
                    try:
                        row["returned_version"] = cache.redis.eval(PUT, 2, *args);row["success"] = True
                    except (RedisError, Failure, ValueError, KeyError, TypeError) as exc:
                        row.update(success=False, failure_type=type(exc).__name__)
                    row["client_command_ms"] = 1000 * (time.perf_counter()-encoded)
                    result["publications"].append(row)
            finally:
                stop.set()
                for future in futures:future.result(timeout=5)
        entries = diagnostic.slowlog_get(4096)
        durations = []
        for entry in entries:
            command = entry["command"]
            if isinstance(command, str):command = command.encode()
            if b" full " in command and cache.key(event).encode() in command:
                durations.append(entry["duration"] / 1000)
        result["server_full_command_ms"] = durations
        raw = diagnostic.hgetall(cache.key(event));rows = [json.loads(v) for k,v in raw.items() if k.startswith("seat:")]
        result["stored_hash_data_bytes"] = sum(len(k.encode())+len(v.encode()) for k,v in raw.items())
        result["source_index_fields"] = sum(k.startswith("source:") for k in raw)
        current = next(row for row in rows if row["seat_id"] == "S0")
        result["audit"] = {"complete_seats": len(rows) == 18000, "publication_complete": "updating" not in raw,
                           "incarnation_stable": raw.get("incarnation") == incarnation,
                           "source_index_matches_rows": all(int(raw.get("source:"+r["seat_id"], r["source_version"])) == r["source_version"] for r in rows),
                           "final_source_version": current["source_version"], "last_attempted_source_version": expected_source,
                           "highest_acknowledged_patch_source": acknowledged_source,
                           "latest_acknowledged_patch_preserved": current["source_version"] >= acknowledged_source,
                           "aggregate_version_matches_rows": int(raw["version"]) == sum(r["source_version"] for r in rows),
                           "partial_maps": result["partial_maps"], "source_regressions": result["source_regressions"]}
        result["audit_pass"] = all(result["audit"][k] for k in ("complete_seats", "publication_complete", "incarnation_stable",
                                                   "latest_acknowledged_patch_preserved", "aggregate_version_matches_rows", "source_index_matches_rows"))
        result["audit_pass"] = result["audit_pass"] and result["partial_maps"] == result["source_regressions"] == 0
        return result
    finally:
        stop.set();diagnostic.delete(cache.key(event), cache.delta_key(event));diagnostic.close();cache.redis.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__);parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path)
    args = parser.parse_args();output = args.output.resolve()
    global PUT, RedisSeats
    source = ROOT / "src/ticketing/infrastructure/cache.py"
    if args.baseline_source:
        source = args.baseline_source.resolve()
        if not source.is_relative_to((ROOT / "tmp").resolve()) or source.is_symlink():
            raise ValueError("Baseline source must be an owned local file under tmp")
        spec = importlib.util.spec_from_file_location("projection_baseline", source)
        module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        PUT, RedisSeats = module.PUT, module.RedisSeats
    if not output.is_relative_to((ROOT / "tmp").resolve()) or output.exists() or output.is_symlink():
        raise ValueError("Fresh owned output under tmp required")
    if shutil.disk_usage(ROOT).free < 256 * 2**20:raise ValueError("Insufficient local workspace space")
    owner = "adr0167-projection-" + uuid4().hex[:12]
    report = {"decision": "ADR0167", "scope": "Isolated local Docker attribution, not cloud capacity",
              "cloud_requests": 0, "measured_socket_timeout_ms": 100, "diagnostic_read_timeout_ms": 1000,
              "redis_image": IMAGE, "redis_cpus": 1, "redis_memory_mib": 256,
              "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
              "baseline_source": bool(args.baseline_source), "redis_client_version": redis.__version__, "publication_retries": 0, "phases": [], "owned_resources_removed": False}
    cid = None;client = None
    try:
        assert json.loads(docker("image", "inspect", IMAGE))[0]["Id"] == IMAGE
        cid = docker("run", "-d", "--name", owner, "--label", "ticketing.projection.owner=" + owner,
                     "--cpus", "1", "--memory", "256m", "-p", "127.0.0.1::6379", IMAGE,
                     "redis-server", "--save", "", "--appendonly", "no", "--slowlog-log-slower-than", "0", "--slowlog-max-len", "4096")
        if not re.fullmatch(r"[0-9a-f]{64}", cid):raise ValueError("Exact container required")
        info = json.loads(docker("inspect", cid))[0]
        port = info["NetworkSettings"]["Ports"]["6379/tcp"][0]
        assert port["HostIp"] == "127.0.0.1"
        url = "redis://127.0.0.1:" + port["HostPort"] + "/0"
        client = Redis.from_url(url, socket_timeout=0.1, socket_connect_timeout=0.1)
        deadline = time.monotonic() + 10
        while True:
            try:
                if client.ping():break
            except Exception:
                if time.monotonic() >= deadline:raise
                time.sleep(0.05)
        for readers in (0,4):
            print(json.dumps({"phase":"projection-contention","readers":readers,"seats":18000}),flush=True)
            report["phases"].append(phase(url, readers))
        report["audit_pass"] = all(p["audit_pass"] for p in report["phases"])
        report["publication_timeouts"] = sum(not r["success"] for p in report["phases"] for r in p["publications"])
    except Exception as exc:  # noqa: BLE001 - persist unexpected diagnostic failure before exact cleanup.
        report["failure_type"] = type(exc).__name__
    finally:
        if client:client.close()
        if cid:
            info = json.loads(docker("inspect", cid))[0]
            if info["Image"] != IMAGE or info["Config"]["Labels"].get("ticketing.projection.owner") != owner:
                report["recovery_required"] = True
            else:
                docker("rm", "-f", "-v", cid);report["owned_resources_removed"] = True
        output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps({k:report.get(k) for k in ("audit_pass","publication_timeouts","owned_resources_removed","failure_type")}),flush=True)


if __name__ == "__main__":main()
