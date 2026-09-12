from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from pathlib import Path as _Path
from urllib.error import URLError
from urllib.request import Request, urlopen

REPO_ROOT = _Path(__file__).resolve().parent.parent

PROJECT = os.environ.get('FLASH_TICKETING_COMPOSE_PROJECT', 'flash-ticketing')
COMPOSE = [
    'docker',
    'compose',
    '-p',
    PROJECT,
    '-f',
    'compose.yaml',
]
for _compose_file in [
    'compose.private.yaml',
    'compose.reconciler.yaml',
    'compose.ingress.yaml',
    'compose.ingress-benchmark.yaml',
]:
    if (REPO_ROOT / _compose_file).exists():
        COMPOSE.extend(['-f', _compose_file])

def run_compose(args, timeout: int | None = None):
    return subprocess.run(
        COMPOSE + args,
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        text=True,
    )

def readiness_wait(origin: str, timeout_seconds: int = 120) -> bool:
    request = Request(f"{origin}/health/ready")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with urlopen(request, timeout=2) as response:
                if response.getcode() == 200:
                    return True
        except (OSError, URLError):
            pass
        time.sleep(1)
    return False

def configure_api(api_workers: int, db_pool_max: int, override_path: Path, origin: str):
    override_path.write_text(
        f"""services:\n  api:\n    command: ['uvicorn', 'ticketing.api:app', '--host', '0.0.0.0', '--port', '8000', '--limit-concurrency', '256', '--timeout-keep-alive', '5', '--workers', '{api_workers}']\n    environment:\n      DB_POOL_MAX: '{db_pool_max}'\n""",
        encoding='utf-8',
    )
    result = run_compose(['-f', str(override_path), 'up', '-d', '--force-recreate', '--no-deps', 'api'], timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f'api reconfigure failed: {result.stdout}')
    if not readiness_wait(origin):
        raise RuntimeError('api readiness did not return 200')

def restore_api() -> None:
    run_compose(['up', '-d', '--force-recreate', '--no-deps', 'api'], timeout=180)

def run_verify(results_dir: Path, stage_dir: Path) -> dict:
    verify_output = stage_dir / 'verify.json'
    result = subprocess.run(
        [
            sys.executable,
            'scripts/verify_cloud_holds.py',
            '--results',
            str(results_dir),
            '--output',
            str(verify_output),
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if verify_output.exists():
        payload = json.loads(verify_output.read_text(encoding='utf-8'))
        payload['verify_exit_code'] = result.returncode
        return payload
    return {'verify_exit_code': result.returncode, 'durability_and_expiry_pass': False}

def run_warmup(manifest: Path, stage_dir: Path) -> dict:
    warm_output = stage_dir / 'warmup.json'
    manifest_payload = json.loads(manifest.read_text(encoding='utf-8'))
    shows = manifest_payload.get('show_ids', [])
    db_url = os.getenv('TEST_DATABASE_URL') or os.getenv('STAGE_DATABASE_URL')
    redis_url = os.getenv('TEST_REDIS_URL') or os.getenv('REDIS_URL') or 'redis://127.0.0.1:6379/0'
    if not db_url:
        error = {'pass': False, 'message': 'missing TEST_DATABASE_URL/STAGE_DATABASE_URL'}
        warm_output.write_text(json.dumps(error, indent=2) + '\n', encoding='utf-8')
        return error

    from ticketing.domain import Failure
    from ticketing.infrastructure.cache import RedisSeats
    from ticketing.infrastructure.postgres import Postgres
    from ticketing.workers import snapshot

    start = datetime.now(UTC).isoformat()
    cache = RedisSeats(redis_url)
    db = Postgres(db_url)
    already_ready = 0
    seeded = 0
    failures = []
    missing = 0
    for event_id in shows:
        try:
            cache.read(event_id)
            already_ready += 1
            continue
        except Failure as exc:
            if exc.code not in {'SEATMAP_WARMING', 'SEATMAP_UNAVAILABLE'}:
                failures.append({'event_id': event_id, 'failure': exc.code})
                continue
            missing += 1
            try:
                snapshot(db, cache, event_id)
                seeded += 1
            except Exception as exc:  # noqa: BLE001
                failures.append({'event_id': event_id, 'failure': type(exc).__name__})
        except Exception as exc:  # noqa: BLE001
            failures.append({'event_id': event_id, 'failure': type(exc).__name__})

    post_ready = 0
    for event_id in shows:
        try:
            cache.read(event_id)
            post_ready += 1
        except Exception as exc:  # noqa: BLE001
            failures.append({'event_id': event_id, 'failure': type(exc).__name__})

    result = {
        'utc': start,
        'show_count': len(shows),
        'already_ready': already_ready,
        'missing_or_stale': missing,
        'seeded': seeded,
        'post_ready': post_ready,
        'failures': failures,
    }
    result['pass'] = not failures and post_ready == len(shows) and len(shows) > 0
    db.close()
    cache.redis.close()
    warm_output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    return result


def run_loader(stage_dir: Path, manifest: Path, rate: int, seconds: int, workers: int, keepalive: float, mixed_hot: bool) -> dict:
    load_dir = stage_dir / 'load'
    load_dir.mkdir(parents=True, exist_ok=True)
    manifest_output = stage_dir / 'manifest.json'
    manifest_output.write_text(manifest.read_text(encoding='utf-8'), encoding='utf-8')
    worker_log = stage_dir / 'worker.log'
    with worker_log.open('w', encoding='utf-8') as handle:
        proc = subprocess.run(
            [
                sys.executable,
                'scripts/parallel_cloud_load.py',
                '--manifest',
                str(manifest_output),
                '--rate',
                str(rate),
                '--seconds',
                str(seconds),
                '--workers',
                str(workers),
                '--output',
                str(load_dir),
                '--keepalive-expiry',
                str(keepalive),
            ]
            + (['--mixed-hot-holds'] if mixed_hot else [])
            + ['--transport-diagnostics'],
            text=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if (load_dir / 'summary.json').exists():
        summary = json.loads((load_dir / 'summary.json').read_text(encoding='utf-8'))
    else:
        summary = {}
    summary['exit_code'] = proc.returncode
    return {'summary': summary, 'load_dir': str(load_dir), 'worker_log': str(worker_log)}

def run_observer(manifest: Path, seconds: int, stage_dir: Path) -> subprocess.Popen:
    output = stage_dir / 'observer.json'
    return subprocess.Popen(
        [
            sys.executable,
            'scripts/cloud_benchmark_observe.py',
            '--fixtures',
            str(manifest),
            '--seconds',
            str(seconds),
            '--output',
            str(output),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

def _metric_delta(samples: list[dict], prefix: str) -> tuple[float, float]:
    if not samples:
        return 0.0, 0.0
    first, last = samples[0], samples[-1]
    delta_sum = 0.0
    metric_count = 0
    for key, last_value in last.get('api_metrics', {}).items():
        if not key.startswith(prefix):
            continue
        first_value = first.get('api_metrics', {}).get(key, 0.0)
        delta = last_value - first_value
        if delta < 0:
            continue
        delta_sum += delta
        metric_count += 1
    return delta_sum, metric_count

def summarize_observer(observer_rows: list[dict]) -> dict:
    if not observer_rows:
        return {}
    return {
        'api_metric_row_count': len(observer_rows),
        'db_commit_delta_count': _metric_delta(observer_rows, 'ticketing_db_commit_seconds_count')[0],
        'db_commit_delta_sum': _metric_delta(observer_rows, 'ticketing_db_commit_seconds_sum')[0],
        'db_commit_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_commit_seconds_sum')[0] /
                          _metric_delta(observer_rows, 'ticketing_db_commit_seconds_count')[0] * 1000)
                          if _metric_delta(observer_rows, 'ticketing_db_commit_seconds_count')[0] else None,
        'db_rollback_delta_count': _metric_delta(observer_rows, 'ticketing_db_rollback_seconds_count')[0],
        'db_rollback_delta_sum': _metric_delta(observer_rows, 'ticketing_db_rollback_seconds_sum')[0],
        'db_rollback_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_rollback_seconds_sum')[0] /
                            _metric_delta(observer_rows, 'ticketing_db_rollback_seconds_count')[0] * 1000)
                            if _metric_delta(observer_rows, 'ticketing_db_rollback_seconds_count')[0] else None,
        'db_pool_return_delta_count': _metric_delta(observer_rows, 'ticketing_db_pool_return_seconds_count')[0],
        'db_pool_return_delta_sum': _metric_delta(observer_rows, 'ticketing_db_pool_return_seconds_sum')[0],
        'db_pool_return_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_pool_return_seconds_sum')[0] /
                               _metric_delta(observer_rows, 'ticketing_db_pool_return_seconds_count')[0] * 1000)
                               if _metric_delta(observer_rows, 'ticketing_db_pool_return_seconds_count')[0] else None,
        'db_pool_acquire_delta_count': _metric_delta(observer_rows, 'ticketing_db_pool_acquire_seconds_count')[0],
        'db_pool_acquire_delta_sum': _metric_delta(observer_rows, 'ticketing_db_pool_acquire_seconds_sum')[0],
        'db_pool_acquire_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_pool_acquire_seconds_sum')[0] /
                               _metric_delta(observer_rows, 'ticketing_db_pool_acquire_seconds_count')[0] * 1000)
                               if _metric_delta(observer_rows, 'ticketing_db_pool_acquire_seconds_count')[0] else None,
        'db_query_count': _metric_delta(observer_rows, 'ticketing_db_query_seconds_count')[0],
        'db_query_sum': _metric_delta(observer_rows, 'ticketing_db_query_seconds_sum')[0],
        'db_query_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_query_seconds_sum')[0] /
                          _metric_delta(observer_rows, 'ticketing_db_query_seconds_count')[0] * 1000)
                          if _metric_delta(observer_rows, 'ticketing_db_query_seconds_count')[0] else None,
        'db_transaction_body_sum': _metric_delta(observer_rows, 'ticketing_db_transaction_body_seconds_sum')[0],
        'db_transaction_body_count': _metric_delta(observer_rows, 'ticketing_db_transaction_body_seconds_count')[0],
        'db_transaction_body_avg_ms': (_metric_delta(observer_rows, 'ticketing_db_transaction_body_seconds_sum')[0] /
                                    _metric_delta(observer_rows, 'ticketing_db_transaction_body_seconds_count')[0] * 1000)
                                    if _metric_delta(observer_rows, 'ticketing_db_transaction_body_seconds_count')[0] else None,
        'worker_busy_delta_sum': _metric_delta(observer_rows, 'ticketing_worker_busy_seconds_total')[0],
        'worker_active_max': max((
            max((
                v for key, v in row.get('api_metrics', {}).items()
                if key.startswith('ticketing_worker_active')
            ), default=0.0)
            for row in observer_rows
        ), default=0.0),
        'http_close_delta': _metric_delta(observer_rows, 'ticketing_http_connection_close_total')[0],
        'lock_waiter_max': max((row.get('lock_waiters', 0) for row in observer_rows), default=0),
        'checkpoint_request_delta': max(
            observer_rows[-1].get('checkpoints_requested', 0) - observer_rows[0].get('checkpoints_requested', 0),
            0,
        ),
        'checkpoint_sync_ms_delta': max(
            observer_rows[-1].get('checkpoint_sync_ms', 0.0) - observer_rows[0].get('checkpoint_sync_ms', 0.0),
            0,
        ),
        'wal_sync_ms_delta': max(
            observer_rows[-1].get('wal_sync_ms', 0.0) - observer_rows[0].get('wal_sync_ms', 0.0),
            0,
        ),
        'wal_write_ms_delta': max(
            observer_rows[-1].get('wal_write_ms', 0.0) - observer_rows[0].get('wal_write_ms', 0.0),
            0,
        ),
        'minimum_ttl_min': min((row.get('minimum_ttl', 10**9) for row in observer_rows), default=None),
    }

def run_stage(
    stage_dir: Path,
    manifest: Path,
    rate: int,
    seconds: int,
    workers: int,
    keepalive: float,
    mixed_hot: bool,
) -> dict:
    stage_dir.mkdir(parents=True, exist_ok=True)
    warmup = run_warmup(manifest, stage_dir)
    if not warmup.get('pass', False):
        return {
            'start_utc': datetime.now(UTC).isoformat(),
            'rate': rate,
            'seconds': seconds,
            'workers': workers,
            'keepalive_expiry_seconds': keepalive,
            'warmup': warmup,
            'passed': False,
        }

    observer = run_observer(manifest, seconds + 36, stage_dir)
    start = datetime.now(UTC)
    try:
        loader_result = run_loader(stage_dir, manifest, rate, seconds, workers, keepalive, mixed_hot)
        observer.wait(timeout=seconds + 60)
    finally:
        if observer.poll() is None:
            observer.terminate()
            observer.wait(timeout=5)

    observer_path = stage_dir / 'observer.json'
    observer_rows = json.loads(observer_path.read_text(encoding='utf-8')) if observer_path.exists() else []
    verify = run_verify(Path(loader_result['load_dir']), stage_dir)
    transport_errors = loader_result['summary'].get('transport_errors', {}) if isinstance(loader_result['summary'], dict) else {}
    reset_like = sum(
        value
        for name, value in transport_errors.items()
        if 'Reset' in name or 'connection' in name.lower() or 'RemoteProtocolError' in name
    )
    passed = (
        loader_result['summary'].get('workload_gate_pass', False)
        and loader_result['summary'].get('exit_code', 1) == 0
        and verify.get('verify_exit_code', 1) == 0
        and verify.get('durability_and_expiry_pass', False)
        and reset_like == 0
    )

    return {
        'start_utc': start.isoformat(),
        'rate': rate,
        'seconds': seconds,
        'workers': workers,
        'keepalive_expiry_seconds': keepalive,
        'transport_error_types': transport_errors,
        'transport_reset_like_count': reset_like,
        'warmup': warmup,
        'observer_summary': summarize_observer(observer_rows),
        'loader': loader_result,
        'verify': verify,
        'passed': bool(passed),
        'load_dir': loader_result['load_dir'],
    }

def run_preflight(manifest: Path, seconds: int, stage_dir: Path) -> dict:
    preflight_output = stage_dir / 'preflight.json'
    stage_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            sys.executable,
            'scripts/cloud_load_preflight.py',
            '--manifest',
            str(manifest),
            '--seconds',
            str(seconds),
            '--output',
            str(preflight_output),
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if preflight_output.exists():
        payload = json.loads(preflight_output.read_text(encoding='utf-8'))
        payload['exit_code'] = proc.returncode
        return payload
    return {'exit_code': proc.returncode, 'pass': False}

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rates', nargs='+', type=int, default=[400, 500])
    parser.add_argument('--seconds', type=int, default=1800)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--keepalive-expiry', nargs='+', type=float, default=[5.0])
    parser.add_argument('--api-workers', nargs='+', type=int, default=[1, 2])
    parser.add_argument('--db-pool-max', type=int, default=12)
    parser.add_argument('--mixed-hot-holds', action='store_true')
    parser.add_argument('--fallback-rates', nargs='+', type=int, default=[200, 100])
    args = parser.parse_args()

    if args.output.exists():
        parser.error('Use a fresh output directory')
    if args.seconds < 1 or min(args.rates) < 1:
        parser.error('Positive seconds and rates required')

    manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
    if manifest.get('environment') != 'development':
        parser.error('Development fixture is required')
    origin = manifest['origin']

    args.output.mkdir(parents=True)

    blocks: list[dict] = []
    base_manifest_text = args.manifest.read_text(encoding='utf-8')

    with tempfile.TemporaryDirectory(prefix='flash-stability-') as scratch_root:
        override = Path(scratch_root) / 'api-workers.yaml'
        for api_workers in args.api_workers:
            block = {'api_workers': api_workers, 'rate_progress': []}
            try:
                configure_api(api_workers=api_workers, db_pool_max=args.db_pool_max, override_path=override, origin=origin)
            except Exception as exc:  # noqa: BLE001
                block['configure_error'] = repr(exc)
                blocks.append(block)
                continue

            for rate in args.rates:
                for keepalive in args.keepalive_expiry:
                    stage_dir = args.output / f'api{api_workers}-r{rate}-ka{str(keepalive).replace(".", "_")}'
                    stage_manifest = Path(scratch_root) / f'{manifest["id"]}-api{api_workers}-r{rate}.json'
                    if not stage_manifest.exists():
                        stage_text = json.loads(base_manifest_text)
                        stage_text['seat_offset'] = len(blocks) * 20
                        stage_manifest.write_text(json.dumps(stage_text), encoding='utf-8')

                    preflight = run_preflight(stage_manifest, args.seconds, stage_dir)
                    if preflight.get('exit_code') != 0 or not preflight.get('pass', False):
                        stage = {
                            'api_workers': api_workers,
                            'rate': rate,
                            'keepalive_expiry_seconds': keepalive,
                            'preflight': preflight,
                            'passed': False,
                        }
                        block['rate_progress'].append(stage)
                        break

                    stage = run_stage(
                        stage_dir=stage_dir,
                        manifest=stage_manifest,
                        rate=rate,
                        seconds=args.seconds,
                        workers=args.workers,
                        keepalive=keepalive,
                        mixed_hot=bool(args.mixed_hot_holds),
                    )
                    stage['api_workers'] = api_workers
                    stage['preflight'] = preflight
                    block['rate_progress'].append(stage)

                    if not stage.get('passed', False):
                        if rate != min(args.rates):
                            for fallback in args.fallback_rates:
                                if fallback >= rate:
                                    continue
                                fallback_dir = args.output / f'api{api_workers}-r{rate}-fallback{fallback}-ka{str(keepalive).replace(".", "_")}'
                                fallback_result = run_stage(
                                    stage_dir=fallback_dir,
                                    manifest=stage_manifest,
                                    rate=fallback,
                                    seconds=min(600, args.seconds),
                                    workers=args.workers,
                                    keepalive=keepalive,
                                    mixed_hot=bool(args.mixed_hot_holds),
                                )
                                fallback_result.update({'api_workers': api_workers, 'fallback_for': rate})
                                block['rate_progress'].append(fallback_result)
                        break

            blocks.append(block)
            restore_api()

    report = {
        'started_utc': datetime.now(UTC).isoformat(),
        'manifest_id': manifest.get('id'),
        'output_dir': str(args.output),
        'rates': args.rates,
        'seconds_per_rate': args.seconds,
        'generator_workers': args.workers,
        'api_workers': args.api_workers,
        'db_pool_max': args.db_pool_max,
        'keepalive_values': args.keepalive_expiry,
        'mixed_hot_holds': bool(args.mixed_hot_holds),
        'stages': blocks,
    }
    output_path = args.output / 'stability_stages.json'
    output_path.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    failed = [entry for entry in blocks if any(item.get('passed') is False for item in entry.get('rate_progress', []))]
    print(json.dumps({'stages_file': str(output_path), 'blocks': len(blocks), 'failed_blocks': len(failed)}), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
