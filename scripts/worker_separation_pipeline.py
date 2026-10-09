"""ADR0184 sealed endpoint adapter; preserves the frozen financial/worker parsers."""
import copy
import io
import math
import re
from datetime import UTC, datetime
from urllib.parse import urlsplit
from urllib.request import urlopen

from worker_separation_inventory import digest

MAX_PAYLOAD = 4 * 2**20
MAX_SERIES = 4096


def install_adapter(module, inventory, *, approved_inventory_sha256, now=None, fetch=urlopen):
    now = now or datetime.now(UTC)
    stamp = datetime.fromisoformat(inventory.get('captured_at', ''))
    if (inventory.get('decision') != 'ADR0184' or inventory.get('schema') != 1
            or inventory.get('runtime_unchanged') is not True or stamp.tzinfo is None
            or not 0 <= (now - stamp).total_seconds() <= 120
            or not re.fullmatch(r'[0-9a-f]{64}', approved_inventory_sha256)
            or digest(inventory) != approved_inventory_sha256):
        raise ValueError('Fresh exact host-qualified inventory binding required')
    from worker_separation_inventory import cpu_spec
    from worker_separation_topology import INFRA, WORKERS
    for host in ('primary', 'secondary'):
        cpu_spec(inventory, host)
    entries = copy.deepcopy([r for r in inventory['containers'] if r['role'] not in set(INFRA) - {'api'}])
    by_id = {r['container_id']: r for r in entries}
    if len(by_id) != sum(WORKERS.values()) + 4 or len(by_id) != len(entries):
        raise ValueError('Every API/worker endpoint required')
    for entry in entries:
        url = urlsplit(entry.get('metrics_url', ''))
        ports = entry.get('ports', [])
        if (url.scheme != 'http' or url.path != '/metrics' or url.username or url.query or url.fragment
                or len(ports) != 1 or url.hostname != ports[0][2] or url.port != ports[0][3]
                or entry.get('metrics_container_port') != ports[0][0]
                or not re.fullmatch(r'[0-9a-f]{64}', entry.get('source_identity_sha256', ''))
                or isinstance(entry.get('process_start_time_seconds'), bool)
                or not isinstance(entry.get('process_start_time_seconds'), (int, float))
                or not math.isfinite(entry['process_start_time_seconds']) or entry['process_start_time_seconds'] <= 0):
            raise ValueError('Exact source-qualified private metrics identity required')
    previous, failed = {}, set()
    original_parser = module.parse_api_metrics
    def discover(host='api', port=8000):
        if host not in {'api', *WORKERS}:
            raise ValueError('Unknown worker discovery rejected')
        return sorted(cid for cid, entry in by_id.items()
                      if entry['role'] == host and entry['metrics_container_port'] == port)
    def metric_fetch(url, *, timeout=2):
        parsed = urlsplit(url)
        entry = by_id.get(parsed.hostname)
        if entry is None or parsed.port != entry['metrics_container_port'] or parsed.path != '/metrics':
            raise ValueError('Unknown metric endpoint rejected')
        cid = entry['container_id']
        if cid in failed:
            raise ValueError('Previously failed metric identity remains failed')
        try:
            with fetch(entry['metrics_url'], timeout=timeout) as response:
                raw = response.read(MAX_PAYLOAD + 1)
            if len(raw) > MAX_PAYLOAD:
                raise ValueError('Metrics payload exceeds bound')
            payload, counters, starts = raw.decode(), {}, []
            for line in payload.splitlines():
                if len(line) > 16384:
                    raise ValueError('Metric line exceeds bound')
                if not line or line.startswith('#'):
                    continue
                metric, value = line.rsplit(' ', 1)
                name = metric.split('{', 1)[0]
                if name == 'process_start_time_seconds':
                    starts.append(float(value))
                if name.startswith(('ticketing_payment_confirmation_', 'ticketing_payment_confirmations_total')):
                    number = float(value)
                    if not math.isfinite(number) or number < 0:
                        raise ValueError('Invalid confirmation metric')
                if name.endswith(('_total', '_sum', '_count', '_bucket')):
                    number = float(value)
                    if not math.isfinite(number) or number < 0 or metric in counters:
                        raise ValueError('Invalid cumulative metric')
                    counters[metric] = number
                    if len(counters) > MAX_SERIES:
                        raise ValueError('Cumulative series exceed bound')
            if entry['role'] == 'api':
                from observe_two_host_pipeline import admission_failure_metrics
                admission_failure_metrics(payload)
            if starts != [entry['process_start_time_seconds']]:
                raise ValueError('Metric process identity changed')
            if any(counters.get(k, -1) < v for k, v in previous.get(cid, {}).items()):
                raise ValueError('Counter reset or disappearing metric')
            previous[cid] = counters
            return io.BytesIO(raw)
        except Exception:
            failed.add(cid)
            raise
    def parse_metrics(payload):
        from observe_two_host_pipeline import extra_api_metrics
        result = {**original_parser(payload), **extra_api_metrics(payload)}
        for line in payload.splitlines():
            if line.startswith(('ticketing_payment_confirmation_', 'ticketing_payment_confirmations_total')):
                name, value = line.rsplit(' ', 1)
                numeric = float(value)
                if not math.isfinite(numeric) or numeric < 0:
                    raise ValueError('Invalid confirmation metric')
                result['confirmation_metric:' + name] = numeric
        return result
    def api_metrics(label):
        from observe_two_host_pipeline import admission_failure_metrics
        if label not in by_id or by_id[label]['role'] != 'api':
            raise ValueError('Exact API metrics identity required')
        with metric_fetch('http://' + label + ':8000/metrics') as response:
            payload = response.read().decode()
        return {**parse_metrics(payload), **admission_failure_metrics(payload)}
    module.parse_api_metrics = parse_metrics
    module.api_replicas = discover
    module.api_metrics = api_metrics
    module.urlopen = metric_fetch
    module.METRICS['confirmation'] = ('confirm_one', 'http://confirmation:9101/metrics')
    # Frozen API metrics uses its own module-global opener, now routed through exact inventory.
    return by_id
