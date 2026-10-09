"""ADR0198 worker-specific wrapper around the unchanged frozen paid observer."""
import argparse
import copy
import json
import math
import os
import re
import sys
from pathlib import Path

from diagnostic_connection import install as install_connection
from diagnostic_connection import load_bundle, specification
from observe_two_host_pipeline import load_frozen, summarize_distribution
from worker_separation_inventory import digest
from worker_separation_pipeline import install_adapter

BINDING_KEYS = {'decision', 'database', 'identity_sha256', 'endpoint', 'bundle_sha256', 'ca_sha256'}
ROLE_COUNTS = {'writer': 3, 'maintenance': 1, 'reconciler': 1, 'simulator': 1,
               'publisher': 1, 'consumer': 6, 'confirmation': 1}


def diagnostic_binding(inventory, approved_sha256):
    """Validate exact public bindings before loading a protected specification."""
    binding = inventory.get('diagnostic_connection_binding')
    if (inventory.get('decision') != 'ADR0184' or digest(inventory) != approved_sha256
            or not isinstance(binding, dict) or set(binding) != BINDING_KEYS
            or binding['decision'] != 'ADR0180' or not isinstance(binding['database'], str) or not binding['database']
            or not isinstance(binding['endpoint'], list) or len(binding['endpoint']) != 2
            or any(not isinstance(binding[k], str) or not re.fullmatch(r'[0-9a-f]{64}', binding[k])
                   for k in ('identity_sha256','bundle_sha256','ca_sha256'))):
        raise ValueError('Exact worker diagnostic inventory binding required')
    return binding


def diagnostic_spec(inventory, *, inventory_path, bundle_path, approved_sha256, source_dsn):
    """Use the existing read-only full-visibility trust/account without changing other connections."""
    binding = diagnostic_binding(inventory, approved_sha256)
    from psycopg.conninfo import conninfo_to_dict
    if not source_dsn or conninfo_to_dict(source_dsn).get('dbname') != binding['database']:
        raise ValueError('Diagnostic and application databases must match')
    return load_bundle(bundle_path, owned_directory=Path(inventory_path).parent,
                       expected_sha256=binding['bundle_sha256'], expected_database=binding['database'],
                       expected_identity=binding['identity_sha256'], expected_endpoint=tuple(binding['endpoint']),
                       expected_ca_sha256=binding['ca_sha256'])


def configure(inventory, frozen_path, *, approved_sha256, diagnostic, source_dsn, now=None, fetch=None):
    """Private imported module; no process-global parser, driver or argv mutation."""
    frozen = load_frozen(Path(frozen_path))
    kwargs = {'approved_inventory_sha256':approved_sha256, 'now':now}
    if fetch is not None:kwargs['fetch'] = fetch
    install_adapter(frozen, inventory, **kwargs)
    binding = diagnostic_binding(inventory, approved_sha256)
    if (diagnostic is None
            or diagnostic.identity_sha256 != binding.get('identity_sha256')):
        raise ValueError('Qualified full-visibility diagnostic specification required')
    # Library entries retain the CLI endpoint, database, TLS and read-only options.
    from psycopg.conninfo import conninfo_to_dict
    parameters = dict(diagnostic.parameters)
    for key in ('autocommit', 'connect_timeout', 'prepare_threshold', 'options'):
        parameters.pop(key, None)
    if conninfo_to_dict(source_dsn).get('dbname') != binding.get('database'):
        raise ValueError('Diagnostic and application databases must match')
    checked = specification(parameters, expected_database=binding['database'],
                            expected_identity=binding['identity_sha256'],
                            ca_path=parameters.get('sslrootcert'), expected_endpoint=tuple(binding['endpoint']))
    if dict(checked.parameters) != dict(diagnostic.parameters):
        raise ValueError('Read-only diagnostic connection options differ')
    install_connection(frozen, source_dsn, diagnostic)
    from database_wait_evidence import install
    install(frozen)
    return frozen


def replica_coverage(row, inventory):
    """Require the exact bound process for every API and worker in each sample."""
    fields = {'api': 'api_replicas', **{role: role + '_db_replicas' for role in ROLE_COUNTS}}
    for role, field in fields.items():
        actual_role = 'reservation-writer' if role == 'writer' else role
        entries = [entry for entry in inventory['containers'] if entry['role'] == actual_role]
        metrics = row.get(field)
        if not isinstance(metrics, dict) or set(metrics) != {entry['container_id'] for entry in entries}:
            return False
        for entry in entries:
            value = metrics[entry['container_id']]
            if not isinstance(value, dict) or value.get('process_start_time_seconds') != entry['process_start_time_seconds']:
                return False
            cpu = value.get('process_cpu_seconds_total')
            if isinstance(cpu, bool) or not isinstance(cpu, (float, int)) or not math.isfinite(cpu) or cpu < 0:
                return False
    return True


def startup(row, inventory=None):
    """Every role, diagnostic collector and process must be present before customers."""
    from observe_paid_pipeline import pipeline_startup_view
    result = pipeline_startup_view(row, 6)
    workers = all(type(row.get(role+'_replicas')) is int and row[role+'_replicas'] == count
                  and isinstance(row.get(role+'_db_replicas'), dict)
                  and len(row[role+'_db_replicas']) == count for role, count in ROLE_COUNTS.items())
    diagnostic = row.get('database_wait_diagnostics', {}).get('complete') is True
    coverage = inventory is None or replica_coverage(row, inventory)
    result.update(all_workers_observed=workers, full_database_diagnostics=diagnostic, exact_process_coverage=coverage)
    result['pass'] = result['pass'] is True and workers and diagnostic and coverage and row.get('extended_diagnostics') is True
    return result


def distribution(rows, inventory, *, offered_start_utc, offered_end_utc):
    """Adapt identities only; preserve the existing strict offered-window gate."""
    apis = [entry for entry in inventory['containers'] if entry['role'] == 'api']
    if len(apis) != 4 or any(entry['host_role'] != 'primary' for entry in apis):
        raise ValueError('Four primary APIs required for fixed worker comparison')
    labels = {entry['container_id']:'primary:'+entry['container_id'] for entry in apis}
    if len(labels) != 4:raise ValueError('Distinct API identities required')
    adapted = []
    for row in rows:
        value = copy.deepcopy(row)
        metrics = row.get('api_replicas', {})
        if not isinstance(metrics, dict) or set(metrics) != set(labels):
            raise ValueError('Exact four primary replica metrics required')
        for entry in apis:
            if metrics[entry['container_id']].get('process_start_time_seconds') != entry['process_start_time_seconds']:
                raise ValueError('Offered-window process identity differs from inventory')
        value['api_replicas'] = {labels[cid]:metric for cid,metric in metrics.items()}
        adapted.append(value)
    return summarize_distribution(adapted, {'apis':apis}, offered_start_utc=offered_start_utc,
                                  offered_end_utc=offered_end_utc)


def summarize(rows, inventory, *, offered_start_utc, offered_end_utc):
    """Extend unchanged historical summaries to confirmation and exact process coverage."""
    from run_two_host_paid_comparison import pipeline_pass
    from summarize_paid_pipeline import summarize as summarize_pipeline
    from summarize_paid_pipeline import summarize_db_replicas
    result = summarize_pipeline(rows)
    confirmation = summarize_db_replicas(rows, 'confirmation_db_replicas')
    result['role_database']['confirmation'] = confirmation
    result['worker_replica_coverage'] = bool(rows) and all(startup(row, inventory)['pass'] for row in rows)
    result['distribution'] = distribution(rows, inventory, offered_start_utc=offered_start_utc,
                                          offered_end_utc=offered_end_utc)
    result['pass'] = (pipeline_pass(result) and result['worker_replica_coverage']
                      and confirmation['observed_replicas'] == 1
                      and not confirmation['counter_reset_detected']
                      and result['distribution']['per_replica_traffic_distribution'])
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--frozen-observer', type=Path, required=True)
    parser.add_argument('--approved-inventory-sha256', required=True)
    parser.add_argument('--diagnostic-connection-bundle', type=Path, required=True)
    args, remaining = parser.parse_known_args(argv)
    inventory = json.loads(args.inventory.read_text(encoding='utf-8'))
    source = os.environ.get('TEST_DATABASE_URL')
    spec = diagnostic_spec(inventory, inventory_path=args.inventory, bundle_path=args.diagnostic_connection_bundle,
                           approved_sha256=args.approved_inventory_sha256, source_dsn=source)
    frozen = configure(inventory, args.frozen_observer, approved_sha256=args.approved_inventory_sha256,
                       diagnostic=spec, source_dsn=source)
    previous = sys.argv
    try:
        sys.argv = [str(args.frozen_observer), *remaining]
        frozen.main()
    finally:sys.argv = previous


if __name__ == '__main__':
    main()
