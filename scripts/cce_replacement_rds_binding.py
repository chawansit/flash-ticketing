"""ADR0352 exact replacement-RDS preparation; historical permits stay unchanged."""
import copy
from urllib.parse import urlsplit, urlunsplit

import work_envelope as policy
from diagnostic_runner_connection import validate_target

DECISION = 'ADR0352'
OLD = 'tmp/adr0349-control-context-79d8adb2cdb5/snapshot.private.json'
NEW = 'tmp/adr0352-new-rds/snapshot.private.json'
OLD_SHA = 'fddaf15ffe0139197fcb8487affaddca111f098284626b13cc211aeda04c8e6d'
NEW_SHA = 'fd0090550f1905464f7a81ecabd9f5ac8ab34943b7dcf52ce0ac41c720bf2f6d'
TARGET = 'tmp/rds-switch-20261018/diagnostic-target.private.json'
RECEIPT = 'docs/capacity/cce/rds-endpoint-switch-2026-10-18.json'
HOST = '8e55812a91354a8dbbcc3ae8cc3b9e14in03.internal.ap-southeast-2.postgresql.rds.myhuaweicloud.com'
CA = '/root/rds-switch-20261018c/rds-ca-bundle.pem'
CA_SHA = '09ae697f83a10a275bf576bfb5a5ecea8d3d387217059f48e66b49b7622a2323'


def selected(goal):
    if goal is None or 'replacement_rds_decision' not in goal:
        return False
    if goal['replacement_rds_decision'] != DECISION:
        raise ValueError('Exact replacement RDS decision required')
    return True


def contract(goal):
    if not selected(goal) or goal.get('payment_slot_snapshot_path') != NEW:
        raise ValueError('Exact replacement RDS snapshot required')
    old = policy.read(policy.ROOT / OLD)
    new = policy.read(policy.ROOT / NEW)
    if policy.digest(old) != OLD_SHA or policy.digest(new) != NEW_SHA:
        raise ValueError('Replacement RDS snapshot identity changed')
    expected = copy.deepcopy(old)
    services = expected['saved']['model']['services']
    if services['pgbouncer']['environment']['DB_HOST'] != '10.1.228.129':
        raise ValueError('Original database authority differs')
    services['pgbouncer']['environment']['DB_HOST'] = HOST
    for role in ('pgbouncer', 'migrate'):
        volumes = services[role]['volumes']
        roots = [v for v in volumes if v['target'].endswith('rds-ca.pem')]
        if len(roots) != 1 or roots[0]['read_only'] is not True:
            raise ValueError('Exact read-only RDS certificate mount required')
        roots[0]['source'] = CA
    value = services['migrate']['environment']['DATABASE_URL']
    uri = urlsplit(value)
    if uri.hostname != '10.1.228.129':
        raise ValueError('Original migration endpoint differs')
    services['migrate']['environment']['DATABASE_URL'] = urlunsplit(
        uri._replace(netloc=uri.netloc.replace('10.1.228.129', HOST)))
    expected['saved']['semantics']['pgbouncer'] = new['saved']['semantics']['pgbouncer']
    if expected != new:
        raise ValueError('Replacement changed unrelated runtime settings')
    target = validate_target(policy.read(policy.ROOT / TARGET))
    if target != {'host': '10.1.21.48', 'port': 5432, 'dbname': 'ticketing', 'user': 'root',
                  'ca_source_path': CA, 'ca_sha256': CA_SHA}:
        raise ValueError('Replacement TLS diagnostic identity differs')
    receipt = policy.read(policy.ROOT / RECEIPT)
    if receipt.get('pass') is not True or receipt.get('new_endpoint') != target['host']:
        raise ValueError('Completed replacement RDS cutover required')
    return {'decision': DECISION, 'snapshot_sha256': NEW_SHA, 'prior_snapshot_sha256': OLD_SHA,
            'diagnostic_target_sha256': policy.digest(target), 'cutover_receipt_sha256': policy.digest(receipt),
            'restoration_endpoint': '10.1.21.48', 'tls_mode': 'verify-full'}


def validate_binding(goal, binding):
    value = contract(goal)
    if binding.get('diagnostic_target_sha256') != value['diagnostic_target_sha256']:
        raise ValueError('Exact replacement diagnostic target must be bound')
    return value
