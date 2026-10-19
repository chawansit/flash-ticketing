"""ADR0367 bounded terminal log evidence; never a customer-success oracle."""
from slot_failure_evidence import validate_record

KEY = 'terminal_admission_decision'
SOURCE = 'one_second_metrics_and_terminal_redacted_logs'
MAXIMUM_BYTES = 64 * 1024 * 1024


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    import cce_guarded_issuance as guarded
    if goal[KEY] != 'ADR0367' or not guarded.selected(goal):
        raise ValueError('Explicit guarded fixed-resource terminal admission mode required')
    return True


def contract():
    return {'decision':'ADR0367','source':SOURCE,'lookback_seconds':600,
            'maximum_bytes_per_pod':MAXIMUM_BYTES,'maximum_records_per_pod':128,
            'maximum_log_requests':4,'during_offered_window_log_requests':0,
            'additional_database_connections':0,'missing_history_fails':True}


def bound(binding):
    value = binding.get('cce_simulator_roundtrip_contract',{}).get('terminal_admission')
    if value is None:
        return False
    if value != contract():
        raise ValueError('Exact source-bound terminal admission budget required')
    return True


def collect(deployment, receipts):
    value=deployment.admission_diagnostics(lookback_seconds=600,maximum_bytes=MAXIMUM_BYTES)
    identities={r['pod_name']:r['pod_uid'] for r in receipts}
    if (len(identities)!=4 or len(set(identities.values()))!=4
            or value.get('run')!=deployment.run or value.get('all_pods_captured') is not True
            or value.get('namespace_uid')!=deployment.namespace_uid
            or value.get('errors') or set(value.get('pods',{}))!=set(identities)):
        raise ValueError('All four owned terminal log identities required')
    records={}
    for name,row in value['pods'].items():
        if (row.get('pod_uid')!=identities[name] or row.get('schema_version')!=1
                or type(row.get('logs_bytes')) is not int or not 0<=row['logs_bytes']<MAXIMUM_BYTES
                or any(row.get(k) not in (False,0) for k in
                       ('byte_limit_reached','overflow_events','malformed_events'))
                or not isinstance(row.get('records'),list) or len(row['records'])>128
                or row.get('failure_events')!=len(row['records'])):
            raise ValueError('Complete bounded terminal admission records required')
        records[name]=[]
        for item in row['records']:
            record=validate_record(item.get('slot_ownership'))
            if item.get('role')!=record['role'] or item.get('reason')!=record['reason']:
                raise ValueError('Terminal slot and log identity disagree')
            records[name].append(record)
    return {'decision':'ADR0367','complete':True,'run':deployment.run,
            'pod_uids':identities,'records':records,'log_requests':4,
            'contract':contract()}


def endpoints(evidence, receipts):
    identities={r['pod_name']:r['pod_uid'] for r in receipts}
    if (not isinstance(evidence,dict) or evidence.get('decision')!='ADR0367'
            or evidence.get('complete') is not True or evidence.get('contract')!=contract()
            or evidence.get('log_requests')!=4 or evidence.get('pod_uids')!=identities
            or any(r.get('startup_proof',{}).get('run')!=evidence.get('run') for r in receipts)
            or set(evidence.get('records',{}))!=set(identities)
            or any(not isinstance(v,list) or len(v)>128 for v in evidence.get('records',{}).values())):
        raise ValueError('Exact independently captured terminal evidence required')
    return {r['private_ipv4']:evidence['records'][r['pod_name']] for r in receipts}
