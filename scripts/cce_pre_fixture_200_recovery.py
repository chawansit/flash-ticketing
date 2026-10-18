"""ADR0360 exact no-dispatch retained-resource recovery; no quality relabeling."""
from datetime import UTC, datetime
from pathlib import Path

import work_envelope as policy

RUN='adr0151-d442b96c8c47'
LEDGER='bounded_cce_ticket_target__adeb90a54841'
RESULT='ea0b500838819f43f69f25f0559968587f819e7f71e8ebedfb8bbcbb2389918a'
GATES=('zero_performance_dispatch','paid_directory_absent','paid_receipt_absent',
       'paid_creation_intent_absent','namespace_removed','captured_owned_resources_removed',
       'helper_credentials_removed','native_configuration_removed','normal_runtime_stable',
       'generator_idle','queues_zero','original_failure_preserved')


def validate(raw,entry,scope,receipt):
    age=(datetime.now(UTC)-datetime.fromisoformat(receipt['verified_at_utc'])).total_seconds()
    if not all((
        raw.get('run')==RUN and policy.digest(raw)==RESULT and raw.get('pass') is False,
        entry.get('ledger')==LEDGER and entry.get('status')=='RECOVERY_REQUIRED',
        entry.get('result_sha256')==scope.get('cce_paid_result_sha256')==RESULT,
        raw.get('binding_sha256')==entry.get('binding_sha256')==policy.digest(scope.get('binding',{})),
        scope.get('paid_runs_started',0)==0 and raw.get('capacity_stages_started')==0,
        raw.get('paid_stage',{}).get('customers_dispatched') is False,
        raw.get('integrity_verified') is True and raw.get('transport_credentials_cleared') is True,
        raw.get('native',{}).get('cleanup_complete') is False,
        raw.get('native',{}).get('safety',{}).get('post_ttl_financial',{}).get('pass') is True,
        receipt.get('decision')=='ADR0360' and receipt.get('run')==RUN,
        receipt.get('ledger')==LEDGER and receipt.get('original_result_sha256')==RESULT,
        receipt.get('binding_sha256')==raw.get('binding_sha256'),
        receipt.get('pass') is True and receipt.get('capacity_qualified') is False,
        all(receipt.get(k) is True for k in GATES),
        0<=age<=900 and 0<receipt.get('elapsed_seconds',0)<=900,
    )):
        raise ValueError('Exact no-dispatch failed report and complete owned-resource recovery required')
    return True


def register(path):
    path=Path(path)
    if path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT/'docs/capacity/cce').resolve()):
        raise ValueError('Owned public cleanup receipt required')
    env=policy.envelope();journal=policy.journal(env);state=policy.read(policy.STATE)
    entry=journal['experiments'][-1]
    if policy.LOCK.exists() or state.get('current_run') is not None or state[LEDGER].get('active_run') is not None:
        raise ValueError('Stopped exact no-dispatch scope required')
    receipt=policy.read(path)
    validate(policy.read(policy.ROOT/'tmp'/RUN/'cce-comparison.private.json'),entry,state[LEDGER],receipt)
    entry.update(initial_status=entry['status'],status='FAILED_RESTORED',
                 recovery={'decision':'ADR0360','evidence':path.relative_to(policy.ROOT).as_posix(),
                           'sha256':policy.digest(receipt),'capacity_qualified':False})
    policy.write(policy.JOURNAL,journal)
    return {'status':'FAILED_RESTORED','owned_cleanup_verified':True,'original_failure_preserved':True}
