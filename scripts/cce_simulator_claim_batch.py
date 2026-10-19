"""ADR0368 exact source-bound callback claim batching qualification."""
import hashlib
import json
import re

import cce_persistent_uuid as parent
import work_envelope as policy

KEY='simulator_claim_batch_decision'
CONTRACT_KEY='simulator_claim_batch'
SOURCE='c1fe95e270e8ae1b723ecc8108d17f59583b557e14ab6ff095e70df6b09eb920'
MANIFEST='sha256:cb03defd3cced76e65ee6c353668f61d2f71cf612998382a05140c3e017d7c47'
CONFIG='sha256:165c6c24b844890bff808be7befececfdc327d4bea8b78255aa957e79e5f9348'
LOCAL='docs/capacity/cce/simulator-claim-progress-local-2026-10-19.json'
PUBLICATION='docs/capacity/cce/simulator-claim-progress-registry-2026-10-19.json'
MODULES={'src/ticketing/config.py','src/ticketing/workers.py'}


def selected(goal):
    if goal is None or KEY not in goal:return False
    import cce_terminal_admission as terminal
    if goal[KEY]!='ADR0368' or not parent.selected(goal) or not terminal.selected(goal):
        raise ValueError('Exact guarded UUID and terminal-observer claim batch required')
    return True


def image():
    before=parent.image()
    local,published=[policy.read(policy.ROOT/name) for name in (LOCAL,PUBLICATION)]
    sources=published.get('runtime_source_sha256',{})
    if (published.get('decision')!='ADR0368' or published.get('progress_correction_decision')!='ADR0370' or published.get('registry_manifest_digest')!=MANIFEST
            or published.get('registry_configuration_digest')!=CONFIG
            or published.get('local_image_id')!=MANIFEST or published.get('registry_pulled_image_id')!=MANIFEST
            or published.get('source_identity_sha256')!=SOURCE
            or published.get('registry_image')!='swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@'+MANIFEST
            or any(published.get(k) is not True for k in ('registry_published','permission_verified','registry_pulled_runtime_verified'))
            or published.get('validation')!={'runtime_modules_verified':22,'default_off':True,'claim_limit':16,'installed_module_matches':True,'progress_entrypoint_installed':True}
            or published.get('platform')!={'os':'linux','architecture':'amd64'} or published.get('runtime_user')!='ticketing'
            or set(sources)!=set(before['runtime_source_sha256'])
            or {k for k in sources if sources[k]!=before['runtime_source_sha256'][k]}!=MODULES
            or hashlib.sha256(json.dumps(sources,sort_keys=True).encode()).hexdigest()!=SOURCE
            or published.get('parent_source_identity_sha256')!=before['source_identity_sha256']
            or published.get('dependency_input_sha256')!=before['dependency_input_sha256']
            or local.get('source_identity_sha256')!=SOURCE or local.get('runtime_source_sha256')!=sources
            or local.get('progress_counter_regression_passed') is not True
            or local.get('integration_cases_passed')!=51 or local.get('owned_local_cleanup_pass') is not True
            or local.get('simulator_transaction_comparison',{}).get('control_transactions')!=32
            or local.get('simulator_transaction_comparison',{}).get('candidate_transactions')!=17
            or local.get('simulator_transaction_comparison',{}).get('all_callback_counts_verified') is not True):
        raise ValueError('Exact executed, published and pulled two-module claim candidate required')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',published.get('registry_configuration_digest','')):
        raise ValueError('Actual immutable configuration digest required')
    for name,key in [('prepare_simulator_claim_progress_image.py','preparation_recipe_sha256'),
                     ('simulator_claim_progress_overlay.py','overlay_sha256')]:
        current=hashlib.sha256((policy.ROOT/'scripts'/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
        if published.get(key)!=current or local.get(key)!=current:
            raise ValueError('Executed preparation recipe drift')
    return {**local,**published,'local_only':False,'runtime_sources_sha256':sources}


def contract(goal):
    if not selected(goal):raise ValueError('Explicit batch claim qualification required')
    value=image()
    return {'decision':'ADR0368','progress_correction_decision':'ADR0370','worker_progress_operation':'simulate_one','source':SOURCE,'manifest':MANIFEST,
            'configuration':value['registry_configuration_digest'],'parent_source':parent.SOURCE,
            'native_environment':{'PERSISTENT_UUID_V7':'1','SIMULATOR_BATCH_CLAIM':'simulator_only:1'},
            'maximum_delivery_slots':16,'simulator_clients':10,'lease_seconds':15,
            'acknowledgement_sql_unchanged':True,'callback_idempotency_unchanged':True,
            'no_delivery_group_barrier':True,'physical_backends':24,'aggregate_resources_unchanged':True,
            'publication_sha256':policy.digest(policy.read(policy.ROOT/PUBLICATION)),
            'local_evidence_sha256':policy.digest(policy.read(policy.ROOT/LOCAL))}


def environment(role,values,goal):
    if not selected(goal):return values
    if 'SIMULATOR_BATCH_CLAIM' in values:raise ValueError('Default-off original bootstrap required')
    result=parent.environment(role,values,goal)
    return {**result,'SIMULATOR_BATCH_CLAIM':'1'} if role=='simulator' else result


def api_environment(values,goal):return environment('api',values,goal)


def normalize_api_environment(values,goal):
    if 'SIMULATOR_BATCH_CLAIM' in values:raise ValueError('Claim batching belongs only to simulator')
    return parent.normalize_api_environment(values,goal)


def validate_previous(goal,previous):
    if not selected(goal):return
    if goal['rate_qualification_rate']==180:
        parent.validate_previous(goal,previous)
    else:
        from cce_fixed_worker_allocation import contract as allocation
        from cce_terminal_admission import contract as observer
        if (previous.get('source')!=SOURCE or previous.get(CONTRACT_KEY)!=contract(goal)
                or previous.get('worker_allocation')!=allocation(goal)
                or previous.get('terminal_admission')!=observer()):
            raise ValueError('200 requires actual clean180 with exact claim batch and observer mode')
