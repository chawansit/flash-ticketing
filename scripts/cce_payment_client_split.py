"""ADR0371 explicit redistribution within six clients and existing queue ceilings."""
import work_envelope as policy

KEY='payment_client_split_decision'
LOCAL='docs/capacity/cce/payment-client-split-local-2026-10-19.json'


def selected(goal):
    if goal is None or KEY not in goal:return False
    if (goal[KEY]!='ADR0371' or goal.get('fixed_worker_allocation_decision')!='ADR0359'
            or goal.get('persistent_uuid_decision')!='ADR0364'
            or goal.get('simulator_claim_batch_decision')!='ADR0368'
            or goal.get('guarded_issuance_decision')!='ADR0365'
            or goal.get('terminal_admission_decision')!='ADR0367'
            or goal.get('rate_qualification_decision')!='ADR0355'
            or type(goal.get('rate_qualification_rate')) is not int
            or goal['rate_qualification_rate'] not in (180,200)
            or goal.get('simulator_roundtrip_arm')!='candidate'):
        raise ValueError('Explicit fixed-resource payment client split required')
    return True


def payment_clients(goal):return 4 if selected(goal) else 3


def contract(goal):
    if not selected(goal):raise ValueError('Explicit payment client split required')
    import cce_simulator_claim_batch as image
    proof=policy.read(policy.ROOT/LOCAL)
    if (proof.get('decision')!='ADR0371' or proof.get('actual_image_pool_tests_pass') is not True
            or proof.get('owned_local_cleanup_pass') is not True
            or proof.get('old_burst_successes')!=20 or proof.get('new_burst_successes')!=21
            or proof.get('general_read_available') is not True
            or proof.get('image_manifest')!=image.MANIFEST
            or proof.get('source_identity_sha256')!=image.SOURCE):
        raise ValueError('Actual bounded pool comparison and read availability required')
    return {'decision':'ADR0371','baseline_api_client_slots':[3,3],'candidate_api_client_slots':[2,4],
            'clients_per_api':6,'aggregate_api_clients':24,'physical_backends':24,
            'shared_acquisition_limit':19,'shared_acquisition_ceiling':20,
            'native_role_waiters':{'general':15,'payment':17},'acquisition_wait_ms':500,
            'common_bootstrap_unchanged':True,'application_image_unchanged':True,
            'local_evidence_sha256':policy.digest(proof)}


def settings(values,goal):
    if not selected(goal):return values
    contract(goal)
    if values.get('DB_POOL_MAX')!='6' or values.get('API_PAYMENT_POOL_MAX')!='3':
        raise ValueError('Exact original six-client split required')
    return {**values,'API_PAYMENT_POOL_MAX':'4'}


def queue(values,goal,budget):
    if not selected(goal) or budget!=20:return values
    if values.get('DB_POOL_MAX')!='6' or values.get('API_PAYMENT_POOL_MAX')!='4' or values.get('DB_POOL_MAX_WAITING')!='20':
        raise ValueError('Exact candidate API queue and split required')
    return {**values,'DB_POOL_MAX_WAITING':'19'}


def normalize(values,goal):
    if not selected(goal):return values
    if (values.get('DB_POOL_MAX')!='6' or values.get('API_PAYMENT_POOL_MAX')!='4'
            or values.get('DB_POOL_MAX_WAITING') not in ('12','19')):
        raise ValueError('Exact bound client split required for semantic normalization')
    return {**values,'API_PAYMENT_POOL_MAX':'3',
            'DB_POOL_MAX_WAITING':'20' if values['DB_POOL_MAX_WAITING']=='19' else '12'}


def require_previous(goal,previous):
    if (selected(goal) and goal['rate_qualification_rate']==200
            and previous.get('payment_client_split')!=contract(goal)):
        raise ValueError('200requires clean180with identical payment client split')
