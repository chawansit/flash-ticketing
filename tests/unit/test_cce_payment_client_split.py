"""Exact configuration boundaries for the fixed six-client redistribution."""
import copy

import cce_payment_client_split as split
import cce_simulator_claim_batch as batch
import cce_simulator_roundtrip_profile as profile
import prepare_rate_qualification as preparer
import pytest
from test_cce_simulator_claim_batch import goal as previous_goal


def goal(rate=180):
    value=previous_goal(rate)
    value[split.KEY]='ADR0371'
    return preparer.goal_for(value,rate,value['rate_qualification_reference'],value['simulator_roundtrip_pair'])


def test_exact_split_and_semantic_normalization_preserve_legacy():
    value=goal()
    old={'DB_POOL_MAX':'6','API_PAYMENT_POOL_MAX':'3','DB_POOL_MAX_WAITING':'12'}
    assert split.settings(old,None)==old
    candidate=split.settings(old,value)
    assert candidate['API_PAYMENT_POOL_MAX']=='4'
    assert split.normalize(candidate,value)==old
    candidate=split.queue({**candidate,'DB_POOL_MAX_WAITING':'20'},value,20)
    assert candidate['DB_POOL_MAX_WAITING']=='19'
    assert split.normalize(candidate,value)=={**old,'DB_POOL_MAX_WAITING':'20'}
    assert batch.normalize_api_environment({**candidate,'PERSISTENT_UUID_V7':'1'},value)=={**old,'DB_POOL_MAX_WAITING':'20'}
    contract=profile.contract(value)
    assert contract['api_client_slots']==[2,4] and contract['shared_admission']==19
    assert contract['payment_client_split']['native_role_waiters']=={'general':15,'payment':17}
    assert contract['physical_backends']==24 and profile.selected(value)


@pytest.mark.parametrize('key',['fixed_worker_allocation_decision','persistent_uuid_decision','simulator_claim_batch_decision','guarded_issuance_decision','terminal_admission_decision'])
def test_orphan_selector_rejected(key):
    value=goal();value.pop(key)
    with pytest.raises(ValueError):split.contract(value)


@pytest.mark.parametrize(('key','bad'), [('DB_POOL_MAX','7'),('API_PAYMENT_POOL_MAX','5'),('DB_POOL_MAX_WAITING','20')])
def test_unbound_normalization_rejected(key,bad):
    candidate={'DB_POOL_MAX':'6','API_PAYMENT_POOL_MAX':'4','DB_POOL_MAX_WAITING':'19',key:bad}
    with pytest.raises(ValueError):split.normalize(candidate,goal())


def test_200_requires_identical_real_lower_rate_split():
    value=goal(200)
    prior=profile.contract(goal())
    split.require_previous(value,prior)
    broken=copy.deepcopy(prior);broken.pop('payment_client_split')
    with pytest.raises(ValueError):split.require_previous(value,broken)
    broken=copy.deepcopy(prior);broken['payment_client_split']['shared_acquisition_limit']=20
    with pytest.raises(ValueError):split.require_previous(value,broken)
