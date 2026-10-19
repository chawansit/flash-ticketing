"""ADR0365 live selection, authority and retained prerequisite boundaries."""
import copy
from uuid import UUID

import cce_guarded_issuance as guarded
import cce_rate_qualification as qualification
import cce_recovery_headroom as headroom
import cce_simulator_roundtrip_profile as profile
import prepare_rate_qualification as preparer
import pytest
import work_envelope as policy
from test_cce_persistent_uuid import goal as uuid_goal


def goal(rate=180):
    value=uuid_goal(rate)
    value[guarded.KEY]='ADR0365'
    return preparer.goal_for(value,rate,value['rate_qualification_reference'],value['simulator_roundtrip_pair'])


def test_actual_clean_parent_allows_fresh180_but_never_old180_for200():
    value=goal()
    assert profile.selected(value)
    assert qualification.require_authority(value)[0]['run']=='adr0151-6668a7e46ed3'
    assert profile.binding_contract(value)['guarded_issuance']==guarded.contract(value)
    assert qualification.contract(value)['single_changed_factor'] is None
    with pytest.raises(ValueError,match='fresh clean180'):
        qualification.require_previous(goal(200))


@pytest.mark.parametrize('rate',[180,200])
def test_actual_live_query_path_and_legacy_unchanged(rate):
    events=[str(UUID(int=i+1)) for i in range(rate)]
    source=headroom.issuance_program(events,1,goal(rate))
    assert f'{rate*300-600}<=row[0]<={rate*300} and {rate}*296<=row[4]<=row[0]' in source
    assert 'row[0]==row[1]==row[2]==row[3]' in source
    assert 'abs(row[5].timestamp()-time.time())<=2' in source
    compile(source,'live','exec')
    legacy=headroom.issuance_program(events,1,uuid_goal(rate))
    assert f'{rate*300-600}<=row[4]<=row[0]<={rate*300}' in legacy
    assert 'guarded_window_seconds' not in legacy


@pytest.mark.parametrize('key,value',[(guarded.KEY,'unknown'),('persistent_uuid_decision','unknown'),
                                   ('fixed_worker_allocation_decision','ADR0357'),
                                   ('rate_qualification_rate',180.0),('simulator_roundtrip_arm','control')])
def test_wrong_modes_rejected(key,value):
    selected=goal();selected[key]=value
    with pytest.raises(ValueError):guarded.contract(selected)


def test_unapproved_criteria_rejected(monkeypatch):
    value=goal();original=policy.read
    def read(path):
        result=original(path)
        if path==policy.ROOT/guarded.APPROVAL:
            result=copy.deepcopy(result);result['guarded_minima']=[53279,59199]
        return result
    monkeypatch.setattr(policy,'read',read)
    with pytest.raises(ValueError,match='approval'):guarded.contract(value)


def test_200_prerequisite_requires_exact_corrected_contract():
    value=goal(200);expected=guarded.contract(goal())
    guarded.require_previous(value,{'guarded_issuance':expected})
    for previous in ({},{'guarded_issuance':{**expected,'minimum_guarded_count':53279}}):
        with pytest.raises(ValueError):guarded.require_previous(value,previous)


def test_actual_changed_transferred_helper_path(monkeypatch,tmp_path):
    import test_cce_persistent_uuid as prior
    monkeypatch.setattr(prior,'goal',goal)
    prior.test_optional_exact_new_image_transferred_helpers(monkeypatch,tmp_path)
