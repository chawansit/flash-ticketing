import copy
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
from worker_separation_retained import capture_retained, program_prefix, row_digest, verify_retained
from worker_separation_runtime import observation_program
from worker_separation_staging import _runtime


def row(identity='a',project='flash-cloud-bench',role='api',running=False):
    return {'Id':identity*64,'Image':'sha256:'+'b'*64,
            'State':{'Running':running,'Status':'running' if running else 'exited','StartedAt':'unchanged'},
            'Config':{'Labels':{'com.docker.compose.project':project,'com.docker.compose.service':role}}}


def test_capture_preserves_exact_historical_resources():
    active=row('c','flash-ticketing','api',True)
    old=[row(),row('d','flash-ticketing','redis')]
    selected,receipt=capture_retained([active,*old])
    assert selected==[active] and receipt=={r['Id']:row_digest(r) for r in old}
    assert verify_retained([*old,active],receipt)==[active]


@pytest.mark.parametrize('change',['missing','image','labels','command','activated','status','new','duplicate'])
def test_retained_drift_never_becomes_managed_or_adopted(change):
    old=row();_,receipt=capture_retained([old]);rows=[copy.deepcopy(old)]
    if change=='missing':rows=[]
    elif change=='image':rows[0]['Image']='sha256:'+'d'*64
    elif change=='labels':rows[0]['Config']['Labels']['other']='changed'
    elif change=='command':rows[0]['Config']['Cmd']=['changed']
    elif change=='activated':rows[0]['State']['Running']=True
    elif change=='status':rows[0]['State']['Status']='created'
    elif change=='new':rows.append(row('e'))
    elif change=='duplicate':rows.append(copy.deepcopy(old))
    with pytest.raises(ValueError):verify_retained(rows,receipt)


@pytest.mark.parametrize('project,role,running',[('unowned','api',False),('other','api',False),
                                               ('flash-cloud-bench','api',True),('flash-ticketing','foreign',True)])
def test_unknown_resources_block_capture(project,role,running):
    with pytest.raises(ValueError):capture_retained([row(project=project,role=role,running=running)])


@pytest.mark.parametrize('role',['api','consumer','reservation-writer','confirmation','kafka','pgbouncer'])
def test_stopped_managed_duplicate_is_never_retained(role):
    old=row(project='flash-ticketing',role=role)
    selected,receipt=capture_retained([old])
    assert selected==[old] and receipt=={}
    with pytest.raises(ValueError):verify_retained([old],{old['Id']:row_digest(old)})


def test_legacy_snapshot_still_rejects_historical_inventory():
    with pytest.raises(ValueError):verify_retained([row()],{})


@pytest.mark.parametrize('receipt',[None,[],{'bad':'b'*64},{'a'*64:'bad'}])
def test_invalid_retained_receipt_rejected_before_remote_program(receipt):
    with pytest.raises(ValueError):program_prefix(receipt)


def test_self_contained_remote_helpers_recheck_full_inspections():
    old=row();receipt={old['Id']:row_digest(old)};namespace={}
    exec(compile(program_prefix(receipt),'retained_remote','exec'),namespace)  # noqa: S102 - test the generated bounded helper
    assert namespace['verify_retained']([old],receipt)==[]
    old['Config']['Labels']['changed']='yes'
    with pytest.raises(ValueError):namespace['verify_retained']([old],receipt)


def test_observation_verifies_retained_before_returning_runtime_rows():
    receipt={row()['Id']:row_digest(row())}
    saved={'broker_volume':{'Name':'existing-kafka','Driver':'local','Options':None},'bind_sha256':{},'retained_inactive_containers':receipt}
    code=observation_program(saved,True)
    compile(code,'observation','exec')
    assert 'rows=verify_retained(rows,retained_expected)' in code and repr(receipt) in code
    assert repr(receipt) not in observation_program(saved,False)


def test_staging_checks_saved_retained_identity(monkeypatch):
    import worker_separation_staging as staging
    from two_host_topology import NORMAL_COUNTS
    rows=[]
    for index,(role,count) in enumerate(NORMAL_COUNTS.items()):
        for replica in range(count):
            value=row();value['Id']=f'{index*10+replica+1:064x}'
            value['Config']['Labels'].update({'com.docker.compose.project':'flash-ticketing','com.docker.compose.service':role})
            value['State']['Running']=True;value['State']['Status']='running';rows.append(value)
    monkeypatch.setattr(staging,'runtime_semantic',lambda r:r)
    old=row();saved={'retained_inactive_containers':{old['Id']:row_digest(old)}}
    class Session:
        def call(self,host,code,timeout):
            if host=='generator':return {'generator_idle':True}
            return rows+[old] if host=='primary' else []
    signature=_runtime(Session(),saved=saved)
    assert len(signature['primary'])==sum(NORMAL_COUNTS.values())
    old['State']['Running']=True
    with pytest.raises(ValueError):_runtime(Session(),saved=saved)
    assert policy.digest(saved)


def test_mount_order_is_canonical_but_every_mount_field_remains_bound():
    old=row();old['Mounts']=[{'Type':'volume','Name':'data','Destination':'/data','RW':True},
                           {'Type':'bind','Source':'/root/config','Destination':'/config','RW':False}]
    _,receipt=capture_retained([old]);changed=copy.deepcopy(old);changed['Mounts'].reverse()
    assert verify_retained([changed],receipt)==[]
    changed['Mounts'][0]['Source']='/root/other'
    with pytest.raises(ValueError):verify_retained([changed],receipt)
