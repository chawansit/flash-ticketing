import copy
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from worker_separation_runtime import observation_program
from worker_separation_snapshot import (
    _broker_inventory,
    broker_records,
    capture_runtime,
    runtime_semantic,
    verify_restored,
)


def fixture():
    spec=importlib.util.spec_from_file_location('broker_staging_fixture',ROOT/'tests/unit/test_worker_separation_lifecycle.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    model,rows,volumes,binds=module.original()
    broker=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']=='kafka')
    broker['Config']['Volumes']={}
    for index,destination in enumerate(('/etc/kafka/secrets','/mnt/shared/config')):
        name='retained-aux-'+str(index);broker['Config']['Volumes'][destination]={}
        broker['Mounts'].append({'Type':'volume','Name':name,'Source':'/var/lib/docker/volumes/'+name+'/_data',
                                'Destination':destination,'RW':True,'Mode':'','Propagation':''})
        volumes.append({'Name':name,'Driver':'local','Options':None})
    return model,rows,volumes,binds,broker


def test_snapshot_keeps_all_existing_volume_identities_external():
    model,rows,volumes,binds,_=fixture();saved=capture_runtime(model,rows,volumes,binds)
    assert len(broker_records(saved))==3
    mounts=saved['model']['services']['kafka']['volumes'];definitions=saved['model']['volumes']
    assert len(mounts)==3 and all(definitions[m['source']]['external'] is True for m in mounts)
    assert {definitions[m['source']]['name'] for m in mounts}=={v['Name'] for v in volumes}
    assert verify_restored(saved,{'primary':rows,'secondary':[]},volumes,binds)['broker_volume_retained']
    compile(observation_program(saved,True),'broker_observation','exec')
    assert "['docker','volume','inspect',*volume]" in observation_program(saved,True)


@pytest.mark.parametrize('change',['missing','extra','driver','options','undeclared','destination','readonly','duplicate'])
def test_unknown_or_replaced_auxiliary_volume_rejected(change):
    _,_,volumes,_,broker=fixture()
    if change=='missing':volumes.pop()
    elif change=='extra':volumes.append({'Name':'unexpected','Driver':'local','Options':None})
    elif change=='driver':volumes[-1]['Driver']='foreign'
    elif change=='options':volumes[-1]['Options']={'device':'foreign'}
    elif change=='undeclared':broker['Config']['Volumes']={}
    elif change=='destination':broker['Mounts'][-1]['Destination']='/foreign'
    elif change=='readonly':broker['Mounts'][-1]['RW']=False
    elif change=='duplicate':broker['Mounts'][-1]['Name']=broker['Mounts'][0]['Name']
    with pytest.raises(ValueError):_broker_inventory({'kafka':[broker]},volumes)


def test_auxiliary_volume_loss_prevents_restoration():
    model,rows,volumes,binds,_=fixture();saved=capture_runtime(model,rows,volumes,binds)
    with pytest.raises(ValueError):verify_restored(saved,{'primary':rows,'secondary':[]},volumes[:-1],binds)


def test_inherited_mode_normalization_preserves_effective_permissions():
    _,_,_,_,broker=fixture();original=runtime_semantic(broker)
    explicit=copy.deepcopy(broker)
    for mount in explicit['Mounts']:mount['Mode']='rw' if mount['RW'] else 'ro'
    assert runtime_semantic(explicit)==original
    explicit['Mounts'][-1]['RW']=False
    assert runtime_semantic(explicit)!=original


def test_original_load_balancer_nofile_limits_are_preserved():
    model,rows,volumes,binds,_=fixture()
    lb=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']=='load-balancer')
    lb['HostConfig']['Ulimits']=[{'Name':'nofile','Soft':65535,'Hard':65535}]
    saved=capture_runtime(model,rows,volumes,binds)
    assert saved['model']['services']['load-balancer']['ulimits']=={'nofile':{'soft':65535,'hard':65535}}
    assert verify_restored(saved,{'primary':rows,'secondary':[]},volumes,binds)['runtime_restored']


@pytest.mark.parametrize('role,soft',[('api',65535),('load-balancer',65534)])
def test_other_limit_changes_are_not_adopted(role,soft):
    model,rows,volumes,binds,_=fixture()
    target=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']==role)
    target['HostConfig']['Ulimits']=[{'Name':'nofile','Soft':soft,'Hard':65535}]
    with pytest.raises(ValueError):capture_runtime(model,rows,volumes,binds)


def test_planned_runtime_requires_exact_original_nofile_limits():
    spec=importlib.util.spec_from_file_location('broker_execution_fixture',ROOT/'tests/unit/test_worker_separation_execution.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    saved,rows,_,_=module.fixture();model=copy.deepcopy(saved['model'])
    model['services']['load-balancer']['ulimits']={'nofile':{'soft':65535,'hard':65535}}
    original=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']=='load-balancer')
    lb=module.rows_for(model,{'load-balancer':1})[0]
    lb['HostConfig']['LogConfig']=copy.deepcopy(original['HostConfig']['LogConfig'])
    if 'Healthcheck' in original['Config']:
        lb['Config']['Healthcheck']=copy.deepcopy(original['Config']['Healthcheck'])
    lb['HostConfig']['Ulimits']=[{'Name':'nofile','Soft':65535,'Hard':65535}]
    from worker_separation_execution import planned_row
    planned_row(lb,model['services']['load-balancer'],model,set())
    lb['HostConfig']['Ulimits'][0]['Soft']=65534
    with pytest.raises(ValueError):planned_row(lb,model['services']['load-balancer'],model,set())
