"""ADR0206 exact declared-range observation; no cloud actions."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from two_host_topology import bindings


def row():
    return {'HostConfig':{'PortBindings':{'8000/tcp':[{'HostIp':'10.0.0.1','HostPort':'8101-8104'}]}},
            'NetworkSettings':{'Ports':{'8000/tcp':[{'HostIp':'10.0.0.1','HostPort':'8103'}]}}}


def test_resolves_exact_actual_assignment():
    assert bindings(row())==[(8000,'tcp','10.0.0.1',8103)]


def test_legacy_fixed_binding_identity_is_unchanged():
    value=row();value['HostConfig']['PortBindings']['8000/tcp'][0]['HostPort']='8001'
    value.pop('NetworkSettings')
    assert bindings(value)==[(8000,'tcp','10.0.0.1',8001)]


@pytest.mark.parametrize('change',['missing','foreign','ambiguous','outside','invalid','duplicate','implicit','unbounded'])
def test_rejects_ambiguous_or_foreign_assignment(change):
    value=row();published=value['NetworkSettings']['Ports']['8000/tcp'];declared=value['HostConfig']['PortBindings']['8000/tcp']
    if change=='missing':published.clear()
    elif change=='foreign':published[0]['HostIp']='10.0.0.2'
    elif change=='ambiguous':published.append({'HostIp':'10.0.0.1','HostPort':'8104'})
    elif change=='outside':published[0]['HostPort']='8105'
    elif change=='invalid':published[0]['HostPort']='8101-8104'
    elif change=='duplicate':declared.append(copy.deepcopy(declared[0]))
    elif change=='implicit':declared[0]['HostIp']=''
    else:declared[0]['HostPort']='0-8104'
    with pytest.raises(ValueError):bindings(value)
