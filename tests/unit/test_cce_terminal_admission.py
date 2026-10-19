"""Terminal records repair burst loss while every completeness gate stays strict."""
import copy
import json
from types import SimpleNamespace

import cce_api_adapter as api
import cce_metric_admission as metric
import cce_terminal_admission as terminal
import pytest
from test_cce_metric_admission import binding, samples
from test_cce_paid_lifecycle import area  # noqa: F401 - shared lifecycle composition fixture.


def burst():
    rows,receipts,start,end=samples()
    records=[{'sequence':i,'captured_at_unix_seconds':float(i),'role':'payment',
              'reason':'native_timeout','holders':[]} for i in range(1,31)]
    for index,row in enumerate(rows):
        for metrics in row['api_replicas'].values():
            count=30 if index>=10 else 0
            metrics['db_failure_diagnostics']={'schema_version':1,'failure_total':count,
                'overwritten_total':14 if count else 0,'diagnostic_errors':0,
                'complete':not count,'records':copy.deepcopy(records[-16:] if count else [])}
            metrics['acquisition_failure:payment:native_timeout']=count
    pods={r['pod_name']:{'pod_uid':r['pod_uid'],'schema_version':1,'logs_bytes':8192,
          'byte_limit_reached':False,'failure_events':30,'overflow_events':0,'malformed_events':0,
          'records':[{'role':'payment','reason':'native_timeout','slot_ownership':copy.deepcopy(v)} for v in records]}
          for r in receipts['receipts']}
    raw={'run':receipts['run'],'namespace_uid':'namespace','all_pods_captured':True,'errors':{},'pods':pods}
    calls=[]
    deployment=SimpleNamespace(run=receipts['run'],namespace_uid='namespace',
        admission_diagnostics=lambda **kwargs:calls.append(kwargs) or raw)
    return rows,receipts,start,end,deployment,raw,calls


def test_burst_lost_from_metrics_is_recovered_only_with_exact_terminal_records():
    rows,receipts,start,end,deployment,_,calls=burst()
    with pytest.raises(metric.CaptureError):metric.summarize(rows,receipts,start,end)
    evidence=terminal.collect(deployment,receipts['receipts'])
    result=metric.summarize(rows,receipts,start,end,terminal_evidence=evidence)
    assert result['complete'] and result['failure_total']==120
    assert result['additional_kubernetes_log_requests']==4
    selected={**binding(),'cce_simulator_roundtrip_contract':{'terminal_admission':terminal.contract()}}
    assert metric.validate_summary(result,selected,receipts['receipts'])
    assert calls==[{'lookback_seconds':600,'maximum_bytes':64*1024*1024}]


@pytest.mark.parametrize('fault',['missing_sequence','conflict','counter','process','receipt','run'])
def test_terminal_logs_cannot_excuse_missing_or_conflicting_evidence(fault):
    rows,receipts,start,end,deployment,_,_=burst()
    evidence=terminal.collect(deployment,receipts['receipts'])
    first=receipts['receipts'][0]
    if fault=='missing_sequence':evidence['records'][first['pod_name']].pop(0)
    elif fault=='conflict':evidence['records'][first['pod_name']][-1]['captured_at_unix_seconds']+=1
    elif fault=='counter':rows[-1]['api_replicas'][first['private_ipv4']]['acquisition_failure:payment:native_timeout']+=1
    elif fault=='process':rows[-1]['api_replicas'][first['private_ipv4']]['process_start_time_seconds']+=1
    elif fault=='receipt':evidence['pod_uids'][first['pod_name']]='foreign'
    else:evidence['run']='foreign'
    with pytest.raises(metric.CaptureError):metric.summarize(rows,receipts,start,end,terminal_evidence=evidence)


@pytest.mark.parametrize('fault',['pod','namespace','missing_pod','truncated','overflow','malformed','too_many','bytes'])
def test_incomplete_log_transport_is_rejected(fault):
    _,receipts,_,_,deployment,raw,_=burst()
    row=next(iter(raw['pods'].values()))
    if fault=='pod':row['pod_uid']='foreign'
    elif fault=='namespace':raw['namespace_uid']='foreign'
    elif fault=='missing_pod':raw['pods'].pop(next(iter(raw['pods'])))
    elif fault=='truncated':row['byte_limit_reached']=True
    elif fault=='overflow':row['overflow_events']=1
    elif fault=='malformed':row['malformed_events']=1
    elif fault=='bytes':row['logs_bytes']=terminal.MAXIMUM_BYTES
    else:row['records']*=5
    with pytest.raises(ValueError):terminal.collect(deployment,receipts['receipts'])


def test_bound_terminal_mode_cannot_accept_metric_only_summary():
    rows,receipts,start,end=samples()
    result=metric.summarize(rows,receipts,start,end)
    selected={**binding(),'cce_simulator_roundtrip_contract':{'terminal_admission':terminal.contract()}}
    with pytest.raises(ValueError):metric.validate_summary(result,selected,receipts['receipts'])
    assert metric.validate_summary(result,binding(),receipts['receipts'])


def test_host_filter_remains_redacted_and_uses_its_actual_byte_ceiling():
    namespace={};exec(api.DIAGNOSTIC_FILTER,namespace)  # noqa: S102 - Exercise the exact reviewed host filter.
    raw=(json.dumps({'event':'other','password':'private-test-value'})+'\n').encode()
    result=namespace['filtered_admission_logs'](raw,maximum_bytes=len(raw))
    assert result['byte_limit_reached'] and result['records']==[]
    assert 'private-test-value' not in json.dumps(result)


@pytest.mark.parametrize('truncated',[False,True])
def test_terminal_capture_follows_dispatch_and_never_skips_financial_cleanup(area,truncated):  # noqa: F811
    value,calls,_,_=area
    rows,receipts,start,end,_,raw,_=burst()
    value.stage.guard.binding={**binding(),'cce_simulator_roundtrip_contract':{'terminal_admission':terminal.contract()}}
    value.__init__(value.stage,value.resources,value.deployment,value.transition,value.observers,value.manifests,
                   qualify_safety=value.qualify_safety,cleanup_safety=value.cleanup_safety,
                   prepare_fixture=value.prepare_fixture,identities=value.identities,
                   background_before=value.background_before,persist=value.persist,
                   live_admission=True,metric_admission=True)
    value.deployment.run=receipts['run'];value.deployment.namespace_uid='namespace'
    if truncated:next(iter(raw['pods'].values()))['byte_limit_reached']=True
    def diagnostics(**kwargs):
        calls.append('terminal_logs')
        assert 'dispatch' in calls and 'collect' not in calls
        assert kwargs=={'lookback_seconds':600,'maximum_bytes':terminal.MAXIMUM_BYTES}
        return raw
    value.deployment.admission_diagnostics=diagnostics
    original=value.stage.dispatch
    def dispatch(*args,**kwargs):
        original(*args,**kwargs)
        value.stage.record['customer']={'pass':False}
        return value.stage.record['customer']
    value.stage.dispatch=dispatch
    def observe():
        calls.append('collect')
        if truncated:
            clean,r,a,b=samples();summary=metric.summarize(clean,r,a,b)
        else:
            summary=metric.summarize(rows,receipts,start,end,
                                     terminal_evidence=value.observers.terminal_admission_evidence)
        return {'observer_pass':True,'native_api_cpu':{'pass':True},'slot_failure_capture':summary}
    value.observers.collect=observe
    if truncated:
        with pytest.raises(ValueError):value.run(None,None,'audit')
    else:
        result=value.run(None,None,'audit')
        assert not result['pass'] and not result['measurement_gates']['customer_load']
        assert result['live_admission_complete']
    assert calls.index('dispatch')<calls.index('terminal_logs')<calls.index('collect')
    assert 'restore' in calls and 'stage_cleanup' in calls
    if not truncated:assert 'audit' in calls
