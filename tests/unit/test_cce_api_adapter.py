"""ADR0228 offline adapter checks; Kubernetes and cloud traffic are simulated."""
import copy
import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_api_adapter as cce
import work_envelope as policy

RUN = 'adr0151-' + 'a' * 12
COMMAND = ['uvicorn', 'ticketing.api:app', '--host', '0.0.0.0', '--port', '8000', '--limit-concurrency', '256', '--timeout-keep-alive', '10']


def service():
    return {'image': cce.dependency.INDEX, 'volumes': [], 'command': COMMAND,
        'environment': {'DATABASE_URL': 'postgresql://actor:p%40ss@pgbouncer:5432/ticketing?sslmode=disable&application_name=test',
            'REDIS_URL': 'redis://:test@10.1.245.61:6379/0', 'JWT_SECRET': 'test-jwt', 'WEBHOOK_SECRET': 'test-webhook'}}


def manifests():
    return cce.objects(RUN, service(), '10.1.137.69', 'registry-user', 'registry-test-password')


def live_pod(index=0):
    expected = manifests()[index + 3]
    pod = copy.deepcopy(expected)
    pod['metadata']['uid'] = 'uid-' + str(index)
    pod['status'] = {'phase': 'Running', 'podIP': '10.2.240.' + str(index + 1),
        'conditions': [{'type': 'Ready', 'status': 'True'}],
        'containerStatuses': [{'name': 'api', 'restartCount': 0, 'ready': True,
            'imageID': cce.dependency.IMAGE, 'containerID': 'container-'+str(index), 'state': {'running': {'startedAt': '2026-10-08T11:00:00Z'}}}]}
    proof = {'run': RUN, 'pod_uid': pod['metadata']['uid'], 'sources': cce.contract()['api_sources'],
        'environment_sha256': expected['metadata']['annotations']['codex-environment-sha256'],
        'command_sha256': policy.digest(COMMAND)}
    return expected, pod, proof


def test_four_pods_share_exact_budget_without_literal_secrets():
    values = manifests()
    assert len(values) == 7
    assert [v['kind'] for v in values].count('Pod') == 4
    for pod in values[3:]:
        container = pod['spec']['containers'][0]
        assert container['image'] == cce.dependency.IMAGE
        assert container['resources'] == cce.contract()['resources']
        assert 'test-jwt' not in json.dumps(pod)
        assert 'test-webhook' not in json.dumps(pod)
        assert 'registry-test-password' not in json.dumps(pod)
        assert pod['spec']['imagePullSecrets'] == [{'name': 'swr-pull'}]
        assert pod['spec']['automountServiceAccountToken'] is False
        assert container['readinessProbe']['httpGet']['path'] == '/health/ready'
        compile(container['command'][3], '<cce-startup>', 'exec')


def test_database_route_preserves_authority_database_options_and_original_input():
    value = service()
    before = copy.deepcopy(value)
    env = cce.api_environment(value, '10.1.137.69')
    assert value == before
    db = urlsplit(env['DATABASE_URL'])
    old = urlsplit(value['environment']['DATABASE_URL'])
    assert (db.username, db.password, db.path, db.query) == (old.username, old.password, old.path, old.query)
    assert (db.hostname, db.port) == ('10.1.137.69', 6432)
    assert env['REDIS_URL'] == value['environment']['REDIS_URL']
    assert all(env[k] == v for k, v in cce.contract()['api_settings'].items())


@pytest.mark.parametrize('key,value', [('image','sha256:'+'b'*64), ('volumes',['/etc/ssl:/ssl'])])
def test_unqualified_image_or_mount_fails(key, value):
    data = service(); data[key] = value
    with pytest.raises(ValueError): cce.api_environment(data, '10.1.137.69')


@pytest.mark.parametrize('url', ['postgresql://u:p@10.1.228.129:5432/ticketing', 'postgresql://u:p@pgbouncer:6432/ticketing', 'postgresql://pgbouncer:5432/ticketing'])
def test_direct_database_and_wrong_pooler_routes_fail(url):
    data = service(); data['environment']['DATABASE_URL'] = url
    with pytest.raises(ValueError): cce.api_environment(data, '10.1.137.69')


@pytest.mark.parametrize('value', ['127.0.0.1', '0.0.0.0', '122.8.157.97', '169.254.0.1', '192.0.2.4', '::1'])
def test_only_vpc_endpoint_addresses(value):
    with pytest.raises(ValueError): cce.private_ip(value)


@pytest.mark.parametrize('field,value', [
    ('uid','replacement'), ('owner','another-run'), ('restart',1), ('restart',False),
    ('ready',False), ('image','docker-pullable://other@sha256:'+'c'*64), ('cpu','2'),
    ('command',['uvicorn','other:app']), ('sidecar',True), ('deleting',True), ('phase','Pending'),
    ('source',{}), ('proof_uid','replacement'), ('env_sha','b'*64), ('command_sha','b'*64), ('public_ip','122.8.1.2')])
def test_admission_identity_source_or_runtime_drift_fails(field, value):
    expected, pod, proof = live_pod()
    if field == 'uid': pod['metadata']['uid'] = value
    elif field == 'owner': pod['metadata']['labels']['codex-owner'] = value
    elif field == 'restart': pod['status']['containerStatuses'][0]['restartCount'] = value
    elif field == 'ready': pod['status']['containerStatuses'][0]['ready'] = value
    elif field == 'image': pod['status']['containerStatuses'][0]['imageID'] = value
    elif field == 'cpu': pod['spec']['containers'][0]['resources']['limits']['cpu'] = value
    elif field == 'command': pod['spec']['containers'][0]['command'] = value
    elif field == 'sidecar': pod['spec']['containers'].append({'name':'unexpected'})
    elif field == 'deleting': pod['metadata']['deletionTimestamp'] = '2026-10-08T11:00:01Z'
    elif field == 'phase': pod['status']['phase'] = value
    elif field == 'source': proof['sources'] = value
    elif field == 'proof_uid': proof['pod_uid'] = value
    elif field == 'env_sha': proof['environment_sha256'] = value
    elif field == 'command_sha': proof['command_sha256'] = value
    elif field == 'public_ip': pod['status']['podIP'] = value
    with pytest.raises(ValueError): cce.receipt(pod, expected, 'uid-0', RUN, proof)


def test_startup_log_is_single_bounded_and_bound():
    expected, _pod, proof = live_pod()
    raw = cce.PROOF_PREFIX + json.dumps(proof)
    sha = expected['metadata']['annotations']['codex-environment-sha256']
    assert cce.parse_startup(raw, sha, COMMAND) == proof
    for invalid in ('', raw+'\n'+raw, 'x'*65537):
        with pytest.raises(ValueError): cce.parse_startup(invalid, sha, COMMAND)


def receipts():
    result = []
    for i in range(4):
        expected, pod, proof = live_pod(i)
        result.append(cce.receipt(pod, expected, 'uid-'+str(i), RUN, proof))
    return result


def test_observer_discovers_cce_apis_but_leaves_worker_dns_unchanged():
    values = receipts()
    starts = {r['private_ipv4']: 100.0 for r in values}
    module = SimpleNamespace(api_replicas=lambda host, port: ['worker-'+host], api_metrics=lambda address: {'cpu':1})
    cce.install_observer(module, values, starts, lambda address: {'process_start_time_seconds':100.0})
    assert module.api_replicas() == sorted(starts)
    assert module.api_replicas('consumer',9101) == ['worker-consumer']
    assert module.api_metrics(next(iter(starts))) == {'cpu':1,'process_start_time_seconds':100.0}


def test_process_restart_fails_observer_instead_of_counting_reset_counters():
    values = receipts(); starts = {r['private_ipv4']:100.0 for r in values}
    module = SimpleNamespace(api_replicas=lambda host, port: [], api_metrics=lambda address: {})
    cce.install_observer(module, values, starts, lambda address: {'process_start_time_seconds':101.0})
    with pytest.raises(ValueError): module.api_metrics(next(iter(starts)))


@pytest.mark.parametrize('fault', ['missing','duplicate_uid','duplicate_ip','duplicate_name'])
def test_all_four_endpoints_are_required(fault):
    values = receipts()
    if fault == 'missing': values.pop()
    else: values[1][{'duplicate_uid':'pod_uid','duplicate_ip':'private_ipv4','duplicate_name':'pod_name'}[fault]] = values[0][{'duplicate_uid':'pod_uid','duplicate_ip':'private_ipv4','duplicate_name':'pod_name'}[fault]]
    with pytest.raises(ValueError): cce.endpoints(values)


@pytest.fixture
def lifecycle(monkeypatch):
    monkeypatch.setattr(cce, "admission_budget", lambda:12)
    # Synthetic qualification only; never modifies the on-disk registry or cloud.
    monkeypatch.setattr(policy, 'PROFILES', {**policy.PROFILES, cce.PROFILE:('bounded_cce_paid_comparison','ADR0228')})
    envelope = copy.deepcopy(policy.envelope()); envelope['qualified_profiles'].append(cce.PROFILE)
    monkeypatch.setattr(policy, 'envelope', lambda: envelope)
    monkeypatch.setattr(cce.dependency, 'authorized_today', lambda value: None)
    monkeypatch.setattr(cce, 'authorized_creation', lambda value: None)
    values = manifests(); events = []; stored = {}
    def request(method, path, body):
        events.append((method,path,body))
        if method == 'GET': return stored.get(path)
        if method == 'POST':
            item = copy.deepcopy(body); item['metadata']['uid'] = 'created-'+item['metadata']['name']
            stored[path+'/'+item['metadata']['name']] = item
            return item
        if method == 'DELETE': stored.pop(path); return {'deleted':True}
        raise AssertionError(method)
    guard = SimpleNamespace(key='bounded_cce_paid_comparison__'+'c'*12,
        binding={'cce_manifest_sha256':policy.digest(values)}, check=lambda seconds: None)
    persisted = []
    deployment = cce.Deployment(request,guard,persisted.append)
    return deployment, values, events, persisted, stored


def test_lifecycle_persists_intent_and_uids_before_cleanup(lifecycle):
    deployment, values, events, persisted, stored = lifecycle
    deployment.create(values)
    assert persisted[0]['namespace_creation_attempted'] is True
    assert len(deployment.pod_uids) == 4
    assert deployment.cleanup() == {'namespace_removed':True}
    deletion = next(e for e in events if e[0]=='DELETE')
    assert deletion[2]['preconditions']['uid'] == deployment.namespace_uid
    assert not stored.get(deployment.path())


def test_dependency_scope_cannot_create_paid_pods(lifecycle):
    deployment, values, events, _, _ = lifecycle
    deployment.guard.key = 'bounded_cce_dependency_probe__'+'c'*12
    with pytest.raises(ValueError): deployment.create(values)
    assert events == []
    assert deployment.attempted is False


def test_unregistered_paid_profile_cannot_mutate(monkeypatch):
    values = manifests()
    monkeypatch.setattr(policy, "PROFILES", {k:v for k,v in policy.PROFILES.items() if k != cce.PROFILE})
    events = []
    deployment = cce.Deployment(lambda *args: events.append(args),
        SimpleNamespace(key='bounded_cce_paid_comparison__'+'c'*12), lambda value: None)
    with pytest.raises(ValueError,match='not been registered'): deployment.create(values)
    assert events == []


@pytest.mark.parametrize('fault', ['uid','owner','missing_uid'])
def test_ambiguous_ownership_forbids_delete(lifecycle, fault):
    deployment, values, events, _, stored = lifecycle
    deployment.create(values)
    if fault == 'uid': stored[deployment.path()]['metadata']['uid'] = 'replacement'
    elif fault == 'owner': stored[deployment.path()]['metadata']['labels']['codex-owner'] = 'another-run'
    else: deployment.namespace_uid = None
    with pytest.raises(ValueError): deployment.cleanup()
    assert not any(e[0]=='DELETE' for e in events)


def test_partial_pod_creation_can_clean_exact_owned_namespace(lifecycle):
    deployment, values, _events, _, _ = lifecycle
    original = deployment.request
    def broken(method,path,body):
        if method == 'POST' and body['metadata']['name']=='api-2': raise TimeoutError('response lost')
        return original(method,path,body)
    deployment.request = broken
    with pytest.raises(TimeoutError): deployment.create(values)
    assert len(deployment.pod_uids) == 2
    assert deployment.cleanup()['namespace_removed'] is True


def test_cleanup_after_pause_and_spending_expiry_remains_available(lifecycle, monkeypatch):
    deployment, values, _, _, _ = lifecycle
    deployment.create(values)
    monkeypatch.setattr(cce.dependency,'authorized_today',lambda value: (_ for _ in ()).throw(ValueError('expired')))
    deployment.guard.check = lambda seconds: (_ for _ in ()).throw(TimeoutError('paused/expired'))
    assert deployment.cleanup()['namespace_removed'] is True


def test_scope_payload_drift_fails_before_creation(lifecycle):
    deployment, values, events, _, _ = lifecycle
    values[-1]['spec']['containers'][0]['resources']['limits']['cpu'] = '2'
    with pytest.raises(ValueError,match='binding'): deployment.create(values)
    assert events == []


@pytest.mark.parametrize('fault', ['none','rotated','forged_previous','missing','restart','source','environment','readiness','metrics','process_restart','container_replaced','replaced_after_admission'])
def test_all_pods_observed_before_admission(lifecycle, fault):
    deployment, values, _events, _persisted, _stored = lifecycle
    deployment.create(values)
    original = deployment.request
    pods, proofs = {}, {}
    for index in range(4):
        _expected, pod, proof = live_pod(index)
        pod['metadata']['uid'] = deployment.pod_uids[f'api-{index}']
        proof['pod_uid'] = pod['metadata']['uid']
        pods[f'api-{index}'], proofs[f'api-{index}'] = pod,proof
    def request(method,path,body):
        if '/pods/api-' in path:
            name=path.split('/pods/')[1].split('/')[0]
            if '/log?' in path:return 'recent requests only' if rotated[0] else cce.PROOF_PREFIX+json.dumps(proofs[name])
            return pods[name]
        return original(method,path,body)
    rotated=[False]
    deployment.request=request
    ready=lambda address:{'status':'ready'}
    metrics=lambda address:{'process_start_time_seconds':100.0,'process_cpu_seconds_total':0.1}
    baseline=deployment.observe(values,ready,metrics)
    assert len(baseline)==4
    if fault=='none':assert deployment.observe(values,ready,metrics,previous=baseline)==baseline;return
    if fault=='rotated':
        rotated[0]=True
        assert deployment.observe(values,ready,metrics,previous=baseline)==baseline
        with pytest.raises(ValueError):deployment.observe(values,ready,metrics)
        return
    if fault=='forged_previous':baseline[0]['startup_proof']['sources']={}
    elif fault=='process_restart':metrics=lambda address:{'process_start_time_seconds':101.,'process_cpu_seconds_total':.2}
    elif fault=='container_replaced':pods['api-2']['status']['containerStatuses'][0]['containerID']='replacement'
    elif fault=='missing':pods['api-2']=None
    elif fault=='restart':pods['api-2']['status']['containerStatuses'][0]['restartCount']=1
    elif fault=='source':proofs['api-2']['sources']={}
    elif fault=='environment':proofs['api-2']['environment_sha256']='b'*64
    elif fault=='readiness':ready=lambda address:{'status':'unavailable'}
    elif fault=='metrics':metrics=lambda address:{'process_cpu_seconds_total':0.1}
    elif fault=='replaced_after_admission':pods['api-2']['status']['containerStatuses'][0]['state']['running']['startedAt']='2026-10-08T11:00:20Z'
    with pytest.raises(ValueError):deployment.observe(values,ready,metrics,previous=baseline)


def test_successful_cleanup_is_idempotent(lifecycle):
    deployment, values, _events, _persisted, _stored = lifecycle
    deployment.create(values)
    assert deployment.cleanup()['namespace_removed'] is True
    assert deployment.cleanup()['namespace_removed'] is True


@pytest.mark.parametrize('path', ['/api/v1/nodes','/api/v1/namespaces/default',
    '/api/v1/namespaces/flash-cce-'+'a'*12+'/secrets',
    '/api/v1/namespaces/flash-cce-'+'a'*12+'/pods/api-0/exec'])
def test_transport_rejects_unowned_or_secret_reads(monkeypatch,path):
    monkeypatch.setattr(cce.dependency,'kube_material',lambda path:{'client.key':'private-test'})
    calls=[]
    transport=cce.KubernetesTransport(SimpleNamespace(call=lambda *a,**k:calls.append(a)),Path('unused'))
    with pytest.raises(ValueError):transport.request('GET',path)
    assert calls==[]


def test_transport_closes_secret_material_and_keeps_cleanup_narrow(monkeypatch):
    monkeypatch.setattr(cce.dependency,'kube_material',lambda path:{'client.key':'private-test'})
    calls=[]
    session=SimpleNamespace(cleanup_mode=True,call=lambda *a,**k:calls.append(a))
    transport=cce.KubernetesTransport(session,Path('unused'))
    path='/api/v1/namespaces/flash-cce-'+'a'*12
    for method,route,body in [('POST','/api/v1/namespaces',{}),('DELETE',path+'/pods/api-0',{'preconditions':{'uid':'u'}}),('DELETE',path,{})]:
        with pytest.raises(ValueError):transport.request(method,route,body)
    transport.close()
    with pytest.raises(ValueError):transport.request('GET',path)
    assert transport.material=={} and calls==[]


def test_transport_requires_profile_before_mutation(monkeypatch):
    monkeypatch.setattr(cce.dependency,'kube_material',lambda path:{'client.key':'private-test'})
    calls=[]
    session=SimpleNamespace(cleanup_mode=False,action_guard=None,call=lambda *a,**k:calls.append(a))
    transport=cce.KubernetesTransport(session,Path('unused'))
    with pytest.raises(ValueError):transport.request('POST','/api/v1/namespaces',{})
    assert calls==[]


def test_generated_tls_transport_is_syntax_valid_and_verifies_server():
    compile(cce.REQUEST,'<verified-cce-transport>','exec')
    assert 'ssl.create_default_context(cafile=' in cce.REQUEST
    assert 'load_cert_chain' in cce.REQUEST
    assert 'limit=8388608 if diagnostic else 65536 if worker_diagnostic else 1048576' in cce.REQUEST
    assert 'response.read(limit+1)' in cce.REQUEST
    assert 'TemporaryDirectory' in cce.REQUEST


@pytest.mark.parametrize('stamp,allowed', [('2026-10-08T14:00:00+00:00',True),
    ('2026-10-08T16:00:00+00:00',False),('2026-10-09T01:00:00+00:00',False)])
def test_creation_reserves_full_cleanup_window_before_spending_expiry(stamp,allowed):
    now=datetime.fromisoformat(stamp)
    envelope = copy.deepcopy(policy.envelope())
    envelope['spending']['temporary_cce_pilot_exception']['expires_at_bangkok'] = '2026-10-08T23:59:59+07:00'
    envelope['spending']['temporary_cce_pilot_exception'].update(boundary_mode='calendar_expiry', authorized_date_bangkok='2026-10-08', no_spending_cap_explicitly_authorized=True)
    if allowed:cce.authorized_creation(envelope,now)
    else:
        with pytest.raises(ValueError):cce.authorized_creation(envelope,now)


def test_failed_verification_context_does_not_publish_configuration_values():
    expected = {'metadata': {'name': 'api-0'}, 'spec': {
        'containers': [{'name': 'api', 'command': ['private-command-value'],
            'env': [{'name': 'PASSWORD', 'value': 'private-env-value'}],
            'resources': {'requests': {'cpu': '1', 'memory': '1Gi'}}}],
        'restartPolicy': 'Never', 'activeDeadlineSeconds': 3000,
        'automountServiceAccountToken': False, 'enableServiceLinks': False,
        'imagePullSecrets': [{'name': 'private-secret-name'}],
        'securityContext': {'runAsUser': 10001}}}
    pod = copy.deepcopy(expected)
    pod['spec']['containers'][0]['resources']['requests']['memory'] = '2Gi'
    pod['spec']['securityContext']['runAsNonRoot'] = True
    pod['status'] = {'phase': 'Running', 'containerStatuses': [{'name': 'api',
        'ready': True, 'restartCount': 0, 'imageID': 'containerd://sha256:example',
        'state': {'running': {'startedAt': 'time'}}}]}
    summary = cce.verification_summary(pod, expected)
    assert summary['different_container_fields'] == ['resources']
    assert summary['different_pod_fields'] == ['securityContext']
    assert summary['resources']['requests']['memory'] == '2Gi'
    text = json.dumps(summary)
    assert all(value not in text for value in ('private-command-value', 'private-env-value', 'private-secret-name'))


def test_exact_live_cce_defaulting_keeps_strict_receipt_valid():
    expected, pod, proof = live_pod()
    pod['spec']['containers'][0]['readinessProbe'] = {
        'httpGet': {'path': '/health/ready', 'port': 8000, 'scheme': 'HTTP'},
        'timeoutSeconds': 5, 'periodSeconds': 10, 'successThreshold': 1, 'failureThreshold': 1}
    pod['spec']['securityContext'] = {
        'runAsUser': 10001, 'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}}
    assert cce.receipt(pod, expected, pod['metadata']['uid'], RUN, proof)['pod_uid'] == pod['metadata']['uid']


@pytest.mark.parametrize('mutation', ['root_user', 'unconfined', 'probe_timeout', 'probe_path'])
def test_admission_correction_still_rejects_real_security_and_probe_changes(mutation):
    expected, pod, proof = live_pod()
    if mutation == 'root_user': pod['spec']['securityContext']['runAsUser'] = 0
    if mutation == 'unconfined': pod['spec']['securityContext']['seccompProfile']['type'] = 'Unconfined'
    if mutation == 'probe_timeout': pod['spec']['containers'][0]['readinessProbe']['timeoutSeconds'] = 100
    if mutation == 'probe_path': pod['spec']['containers'][0]['readinessProbe']['httpGet']['path'] = '/wrong'
    with pytest.raises(ValueError, match='admitted specification drift'):
        cce.receipt(pod, expected, pod['metadata']['uid'], RUN, proof)
