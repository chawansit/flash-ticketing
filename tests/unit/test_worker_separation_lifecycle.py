"""Fault qualification only: simulated actions never access cloud or dispatch buyers."""
import copy
import importlib.util
import json
import sys
from functools import lru_cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import stage_status_refresh_images as staging
import worker_separation_lifecycle as lifecycle
import worker_separation_snapshot as snapshot
from worker_separation_inventory import digest


@lru_cache
def fixture_module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tests/unit' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def original():
    model, rows = fixture_module('test_two_host_deployment').fixture()
    env = ['KAFKA_LOG_DIRS=/var/lib/kafka/data', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092']
    broker = next(r for r in rows if r['Config']['Labels']['com.docker.compose.service'] == 'kafka')
    broker['Config']['Env'] = env
    broker['Mounts'] = [{'Type': 'volume', 'Name': 'existing-kafka-volume', 'Source': '/docker/volumes/existing',
                         'Destination': '/var/lib/kafka/data', 'RW': True}]
    for row in rows:
        row['HostConfig']['NetworkMode'] = 'flash-ticketing_default'
        row['HostConfig']['LogConfig'] = {'Type': 'json-file', 'Config': {'max-size': '10m', 'max-file': '3'}}
    volumes = [{'Name': 'existing-kafka-volume', 'Driver': 'local', 'Options': None,
                'Mountpoint': '/docker/volumes/existing', 'Labels': {'owned': 'historical'}}]
    hashes = {'/actual/runtime-nginx.conf': 'a' * 64}
    return model, rows, volumes, hashes


def pair():
    return fixture_module('test_worker_separation_observers').pair()


def saved():
    return snapshot.capture_runtime(*original())


def test_snapshot_retains_actual_volume_and_runtime_overrides_without_mutation():
    inputs = original()
    before = copy.deepcopy(inputs)
    result = snapshot.capture_runtime(*inputs)
    assert inputs == before
    assert result['counts'] == snapshot.NORMAL_COUNTS
    assert 'reservation-writer' not in result['model']['services']
    assert result['model']['volumes'] == {'owned-kafka-data': {'name': 'existing-kafka-volume', 'external': True}}
    assert result['model']['services']['kafka']['volumes'][0]['source'] == 'owned-kafka-data'
    assert result['model']['services']['api']['environment']['JWT_SECRET'] == 'a$literal#value'
    assert result['model']['services']['api']['logging']['options']['max-size'] == '10m'
    assert result['model']['services']['api']['command'] == ['uvicorn', 'ticketing.api:app']
    assert result['model']['services']['api']['entrypoint'] is None
    assert 'build' not in result['model']['services']['api']
    assert snapshot.verify_restored(result, {'primary': inputs[1], 'secondary': []}, inputs[2], inputs[3]) == {
        'runtime_restored': True, 'broker_volume_retained': True, 'bind_files_restored': True, 'secondary_empty': True}


@pytest.mark.parametrize('change', ['volume_name', 'volume_options', 'file_hash', 'secondary', 'image',
                                   'env', 'restart', 'health', 'network', 'stopped', 'count', 'logging'])
def test_restoration_rejects_identity_settings_and_persistence_drift(change):
    _, rows, volumes, hashes = original()
    before = saved()
    observed = {'primary': rows, 'secondary': []}
    if change == 'volume_name':
        volumes[0]['Name'] = 'replacement-volume'
    elif change == 'volume_options':
        volumes[0]['Labels']['owned'] = 'changed'
    elif change == 'file_hash':
        hashes[next(iter(hashes))] = 'b' * 64
    elif change == 'secondary':
        observed['secondary'] = [copy.deepcopy(rows[0])]
    elif change == 'image':
        rows[0]['Image'] = 'sha256:' + 'b' * 64
    elif change == 'env':
        rows[0]['Config']['Env'].append('UNPLANNED_OPTION=1')
    elif change == 'restart':
        rows[0]['HostConfig']['RestartPolicy']['Name'] = 'always'
    elif change == 'health':
        rows[0]['Config']['Healthcheck'] = {'Test': ['NONE']}
    elif change == 'network':
        rows[0]['HostConfig']['NetworkMode'] = 'host'
    elif change == 'stopped':
        rows[0]['State']['Running'] = False
    elif change == 'logging':
        rows[0]['HostConfig']['LogConfig']['Config']['max-size'] = '99m'
    else:
        rows.pop()
    with pytest.raises(ValueError):
        snapshot.verify_restored(before, observed, volumes, hashes)


@pytest.mark.parametrize('change', ['different_replicas', 'anonymous_volume', 'wrong_volume', 'missing_file',
                                   'extra_file', 'privileged', 'host_network', 'mutable_image', 'stopped'])
def test_snapshot_fails_before_unsafe_restore_model_is_created(change):
    model, rows, volumes, hashes = original()
    if change == 'different_replicas':
        rows[0]['Config']['Cmd'] = ['different']
    elif change == 'anonymous_volume':
        next(r for r in rows if r['Config']['Labels']['com.docker.compose.service'] == 'kafka')['Mounts'][0].pop('Name')
    elif change == 'wrong_volume':
        volumes[0]['Name'] = 'wrong'
    elif change == 'missing_file':
        hashes.clear()
    elif change == 'extra_file':
        hashes['/unowned.conf'] = 'b' * 64
    elif change == 'privileged':
        rows[0]['HostConfig']['Privileged'] = True
    elif change == 'host_network':
        rows[0]['HostConfig']['NetworkMode'] = 'host'
    elif change == 'mutable_image':
        rows[0]['Image'] = 'latest'
    else:
        rows[0]['State']['Running'] = False
    with pytest.raises(ValueError):
        snapshot.capture_runtime(model, rows, volumes, hashes)


def test_runtime_health_duration_restart_and_literal_defaults_are_bound():
    model, rows, volumes, hashes = original()
    for row in rows:
        row['Config']['Healthcheck'] = {'Test': ['CMD', 'check'], 'Interval': 1000000000, 'StartPeriod': 0, 'Retries': 3}
        row['HostConfig']['RestartPolicy'] = {'Name': 'on-failure', 'MaximumRetryCount': 5}
    result = snapshot.capture_runtime(model, rows, volumes, hashes)
    service = result['model']['services']['api']
    assert service['restart'] == 'on-failure:5'
    assert service['healthcheck'] == {'test': ['CMD', 'check'], 'interval': '1000000000ns', 'start_period': '0ns', 'retries': 3}


def images(model):
    return [{'Id': image, 'Config': {'Env': ['PATH=/image/bin', 'GPG_KEY=synthetic-key', 'DB_POOL_MAX=99'],
                                    'Cmd': ['image-default'], 'Entrypoint': ['image-entrypoint']}}
            for image in sorted({s['image'] for s in model['services'].values()})]


def test_image_binding_is_explicit_and_preserves_per_role_overrides():
    source = fixture_module('test_worker_separation_topology').model()
    before = copy.deepcopy(source)
    source['services']['consumer'].pop('command')
    bound = snapshot.bind_images(source, images(source))
    assert source['services']['consumer']['environment'] == before['services']['consumer']['environment']
    env = bound['services']['consumer']['environment']
    assert env['DB_POOL_MAX'] == '8' and env['GPG_KEY'] == 'synthetic-key'
    assert bound['services']['consumer']['command'] == ['image-default']
    assert bound['services']['consumer']['entrypoint'] == ['image-entrypoint']
    assert bound['services']['api']['command'] == before['services']['api']['command']


def test_explicit_entrypoint_and_empty_arrays_do_not_reintroduce_image_cmd():
    source = fixture_module('test_worker_separation_topology').model()
    source['services']['consumer'].update(entrypoint=['custom'], command=None)
    source['services']['publisher'].update(entrypoint=[], command=[])
    bound = snapshot.bind_images(source, images(source))
    assert bound['services']['consumer']['command'] == []
    assert bound['services']['consumer']['entrypoint'] == ['custom']
    assert bound['services']['publisher']['command'] == bound['services']['publisher']['entrypoint'] == []


@pytest.mark.parametrize('change', ['missing', 'extra', 'duplicate', 'mutable', 'duplicate_env', 'empty_env', 'unresolved'])
def test_image_binding_rejects_drift_and_unresolved_defaults(change):
    source = fixture_module('test_worker_separation_topology').model()
    records = images(source)
    if change == 'missing': records.pop()
    elif change == 'extra': records.append({'Id': 'sha256:' + 'f' * 64, 'Config': {}})
    elif change == 'duplicate': records.append(copy.deepcopy(records[0]))
    elif change == 'mutable': source['services']['api']['image'] = 'latest'
    elif change == 'duplicate_env': records[0]['Config']['Env'].append('PATH=second')
    elif change == 'empty_env': records[0]['Config']['Env'].append('=bad')
    else: source['services']['api']['environment']['UNRESOLVED'] = None
    with pytest.raises(ValueError): snapshot.bind_images(source, records)


class Adapter:
    offline_only = True
    def __init__(self, failures=(), invalid=(), pause=None):
        self.calls, self.failures, self.invalid, self.pause = [], set(failures), set(invalid), pause
    def check_forward(self, timeout):
        if self.pause == len(self.calls): raise TimeoutError('synthetic pause/deadline')
    def perform(self, request):
        key = (request['action'], request['cleanup'])
        self.calls.append(key)
        if key in self.failures: raise KeyboardInterrupt('secret must never appear in journal')
        steps = {s[0]: s for s in [*lifecycle.FORWARD, *lifecycle.CLEANUP, lifecycle.STOP, lifecycle.ABSENT,
                                   lifecycle.RESTORE, lifecycle.VERIFY, lifecycle.FILES, lifecycle.DRAIN]}
        result = {'action': request['action'], 'input_sha256': digest(request), 'pass': True,
                  'checks': {k: True for k in steps[request['action']][2]}}
        if key in self.invalid: result['input_sha256'] = 'f' * 64
        return result


def qualification(tmp_path, monkeypatch, adapter=None, arm='candidate', **kwargs):
    monkeypatch.setattr(staging, 'ROOT', tmp_path)
    output = staging.new_stage_output()
    return lifecycle.Qualification(pair(), arm, saved(), adapter or Adapter(), output, **kwargs)


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_successful_local_lifecycle_is_ordered_and_journaled(tmp_path, monkeypatch, arm):
    adapter = Adapter()
    engine = qualification(tmp_path, monkeypatch, adapter, arm)
    result = engine.run()
    assert result['pass'] and result['status'] == 'LOCALLY_QUALIFIED'
    assert result['cloud_deployments'] == result['customer_dispatches'] == 0
    forward = [name for name, cleanup in adapter.calls if not cleanup]
    assert forward == [s[0] for s in lifecycle.FORWARD]
    assert forward.index('verify_all_workers_absent') < forward.index('configure_common_infrastructure') < forward.index('start_arm_workers')
    cleanup = [name for name, clean in adapter.calls if clean]
    assert cleanup.index('audit_full_queue_drain') < cleanup.index('stop_arm_workers')
    assert cleanup.index('verify_all_workers_absent') < cleanup.index('restore_original_runtime')
    assert len(list(engine.journal.output.glob('*-intent.json'))) == len(adapter.calls)
    assert len(list(engine.journal.output.glob('*-ack.json'))) == len(adapter.calls)
    persisted = ''.join(p.read_text() for p in engine.journal.output.glob('*.json'))
    assert 'JWT_SECRET' not in persisted and 'literal#value' not in persisted and 'synthetic' not in persisted
    with pytest.raises(ValueError, match='replayed'): engine.run()


@pytest.mark.parametrize('failure', [s[0] for s in lifecycle.FORWARD])
def test_every_forward_failure_stops_progress_but_runs_all_mandatory_audits(tmp_path, monkeypatch, failure):
    adapter = Adapter(failures={(failure, False)})
    engine = qualification(tmp_path, monkeypatch, adapter)
    result = engine.run()
    forward = [name for name, cleanup in adapter.calls if not cleanup]
    assert forward[-1] == failure and len(forward) == [s[0] for s in lifecycle.FORWARD].index(failure) + 1
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)
    assert not result['pass'] and result['status'] == 'FAILED_RESTORED'
    assert 'secret' not in json.dumps(result)
    assert (('restore_original_runtime', True) in adapter.calls) == engine.runtime_attempted
    assert any(p.name.endswith('-intent.json') for p in engine.journal.output.iterdir())


@pytest.mark.parametrize('failure', [s[0] for s in [*lifecycle.CLEANUP, lifecycle.STOP, lifecycle.ABSENT,
                                                   lifecycle.RESTORE, lifecycle.VERIFY, lifecycle.FILES, lifecycle.DRAIN]])
def test_cleanup_failure_preserves_recovery_required_and_other_audits(tmp_path, monkeypatch, failure):
    adapter = Adapter(failures={(failure, True)})
    result = qualification(tmp_path, monkeypatch, adapter).run()
    assert result['status'] == 'RECOVERY_REQUIRED' and not result['pass']
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)
    if failure == 'verify_all_workers_absent':
        assert ('restore_original_runtime', True) not in adapter.calls
        assert ('cleanup_owned_private_files', True) not in adapter.calls
    assert ('verify_restored_queue_drain', True) in adapter.calls


def test_wrong_action_acknowledgement_is_not_replayed(tmp_path, monkeypatch):
    adapter = Adapter(invalid={('start_arm_workers', False)})
    result = qualification(tmp_path, monkeypatch, adapter).run()
    assert not result['pass'] and result['status'] == 'FAILED_RESTORED'
    assert ('verify_host_aware_inventory', False) not in adapter.calls
    assert adapter.calls.count(('start_arm_workers', False)) == 1


def test_pause_blocks_forward_but_does_not_block_owned_cleanup(tmp_path, monkeypatch):
    adapter = Adapter(pause=8)
    result = qualification(tmp_path, monkeypatch, adapter).run()
    assert not result['pass'] and ('configure_common_infrastructure', False) not in adapter.calls
    assert ('restore_original_runtime', True) in adapter.calls
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)


@pytest.mark.parametrize('kind', ['intent', 'ack', 'summary'])
def test_journal_failure_is_fail_closed_and_owned_cleanup_is_attempted(tmp_path, monkeypatch, kind):
    class BrokenJournal(lifecycle.Journal):
        def write(self, event, value):
            if event == kind and (kind == 'summary' or value.get('action') == 'start_arm_workers'):
                raise OSError('synthetic full disk')
            return super().write(event, value)
    adapter = Adapter()
    result = qualification(tmp_path, monkeypatch, adapter, journal_factory=BrokenJournal).run()
    assert not result['pass'] and result['status'] == 'RECOVERY_REQUIRED' and not result['journal_healthy']
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)
    if kind == 'intent': assert ('start_arm_workers', False) not in adapter.calls
    if kind != 'summary': assert ('verify_host_aware_inventory', False) not in adapter.calls
    assert ('restore_original_runtime', True) in adapter.calls


def test_unregistered_live_adapter_is_rejected_before_creating_journal(tmp_path, monkeypatch):
    adapter = Adapter()
    adapter.offline_only = False
    with pytest.raises(ValueError, match='offline'): qualification(tmp_path, monkeypatch, adapter)
    assert not list(tmp_path.rglob('*.json'))


def test_used_output_cannot_be_reopened_or_old_intent_overwritten(tmp_path, monkeypatch):
    engine = qualification(tmp_path, monkeypatch)
    before = {p.name: p.read_bytes() for p in engine.journal.output.iterdir()}
    with pytest.raises(ValueError, match='Fresh'): lifecycle.Journal(engine.journal.output, engine.binding)
    assert before == {p.name: p.read_bytes() for p in engine.journal.output.iterdir()}


@pytest.mark.parametrize('change', ['propagation', 'mode'])
def test_unsupported_bind_options_are_rejected(change):
    model, rows, volumes, hashes = original()
    mount = next(m for r in rows for m in r['Mounts'] if m['Type'] == 'bind')
    mount['Propagation' if change == 'propagation' else 'Mode'] = 'rshared' if change == 'propagation' else 'z'
    with pytest.raises(ValueError, match='propagation'): snapshot.capture_runtime(model, rows, volumes, hashes)


def test_control_failure_prevents_candidate_creation(tmp_path, monkeypatch):
    monkeypatch.setattr(staging, 'ROOT', tmp_path)
    created = []
    def factory(arm):
        created.append(arm)
        return Adapter(failures={('verify_callback_distribution', False)})
    result = lifecycle.qualify_pair(pair(), saved(), factory)
    assert not result['pass'] and created == ['control'] and set(result['arms']) == {'control'}
    assert result['arms']['control']['status'] == 'FAILED_RESTORED'


def test_pair_reuses_identical_snapshot_and_pair_bindings(tmp_path, monkeypatch):
    monkeypatch.setattr(staging, 'ROOT', tmp_path)
    original_pair, original_saved = pair(), saved()
    before = copy.deepcopy((original_pair, original_saved))
    result = lifecycle.qualify_pair(original_pair, original_saved, lambda arm: Adapter())
    assert result['pass'] and len(result['arms']) == 2
    assert (original_pair, original_saved) == before
    bindings = [json.loads(p.read_text()) for p in (tmp_path / 'tmp').glob('*/*-binding.json')]
    assert len(bindings) == 2
    assert all(b['offline_only'] is True for b in bindings)
    assert result['cloud_deployments'] == result['customer_dispatches'] == 0


def test_persistent_journal_failure_never_suppresses_mandatory_cleanup(tmp_path, monkeypatch):
    class BrokenJournal(lifecycle.Journal):
        def write(self, event, value):
            if self.sequence >= 15: raise OSError('disk unavailable')
            return super().write(event, value)
    adapter = Adapter()
    result = qualification(tmp_path, monkeypatch, adapter, journal_factory=BrokenJournal).run()
    assert result['status'] == 'RECOVERY_REQUIRED' and not result['journal_healthy']
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)
    assert ('stop_arm_workers', True) in adapter.calls
    assert ('verify_all_workers_absent', True) in adapter.calls
    assert ('restore_original_runtime', True) not in adapter.calls



def test_lost_mutation_response_is_not_retried_and_can_be_verified_restored(tmp_path, monkeypatch):
    class LostResponse(Adapter):
        def perform(self, request):
            if request['action'] == 'configure_common_infrastructure' and not request['cleanup']:
                self.calls.append((request['action'], False))
                raise OSError('lost transport response after mutation')
            return super().perform(request)
    adapter = LostResponse()
    result = qualification(tmp_path, monkeypatch, adapter).run()
    assert result['status'] == 'FAILED_RESTORED' and result['journal_healthy']
    assert adapter.calls.count(('configure_common_infrastructure', False)) == 1
    assert ('start_arm_workers', False) not in adapter.calls
    assert ('restore_original_runtime', True) in adapter.calls
    assert result['restoration_verified'] and not result['pass']

@pytest.mark.parametrize('failure', [s[0] for s in lifecycle.CLEANUP] + [lifecycle.STOP[0], lifecycle.DRAIN[0], 'verify_generator_idle'])
def test_recovery_files_survive_failed_mandatory_gate(tmp_path, monkeypatch, failure):
    adapter = Adapter(failures={(failure, True)})
    result = qualification(tmp_path, monkeypatch, adapter).run()
    assert result['status'] == 'RECOVERY_REQUIRED'
    assert ('cleanup_owned_private_files', True) not in adapter.calls
    assert all((s[0], True) in adapter.calls for s in lifecycle.CLEANUP)
    assert ('restore_original_runtime', True) in adapter.calls


def test_recovery_files_survive_journal_failure_after_verified_restoration(tmp_path, monkeypatch):
    class BrokenJournal(lifecycle.Journal):
        def write(self, event, value):
            if event == 'ack' and value.get('action') == 'restore_original_runtime':
                raise OSError('synthetic disk failure')
            return super().write(event, value)
    adapter = Adapter()
    result = qualification(tmp_path, monkeypatch, adapter, journal_factory=BrokenJournal).run()
    assert result['status'] == 'RECOVERY_REQUIRED'
    assert ('verify_exact_restoration', True) in adapter.calls
    assert ('cleanup_owned_private_files', True) not in adapter.calls


def test_image_user_directory_defaults_are_materialized_without_overwriting_overrides():
    source=fixture_module('test_worker_separation_topology').model();records=images(source)
    for row in records:row['Config'].update(User='ticketing',WorkingDir='/app')
    source['services']['api'].update(user='custom',working_dir='/custom')
    bound=snapshot.bind_images(source,records)
    assert bound['services']['consumer']['user']=='ticketing'
    assert bound['services']['consumer']['working_dir']=='/app'
    assert bound['services']['api']['user']=='custom' and bound['services']['api']['working_dir']=='/custom'
    assert 'user' not in source['services']['consumer']


def test_restore_snapshot_preserves_null_entrypoint():
    model,rows,volumes,hashes=original()
    for row in rows:row['Config']['Entrypoint']=None
    saved=snapshot.capture_runtime(model,rows,volumes,hashes)
    assert saved['model']['services']['api']['entrypoint'] is None
