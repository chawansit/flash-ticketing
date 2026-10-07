"""ADR0198 worker diagnostic integration uses synthetic topology and transport."""
import copy
import hashlib
import importlib.util
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import stage_status_refresh_images as staging
import worker_separation_diagnostics as diagnostics
import worker_separation_inventory as inventory
import worker_separation_observer_bundle as bundle
from diagnostic_connection import identity
from diagnostic_runner_connection import ProtectedContext
from fetch_status_refresh_parents import owner_path

CERT = b'-----BEGIN CERTIFICATE-----\nsynthetic-local-test\n-----END CERTIFICATE-----\n'
SECRET = 'local-fixture-secret-never-published'
TARGET = {'host':'10.0.0.8','port':5432,'dbname':'ticketing','user':'root',
          'ca_source_path':'/root/owned/ca.pem','ca_sha256':hashlib.sha256(CERT).hexdigest()}


def setup(arm='candidate'):
    spec = importlib.util.spec_from_file_location('diagnostic_execution_fixture', ROOT / 'tests/unit/test_worker_separation_execution.py')
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    saved, _, _, inputs = module.fixture()
    fields = {'DB_HOST':TARGET['host'], 'DB_PORT':'5432','DB_NAME':'ticketing','DB_USER':'root',
              'SERVER_TLS_CA_FILE':'/etc/pgbouncer/rds-ca.pem','SERVER_TLS_SSLMODE':'verify-full'}
    mount = {'type':'bind','source':TARGET['ca_source_path'],'target':fields['SERVER_TLS_CA_FILE'],'read_only':True}
    saved['model']['services']['pgbouncer']['environment'].update(fields)
    saved['model']['services']['pgbouncer'].setdefault('volumes',[]).append(mount)
    saved['bind_sha256'][TARGET['ca_source_path']] = TARGET['ca_sha256']
    for a in ('control','candidate'):
        pg = inputs['pair'][a]['primary']['services']['pgbouncer']
        pg['environment'].update(fields);pg.setdefault('volumes',[]).append(copy.deepcopy(mount))
    pair = inputs['pair']
    rows = {host:module.rows_for(pair[arm][host],pair[arm]['counts'][host]) if pair[arm][host] else []
            for host in ('primary','secondary')}
    data = {'decision':'ADR0184','arm':arm,'prepared_pair_sha256':inventory.digest(pair),
            'containers':inventory.inspect_layout(pair,arm,rows)}
    sources = bundle.prepare()
    artifact_output = staging.new_stage_output()
    binding = {'worker_observer_owner_name':artifact_output.name, 'worker_observer_manifest_sha256':bundle.validate(sources),
               'worker_diagnostic_target_sha256':inventory.digest(TARGET)}
    events, uploads = [], []
    class Transport:
        def open(self, *args):return io.BytesIO(CERT)
        def close(self):return None
    class Session:
        cleanup_mode = False
        fail = None
        drift = False
        def __init__(self):
            self.config = {'primary':{'repo':'/qualification/repository'}}
            self.clients = {'primary':SimpleNamespace(open_sftp=Transport)}
            self.calls = []
        def api(self, cid, code, timeout):
            compile(code,'synthetic-remote','exec');assert SECRET not in code
            self.calls.append(('api',self.cleanup_mode,code))
            if 'observer_sources_verified' in code:
                return {'observer_sources_verified':True,'manifest_sha256':sources['manifest_sha256']}
            if 'one_data_connection' in code:
                if self.drift:rows['primary'][0]['State']['StartedAt'] = '2026-10-07T12:00:00+00:00'
                value = {'pass':True,'read_only_connection_verified':True,'verified_tls':True,'one_data_connection':True,
                         'identity_sha256':identity('root','ticketing'),'max_collection_ms':2}
                if self.fail:value[self.fail] = False
                return value
            if self.fail=='api_cleanup':raise KeyboardInterrupt('synthetic lost cleanup')
            return {'diagnostic_credentials_removed':True}
        def call(self, host, code, timeout):
            compile(code,'synthetic-host','exec');assert SECRET not in code
            self.calls.append(('call',self.cleanup_mode,code))
            if self.fail=='host_cleanup':raise TimeoutError('synthetic lost cleanup')
            return {'diagnostic_credentials_removed':True}
    session = Session()
    runtime = SimpleNamespace(guard=SimpleNamespace(binding=binding),output=staging.new_stage_output(),
                              _write=lambda k,v:events.append((k,copy.deepcopy(v))))
    engine = SimpleNamespace(session=session,runtime=runtime,pair=pair,saved=saved,arm=arm,
        configurations=SimpleNamespace(seals={'primary':{'owner':owner_path(session.config['primary']['repo'],runtime.output.name)}}),paused=False)
    approved = inventory.digest(binding)
    def guard(timeout,cleanup=False):
        if inventory.digest(runtime.guard.binding)!=approved:raise ValueError('Synthetic binding changed')
        if engine.paused and not cleanup:raise ValueError('Synthetic paused')
    engine._guard = guard
    engine._observe = lambda: {'primary':{'rows':copy.deepcopy(rows['primary']),'volumes':[saved['broker_volume']],
        'bind_sha256':saved['bind_sha256']},'secondary':{'rows':copy.deepcopy(rows['secondary']),'volumes':[],'bind_sha256':{}}}
    def upload(session,cid,owner,directory,name,value):uploads.append((name,value))
    cid = next(r['container_id'] for r in data['containers'] if r['role']=='api')
    component = diagnostics.DiagnosticActions(engine,data,sources,ProtectedContext(TARGET,SECRET),cid,artifact_output=artifact_output,upload=upload)
    return component,engine,events,uploads


@pytest.mark.parametrize('arm',['control','candidate'])
def test_bound_setup_keeps_secret_out_of_commands_journal_and_result(arm):
    component,engine,events,uploads = setup(arm)
    result = component.prepare()
    assert result['receipt']['runtime_unchanged'] is True
    assert result['receipt']['manifest_sha256']==component.sources['manifest_sha256']
    assert len(uploads)==len(component.sources['files'])+3
    private = dict(uploads)['diagnostic.private.json']
    assert json.loads(private)['password']==SECRET
    assert SECRET not in json.dumps(events) and SECRET not in json.dumps(result)
    assert result['inventory']['diagnostic_connection_binding']['endpoint']==[TARGET['host'],5432]
    assert component.inventory.get('prepared_pair_sha256')==inventory.digest(engine.pair)
    sealed = engine.configurations.seals['primary']['owner']
    assert component.owner != sealed and not component.owner.startswith(sealed + '/')
    assert component.artifact_owner_name != engine.runtime.output.name
    with pytest.raises(ValueError,match='replayed'):component.prepare()
    engine.paused = True
    assert component.cleanup()=={'diagnostic_credentials_removed':True}
    assert all(cleanup for _,cleanup,_ in engine.session.calls[-2:])
    assert engine.session.cleanup_mode is False


@pytest.mark.parametrize('gate',['pass','read_only_connection_verified','verified_tls','one_data_connection','identity_sha256','max_collection_ms'])
def test_failed_preflight_keeps_cleanup_and_cannot_replay(gate):
    component,engine,events,_uploads = setup()
    engine.session.fail = gate
    with pytest.raises(ValueError):component.prepare()
    assert component.attempted and not any(k=='worker-diagnostic-ack' for k,_ in events)
    engine.session.fail = None
    engine.paused = True
    assert component.cleanup()['diagnostic_credentials_removed']
    with pytest.raises(ValueError):component.prepare()


@pytest.mark.parametrize('site',['api_cleanup','host_cleanup'])
def test_cleanup_attempts_both_owned_sites_even_on_interruption(site):
    component,engine,_events,_uploads = setup()
    component.prepare();engine.paused=True;engine.session.fail=site
    with pytest.raises(RuntimeError,match='recovery'):component.cleanup()
    assert [call[0] for call in engine.session.calls[-2:]]==['api','call']
    assert engine.session.cleanup_mode is False


def test_partial_transfer_failure_is_marked_before_secret_upload_and_still_cleaned():
    component,_engine,events,_uploads = setup()
    def fail(*args):raise KeyboardInterrupt('synthetic transfer failure')
    component.upload=fail
    with pytest.raises(KeyboardInterrupt):component.prepare()
    assert component.attempted and events[0][0]=='worker-diagnostic-intent'
    assert component.cleanup()['diagnostic_credentials_removed']


def test_runtime_restart_during_preflight_cannot_be_certified():
    component,engine,events,_uploads = setup()
    engine.session.drift=True
    with pytest.raises(ValueError,match='Runtime changed'):component.prepare()
    assert not any(k=='worker-diagnostic-ack' for k,_ in events)
    assert component.cleanup()['diagnostic_credentials_removed']


@pytest.mark.parametrize('change',['source','target','scope','context','pause'])
def test_binding_changes_fail_before_any_transfer(change):
    component,engine,events,uploads = setup()
    if change=='source':component.sources['files']['observe_worker_pipeline.py'] += '\n# changed\n'
    elif change=='target':component.target['host']='10.0.0.9'
    elif change=='scope':engine.runtime.guard.binding['worker_diagnostic_target_sha256']='f'*64
    elif change=='context':component.context.clear()
    else:engine.paused=True
    with pytest.raises(ValueError):component.prepare()
    assert not uploads and not events and not engine.session.calls


def test_sealed_configuration_namespace_cannot_be_used_for_observer_artifacts():
    component,engine,_events,_uploads=setup()
    with pytest.raises(ValueError,match='Separate canonical'):
        diagnostics.DiagnosticActions(engine,component.inventory,component.sources,component.context,component.cid,
                                      artifact_output=engine.runtime.output)


@pytest.mark.parametrize('change',['owner','directory','owner_binding'])
def test_separate_artifact_owner_cannot_change_after_binding(change):
    component,engine,events,uploads=setup()
    if change=='owner':component.owner=engine.configurations.seals['primary']['owner']+'/candidate'
    elif change=='directory':component.directory='/tmp/unowned/candidate'
    else:engine.runtime.guard.binding['worker_observer_owner_name']=engine.runtime.output.name
    with pytest.raises(ValueError):component.prepare()
    assert not uploads and not events and not engine.session.calls
