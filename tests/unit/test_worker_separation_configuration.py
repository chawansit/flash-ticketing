"""ADR0194 shape, authority and crash ambiguity checks; no cloud transport."""
import copy
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import work_envelope as policy
import worker_separation_configuration as config


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tests/unit' / f'{name}.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def prepared(arm='candidate', host='primary'):
    source = load('test_worker_separation_runtime')
    saved, _, _, inputs = source.fixture()
    return config.bundle(inputs['pair'], saved, arm, host, 'a' * 64)


def seal(value, owner='/qualification/repository/tmp/adr0153-parents-000000000001'):
    return {'owner': owner, 'manifest': value['manifest'],
            'directory': {'device': 1, 'inode': 10, 'uid': 0, 'mode': 0o700},
            'files': {name: {'device': 1, 'inode': index + 20, 'uid': 0, 'mode': 0o600, **record}
                      for index, (name, record) in enumerate(value['manifest']['files'].items())}}


def create(monkeypatch):
    source = load('test_worker_separation_runtime')
    runtime, session, checks = source.create(monkeypatch)
    session.config = {host: {'repo': '/qualification/repository'} for host in ('primary', 'secondary')}
    # Reuse authority setup; the configuration component still creates its own fresh journal.
    adapter = config.ConfigurationActions(session, runtime.guard, runtime.inputs, runtime.saved,
                                          'candidate', source.module('test_worker_separation_lifecycle').staging.new_stage_output())
    calls = []
    def call(host, code, timeout):
        calls.append((host, code, timeout, session.cleanup_mode))
        if 'payload=' in code:
            value = config.bundle(adapter.runtime.inputs['pair'], adapter.runtime.saved, adapter.arm, host,
                                  adapter.runtime.scope_binding_sha256)
            owner = config.owner_path(session.config[host]['repo'], adapter.runtime.output.name)
            return seal(value, owner)
        return {'owned_configuration_removed': True} if 'cleanup=True' in code else {'configuration_matches': True}
    session.call = call
    return adapter, session, calls, checks


@pytest.mark.parametrize('arm,host', [('control', 'primary'), ('candidate', 'primary'), ('candidate', 'secondary')])
def test_models_and_hashes_exactly_match_prepared_pair_and_saved_restoration(arm, host):
    source = load('test_worker_separation_runtime')
    saved, _, _, inputs = source.fixture()
    before = copy.deepcopy((inputs, saved))
    value = config.bundle(inputs['pair'], saved, arm, host, 'a' * 64)
    assert json.loads(value['files']['arm.compose.json']) == inputs['pair'][arm][host]
    if host == 'primary':
        assert json.loads(value['files']['restore.compose.json']) == saved['model']
    else:
        assert set(value['files']) == {'arm.compose.json'}
    assert (inputs, saved) == before
    config.validate_manifest(value['manifest'])
    compile(config.install_program(seal(value)['owner'], value), '<installation>', 'exec')
    compile(config.sealed_program(seal(value), cleanup=True), '<cleanup>', 'exec')


def test_control_has_no_secondary_installation():
    with pytest.raises(ValueError, match='No secondary'):
        prepared('control', 'secondary')


@pytest.mark.parametrize('change', ['name', 'hash', 'size', 'oversize', 'decision', 'scope', 'host', 'extra', 'payload'])
def test_drift_rejected_before_a_remote_program_is_returned(change):
    value = prepared()
    if change == 'name':
        for record in (value['files'], value['manifest']['files']):
            record['../unowned'] = record.pop('arm.compose.json')
    elif change == 'hash': value['manifest']['files']['arm.compose.json']['sha256'] = 'b' * 64
    elif change == 'size': value['manifest']['files']['arm.compose.json']['size'] = True
    elif change == 'oversize': value['files']['arm.compose.json'] = 'x' * (config.MAX_FILE + 1)
    elif change == 'decision': value['manifest']['decision'] = 'ADR0000'
    elif change == 'scope': value['manifest']['scope_binding_sha256'] = 'bad'
    elif change == 'host': value['manifest']['host'] = 'secondary'
    elif change == 'extra': value['manifest']['private_secret'] = 'secret'
    else: value['files']['arm.compose.json'] += 'changed'
    with pytest.raises(ValueError): config.install_program(seal(value)['owner'], value)


@pytest.mark.parametrize('change', ['owner', 'inode', 'mode', 'uid_type', 'file', 'extra'])
def test_invalid_seal_cannot_authorize_cleanup(change):
    value = prepared()
    observed = seal(value)
    if change == 'owner': observed['owner'] += '/unowned'
    elif change == 'inode': observed['directory']['inode'] = 0
    elif change == 'mode': observed['directory']['mode'] = 0o755
    elif change == 'uid_type': observed['directory']['uid'] = True
    elif change == 'file': observed['files']['arm.compose.json']['sha256'] = 'b' * 64
    else: observed['extra'] = 'secret'
    with pytest.raises(ValueError): config.validate_seal(observed, seal(value)['owner'], value['manifest'])


def test_install_verify_and_cleanup_are_bound_and_do_not_journal_secret_payload(monkeypatch):
    adapter, _, calls, _ = create(monkeypatch)
    for host in ('primary', 'secondary'):
        assert adapter.install(host)['configuration_installed'] is True
        assert adapter.verify(host) == {'configuration_matches': True}
        assert adapter.cleanup(host) == {'owned_configuration_removed': True}
    assert [row[3] for row in calls] == [False, False, True, False, False, True]
    journals = ''.join(path.read_text() for path in adapter.runtime.output.glob('*.json'))
    assert 'a$literal#value' not in journals and 'DATABASE_URL' not in journals
    with pytest.raises(ValueError, match='Single-use'): adapter.install('primary')
    with pytest.raises(ValueError, match='single-use'): adapter.cleanup('primary')


@pytest.mark.parametrize('failure', ['pause', 'intent', 'lost', 'seal_persistence', 'invalid_seal'])
def test_failures_block_replay_and_cleanup_requires_a_received_seal(monkeypatch, failure):
    adapter, session, calls, _ = create(monkeypatch)
    original_write, original_call = adapter.runtime._write, session.call
    if failure == 'pause':
        monkeypatch.setattr(adapter.runtime, '_authorize', lambda _: (_ for _ in ()).throw(TimeoutError()))
    elif failure == 'intent':
        monkeypatch.setattr(adapter.runtime, '_write', lambda *a: (_ for _ in ()).throw(OSError()))
    elif failure == 'seal_persistence':
        def write(kind, value):
            if kind == 'configuration-seal': raise OSError()
            return original_write(kind, value)
        monkeypatch.setattr(adapter.runtime, '_write', write)
    else:
        def call(host, code, timeout):
            observed = original_call(host, code, timeout)
            if failure == 'lost': raise TimeoutError()
            observed['directory']['mode'] = 0o755
            return observed
        session.call = call
    with pytest.raises((TimeoutError, OSError, ValueError)): adapter.install('primary')
    if failure in {'pause', 'intent'}: assert not calls
    assert bool(adapter.seals) == (failure == 'seal_persistence')
    count = len(calls)
    with pytest.raises((ValueError, TimeoutError)): adapter.install('primary')
    assert len(calls) == count
    if adapter.seals:
        session.call = original_call
        adapter.cleanup('primary')
    else:
        with pytest.raises(ValueError): adapter.cleanup('primary')


def test_pause_after_install_still_allows_exact_cleanup(monkeypatch):
    adapter, session, calls, _ = create(monkeypatch)
    adapter.install('primary')
    monkeypatch.setattr(adapter.runtime, '_authorize', lambda _: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(TimeoutError): adapter.verify('primary')
    assert adapter.cleanup('primary') == {'owned_configuration_removed': True}
    assert calls[-1][3] is True and session.cleanup_mode is False


def test_real_unregistered_profile_prevents_all_remote_work(monkeypatch):
    source = load('test_worker_separation_runtime')
    saved, rows, volumes, inputs = source.fixture()
    guard = object.__new__(policy.ActionGuard)
    guard.key, guard.binding = 'unused', source.runtime.binding_for(inputs)
    session = source.Session(guard, rows, volumes)
    with pytest.raises(ValueError, match='Registered fresh'):
        config.ConfigurationActions(session, guard, inputs, saved, 'control',
                                    source.module('test_worker_separation_lifecycle').staging.new_stage_output())
    assert session.calls == []


@pytest.mark.parametrize('change', ['pair', 'saved', 'guard'])
def test_binding_drift_prevents_remote_installation(monkeypatch, change):
    adapter, _, calls, _ = create(monkeypatch)
    if change == 'pair': adapter.runtime.inputs['pair']['budgets']['api_connections'] += 1
    elif change == 'saved': adapter.runtime.saved['semantics']['api'] = 'b' * 64
    else: adapter.runtime.guard.binding['worker_pair_sha256'] = 'b' * 64
    with pytest.raises(ValueError): adapter.install('primary')
    assert calls == []


@pytest.mark.parametrize('change', ['transport', 'scope', 'scope_key'])
def test_cleanup_cannot_move_to_another_transport_or_scope(monkeypatch, change):
    adapter, session, calls, _ = create(monkeypatch)
    adapter.install('primary')
    if change == 'transport': session.action_guard = object()
    elif change == 'scope': adapter.runtime.guard.binding['worker_pair_sha256'] = 'b' * 64
    else: adapter.runtime.guard.key = 'different'
    count = len(calls)
    with pytest.raises(ValueError, match='Original guarded'): adapter.cleanup('primary')
    assert len(calls) == count and session.cleanup_mode is False


def test_verification_after_cleanup_is_rejected_without_transport(monkeypatch):
    adapter, _, calls, _ = create(monkeypatch)
    adapter.install('primary')
    adapter.cleanup('primary')
    count = len(calls)
    with pytest.raises(ValueError, match='Successful sealed'): adapter.verify('primary')
    assert len(calls) == count


@pytest.mark.parametrize('change', ['lost_ack', 'invalid_ack', 'journal'])
def test_cleanup_failure_preserves_single_use_and_transport_mode(monkeypatch, change):
    adapter, session, calls, _ = create(monkeypatch)
    adapter.install('primary')
    original_call = session.call
    if change == 'journal':
        monkeypatch.setattr(adapter.runtime, '_write', lambda *a: (_ for _ in ()).throw(OSError()))
    else:
        def call(host, code, timeout):
            original_call(host, code, timeout)
            if change == 'lost_ack': raise TimeoutError()
            return {'wrong': True}
        session.call = call
    with pytest.raises((TimeoutError, ValueError, OSError)): adapter.cleanup('primary')
    assert session.cleanup_mode is False and adapter.failed
    count = len(calls)
    with pytest.raises(ValueError, match='single-use'): adapter.cleanup('primary')
    with pytest.raises(ValueError, match='Single-use'): adapter.install('secondary')
    assert len(calls) == count
