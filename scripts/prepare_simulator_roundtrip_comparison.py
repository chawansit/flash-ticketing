"""ADR0347 actual local plan/manifests/binding; no reservation or cloud calls."""
import argparse
import base64
import copy
import hashlib
import json
import tempfile
from pathlib import Path
from uuid import uuid4

import cce_api_client_headroom as clients
import cce_customer_outcome_slo as outcome
import cce_fixture_clock as clock
import cce_fulfillment_batch_profile as fulfillment
import cce_generator_process_scaling as generator
import cce_inventory_renewal as renewal
import cce_metric_admission as metric
import cce_native_workers as workers
import cce_primary_bootstrap as bootstrap
import cce_recovery_headroom as headroom
import cce_simulator_roundtrip_profile as roundtrip
import cce_throughput_push as push
import cce_verification_contract as verification
import run_cce_paid_comparison as core
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_160


def goal_for(base, pair, arm):
    goal = copy.deepcopy(base)
    for key in ('authorized_at_utc', 'authorized_at_bangkok', 'ledger_start_index', 'simulator_roundtrip_control'):
        goal.pop(key, None)
    goal.update(status='PREPARED_NOT_AUTHORIZED', authorization='Local preview only; no paid allowance.',
                throughput_push_decision='ADR0329', offered_journeys_per_second=160,
                simulator_roundtrip_decision=roundtrip.DECISION, simulator_roundtrip_arm=arm,
                simulator_roundtrip_preview=True, simulator_roundtrip_pair=copy.deepcopy(pair),
                api_client_pool_decision='ADR0338', api_client_pool_receipt_sha256=clients.file_sha(clients.LOCAL))
    goal['generator_process_contract_sha256'] = policy.digest(generator.contract(goal))
    goal['throughput_push_contract_sha256'] = policy.digest(push.contract(goal))
    for key, value in (
        ('headroom_contract_sha256', headroom.contract(goal)),
        ('api_client_pool_contract_sha256', clients.contract(goal)),
        ('primary_bootstrap_contract_sha256', bootstrap.contract()),
        ('fulfillment_batch_contract_sha256', fulfillment.contract()),
        ('verification_contract_sha256', verification.contract()),
        ('metric_admission_contract_sha256', metric.contract()),
        ('inventory_renewal_contract_sha256', renewal.contract()),
        ('customer_outcome_contract_sha256', outcome.contract()),
        ('fixture_clock_contract_sha256', clock.contract()),
        ('simulator_roundtrip_contract_sha256', roundtrip.contract()),
    ):
        goal[key] = policy.digest(value)
    return goal


def prepare(base, proof, target):
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in (policy.ENVELOPE, policy.JOURNAL, policy.STATE)}
    envelope = policy.envelope()
    pair = {'id': uuid4().hex, 'started_at_utc': None,
            'ledger_start_index': len(policy.journal(envelope)['experiments'])}
    config = policy.read(policy.ROOT / base['primary_bootstrap_config_path'])
    saved = policy.read(policy.ROOT / base['payment_slot_snapshot_path'])['saved']
    original = policy.ENVELOPE
    arms = {}
    with tempfile.TemporaryDirectory(prefix='adr0345-preview-', dir=policy.ROOT / 'tmp') as directory:
        try:
            policy.ENVELOPE = Path(directory) / 'envelope.json'
            for arm in ('control', 'candidate'):
                goal = goal_for(base, pair, arm)
                current = copy.deepcopy(envelope)
                current['spending']['temporary_cce_pilot_exception']['goal_bounded_authorization'] = goal
                policy.write(policy.ENVELOPE, current)
                run = 'adr0151-034500000000'
                manifests = core.native.objects(run, core.service_for(saved), config['primary']['private_ipv4'],
                                                'preview-user', 'preview-password', acquisition_budget=20,
                                                profile=RATE_PUSH_160)
                binding = core.binding_for(config, proof, manifests, target, saved,
                                           profile=RATE_PUSH_160, preparation_only=True)
                plan = core.plan(profile=RATE_PUSH_160)
                contract = core.worker_contract()
                assert contract.shared['registry_manifest_digest'] == fulfillment.image()['registry_manifest_digest']
                model = contract.primary_model(saved['model'])
                roles = workers.identity.specification('ADR0324')['counts']
                environments = {role: model['services'][role]['environment'] for role in roles}
                manifests += workers.objects(run, environments, config['primary']['private_ipv4'],
                                             'ADR0345', profile=RATE_PUSH_160)
                image = roundtrip.image(goal)
                pods = [o for o in manifests if o['kind'] == 'Pod']
                assert len(pods) == 19
                assert all(o['spec']['containers'][0]['image'] == image['registry_image'] for o in pods)
                for obj in manifests:
                    if obj['kind'] != 'Secret' or obj['metadata']['name'] not in {'api-env', 'simulator-env'}:
                        continue
                    env = {k: base64.b64decode(v).decode() for k, v in obj['data'].items()}
                    expected = ({'DB_POOL_MAX': '6', 'API_PAYMENT_POOL_MAX': '3', 'DB_POOL_WAIT_MS': '500'}
                                if obj['metadata']['name'] == 'api-env' else
                                {'SIMULATOR_CONCURRENCY': '16', 'DB_POOL_MAX': '10'})
                    assert all(env.get(k) == v for k, v in expected.items())
                arms[arm] = {'goal': goal, 'plan': plan, 'binding': binding, 'objects': manifests,
                             'image': image, 'manifest_sha256': policy.digest(manifests)}
        finally:
            policy.ENVELOPE = original
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}
    normalized = {}
    for arm, value in arms.items():
        objects = copy.deepcopy(value['objects'])
        api_secret = next(o for o in objects if o['kind'] == 'Secret' and o['metadata']['name'] == 'api-env')
        api_env = {k: base64.b64decode(v).decode() for k,v in api_secret['data'].items()}
        common_env = {k:v for k,v in api_env.items() if k != 'PAYMENT_CALLBACK_WRITE_PIPELINE'}
        for obj in objects:
            if obj['kind'] == 'Secret' and obj['metadata']['name'] == 'api-env':
                flag = obj['data'].pop('PAYMENT_CALLBACK_WRITE_PIPELINE', None)
                assert flag == (base64.b64encode(b'1').decode() if arm == 'candidate' else None)
            if obj['kind'] == 'Pod':
                container = obj['spec']['containers'][0]
                container['image'] = 'matched-image'
                container['command'][-1] = container['command'][-1].replace(
                    repr(value['image']['runtime_source_sha256']), 'matched-sources')
                if container['name'] == 'api':
                    annotations = obj['metadata']['annotations']
                    assert annotations['codex-environment-sha256'] == policy.digest(api_env)
                    annotations['codex-environment-sha256'] = policy.digest(common_env)
                    container['command'][-1] = container['command'][-1].replace(
                        repr(sorted(api_env)), repr(sorted(common_env))).replace(
                            repr(policy.digest(api_env)), repr(policy.digest(common_env)))
        normalized[arm] = objects
    assert normalized['control'] == normalized['candidate'], 'Arms differ beyond reviewed image/source and callback flag'
    return {'decision': roundtrip.DECISION, 'local_only': True, 'paid_authorized': False,
            'registry_verified': False, 'live_snapshot_reconciled': False, 'pair': pair, 'arms': arms}


def public_summary(value):
    return {k: value[k] for k in ('decision', 'local_only', 'paid_authorized', 'registry_verified', 'live_snapshot_reconciled')} | {
        'unchanged_resources_and_other_settings': True,
        'candidate_only_setting': {'PAYMENT_CALLBACK_WRITE_PIPELINE': '1'}, 'offered_rate_each': 160, 'offered_seconds_each': 300,
        'pair_limit_seconds': 3600, 'stop_after_failed_control': True,
        'arms': {name: {'image': arm['image']['registry_image'], 'source': arm['image']['source_identity_sha256'],
                        'manifest_sha256': arm['manifest_sha256'], 'binding_sha256': policy.digest(arm['binding']),
                        'api_pods': 4, 'worker_pods': 15} for name, arm in value['arms'].items()},
        'remaining_prerequisites': ['Actual registry publication/pull verification for ADR0346.',
                                    'Specific registry-upload approval and live pair start/journal boundary.',
                                    'Live snapshot/readiness qualification; fully passed/restored fresh control before candidate.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-goal', type=Path, required=True)
    parser.add_argument('--proof', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    for path in (args.base_goal, args.proof, args.target, args.output):
        if path.is_symlink() or not path.resolve().is_relative_to(policy.ROOT.resolve()):
            raise ValueError('Explicit owned workspace paths required')
    if not args.output.resolve().is_relative_to((policy.ROOT / 'tmp').resolve()) or args.output.exists():
        raise ValueError('Fresh private preview path under tmp required')
    result = prepare(policy.read(args.base_goal), policy.read(args.proof), policy.read(args.target))
    args.output.write_bytes((json.dumps(result, indent=2) + '\n').encode())
    print(json.dumps(public_summary(result)))


if __name__ == '__main__':
    main()
