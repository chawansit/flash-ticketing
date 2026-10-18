"""ADR0355 actual fixed-image rate plan/manifests/binding; no cloud authority."""
import argparse
import copy
import hashlib
import json
import tempfile
from pathlib import Path
from uuid import uuid4

import cce_rate_qualification as qualification
import cce_simulator_roundtrip_profile as roundtrip
import prepare_simulator_roundtrip_comparison as previous
import run_cce_paid_comparison as core
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_180, RATE_PUSH_200


def goal_for(base, rate, reference, pair):
    seed = copy.deepcopy(base)
    seed.update(rate_qualification_decision='ADR0355', rate_qualification_rate=rate,
                rate_qualification_reference=copy.deepcopy(reference))
    goal = previous.goal_for(seed, pair, 'candidate')
    goal['offered_journeys_per_second'] = rate
    goal['simulator_roundtrip_contract_sha256'] = policy.digest(roundtrip.contract(goal))
    assert roundtrip.selected(goal)
    return goal


def prepare(base, rate, reference, proof, target):
    original = policy.ENVELOPE
    hashes = {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in (policy.ENVELOPE, policy.JOURNAL, policy.STATE)}
    envelope = policy.envelope()
    pair = {'id':uuid4().hex, 'started_at_utc':None,
            'ledger_start_index':len(policy.journal(envelope)['experiments'])}
    goal = goal_for(base, rate, reference, pair)
    qualification.require_previous(goal)
    config = policy.read(policy.ROOT / goal['primary_bootstrap_config_path'])
    saved = policy.read(policy.ROOT / goal['payment_slot_snapshot_path'])['saved']
    profile = {180:RATE_PUSH_180, 200:RATE_PUSH_200}[rate]
    with tempfile.TemporaryDirectory(prefix='adr0355-preview-', dir=policy.ROOT/'tmp') as directory:
        try:
            policy.ENVELOPE = Path(directory)/'envelope.json'
            trial = copy.deepcopy(envelope)
            trial['spending']['temporary_cce_pilot_exception']['goal_bounded_authorization'] = goal
            policy.write(policy.ENVELOPE, trial)
            objects = core.native.objects('adr0151-035500000000', core.service_for(saved),
                                          config['primary']['private_ipv4'], 'offline', 'offline',
                                          acquisition_budget=20, profile=profile)
            binding = core.binding_for(config, proof, objects, target, saved, profile=profile, preparation_only=True)
            plan = core.plan(profile=profile)
            image = roundtrip.image(goal)
            assert plan['kind'] == 'five_minute_rate_qualification'
            assert plan['matched_performance_comparison'] is False
            assert binding['cce_simulator_roundtrip_contract']['offered_rate'] == rate
            assert binding['cce_generator_process_contract']['rate'] == rate
            assert binding['cce_api_client_pool_contract']['offered_rate'] == rate
            assert plan['baseline_sha256'] == reference['sha256']
            assert binding['cce_api_client_pool_max'] == 6 and binding['cce_payment_pool_max'] == 3
            assert binding['cce_simulator_concurrency'] == 16 and binding['cce_simulator_database_pool_max'] == 10
            result = {'decision':'ADR0355','goal':goal,'binding':binding,'plan':plan,'objects':objects,
                      'image':image,'profile_rate':rate,'paid_authorized':False,'cloud_calls':0}
        finally:
            policy.ENVELOPE = original
    assert hashes == {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in hashes}
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rate',type=int,choices=(180,200),required=True)
    parser.add_argument('--base-goal',type=Path,required=True)
    parser.add_argument('--reference',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    for path in (args.base_goal,args.reference,args.output):
        if path.is_symlink() or not path.resolve().is_relative_to(policy.ROOT.resolve()):
            raise ValueError('Owned regular workspace paths required')
    if args.output.exists() or not args.output.resolve().is_relative_to((policy.ROOT/'tmp').resolve()):
        raise ValueError('Fresh private preparation output required')
    target=policy.read(policy.ROOT/'tmp/rds-switch-20261018/diagnostic-target.private.json')
    result=prepare(policy.read(args.base_goal),args.rate,policy.read(args.reference),
                   policy.read(policy.ROOT/'tmp/cce-api-registry-proof.json'),target)
    args.output.write_bytes((json.dumps(result,indent=2)+'\n').encode())
    print(json.dumps({'prepared':True,'rate':args.rate,'seconds':300,'cloud_calls':0,'paid_authorized':False,
                      'source_contract_sha256':policy.digest(roundtrip.contract(result['goal']))}))


if __name__=='__main__':
    main()
