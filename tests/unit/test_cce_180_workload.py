"""ADR0339 explicit rate step with unchanged paid application and connection budgets."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_api_client_headroom as client
import cce_generator_process_scaling as generator
import cce_recovery_headroom as headroom
import cce_throughput_push as push
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_180
from test_cce_api_client_headroom import goal as parent_goal
from test_cce_generator_process_scaling import parent, reads


def goal():
    g = parent_goal()
    g.update(throughput_push_decision='ADR0339', offered_journeys_per_second=180)
    g['api_client_pool_contract_sha256'] = policy.digest(client.contract(g))
    g['generator_process_contract_sha256'] = policy.digest(generator.contract(g))
    g['throughput_push_contract_sha256'] = policy.digest(push.contract(g))
    g['headroom_contract_sha256'] = policy.digest(headroom.contract(g))
    return g


def test_exact_180_scope_and_historical_160_unchanged():
    g = goal()
    assert push.selected(g) and client.selected(g) and generator.selected(g) and headroom.selected(g)
    assert client.selected(headroom.runtime_view(g)) and generator.selected(headroom.runtime_view(g))
    assert push.bounds(g) == (180, 180, 54000, 53400)
    assert (RATE_PUSH_180.duration, RATE_PUSH_180.shows, RATE_PUSH_180.expected) == (300, 180, 54000)
    assert client.contract()['offered_rate'] == generator.contract()['rate'] == 160
    assert client.api_settings({'DB_POOL_MAX':'4', 'API_PAYMENT_POOL_MAX':'2'}, g) == {'DB_POOL_MAX':'6', 'API_PAYMENT_POOL_MAX':'3'}
    assert push.reference(g)[2] == '5ec10ec764bc349911b28e2c5d20883eb7c2f1df58a02bc55e792b181ccaf05b'


@pytest.mark.parametrize('field,value', [
    ('offered_journeys_per_second', 200), ('offered_seconds', 600),
    ('maximum_paid_stages', 2), ('maximum_experiment_seconds', 5400),
    ('generator_concurrency', 1000), ('acquisition_budget', 24),
    ('api_client_pool_contract_sha256', '0' * 64),
    ('payment_delivery_decision', 'ADR0337'), ('pod_vcpu', 2),
])
def test_180_rejects_other_budgets_or_candidate(field, value):
    g = goal()
    g[field] = value
    with pytest.raises(ValueError):
        client.selected(g)


def test_generated_workload_has_four_45_per_second_shards_and_same_leaf_logic():
    g = goal()
    bundle, coordinator = parent()
    bundle, coordinator = reads(bundle, coordinator)
    bundle, coordinator = headroom.qualify_workload(bundle, coordinator, g)
    before = copy.deepcopy(bundle)
    bundle, coordinator = generator.qualify_workload(bundle, coordinator, g)
    source = bundle['scripts/paid_ticket_sharded_generator.py']
    assert b"(180, 300, 800, 8, 1, 1, 3, 'distributed')" in source
    assert b'str(args.rate // 4)' in source
    assert b'"--http-max-connections", "125"' in source
    assert b'(200, 125, 4)' in bundle['scripts/paid_ticket_load_generator.py']
    for name in before:
        if name not in {'scripts/paid_ticket_sharded_generator.py', 'scripts/paid_ticket_load_generator.py'}:
            assert bundle[name] == before[name]
    compile(source, 'coordinator', 'exec')


def test_180_requires_qualified_client_and_generator_selection():
    for key in ('api_client_pool_decision', 'generator_process_decision'):
        g = goal()
        del g[key]
        with pytest.raises(ValueError):
            push.selected(g)
