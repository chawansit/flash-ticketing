"""ADR0357 strict fixed-budget identity, ownership and actual observer paths."""
import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_fixed_worker_allocation as allocation
import cce_paid_observers as observations
import cce_recovery_headroom as headroom
import cce_simulator_roundtrip_profile as profile
import cce_transaction_profile as transaction
import cce_worker_identity as identity
import diagnostic_runner_connection as diagnostics
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_180
from test_cce_rate_qualification import actual
from test_observe_cce_paid_pipeline import bundle as api_bundle


def allocated(monkeypatch, decision="ADR0357"):
    goal = actual()
    goal['fixed_worker_allocation_decision'] = decision
    monkeypatch.setattr(transaction, 'active', lambda: headroom.runtime_view(goal))
    spec = identity.specification(decision, goal=goal)
    image = profile.image(goal)
    run = 'adr0151-034800000000'
    rows = []
    for role,count in spec['counts'].items():
        for i in range(count):
            n = len(rows)+1
            uid = 'synthetic-'+str(n)
            rows.append({'role':role,'pod_name':role+'-'+str(i),'pod_uid':uid,
                         'private_ipv4':'10.2.0.'+str(n),'image_id':image['registry_image'],
                         'resources':copy.deepcopy(identity.resources_for(spec,role)),
                         'process_start_time_seconds':100+n,
                         'startup_proof':{'run':run,'pod_uid':uid,'sources':image['runtime_source_sha256'],
                                          'environment_sha256':'a'*64,
                                          'command_sha256':policy.digest(['python','-m','ticketing.workers',role])}})
    bundle = {'decision':decision,'run':run,'sources':image['runtime_source_sha256'],
              'manifest_digest':spec['manifest'],'resources':spec['resources'],'receipts':rows}
    return goal,bundle


def startup_row():
    row = {'utc':datetime.now(UTC).isoformat(),'issued_tickets':0,
           'api_replicas':{str(i):{} for i in range(4)},'extended_diagnostics':True,
           'host_cpu_ticks':[100]*8,'database_wait_diagnostics':{'complete':True},
           'pgbouncer':{'stats':{'total_xact_count':1},'pools':{'sv_active':1},'maxwait_seconds':0}}
    for role,count in {'writer':5,'consumer':5,'maintenance':1,'reconciler':1,'simulator':1,'publisher':1}.items():
        row[role+'_replicas']=count
        row[role+'_db_replicas']={str(i):{'process_cpu_seconds_total':0} for i in range(count)}
    return row


def test_total_resources_and_local_transferred_identity(monkeypatch):
    goal,bundle=allocated(monkeypatch)
    spec=identity.specification('ADR0357',goal=goal)
    assert sum(spec['counts'].values())==15
    for kind in ('requests','limits'):
        assert sum(int(r['resources'][kind]['cpu'][:-1]) for r in bundle['receipts'])==3750
        assert sum(int(r['resources'][kind]['memory'][:-2]) for r in bundle['receipts'])==7680
    assert len(identity.validate_bundle(bundle))==len(identity.validate_transferred_bundle(bundle))==15
    monkeypatch.setattr(profile,'image',lambda *args:(_ for _ in ()).throw(AssertionError('local authority forbidden')))
    assert len(identity.validate_transferred_bundle(bundle))==15


@pytest.mark.parametrize('fault',['root_cpu','row_cpu','missing','extra_consumer','duplicate_uid','source','control_image'])
def test_allocation_rejects_budget_and_runtime_drift(monkeypatch,fault):
    _,bundle=allocated(monkeypatch)
    if fault=='root_cpu':bundle['resources']['simulator']['limits']['cpu']='750m'
    elif fault=='row_cpu':next(r for r in bundle['receipts'] if r['role']=='simulator')['resources']['limits']['cpu']='250m'
    elif fault=='missing':bundle['receipts'].pop()
    elif fault=='extra_consumer':bundle['receipts'][-1]['role']='consumer'
    elif fault=='duplicate_uid':bundle['receipts'][1]['pod_uid']=bundle['receipts'][0]['pod_uid']
    elif fault=='source':bundle['sources']={}
    else:bundle['manifest_digest']=profile.parent.image()['registry_manifest_digest']
    with pytest.raises(ValueError):identity.validate_transferred_bundle(bundle)


@pytest.mark.parametrize('fault',[None,'writer','consumer','cpu','database_wait','kafka','projection'])
def test_actual_observer_start_uses_strict_allocated_counts(monkeypatch,tmp_path,fault):
    goal,bundle=allocated(monkeypatch)
    row=startup_row()
    if fault=='writer':row['writer_replicas']=4
    if fault=='consumer':row['consumer_replicas']=6
    if fault=='cpu':row['writer_db_replicas']['4'].pop('process_cpu_seconds_total')
    if fault=='database_wait':row['database_wait_diagnostics']['complete']=False
    image=profile.image(goal);api=copy.deepcopy(api_bundle());settings=observations.cce.contract()
    api.update(run=bundle['run'],api_sources=image['runtime_source_sha256'],manifest_digest=image['registry_manifest_digest'],resources=settings['resources'])
    for receipt in api['receipts']:
        receipt.update(image_id=image['registry_image'],resources=settings['resources'])
        receipt['startup_proof'].update(run=api['run'],sources=api['api_sources'])
    monkeypatch.setattr(observations,'api_upload',lambda *args:None)
    monkeypatch.setattr(diagnostics,'prepare',lambda *args:{})
    monkeypatch.setattr(diagnostics,'qualify_bound_inventory',lambda *args:None)
    def call(cid,program,timeout):
        compile(program,'offline-startup','exec')
        if 'native_helpers_verified' in program:return {'native_helpers_verified':True}
        if 'read_text().splitlines()[0]' in program:
            return {'pipeline':row,'kafka':{'utc':row['utc'],'members':6 if fault=='kafka' else 5,'total_lag':0},
                    'projection-kafka':{'utc':row['utc'],'members':1,'total_lag':0,
                        'assigned_partitions':5 if fault=='projection' else 6,'group':'ticketing-seat-projection-v1',
                        'partitions':[{'partition':i} for i in range(6)]}}
        return {'fresh':True}
    stage=SimpleNamespace(profile=RATE_PUSH_180,session=SimpleNamespace(api=call,call=lambda *a:{'fresh':True}),
                          run=api['run'],cid='c'*64,manifest={'environment':'development','show_ids':['synthetic']},
                          output=tmp_path,record={},job_list=[],check=lambda *a:None,checkpoint=lambda:None,
                          guard=SimpleNamespace(binding={}),jobs=SimpleNamespace(launch=lambda *a,**k:True))
    observer=observations.Observers(stage,{},SimpleNamespace(verify_pipeline_startup=lambda value:None),None,'/offline')
    observer.native_workers=bundle
    if fault:
        with pytest.raises(ValueError):observer.start(api['receipts'])
        assert not observer.started
    else:
        observer.start(api['receipts'])
        assert observer.started and observer.record['pipeline_startup']['writer_count']==5
        assert observer.record['kafka_startup']['pass']


@pytest.mark.parametrize('fault',[None,'missing_partition','duplicate_partition','members','unbalanced','ownership'])
def test_five_consumers_own_exactly_six_partitions(monkeypatch,fault):
    _,bundle=allocated(monkeypatch)
    row={'samples':300,'sample_errors':0,'members_min':5,'members_max':5,'ownership_observed':True,
         'max_partitions_per_member':2,'last_member_partition_groups':[[0,5],[1],[2],[3],[4]]}
    if fault=='missing_partition':row['last_member_partition_groups'][0]=[0]
    if fault=='duplicate_partition':row['last_member_partition_groups'][0]=[0,0]
    if fault=='members':row['members_max']=6
    if fault=='unbalanced':row['last_member_partition_groups']=[[0,1,2],[3],[4],[5],[]]
    if fault=='ownership':row['ownership_observed']=False
    assert allocation.kafka_pass(row,bundle,lambda _:False) is (fault is None)


def test_native_safety_keeps_five_member_drain_and_all_financial_gates():
    from cce_paid_inputs import safety_result
    raw=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/cce-comparison.private.json')['native']['safety']
    queues={**raw['global_queues'],'kafka_members':5}
    assert all(safety_result(raw['probe'],raw['post_ttl_financial'],queues,expected_consumers=5)['gates'].values())
    with pytest.raises(ValueError):safety_result(raw['probe'],raw['post_ttl_financial'],queues)


def test_actual_200_audit_builder_preserves_strict_snapshot_and_predicates():
    from cce_bounded_financial_audit import program, relationship_queries
    shows=[str(uuid4()) for _ in range(200)]
    source=program(shows,60000,callbacks=1)
    assert len(relationship_queries())==7
    assert 'REPEATABLE READ READ ONLY' in source
    compile(source,'actual200financialaudit','exec')
    with pytest.raises(ValueError):program(shows,60001,callbacks=1)


def test_optional_actual_image_sealed_allocation_helpers(monkeypatch,tmp_path):
    target=os.getenv('QUALIFY_ALLOCATED_HELPERS_RECEIPT')
    if not target:pytest.skip('Explicit bounded actual-image rehearsal not requested')
    from test_cce_transferred_combined_workers import PROGRAM, capture_helpers
    image,helpers=capture_helpers(monkeypatch,tmp_path,'candidate')
    _,bundle=allocated(monkeypatch)
    (tmp_path/'workers.json').write_bytes(json.dumps(bundle).encode())
    inventory=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/candidate/pre-transition-inventory.private.json')
    inventory['captured_at']=datetime.now(UTC).isoformat()
    (tmp_path/'inventory.json').write_bytes(json.dumps(inventory).encode())
    source=PROGRAM.replace("'reservation-writer',9101))==4","'reservation-writer',9101))==5").replace("'consumer',9101))==6","'consumer',9101))==5")
    source=source.replace("metadata=json.load(sys.stdin)","metadata=json.load(sys.stdin)\nfor name,want in metadata['runtime_sources'].items():assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==want")
    metadata={'helpers':{n:hashlib.sha256(v).hexdigest() for n,v in helpers.items()},'runtime_sources':image['runtime_source_sha256']}
    command=['docker','run','--rm','--network=none','-i','--entrypoint','python','--mount',
             'type=bind,source='+str(tmp_path.resolve())+',target=/qualified,readonly',image['local_image_id'],'-c',source]
    result=subprocess.run(command,input=json.dumps(metadata),text=True,capture_output=True,timeout=90,check=False)
    (policy.ROOT/'tmp/adr0357-actual-image-helper.log').write_text(result.stderr,encoding='utf-8')
    assert result.returncode==0,'Inspect owned actual-image helper log'
    output=Path(target);assert output.resolve().is_relative_to((policy.ROOT/'tmp').resolve()) and not output.exists()
    output.write_bytes((json.dumps({'decision':'ADR0357',**json.loads(result.stdout),'runtime_modules_verified':22,'allocation_counts':allocation.contract(actual_allocated_goal(monkeypatch))['counts']},indent=2)+'\n').encode())


def actual_allocated_goal(monkeypatch):
    return allocated(monkeypatch)[0]



def test_200_requires_same_allocated_lower_configuration(monkeypatch):
    goal,_=allocated(monkeypatch)
    goal['rate_qualification_rate']=200
    goal['rate_qualification_reference']['rate']=180
    value=allocation.contract(goal)
    import cce_rate_qualification as qualification
    with pytest.raises(ValueError):qualification.require_previous(goal)  # Real160 report cannot stand in for180.
    lower=policy.read(policy.STATE)[goal['rate_qualification_reference']['ledger']]['binding']
    before=copy.deepcopy(lower)
    wanted=copy.deepcopy(before)
    wanted['cce_worker_placement_factor'].update(worker_counts=value['counts'],worker_resources=value['resources'])
    wanted['cce_writer_scale_contract'].update(decision='ADR0357',native_counts=value['counts'],worker_resources=value['resources'])
    with pytest.raises(ValueError,match='same worker allocation'):allocation.validate_binding(goal,wanted,before)
    before['cce_simulator_roundtrip_contract']['worker_allocation']=value
    assert allocation.validate_binding(goal,wanted,before)


def test_actual_parent_startup_and_projection_audit_preserved(monkeypatch):
    import cce_writer_scale_verification as verification
    from observe_paid_pipeline import pipeline_startup_view
    _,bundle=allocated(monkeypatch)
    import run_cce_paid_comparison as core
    contract=core.worker_contract()
    assert "result['kafka_members'] in (5,6)" in contract.global_audit
    assert "projection['assigned_partitions']==6" in contract.global_audit
    assert contract.background['consumer']['replicas']==6  # Common ECS bootstrap remains unchanged.
    row=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/candidate/pipeline-startup-row.private.json')
    row['consumer_replicas']=5
    row['writer_replicas']=5
    row['consumer_db_replicas'].pop(next(iter(row['consumer_db_replicas'])))
    row['writer_db_replicas']['synthetic-writer']=copy.deepcopy(next(iter(row['writer_db_replicas'].values())))
    contract.verify_pipeline_startup(row)
    assert verification.startup(row,bundle,pipeline_startup_view)['pass']


@pytest.mark.parametrize('suffix', ['', '/log?container=worker&limitBytes=65536', '/log?container=worker&sinceSeconds=60&limitBytes=65536'])
def test_actual_transport_allows_fifth_allocated_writer_and_rejects_extras(monkeypatch,suffix):
    import cce_api_adapter as adapter
    allocated(monkeypatch)
    monkeypatch.setattr(adapter.dependency,'kube_material',lambda path:{'client.key':'test'})
    calls=[]
    def call(*args,**kwargs):
        calls.append(args)
        return {'value':{'observed':True}}
    transport=adapter.KubernetesTransport(SimpleNamespace(call=call),Path('unused'))
    root='/api/v1/namespaces/flash-cce-'+'a'*12+'/pods/'
    for name in ['reservation-writer-4','consumer-4']:
        assert transport.request('GET',root+name+suffix)=={'observed':True}
    assert len(calls)==2
    for name in ['reservation-writer-5','consumer-5','reservation-writer-40','consumer-6']:
        with pytest.raises(ValueError,match='Bounded owned'):
            transport.request('GET',root+name+suffix)
    assert len(calls)==2
    monkeypatch.setattr(transaction,'active',lambda:None)
    with pytest.raises(ValueError,match='Bounded owned'):
        transport.request('GET',root+'reservation-writer-4'+suffix)
    assert len(calls)==2


@pytest.mark.parametrize('members,expected,passes',[(5,5,True),(6,5,False),(5,6,False),(6,6,True)])
def test_actual_shared_drain_enforces_exact_member_count(monkeypatch,members,expected,passes):
    import run_two_host_paid_comparison as common
    ticks=iter([0,61])
    monkeypatch.setattr(common.time,'monotonic',lambda:next(ticks))
    monkeypatch.setattr(common.time,'sleep',lambda _:None)
    queue={'pass':True,'kafka_members':members}
    session=SimpleNamespace(api=lambda *args:queue)
    if passes:
        assert common.drain(session,'cid','audit',expected_members=expected)==queue
    else:
        with pytest.raises(ValueError,match='global drain'):
            common.drain(session,'cid','audit',expected_members=expected)
    with pytest.raises(ValueError,match='topology'):
        common.drain(session,'cid','audit',expected_members=True)


def test_actual_native_inputs_safety_calls_five_member_drain(monkeypatch):
    from cce_paid_inputs import Inputs
    allocated(monkeypatch)
    retained=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/cce-comparison.private.json')['native']['safety']
    queue={**retained['global_queues'],'kafka_members':5}
    outputs=iter([retained['probe'],retained['post_ttl_financial'],queue])
    inputs=Inputs.__new__(Inputs)
    inputs.record={};inputs.directory='/owned';inputs.stage=SimpleNamespace(cid='cid')
    inputs.session=SimpleNamespace(api=lambda *args:next(outputs),cleanup_mode=False)
    inputs.global_audit='audit';inputs.fixture=lambda *args:{'show_ids':[str(uuid4())]}
    inputs.checkpoint=lambda:None
    assert all(inputs.safety([])['gates'].values())
    assert inputs.record['global_queues']['kafka_members']==5
    assert allocation.native_drain_options(inputs.session)=={'expected_members':5}
    inputs.session.cleanup_mode=True
    assert allocation.native_drain_options(inputs.session)=={}
    monkeypatch.setattr(transaction,'active',lambda:None)
    inputs.session.cleanup_mode=False
    assert allocation.native_drain_options(inputs.session)=={}


@pytest.mark.parametrize('cleanup,members',[(False,5),(True,6)])
def test_actual_paid_stage_audit_uses_native_then_restored_topology(monkeypatch,cleanup,members):
    import cce_paid_stage as stage
    import cce_terminal_audit_admission as terminal
    import cce_verification_contract as verification
    allocated(monkeypatch)
    monkeypatch.setattr(verification,'selected',lambda _:False)
    monkeypatch.setattr(terminal,'selected',lambda _:False)
    paid=stage.PaidStage.__new__(stage.PaidStage)
    paid.record={'customers_dispatched':True};paid.events=[str(uuid4())]
    paid.profile=SimpleNamespace(name='focused_audit',duration=1,expected=25200)
    paid.outcome_enabled=False;paid.cid='cid';paid.checkpoint=lambda:None
    financial={'pass':True,'hold_deadlines_elapsed':True}
    queue={'pass':True,'kafka_members':members}
    responses=iter([financial,queue])
    paid.session=SimpleNamespace(api=lambda *args:next(responses),cleanup_mode=cleanup)
    result=paid.audit('audit')
    assert result['post_ttl_financial_pass'] and result['global_queues_pass']
    assert paid.record['global_queues']['kafka_members']==members
