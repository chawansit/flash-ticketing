"""ADR0197 real PostgreSQL checks; isolated schema supplied by integration fixture."""
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import worker_separation_audits as audit

pytestmark = pytest.mark.integration


@pytest.fixture
def paid(system):
    _, db, event = system
    conn = psycopg.connect(db.pool.conninfo,autocommit=True)
    orders=[];holds=[];bookings=[]
    with conn.transaction():
        for seat in ('A','B'):
            hold,order,payment,booking,ticket = (uuid4() for _ in range(5))
            holds.append(hold);orders.append(order);bookings.append(booking)
            conn.execute("INSERT INTO holds VALUES (%s,%s,%s,clock_timestamp()-interval '10 seconds','CONSUMED')",(hold,'buyer-'+seat,event))
            conn.execute("INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status) VALUES (%s,%s,%s,%s,100,'THB','FULFILLED')",(order,'buyer-'+seat,hold,event))
            conn.execute('INSERT INTO order_items VALUES (%s,%s,%s,100)',(order,event,seat))
            conn.execute('UPDATE event_seats SET booked_order_id=%s WHERE event_id=%s AND seat_id=%s',(order,event,seat))
            conn.execute('INSERT INTO bookings VALUES (%s,%s,%s,%s)',(booking,event,seat,order))
            conn.execute('INSERT INTO tickets(id,booking_id) VALUES (%s,%s)',(ticket,booking))
            conn.execute("INSERT INTO payment_attempts(id,order_id,status,outcome,due_at,deliveries,target_deliveries) VALUES (%s,%s,'SUCCEEDED','SUCCEEDED',clock_timestamp(),3,3)",(payment,order))
            conn.execute("INSERT INTO payment_callbacks(id,payment_id,payload_hash) VALUES (%s,%s,'test')",(uuid4(),payment))
    try:yield conn,event,orders,holds,bookings
    finally:conn.close()


def read(paid):
    conn,event,*_=paid
    return audit.financial_snapshot(conn,[str(event)],2,2,3)


def test_complete_committed_paid_and_issued_relationships_pass(paid):
    result=read(paid)
    assert result['pass'] and all(result['checks'].values())
    assert result['relationships']['unique_issued_tickets']==2
    assert paid[0].execute('SHOW transaction_read_only').fetchone()[0]=='off'


@pytest.mark.parametrize('fault',['actor','hold_status','item','claim','extra_claim','future_ttl','missing_ticket','payment','total','currency'])
def test_equal_aggregate_counts_do_not_hide_relationship_corruption(paid,fault):
    conn,event,orders,holds,bookings=paid
    if fault=='actor':conn.execute("UPDATE holds SET actor='wrong-customer' WHERE id=%s",(holds[0],))
    elif fault=='hold_status':conn.execute("UPDATE holds SET status='EXPIRED' WHERE id=%s",(holds[0],))
    elif fault=='item':conn.execute("UPDATE order_items SET seat_id='C' WHERE order_id=%s",(orders[0],))
    elif fault=='claim':conn.execute('UPDATE event_seats SET booked_order_id=NULL WHERE event_id=%s AND seat_id=\'A\'',(event,))
    elif fault=='extra_claim':conn.execute("UPDATE event_seats SET booked_order_id=%s WHERE event_id=%s AND seat_id='C'",(orders[0],event))
    elif fault=='future_ttl':conn.execute("UPDATE holds SET expires_at=clock_timestamp()+interval '1 hour' WHERE id=%s",(holds[0],))
    elif fault=='missing_ticket':conn.execute('DELETE FROM tickets WHERE booking_id=%s',(bookings[0],))
    elif fault=='total':conn.execute('UPDATE orders SET total=110 WHERE id=%s',(orders[0],))
    elif fault=='currency':conn.execute("UPDATE orders SET currency='USD' WHERE id=%s",(orders[0],))
    else:conn.execute("UPDATE payment_attempts SET outcome='FAILED' WHERE order_id=%s",(orders[0],))
    result=read(paid)
    assert not result['pass']
    if fault not in {'missing_ticket'}:
        assert result['counts']['pass'], 'Aggregate totals alone should miss this corruption'


def test_loss_is_not_certified_by_reducing_expected_count(paid):
    result=audit.financial_snapshot(paid[0],[str(paid[1])],3,3,3)
    assert not result['pass'] and not result['checks']['payments_durable']


def test_global_duplicate_audit_is_independent_of_fixture(paid):
    conn,event,orders,_,_=paid
    assert audit.duplicate_snapshot(conn)['zero_double_booking']
    # The schema is isolated; remove only this test's protection to represent legacy corrupt data.
    conn.execute('ALTER TABLE bookings DROP CONSTRAINT bookings_event_id_seat_id_key')
    conn.execute("INSERT INTO bookings VALUES (%s,%s,'A',%s)",(uuid4(),event,orders[0]))
    assert audit.duplicate_snapshot(conn)=={'duplicate_booked_seats':1,'zero_double_booking':False}


def test_same_snapshot_survives_concurrent_commit_between_queries(paid):
    conn,event,_,_,_=paid
    changed=[]
    class Interleave:
        def transaction(self):return conn.transaction()
        def execute(self,query,params=None):
            value=conn.execute(query,params)
            if query.lstrip().startswith('SELECT') and not changed:
                with psycopg.connect(conn.info.dsn,autocommit=True) as other:
                    other.execute("UPDATE event_seats SET booked_order_id=NULL WHERE event_id=%s AND seat_id='A'",(event,))
                changed.append(True)
            return value
    result=audit.financial_snapshot(Interleave(),[str(event)],2,2,3)
    assert changed and result['pass'], 'Both checks must see one committed snapshot'
    assert not read(paid)['pass'], 'A subsequent snapshot must see the committed corruption'


def test_generated_financial_program_runs_against_postgresql(paid):
    conn,event,*_=paid
    body=audit.financial_body({'show_ids':[str(event)],'expected_orders':2,'expected_paid':2,'callbacks':3})
    result=subprocess.run([sys.executable,'-'],input=body,text=True,capture_output=True,check=False,
                          env={**os.environ,'DATABASE_URL':conn.info.dsn},timeout=30)
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['pass']


def test_generated_financial_program_rejects_unbounded_ttl(paid):
    conn,_,_,holds,_=paid
    conn.execute("UPDATE holds SET expires_at=clock_timestamp()+interval '1 hour' WHERE id=%s",(holds[0],))
    body=audit.financial_body({'show_ids':[str(paid[1])],'expected_orders':2,'expected_paid':2,'callbacks':3})
    result=subprocess.run([sys.executable,'-'],input=body,text=True,capture_output=True,check=False,
                          env={**os.environ,'DATABASE_URL':conn.info.dsn},timeout=30)
    assert result.returncode!=0 and 'Cohort TTL exceeds bounded audit window' in result.stderr


@pytest.mark.parametrize('fault',['none','before_drift','replaced','after_drift','query_failure','alarm_timeout'])
def test_generated_transport_on_real_linux_with_simulated_docker(paid,fault):
    # Real Linux executes the exact generated program and PostgreSQL query;
    # Docker observations/commands inside that host program are simulated.
    spec=importlib.util.spec_from_file_location('audit_transport_fixture',ROOT/'tests/unit/test_worker_separation_execution.py')
    fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
    saved,rows,_,_=fixture.fixture()
    before={'rows':rows,'volumes':[saved['broker_volume']],'bind_sha256':saved['bind_sha256']}
    target=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']=='api')
    body=audit.duplicate_body()
    if fault=='query_failure':body="raise ValueError('Synthetic query failure')"
    elif fault=='alarm_timeout':body='import time;time.sleep(20)'
    code=audit.transport_program(saved,before,audit.row_identity(target),body,8 if fault=='alarm_timeout' else 60)
    start=code.index('def observe():');end=code.index('\ndef bindings(',start)
    code=code[:start]+"def observe():return simulated_observe()\n"+code[end:]
    from psycopg.conninfo import make_conninfo
    network_container=os.environ.get('ADR0197_TEST_POSTGRES_CONTAINER')
    if not network_container:pytest.skip('Isolated Docker PostgreSQL network not configured')
    dsn=make_conninfo(paid[0].info.dsn,host='127.0.0.1',port='5432')
    prelude="import copy,json,os,subprocess,sys\nfrom unittest.mock import patch\n"
    prelude+='before='+repr(before)+'\ntarget='+repr(target)+'\nfault='+repr(fault)+'\ndsn='+repr(dsn)+'\n'
    prelude+=r"""
observations=0
original_run=subprocess.run
def simulated_observe():
 global observations
 observations+=1
 value=copy.deepcopy(before)
 if fault=='before_drift' or fault=='after_drift' and observations>1:value['rows'][0]['State']['StartedAt']='2026-10-08T00:00:00+00:00'
 return value
def run(argv,**kwargs):
 if argv[:2]==['docker','exec']:
  result=original_run([sys.executable,'-'],env={**os.environ,'DATABASE_URL':dsn},**kwargs)
  if result.returncode and fault=='none':raise AssertionError(result.stderr)
  return result
 raise AssertionError('Unexpected remote command')
def check_output(argv,**kwargs):
 if argv==['docker','inspect',target['Id']]:
  value=copy.deepcopy(target)
  if fault=='replaced':value['State']['StartedAt']='2026-10-08T00:00:00+00:00'
  return json.dumps([value])
 raise AssertionError('Unexpected remote observation')
"""
    script=prelude+'\ncode='+repr(code)+r"""
with patch.object(subprocess,'run',run),patch.object(subprocess,'check_output',check_output):
 try:
  exec(compile(code,'generated-audit-transport','exec'),{'simulated_observe':simulated_observe})
 except (ValueError,RuntimeError) as exc:
  if fault=='none':raise
  print(json.dumps({'expected_fault':fault,'exception_type':type(exc).__name__}))
 else:
  if fault!='none':raise AssertionError('Fault was not detected')
"""
    image='sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e'
    from stage_status_refresh_images import new_stage_output
    owner=new_stage_output().name
    result=subprocess.run(['docker','run','--rm','--pull=never','--network','container:'+network_container,'--name',owner+'-audit-native','--label','ticketing.local-financial-audit='+owner,'-i','--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=8m',
                           '--cap-drop','ALL','--security-opt','no-new-privileges','--memory','256m','--cpus','0.5',
                           '--entrypoint','python',image,'-'],input=script,text=True,capture_output=True,check=False,timeout=40)
    remaining=subprocess.check_output(['docker','ps','-aq','--filter','label=ticketing.local-financial-audit='+owner],text=True).strip()
    assert not remaining,'Owned native audit container must be removed'
    assert result.returncode==0,result.stderr[-2000:]
    receipt=json.loads(result.stdout)
    if fault=='none':assert receipt['zero_double_booking']
    else:assert receipt['expected_fault']==fault


def test_generated_program_waits_for_database_hold_expiry(paid):
    import time
    conn,_,_,holds,_=paid
    conn.execute("UPDATE holds SET expires_at=clock_timestamp()+interval '0.4 seconds' WHERE id=%s",(holds[0],))
    body=audit.financial_body({'show_ids':[str(paid[1])],'expected_orders':2,'expected_paid':2,'callbacks':3})
    started=time.monotonic()
    result=subprocess.run([sys.executable,'-'],input=body,text=True,capture_output=True,check=False,
                          env={**os.environ,'DATABASE_URL':conn.info.dsn},timeout=30)
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['checks']['post_ttl_complete']
    assert time.monotonic()-started>=0.4
