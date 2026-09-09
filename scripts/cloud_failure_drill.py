"""Bounded service outages on the explicitly named isolated benchmark stack."""
import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

COMPOSE = ['docker','compose','-p','flash-cloud-bench','-f','compose.yaml','-f','compose.private.yaml',
           '-f','compose.reconciler.yaml','-f','compose.ingress.yaml']


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--seat-offset',type=int,default=290)
    a=p.parse_args()
    if a.output.exists():p.error('Use a fresh result path')
    if not 0 <= a.seat_offset <= 297:p.error('Three fixture seats must fit the inventory')
    seats=[f'S{a.seat_offset+i}' for i in range(3)]
    m=json.loads(a.manifest.read_text())
    if m.get('environment')!='development':p.error('Development fixture required')
    if datetime.fromisoformat(m['expires_at']) <= datetime.now(UTC)+timedelta(minutes=15):
        p.error('Refresh development credentials before the drill')
    origin=m['origin']; token=m['viewer_tokens'][0]; event=m['show_ids'][0]
    report={'started_utc':datetime.now(UTC).isoformat(),'event_id':event,'seat_ids':seats,'cases':[]}
    def save():a.output.write_text(json.dumps(report,indent=2)+'\n')
    def request(method,path,body=None,key=None):
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        if key:headers['Idempotency-Key']=key
        r=urllib.request.Request(origin+path,data=json.dumps(body).encode() if body is not None else None,
                                 headers=headers,method=method)
        started=time.monotonic()
        try:
            with urllib.request.urlopen(r,timeout=10) as response:
                status=response.status; payload=response.read()
        except urllib.error.HTTPError as exc:
            status=exc.code; payload=exc.read()
        return {'status':status,'body':json.loads(payload),'milliseconds':(time.monotonic()-started)*1000}
    def compose(*args):
        subprocess.run(COMPOSE+list(args),check=True,timeout=120,stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    def ready():
        started=time.monotonic()
        while time.monotonic()-started<120:
            try:
                if request('GET','/health/ready')['status']==200:return time.monotonic()-started
            except (urllib.error.URLError,TimeoutError):pass
            time.sleep(1)
        raise AssertionError('Readiness did not recover in 120s')
    def db(query,params):
        code='import os,sys,json,psycopg\nwith psycopg.connect(os.environ["DATABASE_URL"]) as c:\n c.execute("SET TRANSACTION READ ONLY")\n c.execute("SET LOCAL statement_timeout = \'20s\'")\n print(json.dumps(c.execute(sys.argv[1],json.loads(sys.argv[2])).fetchall(),default=str))'
        output=subprocess.check_output(COMPOSE+['exec','-T','api','python','-c',code,query,json.dumps(params)],
                                       timeout=40,stdin=subprocess.DEVNULL,text=True)
        return json.loads(output)
    def hold(seat,key):return request('POST','/v1/holds',{'event_id':event,'seat_ids':[seat]},key)
    def order(order_id):return request('GET',f'/v1/orders/{order_id}')
    save()
    ready()
    report['unused_seat_preflight']=db("SELECT count(*),count(*) FILTER (WHERE booked_order_id IS NOT NULL OR reserved_until>clock_timestamp()) FROM event_seats WHERE event_id=%s AND seat_id=ANY(%s)",[event,seats])
    save()
    assert report['unused_seat_preflight']==[[3,0]],report['unused_seat_preflight']
    for service,seat in [('redis',seats[0]),('postgres',seats[1])]:
        case={'service':service,'started_utc':datetime.now(UTC).isoformat()}
        report['cases'].append(case);save()
        key='fault-'+uuid4().hex
        try:
            try:
                compose('stop','-t','10',service)
                time.sleep(2)
                result=hold(seat,key)
                case['during_outage']=result
                save()
                assert result['status']==503,result
            finally:
                compose('start',service)
            case['readiness_recovery_seconds']=ready()
            case['failed_key_records_before_retry']=db("SELECT count(*) FROM idempotency_records WHERE operation='hold' AND key=%s",[key])[0][0]
            assert case['failed_key_records_before_retry']==0
            first=hold(seat,key);case['after_recovery']=first;save()
            assert first['status']==201,first
            repeated=hold(seat,key);case['idempotent_replay']=repeated
            assert repeated['status']==201 and repeated['body']==first['body'],repeated
            conflict=hold(seat,'conflict-'+uuid4().hex);case['conflict']=conflict
            assert conflict['status']==409 and conflict['body']['code'] in ('SEAT_BUSY','SEAT_UNAVAILABLE'),conflict
            owners=db("SELECT count(*) FROM event_seats WHERE event_id=%s AND seat_id=%s AND hold_id=%s AND reserved_until>clock_timestamp()",[event,seat,first['body']['hold_id']])[0][0]
            assert owners==1
            case['one_current_owner']=True;case['pass']=True
        except BaseException as exc:
            case['pass']=False;case['failure_type']=type(exc).__name__;save();raise
        finally:save()
    case={'service':'kafka','started_utc':datetime.now(UTC).isoformat()}
    report['cases'].append(case);save()
    held=hold(seats[2],'fault-payment-'+uuid4().hex)
    assert held['status']==201,held
    order_id=held['body']['order_id'];case['order_id']=order_id
    try:
        try:
            compose('stop','-t','10','kafka')
            payment=request('POST',f'/v1/orders/{order_id}/payments',{'duplicates':5,'delay_seconds':0},'payment-'+uuid4().hex)
            assert payment['status']==202,payment
            case['payment']=payment;save()
            deadline=time.monotonic()+40
            current=order(order_id)
            while current['body'].get('status')!='PAID' and time.monotonic()<deadline:
                time.sleep(.5);current=order(order_id)
            case['during_outage']=current;save()
            assert current['body']['status']=='PAID' and current['body']['tickets']==[],current
            case['pending_paid_events']=db("SELECT count(*) FROM outbox_events WHERE aggregate_id=%s AND event_type='OrderPaid' AND published_at IS NULL",[order_id])[0][0]
            assert case['pending_paid_events']==1
        finally:compose('start','kafka')
        started=time.monotonic()
        deadline=started+150
        while time.monotonic()<deadline:
            current=order(order_id)
            if current['body'].get('status')=='FULFILLED':break
            time.sleep(.5)
        case['fulfillment_recovery_seconds']=time.monotonic()-started
        case['after_recovery']=current
        assert current['body']['status']=='FULFILLED' and len(current['body']['tickets'])==1,current
        while time.monotonic()<deadline:
            rows=db("SELECT deliveries,target_deliveries FROM payment_attempts WHERE order_id=%s",[order_id])
            if rows[0][0]>=5:break
            time.sleep(.5)
        case['deliveries']=rows[0][0];case['target_deliveries']=rows[0][1]
        assert rows==[[5,5]],rows
        counts=db("SELECT (SELECT count(*) FROM bookings WHERE order_id=%s),(SELECT count(*) FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE b.order_id=%s),(SELECT count(*) FROM payment_callbacks c JOIN payment_attempts p ON p.id=c.payment_id WHERE p.order_id=%s),(SELECT count(*) FROM outbox_events WHERE aggregate_id=%s AND event_type='OrderPaid')",[order_id]*4)[0]
        case['booking_ticket_callback_paid_event_counts']=counts
        assert counts==[1,1,1,1],counts
        case['pass']=True
    except BaseException as exc:
        case['pass']=False;case['failure_type']=type(exc).__name__;save();raise
    finally:save()
    report['completed_utc']=datetime.now(UTC).isoformat()
    report['pass']=all(c['pass'] for c in report['cases']);save()
    print(json.dumps({'pass':report['pass'],'cases':[{k:c[k] for k in ('service','pass')} for c in report['cases']]}))


if __name__=='__main__':main()
