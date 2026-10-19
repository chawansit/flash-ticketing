"""Owned PostgreSQL query-model checks; not an application/financial/load audit."""
import contextlib
import io
import json
import os
import time
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
import pytest
from cce_guarded_issuance_proposal import proposed_program
from cce_hourly_window import QUERY
from psycopg import sql
from psycopg.conninfo import make_conninfo


@pytest.fixture
def model(monkeypatch):
    url=os.environ['TEST_DATABASE_URL']
    assert url.startswith('postgresql://postgres@qualified-postgres:5432/')
    schema='interval_'+uuid4().hex
    with psycopg.connect(url,autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    dsn=make_conninfo(url,options='-c search_path='+schema)
    monkeypatch.setenv('DATABASE_URL',dsn)
    try:
        with psycopg.connect(dsn,autocommit=True) as conn:
            conn.execute('CREATE TABLE orders(id uuid PRIMARY KEY,status text); CREATE TABLE bookings(id uuid PRIMARY KEY,order_id uuid,event_id uuid); CREATE TABLE payment_attempts(id uuid PRIMARY KEY,order_id uuid,status text); CREATE TABLE tickets(id uuid PRIMARY KEY,booking_id uuid,issued_at timestamptz)')
            yield conn
    finally:
        with psycopg.connect(url,autocommit=True) as admin:
            admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def seed(conn,rate):
    events=[str(UUID(int=10_000_000+i)) for i in range(rate)]
    start=int(time.time())-1000
    count=rate*300
    for table in ('orders','bookings','payment_attempts','tickets'):
        with conn.cursor().copy(f'COPY {table} FROM STDIN') as copy:
            for i in range(count):
                identity=UUID(int=i+1)
                if table=='orders':row=(identity,'FULFILLED')
                elif table=='bookings':row=(identity,identity,events[i%rate])
                elif table=='payment_attempts':row=(identity,identity,'SUCCEEDED')
                else:row=(identity,identity,datetime.fromtimestamp(start+i/rate+.25,UTC))
                copy.write_row(row)
        conn.execute('ANALYZE '+table)
    return events,start


def execute(source):
    output=io.StringIO()
    with contextlib.redirect_stdout(output):
        exec(compile(source,'owned-query-model','exec'),{})  # noqa: S102 - exact generated local query, no external code
    return json.loads(output.getvalue())


@pytest.mark.parametrize('rate',[180,200])
def test_actual_sql_uniform_stream_and_identical_guard_counts(model,rate):
    events,start=seed(model,rate)
    original=QUERY.replace('3598','298').replace('3600','300').replace('300000',str(rate*300-600)).replace('302400',str(rate*300))
    before=execute('events='+repr(events)+'\nstart='+repr(start)+'\n'+original)
    after=execute(proposed_program(events,start,rate))
    assert before['pass'] is False and after['pass'] is True
    assert before['conservative_inner_window_tickets']==after['conservative_inner_window_tickets']==rate*296
    assert before['unique_paid_issued_tickets']==after['unique_paid_issued_tickets']
    assert after['guarded_window_tickets_per_second']==rate and after['inner_window_guard_seconds']==2


@pytest.mark.parametrize('fault',['low_full','low_inner','duplicate_join'])
def test_actual_sql_still_rejects_each_required_count_constraint(model,fault):
    events,start=seed(model,180)
    if fault=='low_full':
        model.execute('UPDATE tickets SET issued_at=to_timestamp(%s+301) WHERE id IN (SELECT id FROM tickets ORDER BY id LIMIT 1000)',(start,))
    elif fault=='low_inner':
        model.execute('UPDATE tickets SET issued_at=to_timestamp(%s+.5) WHERE id IN (SELECT id FROM tickets ORDER BY id LIMIT 2000)',(start,))
    else:
        model.execute("INSERT INTO payment_attempts SELECT %s,id,'SUCCEEDED' FROM orders WHERE id=%s",(uuid4(),UUID(int=1001)))
    assert execute(proposed_program(events,start,180))['pass'] is False
