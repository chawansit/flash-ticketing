"""ADR0363 alternating real PostgreSQL comparison; cloud access forbidden."""
import inspect
import json
import math
import os
import statistics
import time
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ticketing import workers
from ticketing.infrastructure.postgres import Postgres


def main():
    url=os.environ['TEST_DATABASE_URL']
    schema='bench_'+uuid4().hex
    with psycopg.connect(url,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    db=None
    try:
        db=Postgres(make_conninfo(url,options=f'-c search_path={schema}'),maximum=4,explicit_start=True)
        db.pool.wait(timeout=10)
        targets=[str(uuid4()) for _ in range(32)]
        with db.transaction() as conn:
            for path in sorted(Path('/app/migrations').glob('*.sql')):
                conn.execute(path.read_text())
            for show in targets:
                conn.execute("INSERT INTO events VALUES (%s,'Benchmark','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')",(show,))
        source=inspect.getsource(workers.consume_refresh_batch)
        assert source.count('with db.transaction(pipeline=True) as conn:')==1
        namespace=dict(vars(workers))
        exec(compile(source.replace('with db.transaction(pipeline=True) as conn:','with db.transaction() as conn:'),'<exact-parent-control>','exec'),namespace)  # noqa: S102 - exact verified image source, no external code
        methods={False:namespace['consume_refresh_batch'],True:workers.consume_refresh_batch}
        calibration=[]
        for _ in range(20):
            with db.connection() as conn:
                start=time.perf_counter();conn.execute('SELECT 1').fetchone();calibration.append((time.perf_counter()-start)*1000)
        samples={False:[],True:[]}
        for pair in range(90):
            for mode in ((False,True) if pair%2==0 else (True,False)):
                batch=[{'schema_version':1,'event_id':str(uuid4()),'event_type':'SeatsChanged','payload':{'event_id':show,'seats':['A']}} for show in reversed(targets)]
                start=time.perf_counter()
                assert methods[mode](db,batch)==32
                elapsed=(time.perf_counter()-start)*1000
                if pair>=10:samples[mode].append(elapsed)
        with db.transaction() as conn:
            rows=conn.execute('SELECT generation,seat_ids FROM seat_refresh_requests').fetchall()
            inbox=conn.execute('SELECT count(*) AS n FROM consumer_inbox').fetchone()['n']
        assert len(rows)==32 and all(row['generation']==180 and row['seat_ids']==['A'] for row in rows)
        assert inbox==5760
        summary={name:{'samples':len(samples[mode]),'median_ms':statistics.median(samples[mode]),'p95_ms':sorted(samples[mode])[math.ceil(.95*len(samples[mode]))-1]} for mode,name in ((False,'control'),(True,'candidate'))}
        summary.update(batch_shows=32,alternating_pairs=80,select1_median_ms=statistics.median(calibration),synthetic_one_way_ms=float(os.getenv('QUALIFY_WIRE_ONE_WAY_MS','0')),inbox_rows=inbox,refresh_rows=len(rows),all_generations_correct=True,local_only=True)
        summary['median_improvement_percent']=100*(1-summary['candidate']['median_ms']/summary['control']['median_ms'])
        print(json.dumps(summary))
    finally:
        if db is not None:db.close()
        with psycopg.connect(url,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


if __name__=='__main__':main()
