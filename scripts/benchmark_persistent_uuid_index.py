"""ADR0364 owned local index locality experiment; never connect to a cloud database."""
import json
import math
import os
import statistics
import time
from uuid import uuid4

import psycopg
from psycopg import sql

from ticketing.domain import persistent_uuid


def main():
    url=os.environ['TEST_DATABASE_URL']
    if not url.startswith('postgresql://postgres@qualified-postgres:5432/'):
        raise ValueError('Owned isolated local PostgreSQL host required')
    schema='uuid_index_'+uuid4().hex
    with psycopg.connect(url,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        try:
            conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            settings=dict(conn.execute("SELECT name,setting FROM pg_settings WHERE name IN ('shared_buffers','full_page_writes','synchronous_commit','wal_compression')").fetchall())
            assert settings['full_page_writes']=='on' and settings['synchronous_commit']=='on' and settings['wal_compression']=='pglz'
            conn.execute('CREATE TABLE entries(id uuid PRIMARY KEY,payload text NOT NULL) WITH (autovacuum_enabled=false)')
            conn.execute("INSERT INTO entries SELECT gen_random_uuid(),repeat(md5(g::text),4) FROM generate_series(1,1000000) AS g")
            before_bytes=conn.execute("SELECT pg_total_relation_size('entries'),pg_indexes_size('entries')").fetchone()
            samples={False:[],True:[]};wal={False:[],True:[]}
            for pair in range(16):
                for mode in ((False,True) if pair%2==0 else (True,False)):
                    os.environ['PERSISTENT_UUID_V7']='1' if mode else '0'
                    identifiers=[persistent_uuid() for _ in range(2000)]
                    conn.execute('CHECKPOINT')
                    start_lsn=conn.execute('SELECT pg_current_wal_insert_lsn()').fetchone()[0]
                    started=time.perf_counter()
                    with conn.transaction():  # noqa: SIM117 - show the measured durable transaction boundary
                        with conn.cursor().copy('COPY entries(id,payload) FROM STDIN') as copy:
                            for identifier in identifiers:copy.write_row((identifier,'x'*128))
                    samples[mode].append((time.perf_counter()-started)*1000)
                    wal[mode].append(int(conn.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s::pg_lsn)',(start_lsn,)).fetchone()[0]))
            versions=dict(conn.execute('SELECT get_byte(uuid_send(id),6)>>4,count(*) FROM entries GROUP BY 1').fetchall())
            assert versions=={4:1032000,7:32000}
            result={name:{'samples':16,'median_ms':statistics.median(samples[mode]),'p95_ms':sorted(samples[mode])[math.ceil(.95*16)-1],'median_wal_bytes':statistics.median(wal[mode]),'total_inserted':32000} for mode,name in ((False,'control'),(True,'candidate'))}
            result.update(local_only=True,cloud_calls=0,scope='Index-locality microbenchmark; not application or cloud throughput.',seed_uuid4_rows=1000000,batch_rows=2000,alternating_pairs=16,checkpoint_before_each_trial=True,checkpoint_time_excluded=True,seed_relation_bytes=before_bytes[0],seed_index_bytes=before_bytes[1],settings=settings,version_counts=versions,all_counts_verified=True)
            result['wal_reduction_percent']=100*(1-result['candidate']['median_wal_bytes']/result['control']['median_wal_bytes'])
            result['median_improvement_percent']=100*(1-result['candidate']['median_ms']/result['control']['median_ms'])
            print(json.dumps(result))
        finally:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


if __name__=='__main__':main()
