"""ADR0362 isolated PostgreSQL comparison; never uses cloud credentials or traffic."""
import json
import math
import os
import re
import statistics
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ticketing.infrastructure.postgres import MeasuredCursor, Postgres
from ticketing.infrastructure.reservations import PostgresReservations, digest


def main():
    url = os.environ["TEST_DATABASE_URL"]
    schema = "bench_" + uuid4().hex
    with psycopg.connect(url,autocommit=True) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    db = None
    original = MeasuredCursor.execute
    try:
        db = Postgres(make_conninfo(url,options=f"-c search_path={schema}"),maximum=4,explicit_start=True)
        db.pool.wait(timeout=10)
        events = [str(uuid4()) for _ in range(20)]
        warm, pairs = 10, 80
        commands = (warm+pairs)*8+16
        with db.transaction() as conn:
            for migration in sorted(Path("/app/migrations").glob("*.sql")):
                conn.execute(migration.read_text())
            for event in events:
                conn.execute("INSERT INTO events VALUES (%s,'Benchmark','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 hour')",(event,))
            for i in range(commands):
                conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)",(events[i%20],"S"+str(i)))
        calls = [0]
        trace_file = [None]
        trace_connection = [None]

        def execute(cursor,query,params=None,**kwargs):
            calls[0] += 1
            if trace_file[0] is not None and trace_connection[0] is None:
                cursor.connection.pgconn.trace(trace_file[0].fileno())
                trace_connection[0] = cursor.connection
            return original(cursor,query,params,**kwargs)

        MeasuredCursor.execute = execute
        calibration = []
        with db.connection() as conn:
            for _ in range(20):
                started = time.perf_counter()
                assert conn.execute("SELECT 1 AS n").fetchone()["n"] == 1
                calibration.append((time.perf_counter()-started)*1000)
        stores = {mode:PostgresReservations(db,None,persist_write_pipeline=True,persist_lookup_claim=mode)
                  for mode in (False,True)}
        samples = {mode:[] for mode in stores}
        queries = {mode:[] for mode in stores}
        index = 0

        def trial(mode):
            nonlocal index
            batch = []
            for _ in range(4):
                event,seat = events[index%20],"S"+str(index)
                expiry = (datetime.now(UTC)+timedelta(seconds=120)).isoformat()
                payload = {"event_id":event,"seat_ids":[seat],"actor":"bench-"+str(index),
                           "idempotency_key":"key","command_id":str(uuid4()),"hold_id":str(uuid4()),
                           "order_id":str(uuid4()),"expires_at":expiry,
                           "request_hash":digest({"event_id":event,"seats":[seat]})}
                batch.append((payload,{"hold_id":payload["hold_id"],"order_id":payload["order_id"],
                                       "expires_at":expiry,"seats":[seat]}))
                index += 1
            before = calls[0]
            started = time.perf_counter()
            outcomes = stores[mode].persist_reservation_commands(batch)
            elapsed = (time.perf_counter()-started)*1000
            assert all(state=="durable" for state,_ in outcomes)
            return elapsed,calls[0]-before

        for pair in range(warm+pairs):
            for mode in ((False,True) if pair%2==0 else (True,False)):
                elapsed,count = trial(mode)
                if pair >= warm:
                    samples[mode].append(elapsed)
                    queries[mode].append(count)
        protocol = []
        for mode in (False,True,True,False):
            with tempfile.TemporaryFile() as stream:
                trace_file[0],trace_connection[0] = stream,None
                try:
                    trial(mode)
                finally:
                    if trace_connection[0] is not None:
                        trace_connection[0].pgconn.untrace()
                    trace_file[0],trace_connection[0] = None,None
                stream.seek(0)
                raw = stream.read()
            protocol.append({"candidate":mode,"ReadyForQuery":raw.count(b"ReadyForQuery"),
                             "Flush":len(re.findall(rb"\bFlush\b",raw)),
                             "Sync":len(re.findall(rb"\bSync\b",raw))})
        MeasuredCursor.execute = original
        with db.transaction() as conn:
            counts = {table:conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
                      for table in ("holds","orders","reservation_commands","idempotency_records","outbox_events")}
        assert all(n==commands for n in counts.values())
        summary = {}
        for mode,values in samples.items():
            summary["candidate" if mode else "control"] = {
                "samples":len(values),"median_ms":statistics.median(values),
                "p95_ms":sorted(values)[math.ceil(.95*len(values))-1],
                "mean_application_queries":statistics.mean(queries[mode])}
        summary.update(postgres_version=version,psycopg_version=psycopg.__version__,
                       batch_size=4,write_pipeline=True,
                       network_delay_injected=float(os.getenv("QUALIFY_WIRE_ONE_WAY_MS","0"))>0,
                       synthetic_one_way_ms=float(os.getenv("QUALIFY_WIRE_ONE_WAY_MS","0")),
                       select1_median_ms=statistics.median(calibration),
                       protocol_trials_excluded_from_timing=protocol,
                       new_commands_only=True,local_only=True,financial_row_counts=counts,
                       median_improvement_percent=100*(summary["control"]["median_ms"]-summary["candidate"]["median_ms"])/summary["control"]["median_ms"])
        print(json.dumps(summary))
    finally:
        MeasuredCursor.execute = original
        if db is not None:
            db.close()
        with psycopg.connect(url,autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
