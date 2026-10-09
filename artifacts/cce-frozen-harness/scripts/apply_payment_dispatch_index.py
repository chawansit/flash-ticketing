"""Bounded concurrent build/verification of the additive payment dispatch index."""

import argparse
import json
import os
from pathlib import Path

import psycopg

INDEX_SQL = """CREATE INDEX CONCURRENTLY payment_attempts_dispatch_due
ON payment_attempts(due_at) WHERE deliveries < target_deliveries"""


def inspect(conn):
    row = conn.execute("""SELECT i.indisvalid,i.indisready,a.amname,i.indisunique,
        pg_get_indexdef(i.indexrelid,1,true),pg_get_expr(i.indpred,i.indrelid),i.indnatts,
        i.indrelid=to_regclass('payment_attempts')
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am a ON a.oid=c.relam
        WHERE i.indexrelid=to_regclass('payment_attempts_dispatch_due')""").fetchone()
    if row is None:
        return None
    valid, ready, method, unique, key, predicate, columns, table = row
    return {'valid': valid, 'ready': ready, 'method': method, 'unique': unique, 'key': key,
            'predicate': predicate, 'columns': columns, 'expected_table': table,
            'pass': (valid and ready and method == 'btree' and not unique and key == 'due_at'
                     and predicate == '(deliveries < target_deliveries)' and columns == 1 and table)}


def apply(conn, verify_only=False):
    if not conn.autocommit:
        raise ValueError('Concurrent index operation requires autocommit')
    conn.execute("SET statement_timeout='60s'")
    conn.execute("SET lock_timeout='1s'")
    before = inspect(conn)
    created = False
    if before is not None and not before['pass']:
        raise RuntimeError('Existing dispatch index is invalid or unexpected; action refused')
    if before is None and not verify_only:
        conn.execute(INDEX_SQL)
        created = True
    after = inspect(conn)
    return {'created': created, 'verification_only': verify_only, 'index': after,
            'pass': after is not None and after['pass'],
            'migration': '007_payment_dispatch_index.sql',
            'note': 'No rows modified; normal migration may subsequently record its IF NOT EXISTS checksum.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Fresh output required')
    url = os.getenv('TEST_DATABASE_URL')
    if not url:
        raise RuntimeError('TEST_DATABASE_URL required')
    with psycopg.connect(url, autocommit=True) as conn:
        result = apply(conn, args.verify_only)
    args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))
    if not result['pass']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
