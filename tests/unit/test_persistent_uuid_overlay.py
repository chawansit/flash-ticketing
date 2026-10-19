"""ADR0364 exact five-module scope and preserved SQL/TTL/token behavior."""
import ast
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from persistent_uuid_overlay import MODULES, overlay


def sources():
    root=Path(__file__).resolve().parents[2]
    receipt=root/'tmp/adr0363-local-image.private.json'
    if not receipt.exists():pytest.skip('Verified projection parent payload required')
    meta=json.loads(receipt.read_text())
    base=Path(meta['payload_path'])
    expected=meta['runtime_source_sha256']
    before={n:(base/n).read_bytes() for n in expected}
    return before,overlay(before,expected)


def test_exact_reviewed_modules_and_original_token_modules():
    before,after=sources()
    assert {n for n in before if before[n]!=after[n]}==MODULES
    for name in ('http.py','infrastructure/cache.py','infrastructure/payment_confirmation.py','infrastructure/postgres.py'):
        key='src/ticketing/'+name
        assert before[key]==after[key]


def test_every_sql_statement_remains_identical():
    before,after=sources()
    def statements(raw):
        return [n.value for n in ast.walk(ast.parse(raw)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and n.value.lstrip().upper().startswith(('SELECT ','INSERT ','UPDATE ','DELETE ','WITH ','BEGIN','SET '))]
    for name in before:assert statements(before[name])==statements(after[name])


def test_lease_and_projection_functions_remain_identical():
    before,after=sources()
    def functions(raw):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)}
    a,b=functions(before['src/ticketing/workers.py']),functions(after['src/ticketing/workers.py'])
    assert {n for n in a if a[n]!=b[n]}=={'_apply_consumer_event'}
    assert a['consume_refresh_batch']==b['consume_refresh_batch']
    old=functions(before['src/ticketing/domain.py']);new=functions(after['src/ticketing/domain.py'])
    assert all(new[n]==v for n,v in old.items())
