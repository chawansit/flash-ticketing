"""ADR0363 exact immutable-parent scope, no SQL/locking/TTL edits."""
import ast
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from projection_refresh_pipeline_overlay import MODULES, overlay


def test_exact_one_call_change_over_complete_parent():
    root=Path(__file__).resolve().parents[2]
    receipt=json.loads((root/'docs/capacity/cce/callback-write-pipeline-registry-2026-10-17.json').read_text())
    base=root/'tmp/adr0356-deployed-source'
    if not base.exists():pytest.skip('Retained immutable parent export required')
    original={n:(base/n).read_bytes() for n in receipt['runtime_source_sha256']}
    result=overlay(original,receipt['runtime_source_sha256'])
    assert MODULES=={n for n in result if result[n]!=original[n]}=={'src/ticketing/workers.py'}
    before=ast.parse(original['src/ticketing/workers.py'])
    after=ast.parse(result['src/ticketing/workers.py'])
    old=next(n for n in before.body if isinstance(n,ast.FunctionDef) and n.name=='consume_refresh_batch')
    new=next(n for n in after.body if isinstance(n,ast.FunctionDef) and n.name=='consume_refresh_batch')
    calls=[n for n in ast.walk(new) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='transaction']
    assert len(calls)==1 and len(calls[0].keywords)==1
    assert calls[0].keywords[0].arg=='pipeline' and calls[0].keywords[0].value.value is True
    calls[0].keywords=[]
    assert ast.dump(old,include_attributes=False)==ast.dump(new,include_attributes=False)


def test_unverified_parent_is_rejected():
    with pytest.raises(ValueError):overlay({}, {})
