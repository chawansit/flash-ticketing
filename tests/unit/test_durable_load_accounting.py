import json
import runpy
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("overlaps,audited,passes", [(0,10,True), (1,10,False), (0,9,False)])
def test_nested_contention_and_mixed_acknowledgements(tmp_path,monkeypatch,overlaps,audited,passes):
 results=tmp_path/'runs';results.mkdir()
 for name,result in {'wave':{'run_id':'a','seat_id':'S1','statuses':{'hold':{'201':1,'409':7,'503':92}}},'mixed':{'run_id':'b','statuses':{'hold':{'201':8},'hot_hold':{'201':1,'409':1}}},'failed':{'run_id':'c','seat_id':'S2','statuses':{'hold':{'500':1}}}}.items():
  (results/(name+'.json')).write_text(json.dumps(result))
 statements=[]
 class Connection:
  def __enter__(self):return self
  def __exit__(self,*args):pass
  def execute(self,query,params=None):
   statements.append(query)
   if 'WITH selected AS' in query:
    assert sorted(params[0])==['a-%','b-%','c-%']
    return SimpleNamespace(fetchone=lambda:(audited,audited,overlaps))
   if params:
    n={'a-%':1,'b-%':9,'c-%':0}[params[0]]
    return SimpleNamespace(fetchone=lambda:(n,n,n,0,0,0,0))
   return SimpleNamespace(fetchone=lambda:(0,0,0))
 monkeypatch.setattr('psycopg.connect',lambda *args,**kwargs:Connection())
 monkeypatch.setenv('TEST_DATABASE_URL','unused')
 output=tmp_path/'verified.json'
 monkeypatch.setattr(sys,'argv',['verify','--results',str(results),'--output',str(output)])
 with nullcontext() if passes else pytest.raises(SystemExit):
  runpy.run_path(str(Path(__file__).parents[2]/'scripts/verify_cloud_holds.py'),run_name='__main__')
 assert statements[:3]==['SET TRANSACTION READ ONLY', "SET LOCAL statement_timeout = '60s'", 'SET LOCAL max_parallel_workers_per_gather = 0']
 result=json.loads(output.read_text())
 assert result['durability_and_expiry_pass'] is passes
 assert result['overlapping_load_hold_intervals'] == overlaps
 assert [r['acknowledged_201'] for r in result['runs']]==[1,9,0]
