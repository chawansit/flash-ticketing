import json
import runpy
import sys
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


@pytest.mark.parametrize('open_count,credential_minutes,passes',[(2,60,True),(0,60,False),(2,1,False)])
def test_preflight_rejects_expiring_credentials_or_closed_sales(tmp_path,monkeypatch,open_count,credential_minutes,passes):
 scripts=Path(__file__).parents[2]/'scripts'
 monkeypatch.syspath_prepend(str(scripts))
 manifest=tmp_path/'private.json'
 manifest.write_text(json.dumps({'environment':'development','show_ids':['one','two'],'expires_at':(datetime.now(UTC)+timedelta(minutes=credential_minutes)).isoformat()}))
 output=tmp_path/'result.json'
 class FakeCursor:
  def __enter__(self):
   return self
  def __exit__(self,*_):
   return None
  def execute(self,query,params):
   assert 'FROM events WHERE id=ANY(%s::uuid[])' in query
   assert params[1]==['one','two']
  def fetchone(self):
   return [2,open_count,'fixture-end']
 class FakeConnection:
  def __init__(self):
   self.commands=[]
  def __enter__(self):
   return self
  def __exit__(self,*_):
   return None
  def execute(self,query):
   self.commands.append(query)
   assert query in {'SET TRANSACTION READ ONLY',"SET LOCAL statement_timeout = '20s'"}
  def cursor(self):
   assert self.commands==['SET TRANSACTION READ ONLY',"SET LOCAL statement_timeout = '20s'"]
   return FakeCursor()
 monkeypatch.setattr('psycopg.connect',lambda *_args,**_kwargs:FakeConnection())
 monkeypatch.setattr(sys,'argv',['preflight','--manifest',str(manifest),'--seconds','300','--output',str(output)])
 with nullcontext() if passes else pytest.raises(SystemExit):
  runpy.run_path(str(scripts/'cloud_load_preflight.py'),run_name='__main__')
 assert json.loads(output.read_text())['pass'] is passes
