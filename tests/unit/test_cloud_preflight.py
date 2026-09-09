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
 def query(command,**kwargs):
  code=command[command.index('-c')+1]
  assert 'SET TRANSACTION READ ONLY' in code
  assert 'default_transaction_read_only' not in code
  assert json.loads(command[-2])==['one','two']
  return json.dumps([2,open_count,'fixture-end'])
 monkeypatch.setattr('subprocess.check_output',query)
 monkeypatch.setattr(sys,'argv',['preflight','--manifest',str(manifest),'--seconds','300','--output',str(output)])
 with nullcontext() if passes else pytest.raises(SystemExit):
  runpy.run_path(str(scripts/'cloud_load_preflight.py'),run_name='__main__')
 assert json.loads(output.read_text())['pass'] is passes
