import copy
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import cce_pre_fixture_200_recovery as recovery
import work_envelope as policy


@pytest.mark.parametrize('fault',[None,'paid','namespace','credentials','capacity','hash','scope','safety'])
def test_exact_no_dispatch_cleanup(fault):
    raw=policy.read(policy.ROOT/'tmp'/recovery.RUN/'cce-comparison.private.json')
    entry=copy.deepcopy(next(e for e in policy.journal(policy.envelope())['experiments'] if e['ledger']==recovery.LEDGER))
    entry['status']=entry.get('initial_status',entry['status'])
    scope=policy.read(policy.STATE)[recovery.LEDGER]
    receipt=policy.read(policy.ROOT/'docs/capacity/cce/pre-fixture-200-cleanup-2026-10-19.json')
    receipt['verified_at_utc']=datetime.now(UTC).isoformat()  # Synthetic freshness for pure classification only.
    if fault=='paid':receipt['zero_performance_dispatch']=False
    elif fault=='namespace':receipt['namespace_removed']=False
    elif fault=='credentials':receipt['helper_credentials_removed']=False
    elif fault=='capacity':receipt['capacity_qualified']=True
    elif fault=='hash':receipt['original_result_sha256']='0'*64
    elif fault=='scope':entry['ledger']='wrong'
    elif fault=='safety':raw['native']['safety']['post_ttl_financial']['pass']=False
    if fault is None:assert recovery.validate(raw,entry,scope,receipt)
    else:
        with pytest.raises(ValueError):recovery.validate(raw,entry,scope,receipt)
