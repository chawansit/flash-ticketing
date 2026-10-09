import importlib.util
from pathlib import Path

import pytest

spec=importlib.util.spec_from_file_location('attribution',Path(__file__).parents[2]/'scripts/analyze_hold_contention.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def sample():
    c={'request_id':'a','status':'503','code':'ADMISSION_FULL',**dict.fromkeys(('total_ms','client_ms','lane_wait_ms','protocol_queue_ms','asgi_headers_ms'),1)}
    s={'request_id':'a','status':503,'error_code':'ADMISSION_FULL','duration_ms':0.1,'hold_phase_ms':{}}
    return c,s


def test_missing_phase_is_not_zero_and_missing_join_fails():
    c,s=sample()
    result=module.analyze([c],[s])
    assert result['groups']['ADMISSION_FULL']['phases']=={}
    with pytest.raises(KeyError):module.analyze([c],[])


def test_duplicate_or_mismatched_outcome_fails():
    c,s=sample()
    with pytest.raises(ValueError):module.analyze([c,c],[s])
    with pytest.raises(ValueError):module.analyze([c],[s,s])
    with pytest.raises(ValueError):module.analyze([c],[{**s,'status':409}])
