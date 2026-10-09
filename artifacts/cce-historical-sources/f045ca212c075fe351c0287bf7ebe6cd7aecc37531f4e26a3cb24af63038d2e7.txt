import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    'summarize_hold_diagnostics', Path(__file__).resolve().parents[2]/'scripts/summarize_hold_diagnostics.py'
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_summary_keeps_accounting_quantiles_and_bounded_redacted_examples():
    records = []
    for duration in range(1, 21):
        records.append({'message':'request','route':'/v1/holds','status':201,
                        'duration_ms':duration,'hold_phase_ms':{'db_exit':duration},
                        'secret':'never retain'})
    for _ in range(120):
        records.append({'message':'request','route':'/v1/holds','status':503,
                        'error_code':'ADMISSION_FULL','secret':'never retain'})
    result = module.summarize(['bad log', *['api-1 | '+json.dumps(r) for r in records]])
    assert result['statuses'] == {'201':20,'503':120}
    assert result['successful_hold_phases']['db_exit']['p95_ms'] == 19
    assert result['successful_hold_phases']['db_exit']['mean_ms'] == 10.5
    assert result['error_codes'] == {'ADMISSION_FULL':120}
    assert len(result['first_100_errors']) == 100
    assert len(result['slowest_20_successful_holds']) == 20
    assert result['slowest_20_successful_holds'][0]['duration_ms'] == 20
    assert 'never retain' not in json.dumps(result)
