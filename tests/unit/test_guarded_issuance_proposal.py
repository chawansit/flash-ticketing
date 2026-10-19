"""Prove the interval mismatch and preserve full-window/clock/count constraints."""
import ast
import sys
from pathlib import Path
from uuid import UUID

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from cce_guarded_issuance_proposal import assessment, proposed_program
from cce_hourly_window import QUERY


@pytest.mark.parametrize('rate',[180,200])
def test_uniform_ideal_stream_is_rejected_by_original_mixed_intervals(rate):
    full=rate*300
    inner=rate*296
    result=assessment(full,inner,rate)
    assert result['original_count_predicate'] is False
    assert result['proposed_count_predicate'] is True
    assert result['guarded_window_tickets_per_second']==rate
    assert not result['load_authorized'] and not result['historical_status_changed']


def test_retained_counts_are_diagnostic_only():
    result=assessment(53660,53305,180)
    assert not result['original_count_predicate'] and result['proposed_count_predicate']
    assert result['minimum_full_count_unchanged']==53400
    assert result['minimum_guarded_count']==53280


@pytest.mark.parametrize('full,inner',[(53399,53280),(54000,53279)])
def test_each_interval_is_independently_required(full,inner):
    assert not assessment(full,inner,180)['proposed_count_predicate']


@pytest.mark.parametrize('full,inner,rate',[(54001,53280,180),(54000,54001,180),(54000.0,53280,180),(54000,-1,180),(54000,53280,180.0),(60000,59200,220)])
def test_invalid_counts_and_rates_are_rejected(full,inner,rate):
    with pytest.raises(ValueError):assessment(full,inner,rate)


@pytest.mark.parametrize('rate',[180,200])
def test_sql_intervals_joins_and_clock_checks_are_preserved(rate):
    events=[str(UUID(int=i+1)) for i in range(rate)]
    source=proposed_program(events,1.0,rate)
    original=QUERY.replace('3598','298').replace('3600','300')
    def sql_text(raw):
        return [n.value for n in ast.walk(ast.parse(raw)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and 'SELECT count(DISTINCT t.id)' in n.value]
    assert sql_text(source)==sql_text(original)
    assert "row[0]==row[1]==row[2]==row[3]" in source
    assert 'abs(row[5].timestamp()-time.time())<=2' in source
    assert 'row[5].timestamp()>=start+300' in source
    compile(source,'proposal','exec')
