"""Exact recovery classification rejects incomplete evidence; no registration/cloud calls."""
import copy
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_projection_rate_recovery as recovery
import work_envelope as policy
from cce_replacement_rds_binding import NEW_SHA


def inputs():
    report=policy.read(policy.ROOT/'tmp'/recovery.RUN/'cce-comparison.private.json')
    entry=next(e for e in policy.journal(policy.envelope())['experiments'] if e['ledger']==recovery.LEDGER)
    entry=copy.deepcopy(entry)
    entry['status']=entry.get('initial_status',entry['status'])  # Validate the retained original failed classification.
    scope=policy.read(policy.STATE)[recovery.LEDGER]
    financial=copy.deepcopy(report['paid_stage']['financial'])
    financial.update(pass_=True)
    financial['pass']=True
    financial['counts'].update(expected=recovery.PAID,expected_paid=recovery.PAID)
    financial['checks']=dict.fromkeys(('post_ttl_complete','payments_durable','ticket_relationships_valid'),True)
    fresh={'decision':'ADR0363','run':recovery.RUN,'ledger':recovery.LEDGER,
           'original_result_sha256':recovery.RESULT,'binding_sha256':report['binding_sha256'],
           'replacement_snapshot_sha256':NEW_SHA,'gates':dict.fromkeys(recovery.GATES,True),
           'pass':True,'capacity_qualified':False,'verified_at_utc':datetime.now(UTC).isoformat(),
           'actual_elapsed_seconds':10,'fresh_financial':financial,
           'fixture_sha256':report['paid_stage']['fixture_identity']['fixture_identity_sha256']}
    return report,entry,scope,fresh


def test_exact_classification_keeps_original_failure():
    report,entry,scope,fresh=inputs()
    assert recovery.validate(report,entry,scope,fresh)
    assert report['pass'] is False and entry['status']=='RECOVERY_REQUIRED'
    assert report['paid_stage']['financial']['expected']==54000


@pytest.mark.parametrize('fault',['missing_ticket','duplicate','financial_check','runtime','naive','hash','quality'])
def test_recovery_cannot_clear_incomplete_evidence(fault):
    report,entry,scope,fresh=inputs()
    if fault=='missing_ticket':fresh['fresh_financial']['counts']['tickets']-=1
    elif fault=='duplicate':fresh['fresh_financial']['counts']['duplicate_booked_seats']=1
    elif fault=='financial_check':fresh['fresh_financial']['checks']['payments_durable']=False
    elif fault=='runtime':fresh['gates']['runtime_stable']=False
    elif fault=='naive':fresh['verified_at_utc']=datetime.now(UTC).replace(tzinfo=None).isoformat()
    elif fault=='hash':fresh['original_result_sha256']='0'*64
    elif fault=='quality':fresh['capacity_qualified']=True
    with pytest.raises(ValueError):recovery.validate(report,entry,scope,fresh)
