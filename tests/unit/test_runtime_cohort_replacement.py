"""A measured query may be replaced; drift must fail before any mutation."""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import paid_cohort_query as candidate
from observe_cce_paid_pipeline import install_indexed_cohort_query
from observe_paid_pipeline import PAID_COHORT_QUERY as base_query


def test_actual_runtime_replacement_receipt_and_repeated_install_rejected():
    module = SimpleNamespace(PAID_COHORT_QUERY=base_query)
    install_indexed_cohort_query(module)
    receipt = candidate.install_runtime_candidate(module)
    assert receipt["baseline_query_sha256"] == candidate.RUNTIME_BASELINE_SHA256
    assert receipt["candidate_query_sha256"] == hashlib.sha256(module.PAID_COHORT_QUERY.encode()).hexdigest()
    assert receipt["sample_columns"] == list(candidate.SAMPLE_COLUMNS)
    assert json.loads(json.dumps(receipt)) == receipt
    with pytest.raises(ValueError):
        candidate.install_runtime_candidate(module)
    assert module.PAID_COHORT_QUERY == candidate.PAID_COHORT_QUERY


@pytest.mark.parametrize("query", [base_query, candidate.PAID_COHORT_QUERY, None, "", "SELECT 1"])
def test_unmeasured_query_rejected_without_mutation(query):
    module = SimpleNamespace(PAID_COHORT_QUERY=query)
    with pytest.raises(ValueError):
        candidate.install_runtime_candidate(module)
    assert module.PAID_COHORT_QUERY == query
