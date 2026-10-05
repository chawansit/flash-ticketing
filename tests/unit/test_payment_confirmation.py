from dataclasses import replace
from types import SimpleNamespace

import pytest
from psycopg.errors import LockNotAvailable

from scripts.checkout_journey_probe import status_poll_delay
from ticketing.config import Settings
from ticketing.domain import Failure
from ticketing.infrastructure.payment_confirmation import retry_delay_ms, transient


@pytest.mark.parametrize("attempt",[1,2,8,100])
def test_retry_jitter_has_positive_capped_bounds(attempt):
    low=retry_delay_ms(attempt,100,0);high=retry_delay_ms(attempt,100,1)
    assert 0 < low <= high <= 30000 and low >= high//2


def test_only_transient_database_errors_retry():
    assert transient(LockNotAvailable())
    assert not transient(Failure('PAYMENT_MISMATCH'))
    assert not transient(ValueError('bug'))


@pytest.mark.parametrize("settings",[{'confirmation_max_pending':0},{'confirmation_max_attempts':0},
    {'confirmation_lease_seconds':1},{'payment_callback_provider':''},{'order_status_poll_ms':10},
    {'pool_max':1,'payment_confirmation_async':True,'confirmation_concurrency':2,'simulator_concurrency':1}])
def test_invalid_queue_settings_fail_before_start(settings):
    with pytest.raises(RuntimeError):replace(Settings(),**settings).validate()


@pytest.mark.parametrize("value",['bad','0','10000',None])
def test_invalid_poll_guidance_retains_baseline(value):
    assert status_poll_delay(SimpleNamespace(headers={'X-Poll-Interval-Ms':value}),.2)==.2


def test_valid_poll_guidance_is_bounded_and_jittered():
    values={status_poll_delay(SimpleNamespace(headers={'X-Poll-Interval-Ms':'500'}),.2) for _ in range(10)}
    assert all(.4<=value<=.6 for value in values) and len(values)>1
