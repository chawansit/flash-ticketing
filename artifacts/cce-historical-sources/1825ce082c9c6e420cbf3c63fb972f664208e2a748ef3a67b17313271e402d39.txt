from dataclasses import replace
from unittest.mock import Mock

import pytest

from ticketing.config import Settings
from ticketing.workers import expire_batch


def test_expiry_batch_delegates_bounded_limit():
    service = Mock()
    service.expire_batch.return_value = 3

    assert expire_batch(service, 8) == 3
    service.expire_batch.assert_called_once_with(8)


@pytest.mark.parametrize("limit", [0, 101])
def test_expiry_batch_configuration_rejects_out_of_range(limit):
    with pytest.raises(RuntimeError, match="EXPIRY_BATCH_SIZE"):
        replace(Settings(), expiry_batch_size=limit).validate()


def test_expiry_batch_configuration_accepts_boundaries():
    replace(Settings(), expiry_batch_size=1).validate()
    replace(Settings(), expiry_batch_size=100).validate()
