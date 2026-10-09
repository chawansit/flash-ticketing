from dataclasses import replace

import pytest

from ticketing.config import Settings


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("consumer_batch_size", 0, "CONSUMER_BATCH_SIZE"),
        ("consumer_batch_size", 101, "CONSUMER_BATCH_SIZE"),
        ("consumer_batch_wait_ms", 0, "CONSUMER_BATCH_WAIT_MS"),
        ("consumer_batch_wait_ms", 101, "CONSUMER_BATCH_WAIT_MS"),
    ],
)
def test_consumer_batch_configuration_rejects_out_of_range(field, value, message):
    with pytest.raises(RuntimeError, match=message):
        replace(Settings(), **{field: value}).validate()


def test_consumer_batch_configuration_accepts_boundaries():
    replace(Settings(), consumer_batch_size=1, consumer_batch_wait_ms=1).validate()
    replace(Settings(), consumer_batch_size=100, consumer_batch_wait_ms=100).validate()
