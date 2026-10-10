"""Real six-partition backlog exceeds the inherited client frame cap."""
import os
import time
from dataclasses import replace
from uuid import uuid4

import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from ticketing import workers
from ticketing.config import Settings

pytestmark = pytest.mark.integration


def test_projection_drains_all_six_partitions_with_large_fetch_backlog(monkeypatch):
    bootstrap = os.environ.get("TEST_KAFKA_BOOTSTRAP")
    if not bootstrap:
        pytest.skip("Real Kafka required")
    topic = "projection-frame-" + uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    producer = consumer = None
    try:
        admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)])
        producer = KafkaProducer(bootstrap_servers=bootstrap, compression_type=None)
        # More than 1 MiB per partition, so an unbounded six-partition response
        # exceeds both the inherited 1,000,000-byte cap and normal small fixtures.
        payload = os.urandom(65536)
        futures = [producer.send(topic, value=payload, partition=partition) for partition in range(6) for _ in range(24)]
        for future in futures: future.get(timeout=15)
        producer.flush(timeout=15)
        monkeypatch.setattr(workers, "EVENT_TOPIC", topic)
        settings = replace(Settings(), kafka_bootstrap=bootstrap, event_consumer_separation=True, consumer_batch_size=32)
        consumer = workers.create_event_consumer(settings, "projection-consumer")
        assert consumer.config["fetch_max_bytes"] == 1024 * 1024
        assert consumer.config["receive_message_max_bytes"] == 8 * 1024 * 1024
        seen = set()
        deadline = time.monotonic() + 45
        while len(seen) < 144 and time.monotonic() < deadline:
            for partition, records in consumer.poll(timeout_ms=500).items():
                for record in records:
                    assert record.value == payload
                    seen.add((partition.partition, record.offset))
        assert seen == {(partition, offset) for partition in range(6) for offset in range(24)}
        assert len(consumer.assignment()) == 6
        consumer.commit()
        assert all(consumer.committed(partition) == 24 for partition in consumer.assignment())
    finally:
        if consumer is not None: consumer.close(autocommit=False)
        if producer is not None: producer.close()
        admin.delete_topics([topic])
        admin.close()
