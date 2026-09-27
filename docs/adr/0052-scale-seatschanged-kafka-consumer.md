# ADR 0052: Scale the SeatsChanged Kafka consumer for refresh ingress

Status: Accepted for a controlled two-consumer capacity experiment; one consumer remains the rollback configuration.

## Context

The 1,000 RPS five-minute diagnostic at revision `b0a1bec` delivered all 300,000 scheduled requests with eight generator workers. The workload produced 29,998 outbox and consumer-inbox insertions over the observer window. With one member in consumer group `ticketing-fulfillment-v1`, Kafka lag became nonzero at 12:07:29 UTC, peaked at 4,000 messages at 12:12:27 UTC, and returned to zero at 12:14:16 UTC: 108.6 seconds after the peak and 406.6 seconds after first becoming nonzero. The six-partition topic therefore had unused parallelism while a single consumer serialized event delivery. Two maintenance workers completed 29,993 of 29,998 observed refresh generations, but the fixed audit still found three pending refresh requests and failed the zero-queue gate. All 14,999 acknowledged holds were durable and no held-seat intervals overlapped.

The same stage also observed a peak of 1,955 overdue active holds and 42.5 seconds oldest overdue age. These values include the delayed SeatsChanged ingress and must not be attributed solely to the maintenance process. Increasing maintenance again before removing consumer ingress lag would not isolate the limiting stage.

## Decision

Run a controlled experiment with two `consumer` replicas in the same Kafka group while keeping six Kafka partitions, two maintenance replicas, four API replicas, admission five per API, PgBouncer budget, workload, hold TTL, retry settings and the 180-second audit unchanged. Kafka assigns partitions to the group members. Existing durable `consumer_inbox` insertion remains the idempotency boundary, so redelivery after a worker failure cannot apply an event twice.

The unattended stage records and verifies the original consumer replica count, scales to two before traffic, waits for both processes to remain running, and restores the original count even when a stage fails. Capture Kafka lag and refresh generation/completion rates for the full load and drain window. Start with a five-minute safety stage. Proceed to 15 minutes only if request accounting, latency, durability, zero overlap, zero queues and rollback all pass. A 30-minute stage remains required before any sustained 1,000 RPS diagnostic claim.

## Alternatives considered

- Add more maintenance workers first: rejected for this experiment because Kafka lag proves refresh requests arrive late to PostgreSQL.
- Increase Kafka partitions: rejected because the existing six partitions can already distribute work across two consumers.
- Process SeatsChanged directly in the API request: rejected because it would extend reservation latency and couple Redis refresh failure to the authoritative PostgreSQL transaction.
- Batch multiple Kafka messages per poll without adding a consumer: deferred; the current code commits one offset at a time and changing batch failure scope requires separate evidence.
- Relax the 180-second queue gate: rejected because it would hide delayed availability updates.

## Consequences

A second consumer uses one additional database pool and Kafka group member. Partition assignment may briefly pause during rebalance. Ordering remains per partition; the inbox and generation-fenced refresh queue preserve idempotency and prevent an older snapshot from becoming authoritative. The result applies only to the measured six-partition topic and workload.

## Failure and recovery behavior

If the second consumer fails, Kafka reassigns its partitions to the remaining member. A message whose database transaction did not commit is replayed; a committed inbox row makes replay a no-op. If scaling or readiness fails, stop before load and restore the original replica count. Any double-booking, missing acknowledged hold, nonzero queue at the fixed audit, unaccounted request, or failed rollback stops promotion.

## Validation evidence

The one-consumer baseline is retained privately at `tmp/capacity/cloudssd-refreshtrace-1000-safety2-20260926`. It recorded 124 valid Kafka samples with no observer error, one member, maximum total lag 4,000, maximum partition lag 1,171 and final lag zero. Read/hold worst-worker p95 were 14.219/78.525 ms. Eight first-attempt admission rejections produced seven successful retries and one exhausted request. Durability and overlap checks passed, but the audit found three pending refresh requests, so the stage failed.
