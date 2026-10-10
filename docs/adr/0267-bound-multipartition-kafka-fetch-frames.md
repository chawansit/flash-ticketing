# ADR0267: Bound multipartition Kafka fetch frames

## Status

Accepted correction for ADR0265/0266 rollout and recovery. Cloud load remains blocked until verified drain and exact restoration. Supersedes the projection lane's inherited fetch defaults; mixed and fulfillment defaults remain unchanged.

## Context

The first ADR0266 deployment was rejected before fixture creation or buyer dispatch. The projection worker joined all six partitions, then rejected a broker response with InvalidReceiveError: Invalid frame length: 6291724. The installed kafka-python 2.3.2 consumer inherits receive_message_max_bytes=1000000, fetch_max_bytes=52428800 and max_partition_fetch_bytes=1048576. It cannot decode the backlog response and therefore cannot commit it. The independent group has not drained; restoration correctly refused to remove it. No paid traffic or capacity improvement was measured.

## Decision

For projection consumers only, request at most 1 MiB per fetch while allowing a bounded 8 MiB network frame. Keep the existing 1 MiB per-partition allowance, bounded poll record count and batch wait unchanged. Kafka may exceed the fetch target for its first record batch, so the frame allowance includes six per-partition batches plus protocol overhead. Do not permit unlimited frames or increase SQL connection pools. Mixed and fulfillment consumers retain their previous configuration. Publish a new immutable derivative image; never patch the deployed filesystem or reuse the previous digest.

Use the corrected projection image first to recover the failed deployment while intake remains undispatched. Retain its group and committed offsets, replay normally without offset resets, then verify both group lags and all durable queues are zero before restoring the exact normal topology. Keep the original failed report failed. A fresh paid experiment is allowed only after independent restoration verification and a new ownership identity.

## Alternatives

Raise receive frame limits without bounding requested fetch size: permits unnecessary buffers during normal polling. Reset the new group to latest: could skip unprocessed refresh work and falsify drain. Add six projection replicas: changes CPU and pool budgets while still exposing each partition's frame boundary. Temporarily patch container files: violates immutable runtime provenance.

## Consequences

The projection process uses a bounded larger decoding allowance, with normal requests targeting 1 MiB. Retained Kafka history can still require replay time and database work; progress and drain must be measured. This is a startup/replay correctness correction, not evidence of greater ticket throughput.

## Failure and recovery behavior

Retain errors and offsets if an oversized response, unsupported envelope or refresh failure persists. Keep rollback blocked until the new lane drains. Never delete messages, rewrite financial rows or reset offsets to obtain a pass. After drain, stop the projection worker and restore original mixed consumers, routing, images and counts; verify generator idle, no customer dispatch and all normal queues zero.

## Validation evidence

Observed ECS worker log proves the 6,291,724-byte frame rejection. The initial failed attempt had zero fixture/customer dispatch, zero durable queue backlog in the fulfillment audit, and six fulfillment members. Projection progress was not verified. Local configuration and real Kafka backlog regression must execute before recovery deployment; record the results here.

Executed candidate-image validation: 21 focused unit tests and 139 isolated integration tests passed against PostgreSQL 17.6, Redis 7.4.5 and Kafka 3.9.1. The added real Kafka regression consumed and committed all 144 uncompressed 64-KiB records across six partitions (9 MiB retained backlog). Registry-pulled runtime sources were verified before the recovery worker was deployed. The corrected worker is committing retained history; recovery is ongoing and no paid load has started.

Recovery uses a separate bounded continuation of up to 60 minutes when the initial ten-minute observation interval expires. This extends mandatory cleanup under the standing envelope, not paid-load duration. Keep API intake stopped, preserve the original failed report and offsets, and stop on a ten-minute no-progress interval. Restore only after both lanes and durable queues drain.

Next-attempt source/image pin and historical profile checks: 84 unit tests passed. After adding retention of actual inventory/queue evidence and the exact readiness rejection before validation, all 23 focused diagnostics/pin/fetch tests passed. Failed cloud evidence remains unchanged; the first recovery observation interval expired with progress, and a separate bounded continuation retains the running projection worker and offsets.
