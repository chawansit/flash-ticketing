# Failure scenarios

| Failure | Behavior |
|---|---|
| Same seat requested concurrently | Shield rejects contenders; PostgreSQL NOWAIT and ownership check protect even without shield |
| One seat in a group unavailable | Entire transaction rolls back; no partial hold/order |
| API dies before commit | PostgreSQL rolls back |
| API dies after commit | Same idempotency key returns original IDs and deadline |
| Same key, different request | 409 IDEMPOTENCY_MISMATCH |
| Redis unavailable/full | New admission and seat-map reads return 503; no mass DB read fallback |
| DB pool full / unavailable | Bounded rejection; no booking granted |
| Expiry worker stopped | Expired inventory is still reclaimable on reservation |
| Cleanup races new owner | Only old hold references are released |
| Payment slow | No database transaction spans gateway latency |
| Duplicate callback ID | Validate payload hash, return duplicate |
| Different callback ID, same succeeded payment | Return duplicate, no extra booking or refund |
| Payment failure then delayed success | Refund; never reanimate a failed checkout |
| Success after deadline or release | Refund request; no seat theft |
| Kafka unavailable | Committed outbox retained; fulfillment delayed |
| Publisher dies after send | Lease expires; stable event ID published again |
| Consumer dies after DB commit | Replayed event is deduplicated |
| Poison event | Bounded retries, durable dead letter, explicit replay |
| Cache event delayed/out of order | Rebuild current state; version-checked replacement |
| Simulator dies after callback | Lease expiry and repeated signed callback recover delivery |

Safety assumes PostgreSQL transactional durability and that seat writes use the reservation
adapter. The local Compose stack does not establish disaster-recovery durability or HA.


## Optimized background work

- Redis write fails: retain the durable dirty generation and retry after refresh lease expiry.
- Refresh finishes after a newer request: acknowledge only the captured generation; newer work remains.
- Old refresher/publisher/simulator loses its lease: matching-token SQL predicates reject its acknowledgement.
- Only part of a Kafka batch is acknowledged: mark only confirmed rows; replay uncertain rows with stable IDs.
- Simulator thread fails: wait for the bounded batch; remaining callbacks keep their finite leases for retry.

See [ADR 0008](adr/0008-bounded-background-processing.md) and the background optimization integration tests.
