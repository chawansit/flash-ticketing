# ADR 0120: Pipelined pending checks before reservation reclaim

Date: 2026-10-03
Status: Accepted for an isolated implementation experiment; cloud capacity validation pending

## Context

ADR 0119's same-load control measured writer command age averaging 1.39s while database pool acquisition averaged 0.0346ms and PostgreSQL batches 143.32ms. Age is measured after stream collection, before database persistence, so it includes prior queueing and the current discovery sweep. It is not a pool-wait measurement. The writer currently makes up to 32 sequential XAUTOCLAIM round trips before reading new commands even when streams have no pending entries. Redis durable-marker and acknowledgement calls averaged about 1.4â€“1.5ms; those are different operations and do not establish an exact discovery RTT.

A 20-sample isolated real-Redis diagnostic on 32 retained, acknowledged streams measured sequential reclaim mean 21.05ms versus a pipelined pending check 1.11ms without injected latency. A separate synthetic 1.5ms transport-delay model measured 86.21ms versus 3.16ms. This models round-trip sensitivity; it is not Huawei latency or backend capacity evidence.

## Decision

Before the existing reclaim pass, execute one bounded nontransactional pipeline of XPENDING summaries for the selected stream window (at most 32 by current configuration). Skip XAUTOCLAIM only for summaries with zero pending entries. For nonzero summaries, retain sequential XAUTOCLAIM with the transaction-wide remaining COUNT and unchanged 30-second idle threshold. Complete this reclaim pass before all new-entry reads. New-entry XREADGROUP remains per event, nonblocking and sequential with the remaining total count.

The pipeline contains read-only commands; it does not assign entries, acknowledge, delete, mutate ownership or combine different event slots into one Redis operation. Do not pipeline ownership-changing reads/claims. Add a redis_claim phase timer around stream collection, including errors, so future controls distinguish collection from database work. No worker, database, admission, callback, TTL or connection budget changes.

This supersedes only the unconditional reclaim check for empty pending lists within ADR 0113. Its nonblocking reads, ADR 0064 rotating discovery, ADR 0063 total batch bound and ADR 0058 at-least-once persistence/commit-before-ACK remain accepted.

## Alternatives

- More writers or larger batches: confounds CPU/transaction budgets and does not remove transport overhead.
- Pipeline XAUTOCLAIM or multi-stream XREADGROUP: can assign more messages than the remaining total and violate slot locality.
- Cache pending summaries or defer reclaim for a fixed interval: adds stale state and recovery scheduling delay.
- Per-event Lua combining claim/read: expands ownership logic and would require a separate proof and recovery contract.
- Keep unconditional sequential reclaim: simple, but pays a network round trip for every selected empty pending list.

## Consequences

Zero-pending windows replace up to 32 reclaim round trips with one read-only pipeline. Redis still processes up to 32 summaries. Fully pending windows add one round trip and summary commands; gain depends on observed pending density and network conditions. One batch cannot claim more than its configured total. No qualified capacity gain is assumed.

## Failure and recovery behavior

A summary error aborts collection before any message is assigned; the existing worker Redis-error reset/backoff rebuilds state. Summary zero is advisory and never causes acknowledgement or removal. A new assignment after a zero snapshot is newly pending and cannot already satisfy the 30-second idle threshold; next bounded rotation checks it again. Explicit zero-idle recovery/tests likewise use the next rotation for assignments concurrent with the snapshot. Existing eligible pending entries at the snapshot are checked before fresh commands. A worker crash, ambiguous claim response or PostgreSQL/Redis failure retains consumer-group replay and PostgreSQL idempotency. No TTL, durability or zero-double-booking guarantee is relaxed.

## Validation evidence

Executed isolated diagnostic: 20 samples per case, 32 streams; owned diagnostic keys removed. Local measurements 21.05â†’1.11ms; synthetic transport model 86.21â†’3.16ms. Raw evidence remains private in tmp/writer-pending-diagnostic.json.

Planned: real Redis tests for acknowledged versus pending streams, reclaim-before-new ordering, cross-stream hard total count, summary failure before assignment, concurrent assignment after snapshot and subsequent recovery; existing fairness/reset/registry races and 100-contender/durability/payment recovery tests. Then one unprofiled 60-buyers/s, 300s candidate versus ADR 0119, unchanged eight delivery slots/all budgets. Exact paid/unpaid post-TTL audit, full Redis/database queues, Kafka, source/settings/restoration/readiness/generator idle/private cleanup remain mandatory. Failed load prohibits rate escalation, promotion and main merge. Executed results will be appended.

Executed local validation: 452 unit/integration tests passed in 75.00s, with two existing dependency warnings. Real Redis proved reclaimed work precedes fresh reads, zero-pending streams avoid claims, total assignment stays at four across streams, discovery never ACKs/deletes, and a pending assignment after the advisory snapshot is recovered on the next rotation. Existing registry races/reset/fairness, PostgreSQL timeout/reuse, post-commit replay, duplicate payment/fulfillment and 100-contender/exactly-one-winner tests passed. Lint and whitespace checks passed. Both owned isolated PG17.6/Redis7.4.5 test containers were removed. Cloud comparison remains pending.
