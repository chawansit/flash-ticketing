# ADR 0135: Bounded single-concert fixtures and disjoint paid-generator seat ranges

- Status: Accepted for local implementation and validation; cloud comparison not executed
- Date: 2026-10-04

## Context

ADR0133 passed all 20 gates for 18,000 unique paid and issued tickets across 60 shows at 60 buyers/s for 300 seconds. ADR0134 validated ambiguous financial commit recovery locally and recorded four preflight rejections: the current runner fixes 300 seats/show, the fixture preparer caps seats/show at 1,000 and two generator processes require separate shows. They cannot represent the proposed one-concert control.

The next comparison changes concert concentration only: 60 shows with 300 seats each become one show with 18,000 seats. Arrival rate, duration, total seats, viewers, two generator processes, eight HTTP clients and 250 connections per process, polling and all service budgets must match ADR0133. It remains a scheduled concentration diagnostic, not simultaneous opening or extra seat-map traffic.

## Decision

Add an explicit fixture-layout option with distributed as the default and single-concert as the opt-in. Single-concert fixtures require exactly one show and at most 18,000 seats total. The live smoke runner permits this option only for the prepared 60 buyers/s, 300-second, 18,000-viewer, two-shard profile and the ADR0133 service/client configuration. Keep existing distributed bounds and defaults.

Partition the single show by contiguous, disjoint seat ranges. At the proposed load shard0 receives seats0 through8999 and shard1 seats9000 through17999. Partition viewer credentials as before; never duplicate a viewer token between these shards. Persist range metadata only in private shard manifests; child validation rejects missing, inconsistent or exceeded ranges before HTTP clients are created. The public result contains counts/ranges and layout identity without credentials. Existing journey seat mapping and idempotency-key construction remain unchanged.

Propagate layout and actual seats/show from fresh fixture preparation through credential export, backend helpers, local manifest preflight and generator allocation. Centralize these bounded allocation rules in a small tooling module and transfer it with the generator scripts. Validate the downloaded private manifest and fixture configuration before generator upload/dispatch. Correct the exporter's hardcoded300 seats/show for fresh fixture metadata while retaining its legacy300 default when metadata is absent.

Extend the ADR0090 loopback responder to reject a second logical hold for an already assigned event/seat and expose exact unique seat assignments. Add a single-concert synthetic mode for local qualification. Synthetic results cannot replace PostgreSQL/Kafka financial and queue gates.

## Persistence, locking, messaging, idempotency, TTL and scaling

No production application, schema, payment query, lock order, writer fairness, pool scheduler or message-delivery change. PostgreSQL remains the financial/inventory authority. Existing durable actor/key/body replay, signatures, inbox/outbox, booking/ticket uniqueness and order/payment/hold/sorted-seat locks remain active. Fixture rows are fresh development UUIDs; no existing data is deleted.

Hold/cache/command/callback/lease/signature TTLs are unchanged. No new API, worker, server connection, admission or client concurrency budget. The only proposed cloud factor is how the same total seats are grouped into shows. Reuse ADR0040 phase orchestration, bounded waits, failure stop and finally cleanup; this is a narrow paid-stage fixture/allocation extension, not automatic future stage progression.

This supplements ADR0040/0090/0133/0134 and supersedes no accepted production decision. Distributed-show comparison defaults remain available.

## Alternatives and consequences

One generator process changes the client control and is rejected. Duplicating the show into both old shard manifests without seat bounds risks double-assignment and is rejected. Increasing shows or reducing the stage avoids the missing tooling but does not isolate the proposed one-concert factor. Increasing fixture limits globally widens unrelated controls and is rejected.

Contiguous ranges fit the existing seat-offset mapping and permit pre-dispatch proof of uniqueness without changing customer HTTP behavior. A large show may expose cache refresh, stream hot-key, Kafka partition, writer or worker contention; that is measured work, not evidence for scaling in advance. Local Windows synthetic timing can fail the responder gate independently of allocation correctness and must be reported honestly.

## Failure and recovery behavior

Reject unsupported layouts, fixture shapes, duplicated tokens, malformed range metadata and insufficient inventory before cloud transport or child HTTP clients. Bound child processes and private files; if launch, wait or interpretation fails, terminate/reap owned children and remove private manifests. Do not report recovered or synthetic journeys as paid backend capacity.

The future cloud comparison retains all 20 ADR0133 gates plus layout/total-seat/disjoint-allocation/client-budget checks. A failed gate triggers fresh financial/post-TTL/zero-double-booking/full-keyspace queue/Kafka audits, source/configuration restoration to passing ADR0133, observer/generator cleanup and temporary access removal, then stops. No second run, increased load, push or main merge on failure. Current scope is local implementation/validation and preparation; no cloud access or load is dispatched by this decision.

## Validation evidence

At decision time this tooling has not been implemented or tested. Cloud source remains deb330e with normal budgets and both acquisition features disabled; no temporary access exists.

- [Prepared single-concert requirements](../capacity/flash-sale-opening/single-concert-same-load-control-requirements-2026-10-04.json)
- [Passing ADR0133](../capacity/flash-sale-opening/shared-acquisition-budget-control-2026-10-04.json)
- [ADR0134 local recovery and rejected preflights](../capacity/flash-sale-opening/payment-commit-boundary-local-validation-2026-10-04.json)

Required local evidence: range uniqueness at all 18,000 assignments; rejection before HTTP/cloud transport; unchanged distributed defaults; signed real-DB financial replay on fresh large-show fixture; synthetic real two-process protocol and private cleanup; all relevant financial/concurrency regressions, shell syntax and tooling source identity. Append executed results and limitations before cloud use.

Initial focused validation: 72 cases passed; the large-show integration case failed during snapshot prewarm before financial replay could begin. The original100ms Redis operation deadline was retained. A separate owned-local diagnostic reproduced the failure: server-side EVAL took249.684ms, client PUT failed at197.665ms, and the cache eventually contained18000 seat fields plus metadata. Eventual cache completion does not qualify the failed warmup or prove live capacity. Cloud dispatch remains blocked pending local large-snapshot qualification. Production cache code is unchanged.

Executed outcome: full693passed/2failed105.56s; no skips. The additional failure measured49.886ms against an existing50ms idle gate; unchanged isolated rerun passed, while the original full-suite failure remains retained. Linux loopback60/s60s completed all3600 uniquely with no errors/drops/retries or duplicate seats, but only3411 met90s completion and arrival lag reached24484.736ms. Responder timing/protocol/connection-close and private-manifest cleanup gates passed. Overall local qualification failed; no cloud access/load, increasedload, push or mainmerge. Large-show signed financial replay has not executed past prewarm. See [executed local qualification](../capacity/flash-sale-opening/single-concert-tooling-local-validation-2026-10-04.json).

Same-load distributed-show synthetic baseline also failed:3600 eventually fulfilled,3346 by90s, dispatch lag p9524246.807ms/max26114.341ms, zero drops/retries/duplicate assignments. Both responder controls passed. These results do not attribute local generator timing failure to concert concentration; runtime/client source was unchanged. No local or cloud capacity qualification is inferred.
