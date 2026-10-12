# ADR0284: Pipeline payment initiation writes

## Status

Accepted for an opt-in implementation and local verification. Default off; not deployed or qualified by a paid cloud comparison. This does not supersede the existing payment locking, persistence, recovery or connection-budget decisions.

## Context

ADR0283 measured 302,298 unique paid-and-issued tickets inside one hour with zero undispatched journeys and zero final customer failures. However, 32 initial payment requests returned 503 and required recovery. Failure-time captures show scarce payment slots occupied by transactions, often in commit. They establish contention but do not establish the underlying cause of occasional slow commits.

Payment initiation currently waits between dependent SQL operations and also between writes whose results are not needed immediately. Reducing unnecessary network waits may shorten slot occupancy without increasing database connections. It will not reduce WAL durability requirements or fix slow storage commits by itself.

## Decision

Add `PAYMENT_WRITE_PIPELINE`, disabled by default. When enabled, only payment initiation uses the existing PostgreSQL transaction pipeline. Keep the same SQL, order lock, idempotency record, ownership checks, hold expiry checks and explicit commit. Fetch dependent results before making the next decision. In particular, retain the fresh payment/expiry query after acquiring the order lock.

Batch transaction setup before the first dependent fetch and batch the final payment insertion with the idempotency-response update. Synchronize the pipeline before committing and return success only after commit. Callbacks, asynchronous confirmation, retry limits, pool budgets and transaction timeouts stay unchanged.

Validate the working source and the exact previously tested image separately. Any candidate cloud image must be derived from that verified baseline with this isolated change; unrelated source differences must not enter a one-factor comparison.

A regression check exposed ADR0249's historical image builder reading the live
payment method. Apply the existing ADR0238 hash-addressed source snapshot pattern
to this historical input before changing that builder: preserve the exact original
candidate hash and reject corrupted snapshots. Historical reproduction must not
inherit the new feature flag. This changes no historical benchmark identity.

## Alternatives

- Increase payment connections: risks moving contention into PostgreSQL and changes the controlled connection budget.
- Extend acquisition waits: delays rejection but does not reduce occupied slots.
- Change callback confirmation to asynchronous: a separate architectural experiment.
- Combine the order-lock and payment lookup: can introduce a stale snapshot race when another payment commits while the order lock is being acquired.
- Change WAL or commit durability: unsupported by the evidence and unacceptable as a way to improve payment reliability.

## Consequences

Expected benefit is fewer network synchronization waits and shorter payment transaction occupancy. SQL statement count, row-lock semantics, write volume and commit count remain the same. Pipelined errors can arrive at synchronization, so rollback and pool reuse must be verified. The default-off flag permits a controlled comparison and immediate rollback without a schema change.

## Failure and recovery behavior

Any setup, query, write or synchronization failure rolls back the transaction. The payment attempt and idempotency response cannot commit independently. Existing lost-response recovery checks the operation and reuses its idempotency key. Concurrent requests still serialize through the order lock and retain exactly one payment attempt. Ownership, expiry, duplicate callback handling and durable ticket issuance are unchanged. Disable the flag to restore the original execution path.

## Validation evidence

Executed on 12 October 2026: 80 focused checks passed on the working repository,
64 passed against the isolated candidate source on Windows, and 78 passed inside
the actual Linux candidate image with isolated PostgreSQL 17.6 and Redis 7.4.5.
The image checks include concurrent payment keys, authorization, expiry, late SQL
failure/rollback, connection reuse, lost responses, duplicate callbacks, ticket
issuance and actual API process restart. The two reduced-query shape checks are
intentionally excluded from the image tests because that separate repository
optimization is absent from the tested baseline. All 22 copied, installed and
imported application modules and eight role settings were verified.

A local alternating sequential comparison (100 samples per arm, one connection,
no artificial latency) measured median payment initiation 7.065ms off versus
6.028ms on. A separate traced attempt per arm verified protocol changes. Pipeline
mode also sends Flush messages, so fewer Sync messages must not be reported as
an equal reduction in network waits. This small local result is exploratory; it
does not reproduce RDS commit stalls, qualify production, or measure cloud capacity.

At the initial checkpoint, the candidate was default off, built locally and not deployed or pushed to SWR.
It preserves the original measured image's payment queries; the repository has
a separate earlier query optimization which must not enter this comparison.
The [local evidence](../capacity/cce/payment-write-pipeline-local-result-2026-10-12.json)
and its hash-bound source patch identify the exact candidate. A same-budget paid
comparison remains required. ADR0283's audit timeout and diagnostic sampling gap
remain open qualification gaps; no historical failed gate is rewritten.

The historical reproduction regression initially failed (29 passes, one failure).
After replacing its live-method input with the unchanged reviewed snapshot, all
31 affected image/source/naming checks passed. The guard rejects corrupted input
and proves later live payment code is not read. Ruff and publication naming
checks passed; isolated local database/cache test containers were removed.

Publication update: the candidate was subsequently pushed, pulled by immutable digest and verified in SWR on 12 October. The [publication receipt](../capacity/cce/payment-write-pipeline-image-2026-10-12.json) preserves the exact 22-module source identity. ADR0285 completes the audit/sampling integration; its fresh pipeline-off control passed all gates and restoration. The subsequent pipeline-on arm also passed, but performance was mixed: payment p95 improved 5.67%, while database hold/commit means and worker CPU increased and inside-window issuance declined. The flag remains default off; the candidate is not adopted. See the [ADR0285 comparison](../capacity/cce/payment-pipeline-comparison-2026-10-12.json).
