# ADR 0122: Reservation-writer visited-stream continuation

Date: 2026-10-03
Status: Accepted; local fairness/concurrency/replay validation and bounded unchanged-load qualification passed; sustained production capacity remains unqualified

## Context

At reporting revision cbdc834, the isolated real-Redis reproduction served 48 hot-stream commands in twelve iterations and never assigned the cold stream's entry. Discovery advances its cursor over the entire selected window before reclaim or fresh reads can stop on total COUNT. Selecting every registered stream therefore restarts at the same hot stream indefinitely. The reclaim exit has the same defect. This is a reproduced scheduling defect, not evidence of financial loss in the audited ADR 0121 cohort.

## Decision

Select a bounded stream window without advancing its cursor. On a full reclaim or fresh-read batch, advance only through the stream whose operation exhausted the total count. On a completed nonfull sweep, advance through the entire selected window. Record continuation before yielding a full operation's assigned entries, so caller suspension cannot leave continuation stale. Mark discovery-cycle completion only when this actual continuation crosses the cached window boundary; retain the existing bounded registry/SCAN refresh and empty-stream race repair.

Continue the read-only XPENDING pipeline and recover eligible pending work before any fresh reads in the selected window. Ownership-changing commands remain sequential and event-local, each using the remaining total COUNT. Recovery priority is window-local, not a global oldest-message guarantee. Progress means each cached stream gets a turn under successful repeated collection and a finite stable registry; new work may wait behind recovery, and continuously failing Redis operations do not promise progress.

This supersedes ADR 0064's whole-selected-batch cursor advancement only. ADR 0120's pending filtering, ADR 0113's nonblocking reads, ADR 0063's total transaction bound and ADR 0058's at-least-once persistence remain accepted. PostgreSQL remains authoritative; seat locking, transaction/savepoint behavior, commit-before-marker/ACK, command/payment idempotency, callback leases and all hold/metadata TTLs remain unchanged. No messaging format, persistent cursor, new lock or shared scheduler is introduced. Each writer retains its own advisory cursor; worker counts, database/connection budgets, stream-window bounds and generator controls remain unchanged. Distributed writer coordination or scaling is future scope.

## Alternatives

- One message per stream: changes batching and transaction cost unnecessarily.
- Increase workers, batch size or load: confounds the scheduling correction and does not prove fairness.
- Separate persistent recovery/fresh cursors or a shared Redis scheduler: adds state and recovery coordination beyond the reproduced defect.
- Advance only after the whole generator is consumed: caller suspension can leave a full operation's continuation stale.
- Preserve whole-window advancement: retains demonstrated starvation.

## Consequences

Hot streams can still use the remaining batch budget, but later streams get the next collection turn instead of being skipped. XPENDING checks can revisit a selected suffix; this is bounded advisory overhead. No throughput gain is assumed. Registry changes and multiple independent writers limit strict ordering; Redis consumer groups and database idempotency provide correctness rather than cursor ownership.

## Failure and recovery behavior

Summary failures do not advance or assign. An ambiguous ownership-changing Redis response remains recoverable in the consumer group's pending list; existing worker reset/backoff clears advisory discovery state. Completed claim/read operations can be replayed after PostgreSQL failure, writer crash or commit-before-ACK failure without duplicate holds, orders, payments or tickets. Cursor reset can temporarily revisit hot streams but does not acknowledge or delete work. Partial iteration retains assigned pending work for existing idle-lease recovery. No durability, expiry, zero-double-booking or queue-drain gate is relaxed.

## Validation evidence

Baseline: [real-Redis starvation reproduction](../capacity/flash-sale-opening/writer-fairness-reproduction-2026-10-03.json); [ADR 0121 same-load control](../capacity/flash-sale-opening/paid-callback-connection-control-2026-10-03.json). Tests and candidate comparison have not yet executed.

Approved validation: adversarial hot/cold fresh and reclaim progress, early COUNT continuation, refresh-boundary fairness, hard total assignment, iterator suspension, summary failure/reset, concurrent consumers, post-commit replay and existing registry/failover/financial/100-contender cases. Then one candidate versus ADR 0121 at exactly 60 buyers/s for 300s, two generator shards, eight callback slots and identical service budgets. Retain exact paid/unpaid post-TTL audit, zero double booking/duplicate linkage, full-keyspace Redis/database queues, Kafka drain, source/settings/restoration/readiness, generator idle and private cleanup. Failed gates prohibit higher load, promotion and main merge. Append executed evidence and limitations.

Executed local validation: 492 unit/integration tests passed in 75.71s with two existing dependency warnings. The four real-Redis adversarial fresh/reclaim cases (immediate and cached refresh) failed against cbdc834 in 1.79s and passed with the fix: cold work is served on iteration two, rather than never in twelve. Nineteen new unit cases cover bounded windows, repeated hot/cold progress, early-full iterator suspension, failed-summary continuation and reset. Real Redis concurrent writers assigned 32 unique entries in bounded batches without discovery ACK/delete. Real PostgreSQL commit-before-marker failure plus Redis connection reset replayed four hot commands exactly once and then persisted the cold command, leaving five holds/orders/commands/outbox events and no pending streams. Existing 100-contender, registry races, failover/reset, payment and duplicate fulfillment tests passed. Changed-file lint and Git whitespace checks passed. Owned isolated PostgreSQL 17.6 and Redis 7.4.5 containers were removed. No candidate cloud result is claimed yet.

Executed cloud comparison (2026-10-04 Bangkok): checkout-20261003T170831Z-3fe2a3 tested c66265e against ADR0121/74364c0. Both hosts matched c66265e; all ten runtime-module hashes matched restored images, and only redis_reservations.py differed from the baseline. Same60buyers/s300s/two shards/500journeys/eight clients per shard/250connections per shard/1s polling/eight callback slots; same four API pool4/waiters12/cache3000, six consumer pool8, three writer batch4 and PgBouncer24 budgets. Effective client fixture remained distinct scheduled actors over18000positions, sixty shows with300seats each.

18000scheduled=18000dispatched=18000customer-confirmed unique orders/tickets; zero drops/customer errors/retries. Confirmations rose37.39% from13101; prior4457drops and442customer503s became zero. Worst-shard (not combined) hold-to-ticket p95 improved14.750s to5.226s (-64.57%); durability2.331s to2.061s (-11.57%). The existing bounded customer load gate passed.

Matched offered-window writer collection mean increased22.088ms to24.656ms; command age improved952.216ms to865.399ms and PostgreSQL batch137.928ms to125.429ms. Callback due-to-claim mean fell7054.313ms to71.370ms and delivery148.108ms to66.962ms. Three internal callback delivery errors recovered before final financial audit; they are not customer errors. This single fresh-fixture comparison does not isolate the cause of the large callback improvement; no CPU mechanism is established.

Exact fresh post-TTL audit:18000orders=18000successful payments=18000bookings=18000issued tickets=18000callbacks; no unpaid/expired/pending orders, incomplete callback deliveries, duplicate seats or multi-booking orders. Full-keyspace Redis/database queues were empty, Kafka lag zero, required pipeline/Kafka observers passed. Source/settings/readiness, service budget restoration, generator idle and private cleanup passed. Restored baseline remains four APIs in postgres mode/pool3/admission4/waiters3/cache0; consumer1/pool12; writer/refresh/expiry0; simulator4/batch; PgBouncer24/reserve0/client160. Candidate source remains in experimental images.

Evidence limitation: the optional cgroup CPU sidecar started before the runner's initial state.json existed and failed before collecting any samples. The operator collection wrapper exited1 after required audits and summaries completed; the original customer-stage runner exited0. This failure is retained in the report and private logs. Report generation used the completed verified evidence separately, without repeating load. No host/cgroup CPU comparison is claimed. A bounded checkpoint-file wait in this temporary collector is future tooling work, not an implemented orchestration capability.

[Same-load evidence](../capacity/flash-sale-opening/paid-writer-fairness-control-2026-10-04.json). Approved implementation and comparison are complete. No rate increase, main merge or promotion occurred. This qualifies only the bounded60buyers/s development-payment profile across sixty shows; sustained300000paid tickets/hour, one-hot-concert behavior and real-provider production capacity remain future work requiring separate authorization.

Temporary CPU sampler readiness behavior is superseded by [ADR0123](0123-bounded-paid-cpu-sampler-readiness.md);application fairness/persistence decisions remain unchanged.
