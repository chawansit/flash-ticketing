# ADR0217: Shared callback API placement comparison

## Status
Accepted under ADR0172. Implemented and locally qualified; no new cloud experiment started.

## Context
ADR0216 passed 60 journeys/s on fixed 2+2 APIs, with shared callback routing reducing end-to-end p95 from 2514.99 to 1658.07 ms and primary CPU from 77.11% to 68.83%. Earlier primary-local callback routing made the 1+3 placement comparison fail. Moving an API away from the host running background services may now reduce contention, but that benefit is unmeasured.

## Decision
Compare 2+2 control against 1+3 candidate with simulator API_URL=http://load-balancer:8000 in both arms. Keep frozen ADR0163 application and generator images, synchronous intake, gateway delays, polling, worker placement and counts, Redis/RDS and aggregate database/admission budgets unchanged. Reuse the protected paid runner, its diagnostic observer and callback coverage gate. Require positive successful callback counter deltas on all four APIs in both measured arms; these are HTTP counters, not unique payments.

Use a fresh registered shared_callback_placement scope with one safety pair, two paid stages and four safety protocols/tickets. Offer 60 journeys/s for 300 seconds per arm: the shortest matched window comparable to the measured routing baseline and covering observed intermittent stalls. The experiment ceiling remains 3600 seconds, including setup and mandatory audits/restoration. Failed control stops candidate. No extra resources or spending. This supersedes ADR0216 fixed 2+2 placement only for this comparison; historical plans and consumed scopes stay unchanged. The normal deployment is restored after testing. Neither 84 tickets/s nor hourly qualification is authorized by this profile.

## Alternatives
Repeat the old local-callback placement test: retains the measured routing bottleneck. Raise load immediately: confounds placement and load. Increase connection slots or deploy different binaries: confounds budgets and sources. Create a new worker-separation runner: deferred while the proven runner can answer this hypothesis.

## Consequences
One API moves from the primary host, which also runs background services, to the existing secondary host. Four API replicas, 16 general pool connections with eight included payment slots, PgBouncer 24 and admission budgets stay fixed. Load balancer forwarding retains proxy_next_upstream off. No payment transaction, locking, TTL or replay semantics change. A passing five-minute comparison measures placement at 60 journeys/s; it cannot prove maximum capacity or 300000 tickets/hour.

## Failure and recovery behavior
Reject route, actual placement, source/image, budget or observer drift before dispatch. Preserve zero double-booking/payment loss, customer authorization, post-TTL durability, relationship checks, full queues/Kafka drain, credential cleanup and exact original runtime restoration. Preserve failed evidence; never replay an ambiguous scope. Unresolved ownership or restoration blocks further load. No relaxed customer gates or extra retries.

## Validation evidence
234 focused unit tests passed, covering exact route/placement, real constructor and observer checks, protected diagnostics, fresh scope accounting and existing failure/restoration gates. Ruff, canonical naming and whitespace checks passed. The existing native Nginx forwarding integration from ADR0216 is unchanged; it was not rerun. Cloud comparison pending. Reference: [ADR0216 measured routing evidence](../capacity/flash-sale-opening/callback-routing-comparison-2026-10-08.json).

Safety scope bounded_shared_callback_placement__e71e05f76ac7 failed qualification before paid dispatch: the diagnostic collector did not resolve ADR0217 and rejected candidate 1+3 as 2+2. Both arms passed one-owner, payment replay, authorization, post-TTL financial and final queue/restoration audits. Failure retained and resolved separately under [ADR0218](0218-safety-only-diagnostic-abort-recovery.md); exact collector correction and 130 regression checks passed. Fresh qualification is required; no placement capacity result yet.
