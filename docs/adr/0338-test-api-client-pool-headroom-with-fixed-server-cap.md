# ADR0338: Test API client pool headroom with a fixed server cap

## Status

Accepted for local qualification and preparation of one configuration comparison. Cloud execution requires a fresh paid-stage approval. Existing defaults and historical profiles remain unchanged. ADR0337's32-slot simulator candidate and ADR0336's authentication candidate remain unadopted.

## Context

The failed ADR0337 test increased simulator delivery jobs16to32, retaining four APIs with2general+2payment client connections each and24 physical PgBouncer backends. API acquisition mean was70.08ms; successful connection hold mean17.69ms, query means about2–3ms and commit mean2.39ms. Independent resource samples recorded PgBouncer maxwait peak9.809ms and full server occupancy at some samples. These different scopes and sampling intervals do not prove every API wait occurs locally or that PostgreSQL has spare capacity. They support testing the application client-pool ceiling rather than increasing callbacks again.

The same-image sixteen-slot ADR0335 reference had31.25ms API acquisition mean,127.15/s conservative throughput, drops and five final customer failures. Best qualified clean short capacity remains138.49/s. Neither failed reference is a passing control.

## Decision

Prepare a single coupled API-client-budget change: DB_POOL_MAX4to6 and API_PAYMENT_POOL_MAX2to3, retaining the equal general/payment split. Four APIs therefore have at most24 rather than16 client connections. Preserve the24 physical database backends, reserve zero, existing pooler timeouts/max clients, shared acquisition ceiling20 per API and500ms acquisition deadline. The existing finite per-role acquisition formula consequently changes each role's ceiling18to17; do not hide this derived change.

Use the immutable ADR0324 image for APIs and all workers, four API/fifteen worker pods at unchanged resources, simulator16/pool10, gateway delays, customer recovery/polling,160 offered journeys/s300seconds,800 active journeys,500 HTTP connections and sixteen clients. Do not apply either unadopted application or32-delivery-slot candidate. Select the experiment explicitly; defaults remain4/2. This increases client connections only and does not reserve more physical RDS connections or increase RDS size.

## Alternatives

More API replicas: changes CPU and client capacity together; defer until the remaining CPU bottleneck is measured. More physical backends: can worsen the observed WAL contention. More callbacks: ADR0337 worsened errors. Move one slot from general to payment: ADR0314 already demonstrated the cost of starving general reads. Remove transaction setup/timeouts, reuse rejected read pipelines, extend retries or weaken quality gates: not justified by this evidence.

## Consequences

More concurrent client transactions may reduce local waits, move waiting into PgBouncer/RDS or saturate API CPU. There is no assumed throughput gain. Report acquisition time, pooler waiting/occupancy, API CPU, initial/recovered/final customer outcomes, generator drops, latency and in-window paid-issued tickets. Explicitly distinguish client24 from physical server24; background clients still share that physical cap.

## Failure and recovery behavior

Keep general/payment physical pool isolation; neither borrows from the other. Preserve bounded shared admission, timeout/reclamation and pool cleanup. Preserve actor authorization, atomic holds, TTL, payment idempotency, payment/outbox commit and duplicate-safe issuance. Stop escalation on any quality failure and restore the exact original runtime. Retain original failed reports and reconcile actual paid and expired unpaid orders independently. No new paid stage is authorized by local implementation.

## Validation evidence

Pending real-PostgreSQL checks for general-read saturation with3+3 slots, concurrent payment/replay, duplicate-safe issuance, shared admission bounds and cleanup. The later cloud comparison must bind actual generated API settings6/3 and unchanged worker settings16/10 before deployment. Local correctness and manifest checks do not establish cloud capacity or300/s.


Executed24 real PostgreSQL17.6/Redis7.4.5 integration checks inside the unchanged ADR0324 image, zero skipped,11.99seconds of tests. Both direct persistence and authenticated HTTP checks verify3+3 payment/callback progress while every general connection is occupied, duplicate callback/issuance replay and actor isolation. Shared20-slot saturation, role17/17 bounds, retained timeout positions and recovery/cleanup passed.48 pool unit checks passed. Initial collection/dependency failures were local test setup errors; separate unit/integration processes and an isolated Redis fixed them. Owned containers/network were removed.

Implemented the explicit ADR0338 API-only settings overlay. Unselected/default profiles remain4/2; workers and simulator remain unchanged. Current and historical source/contract checks are preserved. The existing saved_api_service_sha256 field hashes derived CCE API settings, not the untouched normal runtime: independently recompute current6/3 and normalize only those two fields to4/2, then require the historical service hash. Arbitrary service/source/resource/worker changes still fail. A regression exposed the worker selector reading the consumed global goal; it now uses the selected runtime context.43 focused and then all251 broader runner checks passed. Ruff F/I and535-file reproduction passed.

Full generated API/worker manifests, binding and reservation admission passed against the actual existing image receipt, without publication mocks. Verified four APIs at6/3, fifteen unchanged workers and simulator16/10. Approval/reservation accounting used isolated copies only; live envelope/journal/state hashes remained unchanged. Prepared one unauthorized160/s300s goal with unchanged800 active journeys/500 HTTP connections/sixteen clients,24 physical backends and3600seconds including mandatory safety/audits/drain/restoration. No cloud service, application source, image or live paid allowance changed.

[Local qualification](../capacity/cce/api-client-pool-headroom-local-2026-10-17.json) and [runner readiness](../capacity/cce/api-client-pool-headroom-runner-2026-10-17.json). A fresh comparison approval must explicitly include24 aggregate API clients instead of16 while retaining24 physical backends. No capacity improvement has been measured.
