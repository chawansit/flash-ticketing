# ADR0157: Isolated deduplication comparison profile

- Status: Accepted; corrected bounded cloud safety pair qualified; measured comparison pending
- Date: 2026-10-05

## Context

ADR0156 saves one advisory snapshot read for eligible committed payment/ticket event pairs. Six immutable candidate images were built offline from the exact original role parents and passed source/import/dependency/configuration checks. The ADR0151 runner compares event refresh off/on and its inventory records only that flag. Reusing that factor or its consumed allowance would invalidate a deduplication comparison.

## Decision

Add a separate deduplication contract and entry point, reusing the existing bounded ADR0151 engine in a private module namespace. Load the engine from its existing source without editing or copying its protocol. The profile supplies its own plan, contract, source verifier, adapter identity and authorization ledger. Keep the original exclusive lock and owned run IDs for shared infrastructure exclusion and recovery. Do not mutate the imported original runner or its historical ledger. A separate image-staging profile reuses the existing owned immutable cache upload/seal/load checks, binds to the same new approval, and acquires the shared lock after approval validation. Release the staging lock only after confirmed runtime-preserving success; retain it on an ambiguous failure for explicit recovery.

Both arms use identical ADR0156 images, two APIs per host, 1000 ms API/consumer cache, event refresh enabled in the consumer, unchanged host sizes, pools, admission limits and generator workload. Only consumer ORDER_STATUS_EVENT_REFRESH_DEDUP changes from 0 to 1; every other role explicitly uses 0. Record all contract setting keys in worker evidence and the API dedup flag. Attach an exact ADR0157 inventory marker before validation; the observer accepts it only with the existing approved inventory digest and complete matching factor evidence. Preserve ADR0151 marker behavior.

Bind the new entry point, contract, old engine/shared adapters, plan and source artifact to approval. Require a distinct new bounded_status_refresh_dedup ledger and authorization ID. Preparation defaults to local-only and does not create/reset allowances or cloud state. Retain separate dry qualification and measured-pair invocations, ordered arms, stop-after-failed-control, replay protection, exact source/images, post-TTL durability, zero double-booking, all 32 gates, global queues/Kafka drain and restoration. Reports identify ADR0157; an ADR0151 qualification cannot authorize this factor.

## Alternatives

Retoggle event refresh: compares a different factor. Reset the previous allowance: destroys consumption history. Copy the 500-line engine: duplicates recovery/gate logic. Globally monkeypatch the original runner: contaminates concurrent callers. Rewrite the whole orchestration engine: increases the change surface. A private loaded namespace reuses the tested protocol while keeping profile bindings isolated; cover this boundary with synthetic tests.

## Consequences

The wrapper depends on named engine interfaces; adapter hashes and regression checks fail closed when those interfaces change. Shared collector/observer changes are additive and must pass legacy regression checks. Offline preparation and synthetic gates do not qualify live cloud behavior or increased capacity. Comparison remains 60 journeys/s for 300 seconds per arm if separately authorized; it is not a 300000-ticket/hour qualification.

## Failure and recovery

Missing/drifted source, mutable images, absent factor evidence, mismatched marker, old qualification, missing approval, paused state or consumed allowance blocks cloud work. Existing lock, conservative counters and finally restoration remain authoritative. Ambiguous dispatch keeps ownership/consumption markers for explicit recovery. No automatic replacement or higher load.

## Persistence, locking, messaging, idempotency, TTL and scaling

Application persistence, reservation locking, messaging, payment/inbox idempotency and TTL are unchanged. Reuse the existing orchestration state writes and shared exclusive lock. Fixed 2+2 API placement and total connection budgets remain unchanged. This supersedes only the ADR0151 factor/authorization/marker selection for the new independent profile; the original experiment remains supported and its evidence unchanged.

## Validation evidence

Decision recorded before implementation. Executed **277 broader regression checks** and **32 final profile checks**, all passed (overlapping coverage; 280 distinct cases across the two runs). Existing ADR0151 artifact, staging, paid-runner and inventory regressions passed. Synthetic checks preserve all 32 gates and financial/duplicate/global-queue failures; verify private namespace isolation, single-factor models, wrong/missing flags, old-ledger/qualification rejection, ordered stop/restore and ambiguous ownership retention. Default preparation made zero cloud calls and left state/allowances unchanged. The observer's two uploaded scripts were tested in an isolated interpreter with no dependency on the new host-side profile package. Changed-file Ruff passed.

Six offline immutable candidate images covering seven roles passed source/import/bytecode, app/installed module, dependency and inherited full configuration/parent layer checks. Three flag-setting smoke checks ran in network-disabled, read-only consumer containers and exited successfully. All 19 runtime modules match the prior native-tested ADR0156 candidate; application tests were not repeated for this harness-only stage.

[Preparation evidence](../capacity/flash-sale-opening/order-status-refresh-dedup-preparation-2026-10-05.json) preserves attempts, raw hashes and exact next-scope proposal. No image staging, service deployment, cloud safety ticket or capacity load occurred. The dry proposal permits at most two isolated simulated paid-issued safety tickets and zero capacity stages only if subsequently authorized. Live capability and performance improvement remain unqualified.


## Corrected cloud safety qualification (2026-10-05)

Run **adr0151-a6cdb46f8cff** passed both dedup-off/on arms after ADR0158/0159 harness corrections. Each arm verified source/images/factor,100 cross-host requests with exactly1accepted hold and99expected conflicts,one durable owner,hold/payment replay,other-actor denial and one customer-confirmed paid ticket. After hold expiry each retained1successful payment,1booking,1ticket and3callback deliveries,with0duplicate seats/orders and0pending financial work. Full global queues/Kafka lag returned to0;observer qualification,private cleanup and original topology restoration passed after each arm. Total2simulated safety tickets and0capacity stages; no customer retry hid failures.

[Safety qualification evidence](../capacity/flash-sale-opening/order-status-refresh-dedup-safety-qualification-2026-10-05.json). This qualifies the tested bounded safety configuration only. Deduplication CPU/DB cost and capacity improvement remain unmeasured; a separate fixed-load off/on comparison is proposed but not authorized. Earlier failed reports and consumed scopes remain preserved.
