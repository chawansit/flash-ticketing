# ADR0200: Guarded worker comparison assembly

## Status
Accepted for implementation under ADR0172 and ADR0184 through ADR0199. Adds a concrete bootstrap and paired coordinator alongside the ADR0186 offline-only engine; that engine remains offline-only. Complete local assembly qualification and work-envelope registration are separate gates. No production scaling or customer gate is superseded.

## Context
Individual worker placement, readiness, inventory, paid-stage and restoration components are qualified, but they do not yet form one executable comparison. A boolean passing-control marker alone is insufficient provenance. Bootstrap failures can occur before a paid stage exists and must not strand a changed runtime.

## Decision
Use one original guarded session and immutable input/source/snapshot binding for the comparison. Stage the exact prepared image package once before either arm. For each arm, construct the existing concrete configuration/runtime/execution/audit components, install sealed configuration, verify the original runtime and generator, establish complete queue drain, stop exact original workers, configure common infrastructure, verify private dependencies, start fixed workers and collect the source-bound host inventory. Create the existing diagnostic and paid-stage adapters only after those gates, then use RestoredPaidArm for paid execution and mandatory recovery.

A fully passed, persisted and restored control issues ADR0199 continuation proof. Persist its exact summary and handover hashes in the original scope before constructing the candidate. Candidate paid allowance consumption requires the claimed same-runtime proof and matching persisted authorization; a boolean alone never authorizes dispatch. Failed controls, unknown mutation outcomes, journal loss or uncertain ownership block candidate construction.

Bootstrap recovery observes the actual runtime independently. An unchanged original runtime requires no worker mutation. Otherwise stop only verified arm workers, prove absence and restore through the existing sealed configuration. Unknown partial original-worker removal is not adopted or replayed. Always verify restoration, queues, generator idleness and global zero double-booking. No fixture or customer job exists before the paid-stage boundary: record this explicitly as zero dispatch with unchanged paid allowance, rather than fabricate a financial cohort pass. After a stage exists, retain all existing financial, post-TTL, booking and queue checks. Keep ambiguous configuration ownership for recovery; clean only verified sealed configurations after every applicable safety gate passes.

## Alternatives
Reusing the offline engine for live transport would remove its explicit safety boundary. Independent per-arm scripts could drift and lose paired provenance. A bare passing-control flag could authorize an unrelated candidate. Retrying ambiguous mutations or inferring missing ownership would conceal failures.

## Consequences
The runner coordinates existing primitives and does not change workload, machine sizes, connection budgets, latency/error gates, retries or payment semantics. Qualification uses synthetic transports plus existing native and database checks; live capacity remains unmeasured. Registration remains disabled until the complete integration and accounting/entry-point checks pass.

## Failure and recovery behavior
Journal intent precedes forward mutations. Any failed control stops progression. Original session, source and scope identity remain checked at every action. Pause or deadline blocks new forward actions but permits known cleanup. An unknown original-stop outcome is observed and retained without replay. Partial placement is cleaned only when the existing runtime ownership checks accept it. Missing seals, changed files, incomplete queues or lost cleanup acknowledgements require recovery and retain evidence.

## Validation evidence
Executed evidence: [comparison assembly qualification](../capacity/flash-sale-opening/background-service-separation-comparison-assembly-2026-10-07.json). The broad suite executed 730 cases: 726 passed and four isolated test-stub failures were corrected; the affected suite then passed 42 cases with one native case skipped outside the pinned-image harness. That native case had passed in the broad run. All 730 distinct cases therefore have passing executed evidence across the two runs; there was no single 730-pass rerun. The broad harness included 23 real PostgreSQL financial cases and three isolated Linux cases. Its owned database container was removed. Twenty-two connected coordinator tests exercise progression, exact authorization, unknown mutation outcomes, journal loss, pauses, drift and bootstrap recovery. Staging, inventory and customer replies are simulated in those connected tests. Work-envelope entry-point/accounting integration and registration remain unqualified. No cloud load or capacity improvement is claimed.
