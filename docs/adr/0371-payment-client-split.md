# ADR0371: Redistribute six API clients toward payments

## Status
Accepted2026-10-19 for implementation/local validation within the authorized fixed resource ceilings. Cloud qualification requires a fresh source-bound180 followed by200only after a clean restored180.

## Context
ADR0370 runccfaaa0da420 passed all formal observer, issuance, financial and restoration gates:54000completed,0drops/final failures. Two initial payment503errors recovered, so the strict clean prerequisite still blocks200. Complete slot evidence captured3payment role_limit events. During the later burst three payment clients were committing for~206–220ms while general client capacity was spare. The existing17-payment-waiter guard rejected work before the500ms timeout. Repeating unchanged traffic is not selected.

## Decision
For an explicit ADR0371 CCE candidate only, redistribute each API's existing6clients from3general+3payment to2general+4payment. Reduce the shared acquisition queue from20to19: the existing budget formula then gives15general/17payment native waiters. Thus total clients/backends, maximum native waiters and500ms timeout do not increase. Four APIs still have24clients and share the same24physical backends/no reserve. No infrastructure resize or new cost envelope.

Keep the same verified ADR0370 application image, callback-claim batching, progress instrumentation, terminal failure capture, all SQL/locks/durability/idempotency/TTL, generator settings and all acceptance criteria. Bind actual2+4/19settings separately; saved-service semantic normalization is allowed only for this explicit mode and exact values. Common ECS bootstrap/restoration stays unchanged. Declare the client split as an additional changed factor; do not claim a matched performance comparison.

## Alternatives
Increase timeout, queue or backend ceilings: rejected. Repeat identical180until lucky: rejected. Redistribute to1general+5payment: greater read-starvation risk. Resize RDS: no measured need established. Keep3+3: retains observed unused general capacity during payment queue rejection.

## Consequences
One additional payment client can absorb a burst that previously exceeded3active+17queued. General concurrency is reduced and must be checked for starvation/latency. Shared queue19is below the existing20ceiling; native role queues remain at most17. This is a capacity hypothesis, not a clean200claim. Zero initial/recovered/final errors and drops remain required for progression.

## Failure and recovery
Reject orphan/mixed selectors, unexpected settings, expanded budgets or an incorrect prior proof. A failed local comparison prevents deployment. A cloud failure stops progression and invokes unchanged financial/drain/restoration. Preserve consumed identities and all prior results. No main merge.

## Validation evidence
Executed:2actual immutable-image PostgreSQL burst cases passed (old20/21successes, new21/21; general SQL read available, native waiters17, total clients6, shared budget returned0). Owned containers/network cleanup passed.40focused mode/observer/binding checks passed,1optional unchanged-image helper rehearsal skipped. Ruff passed. Actual preparation and isolated real-parent manifest/binding/reservation passed with6total/4payment clients and queue19verified in generated Secret, unchanged19pod resources; live ledger/goal unchanged. Three immutable registry manifests authenticated and verified. No new cloud performance result yet. The previous180remains PASSED_RESTORED under original SLOs but not clean and cannot authorize200.
