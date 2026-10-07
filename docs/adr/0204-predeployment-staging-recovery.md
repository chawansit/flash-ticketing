# ADR0204: Predeployment staging recovery

## Status
Accepted for implementation under ADR0172. Supersedes ADR0200/ADR0201's missing failed-staging closure path only when no service deployment or customer stage began. No load allowance, customer gate or correctness guarantee changes.

## Context
The first registered worker comparison stopped during immutable image metadata verification. Its original staging receipt reports FAILED_CLEANED, unchanged running containers and removal of the exact uploaded archive. No arm, fixture, customer dispatch or service deployment began. The outer coordinator has no failed-staging recovery branch and retains RECOVERY_REQUIRED. A failed image proof cannot count as a passing staging or comparison result.

## Decision
Keep the original failed result immutable; retain the original reservation identity and every consumed allowance. Independently bind recovery to its exact ledger, original result digest, protected configuration, saved runtime, image contract, helper hashes and cleanup receipts. Read-only verification must prove original container IDs, starts and runtime semantics on both hosts, every retained broker volume and inactive container, original binds, idle generator, global zero double-booking, complete database/Redis/Kafka queue drain and absence of every attempted staging owner/proof container. Prove no paid allowance, arm or fixture was started from the original scope and durable receipts. Do not infer an empty financial cohort after any attempted stage. Preserve any failure or uncertain ownership as blocking.

A successful recovery may close only this failed predeployment reservation with separate append-only recovery evidence and elapsed accounting; retain the failed staging result, do not refund allowances, reopen the experiment, replay staging or claim performance. New tests require new owners and a fresh reservation. The normal coordinator should execute this verification automatically after known failed staging, before final accounting. Recovery never deploys services or deletes unowned resources.

## Alternatives
Leaving a verified harmless staging failure permanently blocking ignores recoverable evidence. Treating FAILED_CLEANED as sufficient omits independent financial/queue checks. Replaying the image package or resetting its counters loses ambiguity protection. Manual fabricated passing results cannot establish recovery.

## Consequences
Additional checks run only on predeployment failures. Recovery remains explicit and conservative. Unexpected running/proof resources, missing receipts, changed runtime or nonzero queues continue to block additional load. Correctness evidence takes priority over fast progression.

## Failure and recovery behavior
Persist recovery intent before read-only probes and retain results even if closure cannot be certified. Credential material remains protected. Unknown acknowledgements do not authorize retry of staging or customer dispatch. A pause permits owned cleanup and mandatory verification only. Every failed verification leaves the original scope blocked and reports the exact unresolved issue.

## Validation evidence
Original protected result: tmp/adr0153-parents-cb01412061a8/worker-envelope-result.json. Staging exception: image metadata differs. At decision time, implementation and independent recovery verification were pending; executed results are recorded below. No customer load or capacity improvement has been measured.

Executed qualification: [staging recovery and portable image identity](../capacity/flash-sale-opening/background-service-separation-staging-recovery-2026-10-07.json). The final affected suite passed 587 tests in 307.29 seconds; a separately added changed-runtime recovery test passed in 4.06 seconds. Recovery closure/rejection tests passed 37 cases. Independent cloud read-only recovery passed and closed the old scope as FAILED_RESTORED, preserving its original failure and elapsed accounting. A fresh schema-2 package passed local validation; no customer load or capacity improvement is claimed.
