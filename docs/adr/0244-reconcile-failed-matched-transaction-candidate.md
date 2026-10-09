# ADR0244: Reconcile failed matched transaction candidate

## Status
Accepted and independently verified. The consumed candidate is FAILED_RESTORED; its original failed customer and aggregate gates remain unchanged.

## Context
ADR0242 control d451a218d696 passed all gates. Candidate 715430323753 dispatched 25,200 journeys, fulfilled 25,198 and returned two payment HTTP 503s. Its aggregate financial audit expected 25,200 and failed, despite observed succeeded payment/ticket counts of 25,198 and zero duplicates. Normal restoration and queue drain passed. Independent exact-cohort verification is required before closing recovery.

## Decision
Reuse ADR0235 read-only, set-based financial relationship auditing for this exact consumed short scope: 84 fingerprint-verified shows, 25,200 orders, 25,198 paid/fulfilled tickets and two expired unpaid orders. Pin original report, ledger and binding hashes. Audit both safety payments, elapsed TTL, every relationship, zero duplicates, all global queues, normal one-consumer membership, the canonical candidate namespace, absent helpers/private manifests, retired owned shows and stable restored runtime. Use a 600-second recovery ceiling, a 60-second paid relationship query bound and existing 20-second safety bounds.

Close only this consumed scope as FAILED_RESTORED after complete proof. Preserve the original failed report, gates and paid-stage consumption. Recovery neither qualifies capacity nor reopens load authorization. This extends exact-scope verification, not production transactions.

## Alternatives
Assume aggregate failure means payment loss: incomplete diagnosis. Mark capacity passed because observed counts match: conceals customer errors. Replay the consumed run: violates ownership. Leave retained, verifiable financial evidence unchecked: blocks safe progress.

## Consequences
Report customer failure separately from independently verified data integrity. Record all recovery attempts and actual time. No financial writes, grants, database settings, resource sizes or new paid stage.

## Failure and recovery behavior
Any ownership, hash, cohort, relationship, count, queue, cleanup identity or runtime mismatch blocks closure. Never reset Redis, delete financial data, recreate journeys or relax original gates. Preserve timed attempt receipts and remove temporary authentication material.

## Validation evidence
Affected financial recovery tests: 44 passed, including wrong-run namespace rejection. The first read-only attempt took 51.688 seconds: paid relationships passed but the historical OR-based safety join hit its 20-second statement timeout. Reuse the accepted set-based SQL for both safety cohorts without changing their bounds or predicates. A subsequent 39.563-second receipt used a historical namespace in the recovery-only driver; retain it as superseded and do not use it to close the ledger. Repeat with the canonical namespace and recursive manifest checks, enforcing exact namespace identity in closure validation.

Final independent verification passed: all 25,198 successful payments have valid unique issued tickets, two unpaid orders expired, both original safety payments retain one ticket and three callback deliveries, all seven relationship predicates reconcile after TTL, every queue is zero, one normal consumer remains, the owned namespace/helpers/private inputs are absent, 86 owned shows are retired and restored runtime is stable. Total recovery accounting is 130.376 seconds, including all three attempts. The original result hash and one consumed paid stage are preserved. [Sanitized candidate and recovery receipt](../capacity/cce/transaction-candidate-2026-10-09.json). No performance improvement or hourly qualification is claimed.
