# ADR0313: Reconcile the failed headroom cohort without qualifying capacity

Status: Accepted; independent read-only verification completed.

## Context
ADR0312's102/s run dispatched28017of30600 journeys, confirmed26636customers, and retained1381final failures plus2583generator drops. It fails the approved SLO. The original terminal audit compared against30600payments, while the actual cohort contains27941paid-issued orders and76expired unpaid orders. Its original RECOVERY_REQUIRED result must remain failed.

## Decision
Independently repeat the canonical bounded read-only audit against the exact retained102-show fixture:28017orders,27941successful payments/tickets,76expired unpaid orders, all seven relationship predicates and completed callbacks. Authenticate the original result463e92253a3613ea9bc5e591823596681b890611290d224bc8ad9a83618b1424, binding, journal and fixture before closure. Verify both safety payments, normal runtime identities, namespace/helper absence, generator idle and global queue drain again.

Only after that evidence passes may the ledger close as FAILED_RESTORED. Preserve the original raw result, failed customer/financial target gates, customer counts and in-window throughput. The recovery is an integrity/restoration proof, never capacity qualification or a retry of failed customers.

## Alternatives
Ignore the recovery block: rejected because future tests could overlap incomplete work. Reclassify the failed workload as passing using actual throughput: rejected. Delete the cohort: destroys financial evidence.

## Consequences
Known paid records and known expired unpaid orders can be reconciled without requiring nonexistent undispatched sales. This does not relax financial correctness: every successful payment must have one booking/ticket and valid inventory/ownership relationships.

## Failure and recovery
Keep recovery blocked on any mismatched count, relationship, missing fixture provenance, pending work, duplicate booking, payment loss or missing restoration. No new paid traffic or infrastructure changes are authorized by this recovery. Read-only verification has a900-second bound; preserve failures.

## Validation evidence
Pending focused adversarial validation tests and independent cloud verification. Measured overload diagnostics show16943order-status503responses, general waiters up to17per replica,80.44msmean pool wait, API CPU2.007of4allocated cores, and WALWrite wait peaks23. General-slot scarcity and database stalls are hypotheses requiring a separately controlled correction; exact root cause is not fully established.

Executed12adversarial validation tests and Ruff passed. Independent cloud audit reconciled28017orders:27941successful payments/bookings/tickets and76expired unpaid orders; all seven relationship checks and both safety payments passed. Namespace/helpers absent, generator idle, queueszero, runtime identities stable. Ledger closed FAILED_RESTORED; original raw463e922... and failed quality/financial expectations preserved; capacity_qualifiedfalse. [Evidence](../capacity/cce/failed-headroom-recovery-2026-10-13.json).

Extension: the same immutable failed-result closure also covers the declared ADR0314case adr0151-4a166cdbdda6 (resultd5a00a...):27276paid-issued orders, zero unpaid orders, zero final customer failures, but3324generator drops. It remains a failed capacity test. Authenticate its separate fixture, result and binding; repeat the complete read-only audit and restoration proof. No generic unknown-case closure is allowed.

The exact ADR0315 cohort adr0151-6fd6d0275799 is additionally eligible for read-only closure: result250aa2541308119f8da10a7ba3ebdf8024d99fb8a1fffbb796e8eead55fbfa2a, ledger bounded_cce_ticket_target__cf9e12d21c2f, 30529 paid-issued orders, no expired unpaid orders, 71 generator drops and zero final failures. Its in-window ticket-target gate passed (30370 tickets, conservative30155), but its customer-quality gate failed. Preserve both facts and the overall failure; no capacity qualification follows from closure. Require the complete independent financial and restoration checks against this exact third fixture/result/binding. Read-only verification is bounded900seconds; no new paid traffic.

Extension for ADR0316: only cohort adr0151-3f899bb32f0e, ledger bounded_cce_ticket_target__debd1beb6d6b and raw result aa6875dbd7ab0669159a60a28c21e97e81f143792f347acc71249f7daeb58c92 may use the same read-only closure. Authenticate 27,975 orders: 27,974 paid-issued and one expired unpaid. Preserve 2,625 generator drops, one final command_http_502 failure, failed quality/in-window gates and the original failed result. Repeat all financial relationships, both safety payments, normal runtime identity, empty queues and absence/credential checks within 900 seconds. Closure remains FAILED_RESTORED and capacity_qualified false; it does not adopt batch eight.


ADR0317 adds one exact case: adr0151-c816742577d3 / bb26a572a41bd4dad7d2f119a80202e00eb419b447686e0ff4fd649b4d2e6174. The actual paid cohort is15,398, all recorded orders paid; one dispatched hold_http_502 has no recorded order. Require original alias/configuration rollback plus fresh financial/runtime and safety checks. Capacity remains failed. This extension is financial closure, not relaxation of the successful-ticket or zero-drop gates.
