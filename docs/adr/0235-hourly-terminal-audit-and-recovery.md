# ADR0235: Hourly terminal audit and independent recovery

Status: Accepted; implemented and independently verified for the consumed hourly recovery.

## Context

ADR0232 run adr0151-0f735e978cba offered 302400 journeys in one continuous hour. The database counted 302262 unique paid-and-issued tickets inside the hour, including 302037 in the conservative inner window. The customer report confirmed 302399 journeys and one payment HTTP 503. Its strict customer gate failed. Pipeline observation collection also failed, and the terminal audit rejected 302400 because its local whitelist still allowed only 18000 and 25200. Original topology restoration and global queue drain succeeded, but the scope remains RECOVERY_REQUIRED. The failed report must remain immutable.

## Decision

Allow exactly 302400 terminal expectations only with the distinct 1008-show hourly cohort; retain existing short bounds. Independently audit the saved, fingerprint-verified fixture for the consumed hourly reservation without offering more customer traffic. Reuse the existing read-only aggregate and relationship SQL in a repeatable-read snapshot. Account for all 302400 orders, 302399 or 302400 successful payments/tickets, and at most one expired unpaid order matching the single customer failure. Require every successful payment to have exactly one valid fulfilled booking and ticket, valid holds/order items/inventory, zero duplicate bookings, elapsed hold TTL, drained global queues, retired owned shows, namespace/helpers absent, generator idle and original runtime semantics restored and stable.

Close only this exact consumed scope as FAILED_RESTORED after independent proof. Retain original result and bindings, paid-stage consumption and failed customer/observation gates. Recovery never qualifies capacity and does not authorize another paid experiment. Inspect retained trace size before changing observation transport; if the full trace is unavailable, report the gap rather than reconstructing or inventing historical CPU/database measurements.

## Alternatives

Mark the hourly run passed from the issuance count, waive the terminal audit, or replay the consumed stage. These would respectively hide a customer failure and observation gap, weaken financial correctness, or violate experiment ownership. Leaving the recovery unresolved would prevent safe future work despite a reconstructible exact fixture identity.

## Consequences

The numerical hourly target is measured, while full production qualification remains failed. A separate recovery receipt records the financial and cleanup proof and its actual duration. Existing backend images, transaction patterns, resources, budgets, retry settings and customer gates stay unchanged. No new infrastructure or paid traffic is required.

## Failure and recovery behavior

Any identity, cardinality, relationship, queue, ownership or restoration mismatch keeps recovery blocked. Do not modify the original report or hide the single 503. Inspect only the owned cohort and existing runtime; do not reset Redis, delete ticket data, recreate customer journeys or infer missing telemetry. Recovery continues under mandatory owned verification authority, with bounded read-only actions.

## Validation evidence

Original private report: tmp/adr0151-0f735e978cba/cce-comparison.private.json. Original result fingerprint: f39a347987c433906b785abcbb6615a58bd720b36e413d857299a5b8968a73cb. Executed local validation: 826 tests passed, one platform skip in 24.76 seconds; lint and naming checks passed. The exact read-only recovery passed all financial relationships, both safety payments, zero-queue and original deployment/cleanup gates. Scope closed FAILED_RESTORED with the original failed report preserved. Sanitized evidence: docs/capacity/flash-sale-opening/cce-hourly-qualification-2026-10-09.json. No passing production qualification claim is made.

## Hourly relationship-query correction

The first independent recovery attempt read 302399 FULFILLED orders and one EXPIRED order, then failed during the relationship audit. The reused relationship query contains a correlated scan of a materialized bookings cohort for each fulfilled order. For the hourly audit only, aggregate order items and issued-ticket counts once per order and join those aggregates. Preserve all seven relationship predicates, read-only repeatable-read semantics, fixed cohort and timeout. This changes verification cost, not application behavior or customer gates. The first failed recovery attempt did not produce an exact elapsed-time receipt; retain that accounting gap explicitly.

EXPLAIN on the revised query showed a total estimated cost of 2496916.07 and expensive whole-table joins for the cross-event booking/inventory checks. Preserve the same union of owned-event rows and rows linked to owned orders by selecting each arm separately and de-duplicating with UNION. This retains detection of foreign-event relationships while allowing indexed cohort filters; no application index, transaction or database parameter changes are made.

The scoped set-based relationship query still exceeded its 20-second statement timeout on the 302400-order cohort. Permit a 60-second statement timeout for this hourly recovery relationship query only, after the unchanged aggregate audit. Keep the existing read-only repeatable-read transaction, two-second lock timeout and 600-second recovery ceiling. This is a verification budget, not a change to customer latency/error gates or database global settings.

## Restored consumer membership

Fresh recovery observed every queue at zero and Kafka lag zero, with one consumer group member. The saved normal deployment explicitly has one consumer; six consumers belong to the temporary paid-test topology. Recovery must require the saved normal one-consumer topology and one Kafka member after restoration. Keep the original paid-stage six-member gate unchanged. This corrects a recovery validator comparing two different lifecycle phases; it does not waive queue drain or deployment identity.

## Supplementary late observation recovery

If the exact owned host trace survived cleanup, collect it read-only with a 512 MiB hard ceiling, 600-second transfer deadline, one-MiB chunks and pre/post regular-file metadata plus SHA-256 verification. Preserve the original failed result. For this supplementary hourly trace only, permit at most 5000 diagnostic rows and 512 MiB while retaining the existing two-MiB line bound, diagnostic completeness/continuity and native process/traffic identity gates. Report independent recovered observations separately; do not silently change the registered runner or declare its original observation gate passed. If limits, identity or coverage fail, retain the explicit observation gap.

## Executed supplementary evidence

Recovered the 264435078-byte retained trace with matching remote pre/post metadata and local/remote SHA-256. It contains 3419 samples and four API PoolTimeout increments: three payment-callback 503s and one customer-payment 503. All successful payments remain issued and queues are empty. The supplementary observation is incomplete: 77 gaps exceed two seconds, maximum 3.022377 seconds, and strict native API coverage also fails. Keep these failed gates explicit; sampled WAL/lock wait peaks are correlations, not proven causes of the payment timeout.

Four timed independent financial/cleanup recovery attempts consumed 436.827 seconds, including the corrected restored-consumer membership and generator process-check failures. Late trace recovery consumed 42.219 seconds. Three earlier failed read-only attempts have no exact elapsed receipts; accounting remains explicitly incomplete. Raw traces and receipts remain private; only sanitized summaries are published.
