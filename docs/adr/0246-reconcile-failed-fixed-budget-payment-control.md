# ADR0246: Reconcile the failed fixed-budget payment control

## Status
Accepted and independently verified. The failed control is FAILED_RESTORED; no candidate progression.

## Context
ADR0245 fresh control 4c6657f3e1d1 scheduled 25,200 journeys, dispatched 23,390, confirmed 23,295 and dropped 1,810. Customer outcomes include 13 payment HTTP 503s and 82 order-status GET HTTP 503s after successful payment intake. Its terminal aggregate found 23,390 orders, 23,377 succeeded payments and issued tickets, and 13 expired unpaid orders, with zero observed duplicates or pending financial work. The original target of 25,200 failed. Normal restoration, credential cleanup and global drain passed, but the original scope required independent recovery before closure.

The strict slot completeness check also failed; retained raw pipeline transfer passed its SHA and size checks. Offline salvage found 37 valid earlier failure frames, predominantly general/global admission failures, and complete database snapshots with peaks of 23 WALWrite and 14 DataFileRead waiters. This does not repair the original diagnostic gate or establish causality. Do not run the smaller general partition candidate against a failed control.

## Decision
Reuse ADR0244 exact-scope independent financial recovery, pinned to this new control's original report, ledger, binding and 84-show fixture hash. Require 23,390 orders, 23,377 durable paid/issued tickets and 13 expired unpaid orders, zero duplicates and complete post-TTL relationships. Preserve the 23,295 customer confirmations, 95 errors and 1,810 drops separately; matching financial totals does not erase customer failures or prove error-to-order identity mapping.

Audit both safety payments, canonical owned namespace and absent helpers/private inputs, 86 retired shows, stable restored four-API runtime, one normal consumer and every global queue. Reuse the accepted set-based SQL with a 60-second paid query bound, 20-second safety bounds, read-only repeatable-read snapshots and a 600-second recovery ceiling. A new exact validator retains the old scope's immutable validator. Close only this consumed control as FAILED_RESTORED; never reopen it or start the candidate.

## Alternatives
Treat unconfirmed customers as lost payments: contradicted by observed counts and incomplete without relationship checks. Accept the lower successful count as capacity: weakens targets. Run the payment partition anyway: violates failed-control progression. Reset the scope or discard missing diagnostic samples: conceals failure.

## Consequences
Customer confirmation and financial durability remain separate. Recovery adds only bounded read-only verification on existing resources. No financial writes, resource changes, retries, timeout changes or new paid traffic.

## Failure and recovery behavior
Any cohort, hash, count, binding, relationship, queue or cleanup mismatch keeps recovery blocked. Retain original failed evidence and timed attempts. Only complete independent proof closes recovery; financial success cannot mark the control or diagnostic gate passed.

## Validation evidence
The paid control failed and the candidate was not started. Original report and raw traces are retained. No improved capacity, stable 84 tickets/s or hourly qualification is claimed from this failed round.

Executed 47 affected recovery tests passed, including wrong partition, old namespace, lost payment, duplicates and dirty queues; Ruff and naming checks passed. Independent read-only verification completed in 50.672 seconds: 23,390 orders, all 23,377 successful payments with unique valid issued tickets, 13 expired unpaid orders, both safety payments and their three callback deliveries, all relationship predicates, zero duplicate bookings, full global queues, stable normal runtime and exact cleanup. The original report hash and one consumed paid stage are preserved. Total experiment/recovery accounting is 1,082.766 seconds. [Sanitized recovery and failed-control evidence](../capacity/cce/payment-partition-control-2026-10-09.json). Financial recovery does not qualify the failed control or complete its missing API diagnostics.
