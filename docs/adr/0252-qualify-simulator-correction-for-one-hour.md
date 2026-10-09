# ADR0252: Qualify the accepted simulator correction for one hour

## Status
Accepted for bounded preparation. One-hour cloud qualification has not run.

## Context
ADR0251 passed the five-minute CCE workload: all 25200 offered journeys produced reconciled paid tickets, without customer errors, retries, drops, duplicate booking or payment loss. The existing hourly entry still selects the older simulator contract and API image. Reusing that entry without an explicit binding would invalidate the progression.

## Decision
Register a fresh hourly scope using exactly the passing ADR0251 API image, simulator image, twelve dispatch jobs, ten simulator DB connections, four CCE API pods, API four connections with two payment slots each, acquisition budget twenty and pooler twenty-four. Keep gateway delays, customer gates, authorization, financial guarantees and mandatory restoration unchanged. Offer 84 journeys/s continuously for 3600 seconds. Reconcile 302400 terminal tickets and require at least 300000 unique paid-and-issued tickets whose database timestamps fall inside the one-hour window. Preserve the explicitly approved 5400-second experiment ceiling, with cleanup allowed to complete safely if needed. No further scaling or factor changes are included.

Pin the fully passing five-minute evidence and its original report digest as the hourly prerequisite. Preserve the old hourly profile for historical reproduction. Declare the current hourly entry as a reviewed overlay, with the original bytes retained. A consumed short scope is never replayed.

## Supersession
Extends ADR0232 hourly qualification binding and ADR0251 simulator correction. Does not supersede locking, TTL, idempotency, transaction boundaries, callback delivery or financial SLOs.

## Alternatives
Run the historical hourly entry unchanged: tests another runtime. Extrapolate five-minute terminal tickets to one hour: cannot qualify the sustained or timestamp requirement. Combine more pods or changed polling: breaks attribution.

## Consequences
A short pass is promising but not proof of sustained performance. A longer run may expose checkpoints, queue growth or resource saturation. Failed quality gates retain their failed status and halt progression.

## Failure and recovery behavior
Stop further dispatch at the bounded deadline or a failure. Independently audit payments, tickets, duplicate bookings, and every queue. Preserve evidence, retire only owned fixtures, remove owned CCE resources and protected transport credentials, and restore normal services. Do not weaken a gate or relabel a failed run.

## Validation evidence
Five-minute prerequisite: docs/capacity/cce/simulator-dispatch-comparison-2026-10-10.json; original report SHA256 f65809964486de89d7d3a24b01311c62c730f0b167804c53f8c697dbb48fd860. Local hourly-binding verification is pending. One-hour and production qualification are pending.

Executed 79 simulator/hourly integration, generator and recovery tests; all passed. Executed 149 CCE API adapter, paid-stage/entry and reproduction tests; all passed. Ruff passed. Historical reproduction verified 535 files, seven explicit current overlays and all 78 frozen generator files, with no cloud calls. The old hourly entry bytes are retained by their original content digest. One-hour cloud qualification remains pending.

Executed hourly run adr0151-ada3471665ef for 3600 offered seconds. Database timestamps identify 301843 unique paid-and-issued tickets inside the hour (301652 within the two-second inner guard), exceeding the 300000 numerical target. Customer quality remains failed: 104 errors among 302028 dispatched journeys (0.034434 percent), and 372 undispatched of 302400 scheduled (0.123016 percent). Terminal totals are 301961 successful payments/tickets and 67 expired unpaid orders; duplicate bookings and queues are zero. Original observer collection failed after bounded slot/log capture became incomplete. No original gate is relabeled. Independent ADR0253 recovery passed financial/relationship/safety/cleanup checks; the failed first SQL-timeout attempt is retained. Executed 30 recovery tests including late-batch corruption and complete disjoint show coverage, and real read-only RDS verification. Status is FAILED_RESTORED with no active load/recovery. See [simulator-hourly-qualification-2026-10-10.json](../capacity/flash-sale-opening/simulator-hourly-qualification-2026-10-10.json). Full capacity and production qualification remain failed/pending.
