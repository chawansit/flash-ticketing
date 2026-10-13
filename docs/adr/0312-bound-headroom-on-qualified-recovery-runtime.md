# ADR0312: Bound headroom on the qualified recovery runtime

Status: Accepted for bounded diagnostic comparison; capacity qualification failed.

## Context
ADR0311's fresh84/s300s run passed all original gates with25,200fulfilled journeys, no drops/errors, and25,099tickets inside the offered window. The existing native runner only registers84/s or a historical168/s ticket-target workload. A100offered/s run cannot deliver30,000tickets inside300seconds because the last arrivals need processing time.

## Decision
Register one explicit102offered/s300s headroom experiment,30600distinct journeys across102isolated300-seat shows, two generator shards and concurrency500. Require at least30000unique paid-and-issued tickets inside the offered window and the existing conservative window target. Preserve the four API/fourteen background pods, immutable application image,24physical connections, admission20,1general+3payment connections per API, gateway delays, bounded gateway-read client and approved0.01% final-error SLO.

Authenticate the fresh passed/restored ADR0311 receipt before registration. Use a separately declared102/s ticket-target profile and sealed sharded-rate ceiling extension. Runtime validation may reuse an explicit84/s view of the identical application topology; actual authorization, plan, arguments, fixture, accounting, issuance target and binding must declare102/s/30600. No inferred rate override or broad admission bypass.

## Alternatives
100offered/s cannot establish100successful tickets/s inside the window. Historical168/s changes too much demand for the next step. Increasing compute or database budgets would prevent attributing this test to offered load. A new deployment framework is unnecessary.

## Consequences
This tests short headroom, not an hour at100tickets/s or simultaneous flash-opening capacity. Report offered, dispatched, successful inside-window, recovered and failed customers separately. Do not extrapolate a passing five-minute run into an hourly qualification.

## Failure and recovery
Stop further increases on any failed dispatch, latency, customer, financial, issuance or drain gate. At30600journeys the0.01% target permits three final failures. Payment loss, duplicate charge and double-booking remain zero tolerance. Preserve failed evidence and mandatory restoration/credential cleanup. Existing retry deadline, seat TTL, persistence, locking and messaging semantics remain unchanged. Rollback selects the prior sealed84/s workload.

## Validation evidence
Pending exact-profile, source-drift, ceiling/coordinator, fixture/audit and runtime-budget checks, then the one bounded cloud experiment. No102/s cloud load has started.

Published comparison: [measured outcomes and independent recovery](../capacity/cce/headroom-slot-writer-comparison-2026-10-13.json). Failed raw results remain unchanged; no four-writer hourly or flash-opening qualification is claimed.
