# ADR0257: Close hourly recovery without qualifying incomplete diagnostics

## Status
Accepted for exact terminal recovery and bounded offline corrections; no repeat cloud load authorized by this decision.

## Context
ADR0256 run adr0151-c11285855ae5 dispatched and fulfilled 302,400 distinct journeys. Its committed issuance audit found 302,295 unique paid-and-issued tickets inside one hour; all 63 initial payment 503 errors recovered. Financial counts and queue drain passed. Overall qualification failed: the 301,601,050-byte / 3,250-row observer trace exceeded the database diagnostic summarizer's 64-MiB / 2,000-row short-test limits. That exception prevented independent native API CPU summarization. There are also 17 observer error rows and 338 statistics gaps exceeding two seconds (maximum 13.53 seconds). No statistics timestamps repeat; cached/stale database samples are not the cause. Some cohort scans took about three seconds. Missing diagnostics cannot be reconstructed or waived.

## Decision
Preserve the original failed report and its exact digest. Independently recheck restored runtime identities, deleted namespace/helpers/private inputs, idle generator, exact owned fixture retirement, global drain, both safety payments, and all 302,400 paid outcomes/relationships after TTL. Close only this consumed run as FAILED_RESTORED; capacity_qualified remains false.

For subsequent offline diagnostic handling, explicitly select hourly mode with a 512-MiB / 4,000-row ceiling and the existing 2-MiB line ceiling; retain short-mode limits and all existing continuity/error/overhead checks. Summarize native API CPU independently so a database summary exception cannot discard available evidence. Supersedes the fixed short diagnostic trace-size interpretation in ADR0173 only for explicitly selected hourly mode. Do not change application images, budgets, sampling SQL, frequency, customer gates or resource topology.

## Alternatives
Clear recovery state without proof: unsafe. Mark full qualification passed from ticket counts: hides missing evidence. Relax continuity or fabricate samples: invalid. Repeat another paid hour now: unnecessary before reviewing retained evidence. Redesign sampling immediately: defer until the measured observer phase is isolated.

## Consequences
The numerical throughput goal is measured, but production qualification remains incomplete. Recovery success does not erase 63 initial errors or establish the underlying commit/WAL cause. The diagnostic correction improves evidence retention, not application capacity.

## Failure and recovery behavior
Any missing identity, altered original result, financial mismatch, remaining resource or nonzero queue blocks closure. Recovery is read-only except writing local terminal evidence/state. Preserve original hashes and consumed scope. Hourly parsing must still report incomplete evidence for error rows, counter resets or gaps; independent CPU success never overrides those failures.

## Validation evidence
Executed: 155 targeted tests passed, one skipped; Ruff passed. Archived-source reproduction verified 535 historical files and 13 explicitly declared overlays. Independent cloud recovery audited 302,400 paid outcomes and valid relationships, both safety payments, empty queues, retired 1,010 owned events, removed namespace/helpers/private inputs and stable restored runtime. Ledger closed FAILED_RESTORED with the original failed report digest unchanged. Offline hourly parsing retained the existing incomplete result; no missing samples were fabricated and no repeat paid load started. See [hourly evidence](../capacity/cce/customer-recovery-hourly-2026-10-10.json).
