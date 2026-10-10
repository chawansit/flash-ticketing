# ADR0260: Close failed shared-image control without qualifying capacity

## Status

Accepted for exact terminal recovery and independent diagnostic retention. No new paid load or candidate dispatch is authorized by this decision.

## Context

ADR0259 control `adr0151-ba78fda091fd` offered 25,200 journeys but dispatched 20,040, with 5,160 generator drops. All dispatched journeys eventually received tickets; 594 journeys recovered initial errors. Journey p95 was 11.24 seconds. The original report is failed and restored, but its scheduled-count financial gate leaves recovery open. Financial counts show 20,040 paid tickets and no pending payments or duplicate seats.

Slot-history summarization failed because the sixteen-event per-process ring rolled over between observations. This exception also prevented independent pipeline, database and API CPU summaries. Offline analysis recovered those summaries without excusing missing slot events. Primary ECS CPU averaged 84.1%; API process CPU totaled 2.08 cores. Heavy status polling and database-slot pressure are measured, but their regression cause is not yet established.

## Decision

Preserve the original report, digest, drops, errors and consumed paid-stage allowance. Independently audit exactly the retained 84 owned shows and 20,040 dispatched paid journeys, using the existing read-only relationship audit. Verify both safety payments, global queue drain, stable restored runtime, idle generator, retired fixtures, deleted namespace/helpers and removed private inputs. Close only this exact run as FAILED_RESTORED after all recovery checks pass. Candidate dispatch still requires a new fully passing control; recovery never grants it.

Collect slot history, pipeline, native CPU and database summaries independently, retaining each failure. Keep the sixteen-event ring, sampling rate, application image, budgets and customer gates unchanged. Missing slot history must continue to fail observation completeness. Extend ADR0257's independent-evidence retention to slot-summary failures; do not relax quality requirements.

## Alternatives

Clear recovery from aggregate counts alone: lacks relationship and cleanup proof. Treat generator drops as successful traffic: invalid. Run the candidate after a failed control: violates comparison gates. Increase buffers or sampling now: changes overhead without attributing the regression. Rebuild the shared image now: defer until worker behavior is compared with the accepted source.

## Consequences

This is an unsuccessful 84-offered-journey/s control, not qualified 84-ticket/s capacity. Dispatched-cohort durability and restoration can succeed while capacity fails. Independent telemetry helps diagnosis without concealing overload or missing evidence.

## Failure and recovery behavior

Any changed report, relationship mismatch, unknown ownership, nonempty queue or failed restoration blocks closure. Keep original reports immutable and attach a separate recovery receipt. Do not repeat paid load or migrate workers during recovery.

## Validation evidence

Implemented independent summary retention and exact consumed-run recovery. Executed 99 targeted tests passed, one skipped; Ruff passed. Read-only cloud recovery verified all 20,040 dispatched paid outcomes and relationships, both safety payments, global drain, 86 retired owned events, absent namespace/helpers/private inputs and stable restored runtime. Ledger closed FAILED_RESTORED with the original failed report digest unchanged. Candidate remains blocked. See [control evidence](../capacity/cce/shared-worker-control-2026-10-10.json). ADR0259 runner integration checks executed: 325 tests passed, one skipped; Ruff passed; archived-source reproduction verified 535 historical inputs and 18 declared overlays. These checks do not qualify cloud capacity.
