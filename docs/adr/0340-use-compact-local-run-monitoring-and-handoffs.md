# ADR0340: Use compact local run monitoring and handoffs

## Status

Accepted for implementation under the user's request to reduce quota usage. No cloud deployment or load is authorized by this workflow.

## Context

Repeated model-driven polling, large file reads, bespoke summaries and recovery scripts added work around the recent tests. ADR0339 also demonstrated that incomplete observation can skip the normal financial audit. Monitoring must reduce repeated reads without weakening verification or turning a failed load into a pass.

## Decision

Use one local command to produce an allowlisted compact run summary, watch explicit local checkpoint files within a bounded timeout, and verify retained evidence. The watcher updates a local summary and emits only phase/status or first-error transitions. It performs no network access, database changes, cleanup, ledger updates or load launch. Reuse the existing bounded database audits and runner restoration; local evidence checks do not replace them. Preserve raw evidence and require a hash-bound independent reconciliation when the normal audit is missing.

Start new implementation chats from one concise repository handoff and the two relevant result files, not this full conversation, the complete ADR corpus, a recursive temporary-directory search or the full cumulative state file. Read precise sections and diffs; rerun only tests justified by the change. Keep required host checkpoints brief and avoid repeated explanations of unchanged state.

## Alternatives

Continue model-driven polling and bespoke per-run status scripts: unnecessary repeated context. Remove audits or use generator confirmations as financial proof: rejected. Add a monitoring platform or new cloud services: outside scope. Rewrite the cloud runner: unnecessary for this quota workflow.

## Consequences

Scripts can wait and summarize without repeatedly asking the model to interpret unchanged JSON. A fresh chat reduces inherited conversation history but does not guarantee a particular quota saving. Compact output omits detail; expand one source section only when a changed gate or failure requires it. Existing protected user changes and historical evidence remain untouched.

## Failure and recovery behavior

Missing, malformed, mismatched or incomplete evidence remains unknown or fails closed. A monitor timeout is not a failed load or a successful cleanup. Local evidence verification never clears recovery or changes the original outcome. Financial correctness, locking, messaging, idempotency, TTL and scaling behavior are unchanged. Pause if ownership/restoration remains unresolved; new paid stages require the current explicit allowance.

## Validation evidence

Pending local tests of summary redaction, bounded watching, evidence mismatch rejection, quality/recovery separation and the current retained results. No cloud calls or capacity improvement are claimed.

Executed15 local unit checks covering output allowlists, digest/count/relationship mismatches, independent recovery without quality promotion, suppressed unchanged events, bounded timeout and raw-window rate calculation. Ruff F/I passed. Retained ADR0338 and ADR0339 evidence checks passed, preserving PASS versus FAIL quality classification. No network or database calls. See [local qualification and compact run summaries](../capacity/cce/quota-workflow-local-2026-10-17.json), [workflow](../capacity/QUOTA_WORKFLOW.md) and [fresh-chat handoff](../capacity/IMPLEMENTATION_HANDOFF.md).
