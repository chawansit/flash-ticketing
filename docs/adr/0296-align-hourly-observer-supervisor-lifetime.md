# ADR0296: Align hourly observer and supervisor lifetimes

## Status

Accepted under standing engineering authorization. Supersedes the inherited 3,780-second supervisor bound only for ADR0294 early hourly observers. Other jobs retain their existing bounds.

## Context

ADR0295 hourly run adr0151-d15d25621ac5 requested 4,200 seconds of early observation, but job_program supervised every hourly subprocess with a 3,780-second timeout. SQL and native CPU traces ended before the offered hour finished. The dispatched cohort independently passed integrity recovery; the qualification remains failed.

## Decision

Use a 4,260-second supervisor ceiling only for existing hourly observation entry points explicitly requesting 4,200 seconds. The native pipeline must also select early hourly bootstrap. Keep ordinary hourly jobs, including the customer generator, at 3,780 seconds; short jobs remain at 600 seconds. Retain process ownership, PID/start/command fencing, bounded termination, one diagnostic data connection, one-second statistics, two-second gap and ten-second cohort freshness gates. The total experiment ceiling remains 5,400 seconds.

## Alternatives

Raise every supervisor timeout: rejected because generator bounds must remain unchanged. Start observation later: rejected because startup must precede expiry safety. Ignore coverage gaps or weaken gates: rejected. Rewrite supervision: unnecessary.

## Consequences

Collectors can cover their declared lifetime and a bounded exit allowance. No application, pool, pod, Redis TTL, Kafka ordering or payment behavior changes. This does not correct customer congestion or establish additional capacity.

## Failure and recovery behavior

Reject an early pipeline request without hourly mode or with a mismatched duration. Supervision and cleanup remain bounded. Preserve partial traces; do not infer coverage from an intended duration. Failed paid stages remain consumed, with financial audit, queue drain and restoration required.

## Validation evidence

Executed source inspection confirms the 3,780-second supervisor timeout and 4,200-second observer request. Latest raw result records incomplete offered-window coverage. Executed 84 focused regressions covering generated subprocess supervision, legacy bounds, hourly integration and early bootstrap; all passed. Ruff passed for the changed runner/tests. Cloud validation is pending; this correction does not establish complete future trace coverage.
