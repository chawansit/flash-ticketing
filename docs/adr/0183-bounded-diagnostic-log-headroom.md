# ADR0183: Bounded diagnostic log headroom

## Status
Accepted; local qualification executed. No replacement cloud run or application change is authorized by this decision alone.

## Context
ADR0181 comparison 647e2505c995 passed its 2+2 control but failed its 1+3 candidate. The candidate produced 243,012 status checks and its primary API log capture reached 33,614,343 bytes, exceeding the 32 MiB ceiling. Only 49 failure records were retained against 52 observed acquisition failures. Capture incompleteness is separate from the real customer failure and must not erase it.

## Decision
Raise the existing streaming input ceiling from 32 MiB to 64 MiB per exact API container. Keep the 10-second deadline, 16 KiB line bound, failure-record limit of 128, slow-phase limit of 512, allowlisted output and immutable container checks unchanged. Memory remains bounded by a read chunk, partial line and capped selected records; raw logs are not retained. Source-pin this correction in a fresh experiment only after local qualification and required recovery verification.

Do not adopt the failed 1+3 placement or increase load. Preserve original reports, financial expectations, failed gates and consumed reservation. A larger capture ceiling improves evidence coverage; it does not fix payment capacity or qualify 84 tickets/second.

## Alternatives
Ignore missing records or mark incomplete evidence as complete: rejected. Capture unlimited logs or increase record/line/time ceilings together: unnecessary and unbounded. Change application logging concurrently with placement: adds a confounding factor. Keep 32 MiB: repeats the demonstrated truncation.

## Consequences
At most twice the input bytes may be scanned within the same deadline. Large or slow streams still fail closed. Published evidence remains sanitized. The collector may still require another isolated correction if a future profile exceeds its declared bounds.

## Failure and recovery behavior
Byte, time, line, record, subprocess or identity failures remain incomplete. Financial audits and owned cleanup continue independently. Historical failed reports are never rewritten. The current reservation remains consumed and recovery-required until a separate verification establishes durable payment reconciliation and restored ownership.

## Validation evidence
Executed: broader diagnostic suite: 84 passed and one old-bound assertion failed in 161.85s; corrected assertion and affected collector suite: 20 passed in 2.19s. A failure after more than 32 MiB of unrelated log lines was retained without raw contents; strict overflow, identity, sanitization, deadline and remote-program bounds remain checked. Ruff passed. See [paired result](../capacity/flash-sale-opening/diagnostic-placement-result-2026-10-06.json). No cloud retest or performance improvement claim.
