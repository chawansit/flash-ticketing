# ADR0272: Capture bounded failure logs during paid traffic

## Status
Accepted measurement correction to ADR0241/0266; customer recovery and application image are unchanged.

## Context
The last run had 149 acquisition failures, but one API's 16-entry ring lost 51 sequences between one-second samples. Final pod log reads hit the 8 MiB limit and contained no failure records. Missing failure-time evidence correctly prevented qualification.

## Decision
During the five-minute experiment collect each owned API's last 30 seconds of already structured logs every ten seconds, including a final settled read. Redact on the primary transport before returning data. Retain only bounded admission fields and validated slot-ownership records; never customer identifiers, authorization or raw logs. Merge by bound pod UID and slot sequence, rejecting contradictory duplicates. Preserve a bounded journal, original metric ring samples and per-read truncation/error evidence.

Combine the journal with metric-ring records only when every terminal sequence and counter agrees. Missing records, log truncation/overflow, identity changes or parse errors still fail completeness; diagnostics never turn a failed customer gate into a pass. Apply collection equally to a future matched control/candidate. No image rebuild, retry change or new generalized runner is needed.

## Alternatives
Enlarge the ring alone: still can overflow under bursts and increases metrics payload. Remove completeness checks: hides the timeout cause. Retrieve all raw pod logs at the end: unbounded and late. Change the financial flow: unrelated to observation loss.

## Consequences
Adds bounded log-read overhead measured alongside workload. Overlapping reads deduplicate safely. A sustained overload can exceed the evidence bounds and fails closed; the collector never promises unlimited lossless diagnostics.

## Failure and recovery behavior
Collector faults are retained and cannot skip customer stop, financial audits, drain or restoration. Join the owned collector before clearing certificate material or deleting pods. Ownership is verified for each read; unknown pod identities are rejected. Do not repair old failed evidence.

## Validation evidence
Pending regression checks for burst gaps, contradictory sequences, truncation, redaction and collector stop. No newly executed cloud load yet.

Executed local validation: 196 affected unit tests passed, including the new worker identity/resource contract, projection batch progress, burst rollover, conflicting duplicate, endpoint replacement and diagnostic completeness gates. Ruff passed. The reproduction verifier checked 535 retained historical files and 20 current overlays without cloud calls. Cloud preflight has started; paid traffic and capacity outcome remain pending.
