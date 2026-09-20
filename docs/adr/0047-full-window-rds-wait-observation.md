# ADR 0047: Observe RDS waits for the full measured stage

Status: Accepted for controlled validation; amends ADR 0040

## Context

The 30-minute 750-RPS stage on 20 September 2026 failed the RDS observer collection step independently of its load failures. The backend helper requested 1,800 seconds, but `rds_wait_observe.py` rejected durations above 900 seconds before opening its output file. As a result, `stop-observers` found no RDS wait samples and the execution gate failed. A 10-minute stage did not expose this mismatch. The observer performs bounded read-only aggregate queries and collects no SQL text or credentials.

## Decision

Allow the existing RDS wait observer to run up to 3,600 seconds at its existing bounded interval, covering the longest 30-minute capacity stage and setup/drain margin. Keep the 0.1–2-second interval bounds, aggregate-only queries, explicit fresh output path and observer-error gate. Build the migrate image from the matched checkout as required by ADR 0046. Do not waive the observer gate for a long run.

## Alternatives considered

- Keep 900 seconds and accept partial data: rejected because the stage requests full-window observation and currently fails before collecting any samples.
- Split a stage into multiple observer files: viable, but adds restart gaps and merge complexity for no current benefit.
- Disable RDS wait observation for long stages: rejected because intermittent WAL stalls are the central unresolved problem.

## Consequences

The observer may write roughly 36,000 aggregate samples at a 0.1-second interval in a one-hour run, increasing bounded temporary disk use and read-only RDS queries. Raw data stays on the backend ECS; the operator receives only the compact summary. This does not improve request throughput.

## Failure and recovery behavior

An observer startup error, missing output or summarization error fails the execution gate and blocks rate escalation. The stage finalizer stops the observer and restores admission even when collection fails. Preserve the raw error log for diagnosis. An explicit upper bound remains in force.

## Validation evidence

The failed stage requested `--seconds 1800`; the observer log recorded `Use 1..900 seconds and a 0.1..2 second interval`, and the expected RDS wait file was absent. Unit validation of duration bounds, live image verification and a full-window long-stage observer summary remain pending.
