# ADR0332: Calibrate generator process scaling on the existing ECS

## Status
Accepted for bounded synthetic diagnostics; application capacity remains unmeasured above the ADR0328 result.

## Context
ADR0329 failed to deliver 160 journeys/s: two generator processes each used approximately one CPU core, dispatch lag exceeded 40 seconds and 125 journeys were dropped. The existing generator ECS has eight vCPUs. The user approved continuing the ECS-first approach toward 300 completed transactions/s.

## Decision
Compare two and four subprocesses at 160 synthetic journeys/s for 45 seconds each, keeping aggregate active journeys and HTTP connections at 500 and aggregate HTTP clients at 16. Reconstruct and verify the sealed paid workload (recovery, timeout retention and gateway-read overlays) before copying its leaf generator unchanged, a loopback-only synthetic responder and disjoint synthetic shows/viewers. Keep polling at one second, one callback, bounded recovery at three attempts, 30ms synthetic HTTP delay, 0.5s persistence delay and 0.5s issuance delay. Record all source hashes and each process result. Only after a valid passing four-process 160 control, attempt one four-process 200/s control under identical settings. Bound the whole remote diagnostic to 600 seconds. No real payment traffic, datastore access, production deployment, cloud resize or CCE migration occurs in this diagnostic.

## Alternatives
Moving immediately to CCE adds placement and orchestration variables. Increasing concurrency alone confounds CPU and occupancy effects. Repeating real paid traffic with an uncalibrated generator cannot identify application limits.

## Consequences
This is a generator qualification under specified synthetic latency, not evidence of financial correctness or production throughput. Keep the sealed two-shard paid runner and its acceptance checks unchanged until synthetic evidence supports a separate integration decision. Higher rates may need greater concurrency; do not conceal drops by changing it during this comparison. No persistence, locking, messaging, idempotency, TTL or application scaling decision is superseded.

## Failure and recovery behavior
Reject invalid responder timing or protocol errors, missing process results, nonzero exits, incomplete/duplicate accounting, generator drops or dispatch p95 above 100ms/max above 500ms. Preserve failed results. Stop escalation when the four-process control fails. Terminate only owned subprocesses and close loopback sockets; keep remote evidence in an isolated owned directory. Production services remain untouched.

## Validation evidence
Pending execution. Record local regression checks, remote reachability, source digests and measured controls before interpreting results. ADR0328 remains the best qualified short paid throughput (138.49/s).

## Executed validation and result

Nine local tests passed, covering disjoint sufficient allocations, forbidden real origins, incomplete/late controls, missing responder timings and sealed runtime source drift. The import/undefined-name check passed. Reconstructed the sealed recovery/timeout/gateway-read leaf before the authoritative remote controls; all copied hashes are in the [result](../capacity/cce/generator-process-calibration-2026-10-17.json). An earlier exploratory set using development files is retained separately and excluded from the decision.

On the existing eight-vCPU ECS, all three 45-second controls passed: 160/s with two processes (7200 journeys), 160/s with four (7200), and 200/s with four (9000). Every scheduled synthetic journey completed, with zero drops or initial/final customer errors. Worst-shard dispatch p95 improved from 8.93ms to 1.76ms at160/s; per-process CPU fell from approximately0.73 cores to0.40. At200/s four processes used approximately0.49 cores each, with2.07ms worst-shard dispatch p95. Responder validity checks passed and all child exit codes were zero.

These are synthetic generator measurements only. The two-process control also passed, so this does not establish that CPU redistribution alone fixes the real160/s failure. Application capacity remains138.49/s qualified over five minutes. The approximately2.2-second synthetic mean implies at least660 active journeys at300/s before headroom; increasing concurrency requires a separately controlled step.

For repeatable preparation, run `python scripts/calibrate_generator_processes.py --prepare-runtime <fresh-directory>`, transfer that directory to the generator host, and run its `calibrate_generator_processes.py --output <fresh-results-directory>` with HTTPX0.28.1 installed. The committed CLI checks the source receipt before opening the responder. This additional admission check was tested locally after the remote measurements; the executed calibrator hash remains explicit in evidence. Synthetic manifests are never used for real backend load.
