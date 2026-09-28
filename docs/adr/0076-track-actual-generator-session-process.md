# ADR 0076: Track the actual generator session process

Status: Accepted

## Context

The cloud capacity stage starts its generator asynchronously with `nohup setsid sh -c ... &`, then writes `$!` as the job PID. On some launches, `setsid` forks before executing the session leader. The recorded PID belongs to the short-lived parent, so `status` reports `missing` even while the generator continues. In run `20260928T155410Z-c8f6469b`, this caused the stage to mark the load failed. The generator later produced all worker summaries, and its job status eventually recorded exit code zero. The stage correctly did not accept the capacity result, but the status race wastes a cloud run and can trigger an unnecessary stop.

The job needs one authoritative PID that identifies the actual process group so that status and stop agree on which work is running.

## Decision

The session leader writes its own PID to a private temporary file and atomically renames it to `job.pid` before running the helper. The starting shell waits for that PID file to appear and verifies that the process is alive before returning `started`. It no longer records `$!` from the outer `setsid` launcher. The existing atomically renamed `job-status.json` remains the authoritative terminal result. If the session cannot publish a live PID promptly, start fails closed.

The stop path retains its process-group termination behavior, but it uses the published session-leader PID. This decision does not change offered traffic, retry policy, reservation semantics, Redis or PostgreSQL.

## Alternatives considered

- Treat a missing PID as running whenever any result file exists. Rejected: partial result files do not prove the generator has stopped or succeeded.
- Poll indefinitely for `job-status.json`. Rejected: an orphaned generator would make the coordinator hang.
- Use `setsid --wait` and keep the outer PID. Rejected: the outer waiting PID may not be the session leader and would break the existing process-group stop contract.
- Run the generator synchronously over SSH. Rejected: a lost SSH session could interrupt an otherwise healthy load and remove independent status checks.

## Consequences

- Start waits briefly for the real process identity to be published.
- Status can distinguish a running job from a finished job without a transient-parent race.
- A failed launch is reported before the stage begins polling.
- The private PID and status files stay outside published benchmark artifacts.

## Failure and recovery behavior

If the session leader exits before publishing a PID, start reports failure; the coordinator does not claim a valid load. If it exits after publishing but before writing terminal status, `status` reports `missing` and the stage fails closed. `stop` may terminate only the verified session process group; it does not kill unrelated processes. Cleanup retains private evidence until the stage has collected results.

## Validation evidence

Pre-change evidence is the 30-second diagnostic run `20260928T155410Z-c8f6469b`: the first status poll returned `missing` and exit code one after `start` reported `started`; later `job-status.json` recorded `finished` with exit code zero and the generator wrote all eight worker summaries. The run is not accepted for capacity.

Acceptance requires a focused test of the session-leader PID publication and stop behavior, shell syntax validation, and a short cloud stage where the coordinator sees `running` then `finished` without `missing`. The reservation integrity audit must still pass independently.

### Implementation evidence

The generator now has its session leader atomically publish its own PID before the launcher reports started. A five-second bounded start wait checks that the published PID is live. A real generator-ECS isolated smoke test on 2026-09-28 exercised the candidate script with a fake three-second helper: status progressed from started to running to finished with exit code zero, and the published PID matched the process group ID. A second fake long-running helper progressed from started to running to stopped with exit code -1 after the existing group-stop path. Linux shell syntax validation passed. A cloud capacity stage using this change remains required before acceptance.

### Cloud validation

Run 20260928T161113Z-108402fb at revision 18a3890 confirmed that the coordinator observed the generator running and then load_finished without a missing-PID error. The load wrote all eight worker summaries and the durability audit confirmed 3,570 acknowledged holds, zero overlapping booking intervals and drained queues. The overall stage still failed its capacity gates: 451 generator drops, read/hold p95 of 322.638/716.695 ms, and a separate DCS observer permission error caused observer collection to fail. Those failures do not invalidate the generator process-supervision result, but no capacity result is accepted from this run.
