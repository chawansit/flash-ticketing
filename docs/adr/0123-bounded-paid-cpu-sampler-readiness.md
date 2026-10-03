# ADR 0123: Bounded readiness for paid-control CPU sampling

Date: 2026-10-04
Status: Accepted for the authorized diagnostic repair and one unchanged-load repeat; local validation passed; unchanged-load repeat pending

## Context

ADR 0122's application and required financial/drain gates passed at 60 buyers/s for 300 seconds. Its optional CPU collector started after the run directory appeared but before state.json existed. FileNotFoundError ended collection before any CPU sample, and the operator wrapper exited 1 after mandatory audits completed. A directory's existence is not the runner's dispatch-readiness contract.

## Decision

Promote the existing read-only CPU collector to a repository script. Within the existing 180-second startup deadline, wait for a complete runner state.json and the existing generator-script-directory phase. Missing files and incomplete JSON are transient only within that same deadline. Validate run identity and state shape; stop immediately on a recorded error or completed probe/rollback rather than sampling restored services. Use bounded two-second sleeps and a monotonic deadline. Do not dispatch, retry or alter customer load.

Preserve the existing one SSH invocation, batch authentication, bounded connection/liveness options, 49 cumulative host/cgroup samples at five-second intervals over 240 seconds, 265-second subprocess deadline and candidate replica checks (four APIs, six consumers, three writers). Retain raw samples/stderr locally and reject incomplete samples, changed roles, counter resets or invalid timing before publishing a CPU summary. Sampler failure remains separately visible; it must not bypass financial audits or block the existing runner's restoration.

This supersedes only ADR 0122's temporary CPU collector readiness behavior; reservation-writer scheduling remains accepted and unchanged. Reuse ADR 0040's existing stage runner/helpers and ADR 0090's bounded generator controls. This is a diagnostic lifecycle repair, not an extension of paid-stage progression or an unattended schedule. Repeat exactly the previous 60-buyers/s, 300-second application profile and budgets.

## Persistence, messaging, idempotency, TTL and scaling

State.json is advisory local orchestration evidence, not payment or reservation authority. No new lock, persistent scheduler or Redis/database write is introduced. Cumulative kernel counters are read directly; no API, database, Redis or Kafka requests are added by the CPU sampler. Existing PostgreSQL locking, commit-before-ACK, messaging and payment/reservation replay idempotency remain unchanged. Hold/command/callback TTLs and leases remain unchanged. No process, worker, connection, database or generator budget increases. Raw diagnostic evidence is retained separately from private credential-bearing manifests.

## Alternatives

- Wait a fixed delay after directory creation: does not prove checkpoint or dispatch readiness.
- Ignore missing CPU evidence: repeats the known diagnostic gap.
- Couple readiness to new runner phases or alter stage progression: unnecessary orchestration expansion.
- Restart the load after a sampler error: creates another experiment and confounds the approved repeat.
- Poll through SSH: adds remote round trips; the local checkpoint already exposes readiness.

## Consequences and failure/recovery behavior

Missing or partial checkpoint writes may delay sampling, but cannot exceed the original startup bound. Corrupt JSON that never completes times out; malformed completed state or wrong run identity fails closed. A stage error or ended dispatch aborts the sampler before SSH. Permission errors remain errors. SSH failure, disappearing cgroups, counter resets, incomplete samples or subprocess timeout retain evidence and fail the diagnostic without accepting fabricated CPU results. The existing stage runner retains finally-based audits/restoration/cleanup; no new load follows a failed gate.

## Validation evidence

Previous executed evidence: [ADR 0122 control](../capacity/flash-sale-opening/paid-writer-fairness-control-2026-10-04.json), including the retained FileNotFoundError and wrapper failure. New code/tests and repeat have not yet executed.

Authorized validation: delayed file creation, partial writes, ready checkpoint, missing/corrupt checkpoint timeout, failed/finished stage rejection, state identity/shape checks, incomplete/counter-reset/topology/timing evidence rejection and transport failure retention. Then one unchanged application control against the ADR 0122 passing baseline, with all ten runtime hashes unchanged and exact paid/unpaid post-TTL, durability, zero-double-booking, full-keyspace queues, Kafka, observers, source/settings/readiness/restoration/idle/private-cleanup checks. No higher load or main merge is authorized by this repair/repeat. Sustained 300000/hour and single-concert production proof remain future scope.

Executed local validation:41 focused diagnostic/observer/runner tests passed in0.59s;changed-file lint and Git whitespace checks passed. New tests prove delayed checkpoint creation/partial writes wait until dispatch, missing/corrupt checkpoints time out at the single deadline, failed/finished or wrong-identity stages never start SSH, and incomplete/topology/reset/nonfinite/timing/transport failures reject evidence and retain diagnostics. Application runtime is unchanged;ADR0122's492unit/integration result is retained, not rerun for this diagnostic-only change. Cloud repeat pending.
