# ADR 0043: Detach and poll long generator jobs

Status: Accepted for implementation

## Context

On 2026-09-20 the clean-revision 750 RPS safety workload completed all 450,000
responses with no unexpected responses or generator drops. Its exact
post-expiry audit and rollback passed. The operator's long-lived SSH command
nevertheless remained open after the generator wrote its summary, then timed
out at 840 seconds. The stage correctly failed its execution gate under ADRs
0040 and 0041. This repeated an earlier SSH timeout and prevents an unattended
stage from passing even when all traffic and integrity gates pass.

## Decision

Keep the separate generator ECS and no-retry HTTP workload. Replace the one
long-lived generator SSH call with a short start call that launches a detached,
bounded job, followed by short status calls at a fixed interval. The remote
supervisor writes an atomic, credential-free exit-status file. The operator
accepts only a reported completed exit code of zero, plus all existing
workload, admission, latency, durability, overlap, queue and rollback gates.

The generator job has a hard runtime bound and a process group that can be
stopped before private-manifest cleanup. An absent or malformed status, failed
SSH poll, exceeded deadline or forced stop fails the stage. Completed worker
files may still be collected and audited under ADR 0041. No failed HTTP
request is retried and no load level is promoted on a workflow failure.

This amends ADR 0040's long-lived SSH execution mechanism. ADR 0041's
fail-closed evidence recovery and rollback remain in force.

## Alternatives considered

- Increase the SSH timeout again. Rejected because the remote summary already
  existed before both observed timeouts.
- Treat a completed summary as equivalent to successful remote execution.
  Rejected because the operator cannot verify that the job exited cleanly.
- Run the load on the API ECS. Rejected because it changes the measured
  topology and CPU contention.

## Consequences

The operator opens roughly one short SSH connection per ten seconds during a
stage. This adds small control traffic but avoids relying on one long channel.
The generator keeps only a PID, bounded control log and compact exit status
beside its existing per-run output. The effective workload rate and request
semantics are unchanged.

## Failure and recovery behavior

A status-poll failure or timeout triggers a process-group stop before evidence
collection and private cleanup. If the generator has already completed, stop
does nothing and its worker results are audited. Missing worker results keep
the durability gate false. If stop or rollback cannot be verified, the stage
remains failed and an operator must inspect the remote process state before
another run.

## Validation evidence

Unit tests must cover clean completion, nonzero exit, status timeout, malformed
status, mandatory stop and rollback. A local shell smoke test must prove the
detached job writes its exit status and returns promptly. The 2026-09-20
750 RPS run is motivation for this decision, not proof of the new workflow.
Live validation requires a fresh safety stage on a matched revision.

Implementation validation: eleven focused workflow tests and 112 complete unit
tests passed. Ruff and POSIX shell syntax passed. A disposable Linux smoke test
verified prompt detached completion; a second verified active-job cleanup is
refused and stop terminates the process group. Live validation is pending.

The operator also verifies equal 40-character Git commit IDs on backend and
generator before deployment, as required by ADR 0040. A unit test proves a
mismatch stops before any candidate deployment.

Live failure-path validation: the 2026-09-20 matched-revision 750 RPS stage
received a definitive generator exit code 1 through short status polls, copied
all worker evidence, completed exact post-expiry audit, restored admission and
removed private manifests. The execution/cleanup gate passed without a long
SSH timeout. The workload itself failed on five unexpected database responses,
so this validates the workflow mechanics but not 750 RPS capacity.
