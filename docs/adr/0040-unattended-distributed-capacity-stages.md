# ADR 0040: Unattended distributed capacity-stage orchestration

Status: Accepted for implementation

## Context

Huawei capacity validation uses an API/backend ECS, a separate generator ECS and
managed PostgreSQL. The existing scripts implement load generation, preflight,
observation and durability checks, but an operator or Codex session has been
coordinating them interactively. Minute-by-minute polling consumes model quota,
creates avoidable command errors and can leave temporary manifests or candidate
configuration behind when a session ends.

The existing `cloud_stability_suite.py` assumes Compose, database access and load
generation share one host. It also copies the credential-bearing manifest into
the evidence directory. That behavior does not fit the distributed Huawei
topology or the repository rule that private manifests stay out of retained
artifacts.

## Decision

Add an operator-side Python orchestrator with two repository-owned remote helper
scripts. The operator invokes one command using preconfigured SSH host aliases and
key-based, batch-mode authentication. The backend helper prepares a new
development-only fixture, exports the short-lived private manifest, deploys the
bounded admission candidate, runs preflight and observers, performs the exact-run
post-TTL audit and restores the previous admission setting. The generator helper
warms all target shows and runs the existing no-retry parallel load generator.

The orchestrator transfers the private manifest through a restrictive local
temporary directory, deletes every copy in a guaranteed cleanup path and retains
only compact JSON evidence. It records a durable local state file after each
phase. It never embeds passwords, database URLs, JWT secrets or manifest contents
in commands, logs or evidence.

Every stage is fail-closed. Preflight, warmup, load, latency, unexpected-response,
generator-drop, admission, durability, overlap and queue gates must all pass
before a later stage can be invoked. Rollback runs from `finally` after any
success, failure, timeout or interruption. Stage duration itself consumes no
model interaction; Codex is needed only to review the plan and final compact
summary.

## Alternatives considered

- Continue interactive SSH polling. Rejected because it consumes model quota and
  has already produced quoting and timing errors.
- Run generator traffic on the backend ECS. Rejected because it changes the
  measured topology and reintroduces CPU contention with the API.
- Put SSH passwords in command-line arguments or the repository. Rejected because
  process listings, logs and Git could expose them.
- Copy the complete manifest into retained evidence. Rejected because it contains
  bearer credentials.
- Use retries to make a stage pass. Rejected because retries change offered load
  and hide availability failures.
- Move orchestration to a hosted CI runner. Deferred because the private VPC is
  not currently connected to a runner and adding that network path is separate
  infrastructure work.

## Consequences

Capacity stages become repeatable from one operator command and can run while no
model turn is active. The operator host must have Python 3.12+, OpenSSH `ssh`
and `scp`, configured host aliases and noninteractive keys. Both ECS checkouts
must contain the same orchestrator-helper commit.

The helper scripts are specific to the current Compose topology and service
names. Changes to service topology, connection budgets, admission semantics,
fixture shape or authentication require a new or superseding ADR. Raw observers
remain on the ECS and compact summaries are copied to the operator workspace.

## Failure and recovery behavior

The orchestrator writes the last completed phase before continuing. Any nonzero
remote command, transfer error, timeout, failed gate or interruption prevents
escalation. The finalizer restores the original admission value, verifies four
healthy APIs, stops observers and deletes private manifests on both ECSs and the
operator host.

If rollback fails, the final result is `rollback_failed` even if load gates
passed, and the operator must restore the saved private environment backup before
new traffic. Existing production data is never deleted; fresh development
fixtures use new UUIDs and bounded sale windows.

## Validation evidence

Implementation must include unit tests for ordered phase execution, failure
short-circuiting, guaranteed rollback, output redaction, private-manifest cleanup
and compact gate evaluation. A dry run must emit the phase plan without network
access or secrets.

Live acceptance requires a fresh 800-show fixture followed by a 750 RPS,
ten-minute safety stage. The workflow is accepted operationally only after it
produces compact evidence, exact durability, zero overlap, drained queues and a
verified rollback without interactive polling.


Implemented validation evidence:

- Focused unit and related gate suite: 12 tests passed.
- Ruff validation passed for all new Python helpers and tests.
- Both remote shell helpers passed POSIX shell syntax validation in Alpine.
- The unit suite verifies ordered phases, failure short-circuiting, rollback,
  secret redaction, local private-manifest cleanup and compact gate evaluation.
- Live 750 RPS acceptance remains required before this workflow is considered
  operationally proven.
