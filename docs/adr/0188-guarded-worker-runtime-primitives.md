# ADR0188: Guarded worker runtime primitives

## Status
Accepted for local implementation and fault qualification under ADR0172, ADR0184, ADR0186 and ADR0187. The worker profile remains unregistered. This component has no SSH connection factory, command-line execution, dispatch or restoration command. No accepted ADR is superseded.

## Context
The offline lifecycle and image staging contracts need concrete runtime operations. A role-based Docker stop can remove an unrelated service or a replacement container after a lost response. A successful stop command alone does not prove that restart policies or overlapping workers are absent. Historical financial recovery remains unresolved.

## Decision
Implement the first transport-bound runtime adapter actions: verify the exact original runtime and retained broker volume/bind files, verify an idle generator, stop only the five original worker containers, and verify worker absence on both hosts. Require the same real ActionGuard attached to the supplied session, a future registered worker profile, and exact pair, saved snapshot, source and archive bindings before any remote call. Do not register the profile or relax the offline-only lifecycle.

Before stopping workers, require a fresh receipt bound to the complete fresh scope and selected arm for stop-dispatch/full-queue/Kafka-drain checks from a supplied audit provider, recheck generator idleness and original runtime, and bind immutable container IDs, start times and runtime semantics. The remote action checks the complete original primary inventory, broker volume and bind hashes before removal and rechecks each target immediately before removing its exact ID. Bound subprocess timeouts with a shared deadline, inventory size and regular bind-file reads. Recheck original container start times before handover and target role/project labels immediately before removal. Never use Compose project-wide removal, image pruning or volume deletion.

Write exclusive flushed intent and acknowledgement files using a fresh directory from the shared staging factory. Every action is single-use; a lost acknowledgement, partial removal, journal failure or guard failure blocks further forward actions. No remote retry is provided. Return only action and binding digests plus fixed check results; raw observations remain private in memory. Independently verify worker absence, including stopped or restarting containers and workers in unexpected projects. Missing drain providers or unsupported lifecycle actions fail closed.

## Alternatives
Generic role filters alone can select an unowned replacement. Treating a Docker exit code as absence misses restart or overlap. Wiring the incomplete adapter into the registry would enable experiments before restoration, financial auditing and observer integration are qualified. Retrying a lost acknowledgement risks replaying a destructive action.

## Consequences
These primitives make the ownership boundary executable and testable locally. They are a partial adapter, not a deployable lifecycle. Owned Compose file installation, common infrastructure changes, destination starts, restoration, complete audit/observer integration and registration remain required. Passing simulated transports is not live qualification or a capacity measurement.

## Failure and recovery behavior
Preserve the pending intent after any ambiguous action. Stop forward progression and retain the exact observed target IDs and private snapshot supplied by the future owner. The complete future lifecycle must still perform mandatory financial checks and owned restoration; this partial adapter never reports restored or clears recovery. Human pause and expired scopes block every forward call. Existing recovery classifications and consumed scopes are unchanged.

## Validation evidence
Executed: 327 affected tests passed in 27.23 seconds; Ruff and repository naming checks passed. Generated remote programs executed against simulated Docker and POSIX surfaces only. Initial fixture failures and all subsequent qualification runs are retained in the [sanitized runtime evidence](../capacity/flash-sale-opening/background-service-separation-runtime-2026-10-06.json). The real read-only preflight remains blocked for unresolved recovery, unregistered profile and missing fresh scope. No cloud calls, mutation, customer load or capacity result. Full deployment, restoration and financial/observer integration remain pending.
