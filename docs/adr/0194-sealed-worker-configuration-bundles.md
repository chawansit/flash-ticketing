# ADR0194: Sealed worker configuration bundles

## Status
Accepted for local implementation and fault qualification under ADR0172 and ADR0184. This does not register the worker-separation profile or authorize a standalone cloud entry point. No accepted decision is superseded.

## Context
ADR0193 measured primary CPU around 70.6% and secondary CPU around 17.3% at 60 paid journeys/second, with consumers the largest movable background role. ADR0184 already defines a fixed-budget background placement comparison. Its runtime primitives cannot yet install or safely remove private deployment configuration. Reusing mutable Compose files risks configuration drift and losing the original restoration definition.

## Decision
Prepare exact arm and restoration Compose JSON in memory from the validated ADR0184 pair and ADR0186 snapshot. Install only these generated documents in a fresh directory named by the shared staging factory. Do not overwrite original configuration, copy arbitrary directories, move persistent data or start containers. Bind the documents to the pair, saved runtime, selected arm, host and fresh scope. Primary bundles include the original restoration model; secondary bundles contain only candidate workers.

Use exclusive, owner-only files and directory-relative POSIX file descriptors with no-follow checks. Flush each file and directory before returning a seal containing directory identity, file identities, sizes and hashes. Verify the complete file set, ownership, permissions and content before use or cleanup. Cleanup unlinks only the exact sealed regular files and removes the exact owned directory; never recursively delete a computed path. Keep private payloads in memory and store payload-free ownership seals in private journals. Public evidence includes only hashes and fixed results. A lost or missing seal means unknown ownership, not permission to infer cleanup targets.

Provide a transport-bound adapter using the same ActionGuard and private session as ADR0188. Every installation is single-use, with a flushed intent before the remote action and acknowledgement afterward. Guard/policy/binding failures prevent installation. Cleanup of an already sealed bundle remains allowed during pause or deadline expiry and temporarily uses the session cleanup mode. Align the worker authority check with the envelope by calling the existing ADR0190 exact historical-exception verifier. Unknown failures, changed bindings or missing exception receipts still block; the old failed scope is never reopened or certified. The profile remains unregistered until the full lifecycle, dependency readiness, financial audits, observers and restoration are locally qualified.

## Alternatives
Overwrite existing Compose files: risks changing the restoration source. Recursive directory cleanup: can remove unowned or replaced files. Retry an ambiguous install: can select a different owner after a lost response. Treat a configuration-only test as a qualified live lifecycle: omits worker absence, Kafka volume retention and mandatory financial checks.

## Consequences
This completes the owned private configuration prerequisite for service separation. It does not establish private Kafka reachability, deployment safety, capacity improvement or production qualification. Configuration payloads contain secrets and must never enter public evidence, exceptions or command-line arguments.

## Failure and recovery behavior
A partial install or lost acknowledgement preserves the intent and blocks replay. An unexpected file, changed inode, symlink, ownership/mode change or hash mismatch blocks cleanup. Failed local journal persistence still permits cleanup only when an exact remote seal was received. No broker/runtime mutation or customer dispatch is part of this component. Full lifecycle restoration and queue/payment checks remain mandatory before any future load.

## Validation evidence
Executed: 336 affected tests passed in 31.23 seconds with no failures or skips, including 14 real Linux generated-program cases. The exact owned network-isolated local container was removed. Ruff and working-tree repository naming checks passed. A local test-template quoting error was corrected before the final suite. Review found that a cleanup failure needed to fence further installation; this was corrected and qualified. Authority tests use isolated synthetic retained evidence, preserving reproducibility without private cloud files. See [configuration evidence](../capacity/flash-sale-opening/background-service-separation-configuration-2026-10-07.json). No cloud call, deployment, customer dispatch, capacity gain or hourly qualification. The profile remains unregistered; common infrastructure, worker starts, exact restoration and full lifecycle qualification remain pending.
