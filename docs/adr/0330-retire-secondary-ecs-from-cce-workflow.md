# ADR0330: Retire the secondary ECS from the CCE workflow

## Status
Accepted; primary-only bootstrap qualified live. Secondary VM deletion remains pending because cloud management access is unavailable.

## Context
The primary ECS hosts Kafka, PgBouncer and restored fallback services. The generator ECS is dedicated to load generation. The secondary ECS (ecs-chawansit-de5e, private10.1.207.148) is empty after cleanup and is not used by the paid CCE workload, but historical setup, inventory, transition and CPU-observer code still require it. The user authorized removing this dependency and retiring that VM. Last verified clean short throughput remains138.49 paid-issued/s; the160/s run remains failed and recovered.

## Decision
Add an explicit source-bound primary-only CCE bootstrap, leaving historical two-host protocols available for reproduction. Four bootstrap APIs run on the primary, then all four stop before native CCE APIs/workers activate. Collect real primary and generator identities; never fabricate a secondary inventory, alias it to the primary, or report an absent CPU observation as passed. Preserve immutable application images/configuration, Redis atomicity, payment idempotency, post-TTL durability, complete financial checks, CCE safety across all four pods, queue drain, connection budgets and exact restoration. A new topology marker and fresh configuration binding must select the new path; existing consumed scopes remain unusable. The generator and primary remain in service.

The secondary is removed from SSH connections, bootstrap deployment, inventory refresh, API transition, CPU monitoring and cleanup. The absence is explicitly recorded as not applicable, with the primary's actual CPU interval verified. Historical two-host placement performance is not directly comparable to bootstrap CPU placement; native paid-stage application resources remain unchanged. No paid load is authorized by implementing retirement.

## Alternatives
Keep paying for an empty secondary solely for historical bootstrap; delete it immediately and break the next run; or rewrite the full runner. Prefer the bounded opt-in topology change, with regression coverage for both paths and denial of any secondary access in the new one.

## Consequences
One less VM is needed by the CCE workflow once the new path is verified. Prior results and failed evidence remain unchanged. This does not fix generator throughput, qualify higher capacity, move Kafka/PgBouncer or change persistence/messaging/TTL semantics. Supersedes the CCE workflow's mandatory1+3 ECS bootstrap fromADR0228 only when the explicitADR0330 marker is active; historical two-host experiments retain their original contracts.

## Failure and recovery behavior
Reject malformed/mixed configurations, missing primary observations, changed image/resources, unbound topology flags or secondary access attempts. Always stop customer jobs, audit and drain before cleanup, prove native worker absence before restoring ECS workers, and retain failed artifacts. Keep the VM until local tests and a bounded non-paid qualification prove independence. Verify exact instance identity and emptiness before deletion; do not delete primary/generator, shared services or unrelated storage. If management access is unavailable, leave the VM intact and report the limitation with a concrete handoff.

## Validation evidence
The regression suite executed: **475 passed, 1 skipped** in 255.95 seconds. It includes actual transition capture/activation/restoration and composed lifecycle tests that reject secondary-host access. After import-only cleanup, the final source subset executed **106 tests, all passed** in 44.95 seconds. Ruff F/I and repository naming checks passed. Historical reproduction checked 535 files, including 30 current overlays and 78 frozen parent files. Complete primary-only binding validation passed without registering a paid goal.

Live run `adr0151-d54b7d9bccb9` connected only primary and generator, with zero secondary-access attempts, then failed preflight before service mutation or safety traffic: RDS was unreachable. A separate read-only check also found Redis connection refused. Credentials were removed and no paid capacity stage started. This attempt used an earlier source seal; it does not qualify the final source for live use. The native CCE lifecycle was verified offline, not exercised live here.

[Retirement preparation evidence](../capacity/cce/secondary-ecs-retirement-preparation-2026-10-15.json) records the final source seal, test results and remaining checks. No VM was deleted. Management console automation and a configured cloud management CLI were unavailable. Keep the secondary until a fresh bounded qualification and exact-instance/storage checks pass. Best verified clean short throughput remains 138.49 paid-and-issued tickets/s; this work measures no capacity improvement.


## Live follow-up correction
After RDS/Redis restart, live run `adr0151-8eff27d6e599` passed connectivity and bootstrap readiness, then the legacy probe rejected same-host API origins before sending its hold wave. Runtime restoration and queue drain passed. Extend only the explicitly selected primary-only probe mode to require two distinct published ports on one private host; preserve the default distinct-host requirement, authorization, atomic-owner, replay, payment and post-TTL checks. Report cross-API evidence without claiming cross-host coverage. A fresh qualification is required.


## Final live validation
Fresh run `adr0151-2f1c18800987` passed on the final source seal: all four primary APIs ready, 100 concurrent hold attempts (one accepted and 99 expected seat conflicts), cross-API hold/payment replay, authorization, one issued ticket, post-TTL financial audit, full queue drain and exact runtime restoration. Only primary and generator were contacted by the qualifier, with zero secondary attempts. Protected credential snapshots were removed and the generator is idle. The final source regression subset passed 111 tests in 55.45 seconds; source binding, Ruff and historical reproduction checks passed (535 files, 31 current overlays).

A separate read-only check confirmed the exact secondary instance, no containers, no Docker volumes and one 40 GB system disk. [Live evidence](../capacity/cce/secondary-ecs-retirement-live-2026-10-15.json) preserves both failed attempts and the passing receipt. Native CCE lifecycle remains offline-tested, not exercised live by this qualifier. No paid capacity stage or VM deletion occurred. Browser/Windows automation still cannot initialize and no cloud management credentials are configured, so the deletion itself requires console access.


## Paid-observer compatibility correction, 2026-10-17
Run `adr0151-2a3707028c36` passed native safety, both fixture-clock proofs and manifest upload, but stopped before paid dispatch. The composed observer compared the live bootstrap API image against the frozen historical parser image. These are different identities; the sealed inventory correctly recorded the live image. Use the approved inventory's API image for the existing primary-only validator, as the legacy qualified path already does. Retain exact inventory digest, source/worker identity, authority, settings, freshness and database-budget checks. No application image, resources or acceptance gate changes. The failed run restored successfully with zero paid stages. A composed slot-entry regression exercises the actual frozen parser and rejects changed image, settings, authority, marker or seal; 123 focused tests passed in 15.41 seconds, including the composed adapter regression. Full primary-only source binding, isolated admission and historical reproduction (535 files) passed.
