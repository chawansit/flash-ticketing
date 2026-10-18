# ADR0360: Recover the pre-fixture200 preparation stop

## Status
Accepted for exact operational cleanup and completion of bounded200 fixture preparation. No new architectural application pattern.

## Context
ADR0359 first200 run adr0151-d442b96c8c47 passed native safety financial/post-TTL, all replay gates, zero queues and six-member ownership. fixture_program rejected200 locally before session.api could invoke the fixture producer. Lost-receipt fallback marked unknown identity and retained namespace/helpers. Original runner restored normal new-RDS runtime and generator idle but native cleanup incomplete. Do not treat failed-restored journal status alone as complete native cleanup.

## Decision
Before any new round, preserve raw report/hash; verify exact retained binding and zero performance dispatch, inspect the original audit container for absence of paid fixture directory/receipt/intent, and verify normal runtime/generator/global queues. Rehydrate existing UID-guarded namespace cleanup and captured execution-hash Docker resource cleanup only from this run's retained receipts. Verify absence after removal and retain separate recovery receipt. Do not rewrite original failure. Add explicit200 to fixture builder and provenance count allowlists, preserving development/distributed/300seats/UUID uniqueness/freshness/producer and TTL checks. Exercise the actual producer command and complete Inputs.paid preparation against60000-viewer manifest before a fresh200 identity.

## Alternatives
Reset consumed attempt, ignore retained cloud resources, relax provenance/TTL, or infer cleanup solely from journal: rejected.

## Consequences
Cleanup only removes exact owned resources; no application or database mutation is needed for this no-paid stop. Existing user preapproval remains valid after recovery and new preparation validation.

## Persistence, locking, messaging, idempotency, TTL and scaling
No application SQL/locking/messaging/idempotency/TTL/resource change. Extend only exact bounded fixture count200, matching the approved60000-journey workload. Preserve immutable old evidence and exact ownership before deletion.

## Failure and recovery
Fail closed if any paid fixture exists, resource UID/execution differs, generator active, financial/queue evidence invalid or cleanup absence cannot be proved. Keep original evidence and no fresh paid run until recovery passes.

## Validation evidence
Actual cleanup passed: original audit container confirmed no paid directory/receipt/intent; original namespace UID and Docker execution identities verified; existing guarded cleanup removed namespace/helpers/private config. Normal runtime remained stable, generator idle and queues zero. Registered exact recovery without raw rewrite. Eight pure negative recovery cases passed;46 fixture/provenance/full Inputs.paid/reproduction cases passed. Separately actual candidate-image producer validation and generated fixture builder dry execution passed with network disabled, database execution intercepted and synthetic200show receipt clearly marked. No paid/database fixture execution is claimed for this dry rehearsal. Actual200 all15worker manifests/binding and isolated reservation passed; registry and Ruff passed. Hashes and scopes in docs/capacity/cce/bounded-200-fixture-preparation-2026-10-19.json. No200performance pass yet.
