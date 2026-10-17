# ADR0349: Seal transferred combined worker verification

## Status

Accepted for local implementation/qualification only. Extends ADR0347/0348 observer integration. No new cloud retry is authorized by this decision.

## Context

The user approved a fresh160/s control after ADR0348. A network-disabled helper preflight in the actual immutable parent image then proved that transferred cce_worker_identity.validate_bundle imports cce_simulator_roundtrip_profile, which is absent from the declared remote helper transfer. Sending the whole local profile would also require local authority/state files that do not belong in a remote metrics observer. The fresh cloud run is being stopped before paid traffic through an explicitly agent-owned stop marker; mandatory restoration remains in force.

## Decision

Keep local admission validation bound to the active exact pair. Add a separate transferred-bundle validator using static reviewed control/candidate immutable manifest and22-module source-identity pins, with the unchanged15-worker resource/count model. Transfer that small pins module with the existing reviewed helper set for combined comparisons. The remote observer receives an already hash-sealed admitted bundle; it must accept only one of these exact immutable identities and validate all existing schema/source/image/UID/IP/process/command/environment/resource checks. It must not read local work-envelope or registry files.

Factor shared validation without weakening default local validation. Exercise the actual observer.start helper-selection/upload path with offline sessions, then import/install its captured helper package in both actual application images with networking disabled. Test both admitted arms and tampered manifests/sources/UIDs/counts. Compare static pins to real published receipts locally. This is helper/deployment qualification, not cloud capacity evidence.

## Alternatives

Transfer local authority/profile/files: couples remote observations to local state and risks unnecessary private exports. Accept image/source fields from the bundle without independent pins: rejected. Skip worker validation, change the financial workload or add retries: rejected. Rebuild the application image: unnecessary; the correction belongs to the transferred observer harness.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application persistence/lock/message/idempotency/TTL/scaling change. PostgreSQL transactions, callback guarantees, cache/polling, four API pods,15workers,24backends and16delivery slots remain unchanged. Pins and admitted-bundle hash supplement remote observation identity only.

## Failure and recovery

Missing helper or invalid sealed identity fails before observer readiness and paid dispatch. Default local admission still requires the active exact image and pair. Existing cleanup/restoration always runs; an agent-owned stop marker is removed only after terminal restoration and no active lock, and never treated as user-requested pause. Preserve all stopped/failed results and do not reset consumed pairs. No automatic paid rerun, resize, gate relaxation or main merge.

## Validation evidence

Initial network-disabled preflight failed with missing cce_simulator_roundtrip_profile in immutable parent image. Correction implemented and executed:17 focused checks passed; actual22-file helper selection/upload captured from observer.start, then both actual immutable images passed network-disabled sealed4API/15worker bundle validation, outer observer CLI/hash rejection, adapter installation and retained inventory structural replay with explicitly synthetic fresh timestamp. This tests the transferred package without local work-envelope/profile dependencies; it does not execute financial load or establish cloud throughput.267 broad regression checks passed with2 skips (Docker explicitly rehearsed separately; host symlink unavailable). Ruff passed. Actual plan/manifests/binding and actual-publication reservation in isolated bookkeeping passed. [Compact local proof](../capacity/cce/combined-observer-helper-local-2026-10-17.json). Logs: tmp/adr0349-helper-tests.log, tmp/adr0349-full-helper-test.log, tmp/adr0349-focused-tests-final.log, tmp/adr0349-preparation.log, tmp/adr0349-control-admission.log.

Fresh approved cloud attempt adr0151-d47e3c15103e was stopped by an explicitly agent-owned marker before safety or paid dispatch. Original runtime restoration passed; original integrity status remained RECOVERY_REQUIRED because the stop blocked final verification. Separate read-only runtime/queue/helper/namespace/generator verification passed and closed only operational recovery as FAILED_RESTORED. Original raw/hash unchanged; no paid throughput or candidate run. [Stopped result](../capacity/cce/combined-confirmation-fresh-pre-dispatch-result-2026-10-17.json), [independent recovery](../capacity/cce/combined-confirmation-fresh-pre-dispatch-recovery-2026-10-17.json). Owned stop marker removed after verification; no human pause was requested. No further cloud retry or allowance opened.
