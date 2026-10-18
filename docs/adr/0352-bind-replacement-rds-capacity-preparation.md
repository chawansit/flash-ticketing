# ADR0352: Bind replacement-RDS capacity preparation

## Status

Accepted for preparation and validation only. Extends ADR0347 and ADR0351. User requested readiness notification before the next paid pair; no live allowance is opened.

## Context

Observer qualification under ADR0350 must precede a fresh160/s control and conditional candidate on the replacement RDS. Historical pair bindings retain the old diagnostic target and restoration snapshot. Read-only live capture on the replacement RDS verified normal service counts, generator idle and drained global queues. Compared with the previous normal snapshot, only PgBouncer's upstream/CA, migration endpoint/CA and pooler semantic fingerprint changed.

## Decision

Bind the exact reviewed old/new snapshot hashes, replacement diagnostic target and cutover receipt through a distinct optional ADR0352 contract inside the combined comparison contract. Reject all other snapshot changes. Permit only the diagnostic-target digest to differ from the historical reference when this exact contract is selected. Both measured arms must carry the same replacement binding, image/source identities and unchanged resource/settings contracts. Old160/s evidence is an orchestration reference, not qualification of this RDS. Preserve every historical result and consumed pair.

Exercise the actual plan, manifests and binding preparation with both published immutable images and a fresh snapshot. Test local reservation only in isolated copied bookkeeping; candidate execution still requires a real fully passed/restored same-pair control. Check replacement TLS and live services read-only. Do not deploy a measurement topology or start fixtures/paid traffic in this preparation task.

## Alternatives

Overwrite old snapshots/results: rejected. Ignore diagnostic identity mismatch or allow arbitrary endpoint changes: rejected. Treat restored data as proof of equal capacity: rejected. Begin a paid control before notifying the user: outside this request.

## Persistence, locking, messaging, idempotency, TTL and scaling

ADR0351 already switched database authority. This decision changes only experiment evidence binding and restoration inputs. Application transactions, locks, messaging, idempotency, TTLs and fixed pool/scaling limits remain unchanged. No workload correctness gates are relaxed.

## Failure and recovery

Fail closed on target, certificate, snapshot, source or same-pair differences. A consumed or failed arm is never reset. Fresh pair time and journal boundary are assigned only after paid-run approval; candidate remains conditional on passed/restored control. Existing financial reconciliation, drain and restoration remain mandatory during a future run. Restore to the replacement RDS snapshot; never silently return to the old database after new writes.

## Validation evidence

Read-only live capture and final recapture succeeded with normal service counts, generator idle and global queues drained; no customer writes or capacity stage. Existing diagnostic prepare/adapter passed against the replacement endpoint with verify-full, full visibility, one read-only data connection and credential cleanup. Both arms' actual published-image plans/manifests/bindings passed; actual control reservation passed only in isolated copied bookkeeping. Live bookkeeping remained unchanged by those preparation checks. Consumed prior allowance was closed separately without opening a new one. Both-image helper rehearsal and corrected regressions are recorded in ADR0350. Reproduction overlay preserves historical blobs and binds current inputs.

Stored registry credentials were rejected; the user supplied a fresh login, which was installed through password stdin/protected credential storage. Authenticated reads of both immutable manifests and exact configuration digests passed; no publication occurred. Four execution/authorization templates compile against the replacement target and snapshot. Future authorizer requires a separate fresh human approval receipt, current registry proof and matching prepared source contract. No new sequence, pair clock, live reservation or paid run exists. Candidate still requires the real fully passed/restored same-pair control; it was not granted live admission during preparation.

[Readiness receipt](../capacity/cce/combined-observer-new-rds-readiness-2026-10-18.json). Private preparation: tmp/adr0352-pair.prepared.private.json, tmp/adr0352-control-goal.prepared.private.json, tmp/adr0352-control-admission-qualified.json. Source contract890a80df8fbced0083a31f78a5777b04dc7b7fb78e4797ebc3af529294dc3c39. Logs: tmp/adr0352-final-preparation.log, tmp/adr0352-live-capture-final.log, tmp/adr0352-diagnostic-preflight.log. No throughput gain or replacement-RDS capacity qualification is claimed.
