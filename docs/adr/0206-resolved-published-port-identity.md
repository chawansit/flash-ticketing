# ADR0206: Resolved published port identity

## Status
Accepted for implementation and mandatory recovery. Supersedes the HostConfig numeric-only port observation assumption in ADR0147/ADR0195 for declared published ranges. No topology, port, connection budget or customer gate changes.

## Context
The corrected image staging passed on both ECS hosts. Common infrastructure was applied, including four APIs with declared private published range 8101-8104. Docker retains that range in HostConfig.PortBindings and exposes each assigned concrete port in NetworkSettings.Ports. The observer tried to parse the range as one integer, blocking phase validation and restoration after original workers had stopped. No paid stage, fixture or customer dispatch began.

## Decision
Keep historical concrete-port observations unchanged. For an explicit declared range, require one matching resolved NetworkSettings binding on the exact declared interface and target/protocol. Validate the range and selected integer port, require it to lie within the declared bounds, and reject missing, ambiguous, duplicate or foreign mappings. Use the resolved concrete port in runtime identity and existing planned-port budget/uniqueness checks. Do not select a guessed port or alter publication settings. Bind this shared observer source in future worker entry-point identities.

Restore the existing consumed scope using its original sealed restore document only after independently proving the current layout is the exact owned common infrastructure, its queues are drained and no customer stage began. Recheck original semantics, complete queues, global duplicate booking, idle generator and exact private configuration cleanup. Preserve the failed result and consumed counters; do not replay the failed common-configuration step or start more load.

Recovery inspection also found that the prepared API and pooler models omitted the image-owned User and WorkingDir defaults. The pooler defaults match the original image snapshot; API defaults are proved by the staged schema-2 immutable content metadata. For recovery recognition only, resolve missing fields from those exact original or staged immutable image defaults before checking every planned setting; never overwrite explicit values or alter the sealed application/restore documents. Future preparation must materialize these existing defaults before sealing, as intended by the existing full-executable-settings contract. This is an implementation correction, not a privilege or directory change.

Health duration checks must also compare equivalent integer Compose units (ns, us, ms, s, m, h) to Docker nanoseconds; reject unsupported/fractional values rather than require all declarations to have been written as ns. Preserve every test, retry and timer value exactly.

Docker recreation also encodes no entrypoint as an empty list instead of the original null. During this recovery only, authenticate every original raw row against its saved semantic hash, then permit null-to-empty-list equivalence only for roles whose sealed restore document explicitly disables the entrypoint. All other runtime fields, image, command, volume, binding and counts remain exact. Future snapshots must preserve the observed null declaration, so restoration inherits the same immutable image default rather than forcing a new representation. Independent recovery evidence closes only the zero-dispatch bootstrap failure, retains the original failed report and counters and adds actual recovery time without reopening the scope.

## Alternatives
Treating a range as one integer fails on the real Docker response. Guessing the first port can conceal replica overlap. Ignoring port identity weakens private interface and topology checks. Recreating unverified resources would violate owned recovery.

## Consequences
Runtime identity reflects actual assigned ports for ranges while historical fixed-port hashes remain compatible. Unsupported or ambiguous mappings fail closed. This corrects observation, not application performance or deployment topology.

## Failure and recovery behavior
Unrecognized layouts or range/interface mismatches continue to block load and unsafe restoration. Mandatory owned recovery uses original snapshots and seals and remains allowed after a failed experiment. Preserve private observations and original failed evidence. Never claim a capacity result from setup or recovery.

## Validation evidence
Protected partial runtime: tmp/adr0153-parents-3832a9c0dbd6/observation.private.json. The failed parser received 8101-8104 in HostConfig while the four concrete NetworkSettings bindings were 8101, 8102, 8103 and 8104 on the private interface. The original twelve-service runtime was restored; immutable images, commands, settings, broker volumes, binds, retained inactive inventory and empty secondary were verified. Generator idle, global duplicate booking zero and all financial/reservation/outbox/refresh/Kafka queues zero passed. Both staged archive owners and the sealed private configuration were removed. The failed scope was closed FAILED_RESTORED without changing its original result or consumed paid counters (all zero). Recovery accounting adds 121.5 seconds, including two measured failed recovery attempts. Evidence: docs/capacity/flash-sale-opening/background-service-separation-bootstrap-recovery-2026-10-07.json.

Focused regression qualification executed: 196 passed in 26.48 seconds, covering exact port resolution, inherited defaults, health-unit equivalence, authenticated historical empty-entrypoint recovery and closure rejection for missing proofs or customer dispatch. Raw evidence: tmp/adr0206-targeted-final.txt. Broader affected runner qualification executed: 627 passed, zero failed/skipped in 271.45 pytest seconds (272.08 harness seconds), including three isolated Linux checks. Raw evidence: tmp/adr0153-parents-3f3f279039ce/broker-qualification.txt. Naming, lint and diff checks passed. No customer load or capacity improvement has been measured.
