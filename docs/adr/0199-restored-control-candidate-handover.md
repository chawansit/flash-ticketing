# ADR0199: Restored control candidate handover

## Status
Accepted and locally qualified within ADR0172. Extends ADR0198 and supersedes the ADR0188 initial-container-ID requirement only for a candidate continuing a fully passed, durably recorded and restored control in the same guarded session. Original runtime semantics, immutable baseline snapshot, exact ownership and customer gates remain required. Worker profile registration and cloud execution remain pending.

## Context
Restoration can recreate original services with different container IDs. Reusing the baseline IDs correctly fails ownership verification, but prevents the candidate from starting. Silently replacing the baseline snapshot would weaken the comparison binding and could adopt unrelated containers.

## Decision
Issue one immutable handover receipt from the actual completed RestoredPaidArm control object. Require its persisted passing summary, successful financial and booking audits, complete queue drain, restored runtime, idle generator and verified private-file cleanup. Independently observe the restored runtime before and after a fresh full queue/idle check. Bind its container IDs, start times and semantics to the original scope, session, source contract, pair, saved snapshot and control summary hash.

Consume the handover once, with durable intent before candidate construction. A candidate with this receipt retains the unchanged baseline snapshot and scope hashes, adds the handover hash to its lifecycle binding, and checks the exact restored container identities on every original-runtime observation. Generate the existing exact-ID stop program using a temporary target-ID copy; never rewrite the baseline or scope. Recheck the complete runtime and every target immediately before removal. Initial standalone component construction retains its existing original-ID guard; the future full comparison must require this continuation proof for its candidate.

## Alternatives
Removing the identity guard adopts unproved resources. Updating the original snapshot invalidates immutable comparison evidence. Reusing a restored summary dictionary without provenance permits failed or unrelated controls. Separate reservations for each arm require different accounting and do not solve provenance by themselves.

## Consequences
The handover explicitly distinguishes original semantic restoration from container identity continuity. No extra paid stage, resource or customer retry is introduced. A failed or ambiguous handover is not replayed and cannot authorize more load. Component qualification does not certify the complete runner or production capacity.

## Failure and recovery behavior
Reject failed controls, missing or modified summaries/receipts, different sessions/scopes/sources/snapshots, stale issuance, changed IDs/start times/images/binds, busy generator or pending queues. Claim persistence failure consumes the continuation locally and blocks another claim. Human pause, deadline and original authority checks still block forward actions; owned cleanup follows the existing guards.

## Validation evidence
The final affected suite passed 708 tests with zero failures or skips in 292.92 seconds, including 23 existing real PostgreSQL financial cases and three isolated Linux cases. The focused handover/restoration suite passed 54 tests in 52.76 seconds before additional identity and receipt-reader cases were added. Runtime and handover transports are synthetic; the generated exact-ID stop program is executed against simulated Docker observations. These checks do not qualify the complete cloud comparison runner.

The first development run had 12 failures and 42 passes: Windows path stat and descriptor fstat exposed different ctime semantics. The corrected reader checks device/inode/size/mtime across observations, ctime within each observation method, and immutable evidence content hashes. File replacement and mid-read modification remain covered. The failed raw evidence is retained.

Successful restoration with new IDs, exact five-worker removal, failed controls, summary/receipt/source tampering, identity drift, session/scope mismatch, stale issuance, journal loss, unknown stop acknowledgement, configuration routing and single-use enforcement passed. Temporary local test containers were removed. Ruff, naming and diff checks passed. No cloud load or capacity improvement was measured; the worker profile remains unregistered.

See [the compact handover checkpoint](../capacity/flash-sale-opening/background-service-separation-handover-2026-10-07.json).
