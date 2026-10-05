# ADR0150: Import identity and bounded observer recovery

- Status: Accepted; local validation passed, cloud execution/qualification pending
- Date: 2026-10-05

## Context

ADR0148 paid072a42fcef02 failed customer/load gates, then secondary SSH reset during owned observer stopping. Raw traces survived but their summaries/distribution were absent from the original gate record. Fresh pinned private-hop recovery completed cleanup. Checks hashed /app copies while worker tracebacks used installed package paths. Subsequent read-only import resolution matched the restored frozen runtime, but future candidate deployment must verify both copies before dispatch.

## Decision

Generate a self-contained verifier from an allowlisted src/ticketing module-to-SHA256 map. Check normalized /app source, Python import-resolved source and the loader code object against a fresh compile of that resolved source (including cached-bytecode mismatch detection). Record origin/hash/code equality separately. Reject missing modules, unsupported loaders, unsafe paths, source or code mismatches. Avoid importing application modules or starting services. Existing source_hashes_match gates require all three checks; this proves the verification interpreter's import resolution, not arbitrary previously loaded objects. Before dispatch, verify all four APIs and all thirteen Python background containers (six consumers, three writers and one each maintenance/publisher/reconciler/simulator). Use the saved immutable image for each role, compare inspected container/start identity before and after the source proof, and retain module origins/hashes in private inventory. Any mismatch stops qualification before fixtures/customers.

Retain raw pipeline/Kafka traces and summarize them in finally even if an earlier owned stop/transport action fails. Summary failures remain explicit; preserve original stage failure and false pass state. Derive offered-window replica distribution only when the declared window/inventory exists. Do not infer full financial success from zero observed duplicates, or waive financial/queue gates.

Add explicit bounded Session.reconnect(role), with at most two reconnect attempts per role per session, counted before connection and checkpointed. Retain protected password only in memory until close, reuse pinned known-host verification and fixed timeouts. Opt-in secondary private-hop fallback uses only its configured validated private IPv4 through the pinned primary. Never fall back on authentication/host-key rejection. A primary replacement invalidates dependent hop clients. No call/put/deployment/job-launch/customer request is automatically replayed. Exact PID/start-tick/command-hash-owned job stopping may retry once after transport reconnection; each attempt revalidates ownership. Other remote/ownership errors do not trigger reconnect/replay. Mandatory topology/audit/credential recovery remains explicitly scoped and fail-closed; do not claim fully autonomous restoration of every failed deployment.

## Alternatives

Rely only on /app hashes: can miss installed source/bytecode drift. Blind RPC retries: can repeat paid dispatch or mutations after lost responses. Treat finally collection as automatically passing: conceals original failure. Unbounded retries or another broad background recovery service: unnecessary new ownership/execution risks. Existing transport keepalive alone did not prevent the observed failure.

## Consequences

Verification adds bounded preflight work and may fail on legitimate unsupported/custom loaders; qualify adapters before load. Import resolution can inspect already cached bytecode without executing application modules; it cannot certify live objects in an arbitrary old process. Reconnection may still fail due routing/host availability. Paid launch accounting stays consumed on ambiguous failure. Private-hop access uses the same pinned secondary key, with no temporary key installation. Retained summaries improve diagnosis but never retroactively turn a failed original run into a success.

## Persistence, messaging, idempotency, TTL and scaling

No application/pool/transaction/lock/Redis hold, outbox/Kafka semantics, payment idempotency or TTL change. No new host/service, worker scaling or connections. Extends ADR0147/0148 evidence/owned cleanup contracts only; no accepted financial/scaling decision superseded. Existing source/observer/customer/financial/zero-double-booking/full queue/Kafka/restore gates remain mandatory. ADR0149 stays disabled in cloud.

## Failure and recovery

Invalid ownership prevents any stop. Initial connection failure closes all partial resources and clears the retained password. Closed sessions cannot reconnect. SSH or source proof failure stops dispatch. A failed stage keeps failure metadata, raw traces and any separately recovered summaries; missing summary evidence is explicit. No new cloud load, safety ticket, paid replacement or schedule is authorized here.

## Validation evidence

Recorded before implementation. Executed 270 focused Windows tests with 0 failures, changed-script/test Ruff and git diff check. Actual synthetic package subprocesses detected installed-copy drift and timestamp-valid altered bytecode without executing target application modules. Embedded container proof rejected image/start/role/running/import drift before or after verification. Synthetic live inventory required all four APIs and thirteen background workers against saved per-role images. Pinned transport tests covered opt-in private hop, rejected authentication/key changes, two-attempt session budget, partial-client/password cleanup and no call replay. Exact-owned stopping retried once only for transport failures; existing PID/start/command ownership rejection tests remained passing. Finally-stage wiring retained summaries and original failure without customer dispatch.

Offline recovery of retained ADR0148 paid072a42fcef02 traces produced 392 pipeline and 231 Kafka samples. Original failure, customer, financial and queue fields stayed identical and pass stayed false. Offered-window distribution remained explicitly unqualified: the strict checker reported a metric sampling gap. An initial offline assertion incorrectly expected distribution recovery and failed; it was corrected after inspecting that coverage failure, without rewriting the original run or loosening the gate.

[Local validation report](../capacity/flash-sale-opening/import-identity-observer-recovery-local-validation-2026-10-05.json) contains bounded evidence hashes and limitations. No live SSH reconnection, cloud preflight, new safety/paid ticket, image build or performance improvement was executed. Changed adapter identity requires fresh qualification before any separately approved cloud experiment.
