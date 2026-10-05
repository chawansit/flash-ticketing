# ADR0163: Isolated partial-timeout reclamation source and comparison profile

- Status: Accepted for local source/profile preparation; cloud execution pending qualification
- Date: 2026-10-06

## Context

ADR0162 reproduced conservative timeout-slot retention after partial native FIFO pruning and implemented a default-off correction plus failure-time evidence. The engineering tree also contains callback-reserve wiring and other unqualified changes excluded from the frozen ADR0160 source used by the failed ADR0161 control. Copying the working tree would introduce additional factors. Prior cloud scope remains closed; an intermittent local18,000-seat projection deadline failure remains recorded.

## Decision

Reproduce the frozen ADR0160 export through its existing pinned parent chain. Apply an allowlisted text overlay to exactly API/config/HTTP/observability/PostgreSQL. Port ADR0162 reclamation and diagnostics onto the original general/payment guard without callback alias/reserve wiring. Preserve original role/shared/native limits and financial transaction code. Hash the complete runtime and source trees, parent manifest, overlay patch and unchanged authority/dependency inputs. Reject source drift, path escapes, extra overlay files and symlinks before image construction.

Prepare an offline-only comparison contract: fixed2+2API and original total connection/admission budgets; identical immutable role images in both arms. Keep synchronous callback intake,1000ms status cache,consumer refresh on,dedup off,500ms polling,simulator10connections and the existing idle confirmation worker2connections in both arms. Only API_PARTIAL_TIMEOUT_RECLAIM changes0->1 on APIs; background flag remains0. Diagnostics are common to both arms. Explicitly exclude callback-reserve activation and dependency changes.

Reuse pinned export/image verification utilities. The preparation command emits no SSH calls, credentials, deployment or customer traffic. This decision does not add a cloud-capable runner, reopen prior ledgers or claim paid-stage execution implemented. Qualify the exported source with native admission/financial tests and immutable-image source/import/dependency/config checks before a separate fresh bounded cloud protocol.

## Alternatives

Exporting the engineering tree risks including unrelated factors. Reusing old images cannot exercise the fix. Increasing waiters/connections/deadlines or introducing retry would confound diagnosis. Changing async confirmation simultaneously prevents attributing a result. Copying the paid runner would fork its recovery/financial gates; reuse that engine later through an exact separately qualified profile.

## Consequences

The port is a second source representation and needs explicit parity checks with ADR0162's reclamation/failure-recording methods, plus native validation of the exported adapter and API. Diagnostics may affect both arms equally; the updated control is not byte-identical to the historical control. Same-load differences describe reclamation only and do not qualify300000paid-issued tickets/hour. Retain intermittent projection failure and all earlier failed reports.

## Failure and recovery

Fail preparation on parent/overlay/tree/hash/config mismatch. Use fresh owned tmp directories and read-only source inputs. Native test resources have exact ownership labels and bounded cleanup. No live state changes occur during preparation; original cloud deployment and closed ledgers stay intact. Later cloud qualification must retain100-way atomic hold,authorization,replay,payment durability,post-TTL and full queue/Kafka drain gates, and stop after failed control. HoldTTL,booking uniqueness,payment/refund authority,outbox andKafka semantics remain unchanged.

## Relationships

Uses ADR0162's partial reclamation rule, ADR0152/0153 reproducible export/image verification and ADR0161's common placement/budgets. Does not supersede cloud authorization or enable ADR0145 callback reserve. Paid runner integration and fresh safety/control/candidate stages remain future work.

## Validation evidence

Pending: exact frozen export, five-file overlay/authority/transaction parity, off/on sole-factor profile, tamper rejection and native exported-source tests. Append only executed results. [ADR0162 local evidence](../capacity/flash-sale-opening/partial-timeout-reclamation-local-validation-2026-10-05.json).


Executed local qualification: reproducible213-file/21-module export passed five-file allowlist, full immutable input/dependency map, protected authority hashes, financial transaction AST and ADR0162 reclamation/diagnostic-method parity. Six distinct immutable images passed original-parent and installed source/import/dependency/inherited-config checks. Two actual API-image settings probes confirmed reclamation0/1 with synchronous intake and unchanged pool/waiter/deadline budgets.

32 host profile tests passed, including full synthetic inventory qualification and tampered-source/receipt/dependency/image/marker rejection.272 native exported-source application tests passed, covering100-way holds, financial/replay/expiry/recovery and candidate-enabled mixed admission bursts. Source inputs remained read-only and owned containers/network were removed. Changed-file Ruff and diff checks passed. No cloud call, staging, deployment or customer dispatch occurred.

[Local validation summary](../capacity/flash-sale-opening/partial-timeout-profile-local-validation-2026-10-06.json) and [off/on plan](../capacity/flash-sale-opening/partial-timeout-comparison-plan-2026-10-06.json) retain exact hashes/budgets and evidence paths. The earlier engineering-tree18,000-seat projection failure remains recorded; its unqualified projection implementation/test is outside this frozen export, so these passes do not close that stability gate. Capacity benefit and cloud503attribution remain unmeasured. Cloud-capable runner/observer integration and a fresh bounded scope are still pending.
