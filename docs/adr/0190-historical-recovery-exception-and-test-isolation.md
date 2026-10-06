# ADR0190: Historical recovery exception and test isolation

## Status
Accepted under explicit human authorization on 2026-10-06; exception implemented and locally tested, fresh cloud restoration verified. New control test pending. The user approved proceeding when the old fixture information cannot be recovered. This replaces the earlier proposal to create dedicated service namespaces with fresh, exactly owned fixtures on the restored existing environment. It does not claim hard database, Redis or Kafka namespace isolation.

## Context
The paid comparison adr0151-647e2505c995 lost its paid fixture identifiers after containers were recreated and private manifests removed. A fresh read-only inspection checked 16 exact artifact locations and found none available. The user reports no snapshot or fixture backup and subsequently instructed: "yes but if there is not information. You can ignore it and proceed test again."

The historical candidate remains failed and per-entity financial reconciliation remains unproven. Runtime restoration and empty global queues do not make that result pass. ADR0172 otherwise blocks unresolved ownership; ADR0182 covers a different pre-dispatch abort and must remain restricted. ADR0185 now retains ownership before buyer dispatch.

## Decision
Record a narrow, append-only human-authorized exception for bounded_diagnostic_placement__9647f50ed259 and its exact immutable binding/entry/report artifacts. Preserve the original RECOVERY_REQUIRED entry, all counts, failed gates, data and consumed scope. Never call this financial recovery or a passed benchmark.

Require retained restoration and credential cleanup evidence plus a fresh read-only observation (at most 120 seconds old when recording the exception) of the unchanged original four-API runtime, absent secondary test resources, idle generator, absent owned credential snapshots and all global queues zero. Any other unresolved recovery, pause, active run, changed evidence or changed infrastructure configuration still blocks new admission.

The runner recognizes this receipt only as an exception to historical admission blocking. It never unlocks or replays the consumed scope. A new experiment still requires a registered profile, fresh reservation, source/image/configuration binding, mandatory safety qualification and complete unchanged customer, authorization, payment durability, zero-double-booking, post-TTL, queue-drain and restoration gates.

Use new random fixture/show identities and durable exclusive local identity receipts, hashed into the stage before viewer tokens or customer dispatch. Existing PostgreSQL foreign keys and show/seat uniqueness keep new fixture records separate from historical inventory. Global queues remain shared and must be audited as such; exact ownership alone is not hard service namespace isolation. Retire only known new shows. Do not delete old records or reconstruct ownership from titles, dates or counts.

Start with the already registered 60 journeys/second, 300-second control on unchanged machines and connection budgets. Its result measures that baseline, not the locally implemented ADR0189 callback candidate or 84 tickets/second qualification. Failed controls stop progression. No additional infrastructure, resizing, service namespaces, paid resources or main merge is authorized.

## Alternatives
Dedicated database/Redis/Kafka namespaces: earlier proposed option, deferred following the user's instruction to proceed without recovering unavailable historical information; would require additional isolation implementation and a fresh comparable baseline. Restore a pre-removal fixture backup: unavailable according to the user. Infer ownership or mark equal aggregate totals as recovered: rejected. Reset the old database: rejected. Permanently stop all cloud testing: unnecessary after the explicit scoped exception, but remains the default for any other unresolved ownership.

## Consequences
Historical uncertainty is explicitly accepted and stays visible. There is no new financial-loss or double-booking tolerance for future tests. Global services remain shared, so newly detected unresolved work stops testing. The exception adds no capacity improvement and does not establish production readiness.

ADR0172's default blocking rule gains only this human-approved historical exception; all other boundaries remain unchanged. ADR0182 is not superseded or broadened. ADR0185 remains mandatory for new paid fixtures.

## Failure and recovery behavior
Reject absent or changed original evidence, stale/future restoration observations, missing consent, unknown scopes, changed configurations, nonzero queues, active runs, pauses, symlink/path escape, duplicate exception writes and old-scope replay. Preserve partial exception receipts without admission if the journal append fails; do not overwrite them automatically. Subsequent failed runs do not inherit this exception.

## Validation evidence
Original read-only inspection completed in 38.703 seconds: 16 exact artifact locations, zero available; primary runtime unchanged, generator idle and checked global queues zero. Original ledger bytes unchanged at that checkpoint. See [inspection evidence](../capacity/flash-sale-opening/paid-fixture-recovery-inspection-2026-10-06.json).

Executed 126 exception/envelope/abort/fixture retention tests in 13.09 seconds and 129 affected runner/diagnostic tests in 34.69 seconds; Ruff and naming checks passed. An earlier synthetic test setup omitted the receipt parent directory (12 failures); corrected setup uses self-contained synthetic evidence, with no private artifacts required in CI.

Fresh cloud observation completed in 24.219 seconds and passed all eight exception gates. The append-only [historical exception](../capacity/flash-sale-opening/historical-paid-fixture-exception-2026-10-06.json) is installed; every original experiment entry is unchanged. The consumed scope remains RECOVERY_REQUIRED. New admission is available only through ordinary registered fresh reservations. New registered cloud control is pending; no historical reconciliation or new throughput result is claimed.
