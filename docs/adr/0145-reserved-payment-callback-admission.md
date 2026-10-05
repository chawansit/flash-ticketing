# ADR0145: Reserve admission for payment callbacks within the API budget

- Status: Accepted for opt-in implementation and local validation; cloud unqualified
- Date: 2026-10-04

## Context

ADR0144 failed customer and exact paid-ticket gates. API admission waits averaged 64.660 ms, callback backlog reached 5,909 and host CPU sampled 97.580%. Payment submissions and callbacks share the same protected payment pool and admission role. General requests have a limit of 10 and the total is 12, leaving two payment acquisition slots. Submissions can occupy that headroom before callbacks arrive. These measurements establish pressure, not the initiating regression cause.

## Decision

Add opt-in API_CALLBACK_ACQUISITION_RESERVE, default 0. With the existing payment partition and shared admission guard enabled, signed callbacks use a separate admission role over the same native payment pool. Reserve that many total acquisition slots against all non-callback acquisitions. Limit combined submission/callback counts, including retained timeouts, to the existing native payment queue ceiling. Also reserve callback headroom inside that shared native queue: submissions alone cannot exceed its ceiling minus the callback reserve. Preserve the general and total ceilings. For pool4/payment2/acquisition12/reserve2, general+submission acquisitions are at most10, submission+callback at most10, submissions alone at most8, and total at most12.

This protects admission; it does not reorder the native FIFO or preempt transactions. Callbacks can wait behind bounded admitted submissions. No connection, physical pool, worker, outer waiting queue or retry is added. A borrowed callback adapter does not own or close the native pool. Add fixed callback count/rejection telemetry to the existing pool metrics.

## Alternatives

Increasing the budget adds contention. A third physical callback pool repartitions existing payment connections and changes native scheduling. An outer priority scheduler adds a queue and timeout/cancellation lifecycle. Retries conceal rejection. These require separate decisions.

## Consequences and fairness

Reserved slots can remain idle without callbacks, and non-callback requests can receive earlier503 responses. The shared native payment ceiling preserves general headroom even during a callback flood. Payment submissions remain bounded but are not guaranteed admission during a sustained callback flood. Existing FIFO progress and timeout deadlines remain; this cannot guarantee a callback deadline during database stalls or CPU saturation. Reject configurations whose reserve consumes the entire payment native queue; at least one submission acquisition slot must remain. Default0 preserves behavior.

## Persistence, locking, messaging, idempotency, TTL and scaling

No SQL, seat ownership, Redis atomic hold, outbox, Kafka, payment idempotency, customer authorization, cache age, hold TTL, schema or scaling behavior changes. This extends ADR0133 with an optional callback reserve; its total ceiling and conservative timeout-retention decisions remain accepted.

## Failure and recovery

Release admission on success and all native exceptions including BaseException. Retain timed-out positions until the native queue empties, including positions shared by submission/callback roles. Never bypass the guard or fall back to the general pool. Preserve visible503/Retry-After. Invalid settings fail before resources open. Borrowed adapter closure must not close its owner's pool. Disable reserve to restore prior behavior without data migration.

## Validation evidence

Execute synchronized mixed-role saturation, timeout-retention, exception, shared ownership and route/signature tests. Real PostgreSQL/Redis checks must verify callback progress under general/submission pressure, bounded queues, duplicate callbacks, payment recovery, exactly one winner among100 concurrent holds, unique tickets and drained outbox. Verify source/locked dependencies in owned native Linux containers and remove owned resources. Append executed results, including failures. Cloud qualification is a later separately prepared experiment; no new cloud load follows this implementation automatically.


## Executed local validation

Final source passed68 Windows unit checks,193 focused native Linux tests and765 full native unit/integration tests, zero skipped. Source bytes and locked dependencies were verified; changed-source/test lint passed. Real PostgreSQL queues were filled with eight submission acquisitions, two callbacks and zero or two general readers; protected callbacks completed after owned test transactions released their connections. Duplicate replay after commit retained unique tickets; outbox drained with a mocked acknowledged broker. Existing100-request Redis-first atomic holds and payment commit recovery regressions passed. Owned containers, network and copied context were removed.

Earlier attempts remain private evidence: duplicate module basename prevented collection; simultaneous duplicate callbacks exposed existing NOWAIT row-lock rejection (188passed/1failed); distinct semantic duplicate callback-ID count was incorrectly expected to be2 rather than4 (189passed/2failed). These test setup/assertion failures were corrected without changing financial SQL or suppressing callback errors. A191focused/763full passing run preceded the extra startup guard; final193/765 results include it. Native payment-only saturation review also required reserving space inside the shared payment FIFO, rather than merely in the outer budget.

The implementation remains opt-in/default0 and cloud-unqualified. No cloud access, deployment, load, extra connection or GitHub publication occurred. This development branch includes earlier unqualified projection/coalescing candidates; a later one-factor cloud comparison must isolate only ADR0145 source and match the previous workload/budgets. This change does not prove improved RPS or the300000tickets/hour goal. [Local evidence](../capacity/flash-sale-opening/callback-admission-reserve-local-validation-2026-10-04.json).
