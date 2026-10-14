# Customer port acceptance checklist

Every scenario below starts **NOT RUN** against the customer implementation. Reference Python tests and benchmarks provide examples, not customer pass results. Record target commit/image digest, configuration, schema migration, gateway sandbox, broker settings, routing, fixture IDs and retained evidence for each execution.

## Required correctness scenarios

| ID | Scenario and injected condition | Required outcome | Status |
| --- | --- | --- | --- |
| HOLD01 | At least 100 customers simultaneously request the same seat | Exactly one durable owner; losers have explicit outcomes; no duplicate booking | NOT RUN |
| HOLD02 | Multi-seat request includes one unavailable seat | Whole request rejects; no partial ownership or orphan order | NOT RUN |
| HOLD03 | Same hold key/payload arrives concurrently through different API pods | Same original hold/order/expiry; one durable operation | NOT RUN |
| HOLD04 | Same scoped key with changed payload | Conflict; no mutation or disclosure | NOT RUN |
| HOLD05 | Expiry worker races a new hold and delayed release of the old token | New owner preserved; old token cannot release or reclaim it | NOT RUN |
| HOLD06 | Server expiry boundary and retry after expiry | Original expiry retained; new initiation blocked; existing payment still inspectable | NOT RUN |
| HOLD07 | Redis-first writer stops before commit and after commit before ACK | PENDING blocks checkout; replay creates one durable order; expired command cannot revive | NOT RUN |
| HOLD08 | Redis-first failover loses provisional state or intake is full | Fail closed/processing according to contract; reconcile primary; bounded backlog | NOT RUN |
| PAY01 | Identical payment requests arrive concurrently on different pods | One durable operation and stable provider identity | NOT RUN |
| PAY02 | Gateway accepts charge but response is lost | Inquiry finds original operation; no new-key charge; eventual settlement reconciles | NOT RUN |
| PAY03 | API commits initiation then restarts before response | Authorized inquiry returns original identity; replay has no duplicate effect | NOT RUN |
| PAY04 | Inquiry returns NOT_STARTED while another request commits | Identical-key replay is safe; one operation despite the race | NOT RUN |
| PAY05 | Inquiry unavailable during ambiguous payment timeout | Customer sees processing; no blind resubmission; recover existing operation later | NOT RUN |
| PAY06 | Temporary 429/503 with Retry-After and deadline pressure | Bounded jittered attempts; original key/payload; no extended hold or deadline | NOT RUN |
| PAY07 | Wrong customer, changed payload, expiry or deterministic decline | No automatic initiation retry; no other-customer details | NOT RUN |
| PAY08 | Invalid signature, amount/currency mismatch or changed callback payload | No booking/financial mutation; explicit rejection or quarantine | NOT RUN |
| PAY09 | Identical and distinct callback IDs repeat the same successful payment | One financial/booking effect; valid duplicate ACK only after durable result | NOT RUN |
| PAY10 | Successful callback arrives after expiry and seat reallocation | Preserve new owner; persist success and refund/review obligation | NOT RUN |
| OUT01 | Financial transaction errors or COMMIT fails | Payment state, booking and outbox effects all roll back | NOT RUN |
| OUT02 | Financial commit succeeds but callback response disappears | Replay returns committed result; one business event/effect | NOT RUN |
| OUT03 | Broker unavailable while payment completes | Durable outbox remains pending; processing visible; no lost payment | NOT RUN |
| OUT04 | Publisher stops after broker ACK but before publication marker | Retry uses original event ID; duplicate delivery remains safe | NOT RUN |
| TKT01 | Worker stops before COMMIT | Inbox and tickets roll back together; redelivery completes | NOT RUN |
| TKT02 | Worker stops after COMMIT before broker ACK/offset update | Replay yields original tickets; no repeated financial effect | NOT RUN |
| TKT03 | Different event IDs reference the same paid order | Business uniqueness prevents additional tickets | NOT RUN |
| TKT04 | Concurrent/reordered paid/refund events for the same order | Valid state transitions preserved; never issue invalid or duplicate tickets | NOT RUN |
| TKT05 | Malformed event, unknown schema or repeated processing failure | Bounded retry; durable quarantine/review; no silent loss of paid work | NOT RUN |
| CACHE01 | Other customer requests cached order/tickets | Ownership enforced on every request; no disclosure | NOT RUN |
| CACHE02 | Delayed/duplicate/older cache publication arrives | Freshness not extended; newer valid snapshot not replaced by older state | NOT RUN |
| CACHE03 | Cache unavailable or cold; simultaneous status misses | Coalescing and aggregate fallback limits hold; financial slots protected | NOT RUN |
| CACHE04 | Financial transaction rolls back while snapshot was captured | No uncommitted payment or ticket snapshot published | NOT RUN |
| DB01 | Replica lag injected during payment inquiry or immediate ticket read | Critical request uses primary; no false NOT_STARTED from stale replica | NOT RUN |
| DB02 | Slow COMMIT, idle disconnect, restart and managed primary failover | No blind replay; connections recovered within bounds; failed evidence retained | NOT RUN |
| DB03 | Max pod replicas plus rollout surge and all background roles active | Total frontend/backend budgets respected; no unbounded waiters or pool amplification | NOT RUN |
| MIG01 | Rollback with pending commands, callbacks and outbox work | One inventory writer; durable work drained/transferred and reconciled | NOT RUN |

HOLD07 and HOLD08 are mandatory before Redis-first activation; they are not waived by a PostgreSQL-first pilot pass. If transaction grouping is enabled, additionally test full batch rollback, deferred COMMIT failure, opposite lock order, repeated-order splitting, cache publication after connection release and transport acknowledgement boundaries.

## Measurement and quality gates

Maintain separate counts for scheduled, dispatched, initially failed, recovered, finally failed and unique paid-and-issued outcomes. An undispatched journey is visible workload loss, not a successful recovery. Record physical attempts and inquiry calls to show recovery amplification.

| Gate | Contract |
| --- | --- |
| Financial integrity | Zero double-booking, duplicate charges, lost payments and inconsistent ticket relationships |
| Unresolved successful payment | Durable refund/review obligation and settlement tracking; never silently mark it declined |
| Generator drops | Zero for a qualified capacity comparison |
| Final customer failure | At most 0.01% of dispatched journeys; first errors and recovery reported separately |
| Total customer latency | Agreed profile thresholds, including gateway delay, polling and recovery |
| Cache freshness | Explicit configured maximum age, authorization and fallback bounds |
| Queue recovery | All fixture-owned work drained within the declared window; report ages, not just depths |
| Short target | At least 84 unique paid-and-issued tickets/s inside the offered window, not only after drain |
| Hour target | At least 300,000 unique paid-and-issued tickets inside one hour plus financial and customer gates |

At 84 offered journeys/s for five minutes there are 25,200 scheduled journeys; with zero drops, 0.01% permits at most two final failures. At 84/s for an hour there are 302,400 journeys; at most 30 final failures are allowed by that rate, but the independent 300,000 in-window ticket gate must also pass. Do not round a fractional failure allowance upward.

For fair comparison, keep offered workload, seat layout/contention, payment delays, connection budget, resource allocations and recovery policy unchanged; modify one measured factor. Record API/worker CPU separately from RDS CPU and inspect payment acquisition, SQL, lock and COMMIT waits. A five-minute rate multiplied by 3600 is a projection, not hourly qualification.

Test actual concert-opening contention separately from distributed-show traffic. A provider sandbox test does not establish production settlement behavior.

## Evidence required for each pass

- Timestamp window and synchronized schedule, with image digest, source/configuration identities and fixture ownership.
- Complete customer outcomes, first errors, recovery attempts, final failures, full latency and generator drops.
- Unique payments/orders/bookings/tickets reconciled by identities, not independent row counts alone.
- Gateway inquiry/settlement and refund/review reconciliation without secrets or personal data in public evidence.
- Queue ages/depths before, during and after; consumer lag and quarantined messages included.
- Role connection occupancy, acquisition/lock/SQL/COMMIT timings, RDS waits and replica lag.
- Injected failure and restart evidence, retained failed results, cleanup/restoration confirmation.

## Reference tests to translate

| Contract | Existing reference tests |
| --- | --- |
| Atomic ownership, replay, expiry | [reservations](../../../tests/integration/test_reservations.py), [Redis-first](../../../tests/integration/test_redis_first_reservations.py) |
| Payment context and commit recovery | [payment context](../../../tests/integration/test_payment_context.py), [commit recovery](../../../tests/integration/test_payment_commit_recovery.py) |
| Customer retry and inquiry | [recovery client](../../../tests/unit/test_customer_recovery.py), [journey accounting](../../../tests/unit/test_customer_recovery_journey.py), [restart recovery](../../../tests/integration/test_recovery.py) |
| Callback receipt option | [asynchronous confirmation](../../../tests/integration/test_async_payment_confirmation.py) |
| Consumer separation and transaction grouping | [event separation](../../../tests/integration/test_event_consumer_separation.py), [real transaction batching](../../../tests/integration/test_fulfillment_transaction_batching.py) and [batching boundaries](../../../tests/unit/test_shared_consumer_batching.py) |
| Authorized cache and event refresh | [cache integration](../../../tests/integration/test_order_status_cache_integration.py), [event refresh](../../../tests/integration/test_order_status_event_refresh.py), [committed snapshot](../../../tests/unit/test_committed_fulfillment_status.py) |

The target team should run these scenarios through its own drivers, broker, PGpool and gateway adapter. Mark a scenario PASS only after the required outcome and evidence are verified; a document check is not a correctness test.
