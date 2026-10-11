# ADR0283: Bound hourly observer cost and preserve diagnostics

## Status

Accepted for the bounded observer correction; offline and read-only cloud validation executed. Five-minute paid comparison passed and restored. Subsequent hour measured the 300,000-ticket target with independently verified correctness; original qualification failed financial/diagnostic gates and remains FAILED_RESTORED. No maximum capacity or production qualification.

## Context

ADR0281 missed the in-hour target by 218 tickets, with 2,172 undispatched journeys and 156 observer SQL cancellations. A read-only EXPLAIN ANALYZE against its restored 300,228-order fixture took 3.73 seconds while idle: the current observer scanned 2,966,625 payment attempts and 2,944,563 tickets, including unrelated historical runs. Its three-second deadline can therefore fail without application load. Failed full-cohort sampling currently retries at every tick, blocking fresh API, worker and failure evidence. The CPU collector also rejects hourly mode for the already accepted CCE worker placement.

## Decision

Compare an observer-only query that performs parameterized lookups through the existing payment order and ticket booking indexes against the unchanged query. Keep every count, cohort predicate and result column. Use explicit lateral subqueries with OFFSET 0 to prevent flattening into global historical scans; no application schema/index or transaction changes. The initial full-cohort indexed query still took 3.34 seconds and is insufficient. Divide the admitted cohort deterministically into eight disjoint slices, prime every slice before the first accepted observation, then update one slice per second in round-robin order. Aggregate all seven unchanged cohort counters; read global outbox/lock counts on every sample. Keep the existing diagnostic connection and three-second statement deadline; no additional connections. Require every slice age to remain within the existing ten-second freshness bound. Publish the oldest slice age and explicit slice count/completeness. These are bounded-age observations from different snapshots, not a single instantaneous full-cohort snapshot; unchanged terminal financial audits establish consistency independently. Accept only after exact terminal equality and measured per-slice cost. A failed slice retains its error until repaired and is attempted again only on the next round, not each tick. Never publish stale/incomplete cohort counts as fresh or qualify incomplete evidence.

Short comparisons use the same indexed query while retaining one-second full-cohort observation; only hourly collection uses disjoint slices. Correct hourly CPU collection and comparison to accept the validated ADR0259 worker-isolation placement, while preserving exact offered windows, identities, counter-reset checks and strict coverage gates. If the targeted query cannot meet the observation budget, retain failure and reconsider the execution pattern before another load experiment.

## Alternatives

- Increase SQL timeout or weaken coverage: masks excessive observation work and missing evidence.
- Add a separate cohort connection/thread immediately: increases observation connection demand and complicates shutdown before confirming the simpler correction.
- Drop counts or infer tickets from order state: weakens independent evidence.
- Change application resources or indexes now: confounds the measured baseline.

## Consequences

The same application image, workload, physical connection/admission budgets, gateway settings and quality gates remain. Observer execution changes are disclosed and sealed; historical sources/results remain immutable. Removing observer work is not yet evidence of improved customer capacity. Full-cohort cost still grows with this run's orders, so a passing short test alone cannot qualify the hour.

## Failure and recovery behavior

Retain every SQL cancellation and incomplete diagnostic gate. Failed refreshes expose their error and age; no hidden retry or synthesized counts. Use fresh experiment identities, stop increasing load when a gate fails, and perform the unchanged financial, double-booking, queue-drain and restoration checks. Mandatory cleanup may exceed an experiment deadline and must be recorded.

## Supersession

Supersedes ADR0252/ADR0281 full-cohort query planning and refresh scheduling only after validation. Does not supersede their acceptance gates or correctness requirements.

## Validation evidence

The original idle plan used 141,109 shared-hit blocks, zero disk-read blocks, and took 3,710.8 ms execution; global ticket and payment hash joins dominated. Existing unique indexes payment_attempts_order_id_key and tickets_booking_id_key are present. The unsliced indexed candidate took 3.34 seconds; slicing, equality and regression validation remain pending. No causal claim about the application's payment slots, WAL or maximum capacity.


The eight-slice query exactly matched all nine original result columns within one read-only repeatable-read snapshot. Individual slices took 365.8–387.7 ms; total sliced work was 2.999 seconds versus 2.990 seconds for the warmed original, so total database CPU reduction is not established. A subsequent live-loop check exposed skipped alternate refresh ticks and correctly failed the ten-second freshness gate at 10.19 seconds. Schedule from query start with bounded clock tolerance; retain that failure and add an aligned-clock regression. The corrected actual sampling function executed twenty read-only observations: maximum refresh 430.0 ms, maximum observation gap 1.368 seconds, oldest slice 7.366 seconds, zero database errors, one data connection, three-second statement deadline, all 300,228 existing tickets counted. No paid traffic was dispatched by these probes. Executed 224 affected regressions passed, one existing platform-dependent test skipped; Ruff passed. Historical observer sources remain immutable. The next experiment repeats 84 offered journeys/s for five minutes with the same application image and budgets before considering another hour.


Paid comparison adr0151-0c14342140e2 passed all ten original native gates and restored the runtime. Exactly 25,200 scheduled/dispatched/confirmed/terminal unique paid tickets, zero drops, initial errors, retries or final customer failures. Payment loss/double bookings zero; queues drained; namespace/helpers/credentials removed. Worst-shard journey p95 was 2,582.30 ms versus 4,661.10 ms in ADR0280 (44.60% lower in this one comparison); API CPU 1.175 cores, worker CPU 1.264 cores, primary host CPU 39.91%. All CPU, slot and database-wait captures completed, zero observer SQL errors. 25,095 tickets issued inside the 300-second offered window and 105 afterward; two fewer in-window tickets than the reference, so increased ticket capacity is not established. The existing separate short issuance flag remains false under its conservative two-second boundary guard; no threshold was weakened. Fresh-cohort cache/checkpoint and Kafka partition variation prevent attributing the entire latency difference to the observer correction. The failed ADR0281 hour remains failed; a new hour is required.

[Read-only validation](../capacity/cce/hourly-observer-cost-validation-2026-10-12.json) and [paid comparison](../capacity/cce/observer-cost-paid-result-2026-10-12.json) preserve the evidence and original gates.


The fresh one-hour run adr0151-64ea9a9f34d2 scheduled, dispatched and completed all 302,400 journeys, with zero drops or final failures. Thirty-two initial payment 503 journeys (0.01058%) recovered with 32 retries; worst-shard total journey p95 was 2,313.68 ms. PostgreSQL counted 302,298 unique paid tickets inside the exact 3,600-second window (83.9717/s), 302,078 in the conservative guarded window, and 102 afterward. The application image, four API pods, fourteen workers and 24 physical DB connections were unchanged; no resources were added. API process CPU was 1.213 cores, workers 1.366 cores, and primary ECS CPU 38.37%. Paid-unfulfilled backlog peaked at 72 (p95 14); both Kafka lanes drained to zero.

The observer correction produced zero SQL errors and maximum cohort age of 8.035 seconds. API and worker CPU captures and all 38 failure-time slot records were complete. The unchanged DB diagnostic gate rejected one 2.012074-second sampling gap against its two-second bound, without counter reset or decrease. The canonical terminal financial statement timed out, so native aggregate gates were not produced and the original report remains failed. Retained slot records frequently show payment commit holders; WALWrite wait snapshots reached 24, but unavailable WAL timing and snapshot correlation do not establish the cause of individual payment timeouts.

Applied the existing ADR0282 independent recovery audit without changing any financial predicate or application state: all 302,400 payments, bookings and tickets matched, every relationship counter was zero, all holds were past TTL, both safety fixtures and global duplicate checks passed, all queues were empty, exact normal runtime was stable, and namespace, helpers and private inputs were removed. Recovery took 177.5 seconds; total experiment including independent verification was 78.02 minutes, inside the approved 90-minute envelope. This verifies measured ticket throughput and correctness; it does not rewrite the original failed qualification or establish flash-opening or maximum capacity.

An auxiliary short-prerequisite digest annotation mistakenly hashed the preflight restoration proof after a launch-wrapper variable was reused. The actual passing short receipt was checked before cloud preflight. Original bindings and wrapper were retained; the supplemental hourly report records the correct immutable short-receipt digest separately. The future wrapper captures that digest before preflight, corrected after this run. No in-flight source, workload, gates or canonical image/observer bindings were changed.

[Hourly result](../capacity/cce/observer-cost-hourly-result-2026-10-12.json) and [independent recovery](../capacity/cce/observer-cost-hourly-terminal-recovery-2026-10-12.json) preserve both failures and successful verification. Next: integrate the already tested bounded financial audit and isolate DB-statistics sampling from the broader collection loop before another fully qualified hour. No load increase from this failed original qualification.
