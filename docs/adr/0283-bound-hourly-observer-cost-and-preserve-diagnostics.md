# ADR0283: Bound hourly observer cost and preserve diagnostics

## Status

Accepted for the bounded observer correction; offline and read-only cloud validation executed. Paid comparison pending; no capacity qualification.

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
