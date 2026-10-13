# ADR0301: Attribute unchanged paid-stage database cost

## Status

Accepted under the standing work envelope for one fresh bounded 84/s stage.

## Context

ADR0298's committed-status candidate had four recoverable payment503s and zero final failures. Aggregate wait samples cannot identify which operations create WAL and consume database work. ADR0299 exposes already collected query statistics without a restart. ADR0300 compared the base observer SQL and the ADR0247 alternative, but inspection of the live runner reveals that ADR0283 already applies a parameterized LATERAL index rewrite. ADR0300 therefore does not measure a replacement for the actual runtime query and cannot justify its activation.

## Decision

Repeat the existing ADR0298 candidate using the passed same-image control prerequisite. Keep the immutable image, consumer-only flag, workload, pods, total connections, ingress, gateway delays, recovery and all quality gates unchanged. Reserve a fresh single-stage scope: 84 offered journeys/s for 300 seconds, 3,600-second experiment ceiling including recovery. No new runner, permanent deployment, infrastructure or rate increase.

Use the existing runner's dispatch boundary to capture supplementary query-statistics snapshots before generator launch and after customer completion, before financial audits. This is a dispatch-plus-completion span, not exactly the 300-second offered window. Both snapshots use one short-lived verified-TLS diagnostic connection, closed before/after offered traffic; never add a steady-state observer connection or poll pg_stat_statements during offered traffic.

Bind supplementary capture to the actual run and existing reservation digest, record its helper and local execution-wrapper hashes, retain private raw statements, and publish only role-qualified query identities/categories and cost deltas. Include userid, dbid, queryid and toplevel in identity. Require matching reset epochs, no intervening eviction and nondecreasing counters. Detect bound exhaustion and preserve incomplete evidence; never fabricate zeroes for resets or replace a failed measurement with retries.

The extra snapshots must not bypass existing source/deployment/financial gates. Capture failures stop before load or remain visible after load; mandatory audit, queue drain and restoration still run. A performance correction is selected only after examining actual stage costs and failure-time holders.

## Alternatives

Activate ADR0247 from its base-query microbenchmark: rejected because it does not compare the current runtime. Change WAL settings, delete historical records, resize RDS, increase pool size or change multiple factors: defer without attribution. Another whole runner or a new periodic diagnostic connection: rejected.

## Consequences

The snapshots include a short launch lead-in, customer completion grace and normal background work. They exclude fixture preparation and mandatory financial audit costs. Their aggregate deltas cannot prove an individual payment503's exact backend cause or represent server commit/fsync time when unavailable. No correction or capacity improvement is implied.

## Failure and recovery behavior

Require fresh scope, pinned TLS certificate, source/image proof, idle generator before setup and passed/restored control evidence. Stop on pre-dispatch snapshot failure. Keep original failure evidence and all financial/owned-resource recovery. If the second snapshot fails, report attribution incomplete alongside original customer results. Do not change business tables, reset statistics or retry a spent experiment.

## Validation evidence

The query-delta tests passed: 19 tests cover role-qualified identities, actual PostgreSQL JSON OID strings, resets, evictions, truncation, duplicate identities and invalid counters. An earlier broader run passed 46 tests covering the existing comparison and observer contracts plus the initial delta tests; these counts overlap. Ruff passed for the new helper and tests. All 426 entries from an actual read-only catalog snapshot validated after correcting the OID parser. Existing ADR0283 runtime rewrite was verified in scripts/observe_cce_paid_pipeline.py; ADR0300 is preserved with the runtime-comparator limitation explicit.

The first fresh attempt, adr0151-ce7e5ab27bd4, stopped before dispatch because the diagnostic parser expected integer OIDs while catalog JSON returned decimal strings. It started no paid load, completed cleanup/restoration, and remains a spent failed scope. Evidence: [pre-dispatch abort](../capacity/cce/query-attribution-pre-dispatch-abort-2026-10-13.json). The corrected parser completed fresh run adr0151-43fb55d3f651, ledger bounded_cce_paid_comparison__30646dcdf494: PASSED_RESTORED. All 25,200 scheduled journeys dispatched and fulfilled, with zero generator drops, initial errors, retries or final failures. All measurement gates, post-TTL financial audit, zero double-booking, payment durability, full queue drain, resource cleanup, restoration and integrity checks passed.

Within the exact 300-second offered window, 25,097 unique tickets were issued (83.6567/s), with 25,200 complete after drain. Worst-shard journey p95 was 1,779.89ms and payment HTTP p95 104.87ms. The short issuance check for all 25,200 inside the offered window is false; offered 84/s and post-drain completion do not equal 84/s issued inside that window. No hourly qualification or 100/s result is claimed.

Both 426-entry snapshots passed continuity, identity and counter checks over 332.53 seconds from dispatch boundary through completion. The actual runtime cohort observer executed 332 times, accumulated 44.43 seconds of SQL execution and 31.40 million buffer hits (62.57% of recorded hits). Outbox inserts executed 100,800 times and generated 545.45MB WAL. Overall query WAL was 2.987GB; these are measured-span aggregates, not per-request attribution or CPU time. Independent waits observed WALWrite contention despite zero customer errors; no past payment503 backend cause is proven. Evidence: [paid repeat](../capacity/cce/query-attribution-paid-repeat-2026-10-13.json), [query costs](../capacity/cce/paid-stage-query-costs-2026-10-13.json).

The application and monitoring queries were unchanged. Lower latency and fewer errors than the previous candidate do not prove a new correction or permanent stability. Compare the actual ADR0283 runtime query against any proposed correction before adoption. Preserve sampling cadence, exact counters and independent financial verification; keep load unchanged until a justified correction passes comparison.
