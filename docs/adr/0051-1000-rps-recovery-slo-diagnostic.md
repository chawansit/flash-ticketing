# ADR 0051: Assess 1,000 RPS recovery with a bounded service-error budget

Status: Accepted for a diagnostic stage only; ADRs 0040 and 0048 remain the authority for certified capacity.

## Context

The first 1,000 RPS five-minute stage with two maintenance workers and four generator workers missed 584 scheduled requests. A matched eight-generator-worker stage delivered all 300,000 scheduled requests with no late deliveries or transport errors. Its read/hold worst-worker p95 was 20.976/98.395 ms, 14,990 acknowledged holds were durable, no held-seat intervals overlapped, all queues drained within the 180-second audit, and the maintenance count rolled back. PostgreSQL wait samples included WAL synchronization and write-lock waits. The stage still produced 65 first-attempt `ADMISSION_FULL` responses; bounded same-key retries recovered 55 and exhausted 10. This is a final unexpected-response fraction of 10/300,000 (0.00333%) and fails the existing zero-error recovery and capacity gates. Increasing API admission without evidence could add WAL pressure.

## Decision

Allow a separately labeled 1,000 RPS *recovery-SLO diagnostic* with the same eight generator workers, two maintenance workers, four API replicas, hold admission five per API, PgBouncer budget, 95% conditional-read/5% unique-hold workload, 120-second hold TTL, 180-second post-load audit, and bounded two-attempt same-key retry. A 15-minute stage may follow the five-minute safety stage because the only remaining request gate failure was the measured final `ADMISSION_FULL` fraction below 0.01%; all load-fidelity, latency, durability, overlap, drain, and rollback gates passed. Keep the automation's strict `pass: false` verdict on any unexpected response.

The diagnostic service-availability gate is final unexpected HTTP or transport outcomes below 0.01% of scheduled requests, with zero generator drops, complete request accounting, and separately reported first-attempt and physical-retry counts. No correctness budget is allowed: double-booking, missing acknowledged holds, broken links, failed expiry/drain audit, or failed rollback stop the experiment. If the 15-minute stage passes these diagnostic gates, a fresh 30-minute stage is required for sustained diagnostic evidence. Neither stage certifies 1,000 RPS under ADR 0040; certification still requires the strict no-retry, zero-unexpected-error run.

## Alternatives considered

- Raise per-API admission above five: deferred because sampled RDS sessions already wait on WAL sync/write and a higher permit count may increase contention.
- Add a third HTTP attempt or hide first-attempt failures: rejected for this experiment; either changes the recovery workload or obscures overload.
- Relax the strict capacity gate: rejected; the diagnostic and certified verdicts remain distinct.
- Stop all exploration after a 0.00333% final-error safety result: rejected for the explicitly measured service-SLO diagnostic, provided every correctness and cleanup gate remains intact.

## Consequences

The diagnostic may reveal whether rare overload responses remain bounded over a longer run and whether two maintenance workers keep expiry current. It cannot establish a production SLA or all-hold throughput. Every report must show scheduled, physical, first-attempt failure, recovered, exhausted, and final-error counts, plus both strict and diagnostic verdicts.

## Failure and recovery behavior

Stop before a longer stage if final unexpected outcomes reach 0.01%, any generator drop or transport error is unaccounted, latency breaches its limit, or any correctness, drain, readiness, or rollback gate fails. Restore the original API admission and maintenance replica count even after a failed stage. Preserve failed evidence and do not retry with a new idempotency key.

## Validation evidence

The eight-generator-worker safety result is retained privately at `tmp/capacity/cloudssd-maint2-1000-safety8-20260922`. Its strict verdict is fail, while its measured diagnostic availability and all correctness and cleanup gates pass. The longer-stage outcome must be appended here after execution; the five-minute result alone is not a sustained-capacity claim.

The 15-minute diagnostic at revision `2cad236` scheduled and delivered all 900,000 requests with eight generator workers and no late deliveries or transport errors. Twenty first-attempt `ADMISSION_FULL` responses led to ten recovered and ten exhausted holds; the final unexpected-response fraction was 10/900,000 (0.00111%), below the candidate 0.01% request budget. Worst-worker read/hold p95 were 13.335/78.029 ms. All 44,990 acknowledged holds matched durable records, zero intervals overlapped, no overdue active holds remained, and rollback succeeded. The fixed audit still found 237 pending seat-refresh requests, so the diagnostic **failed** its zero-queue gate; no 30-minute promotion is authorized. A later zero-queue observation is recovery evidence only, not a retroactive pass. Private evidence is retained at `tmp/capacity/cloudssd-maint2-1000-diagnostic15-20260922`.
