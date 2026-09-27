# ADR 0048: Separate service-SLO diagnostics from capacity certification

Status: Accepted for explicitly labeled diagnostic stages. This narrows the stage-promotion rule in ADR 0040 only for diagnostic SLO experiments; ADR 0040's zero-generator-drop rule remains in force for certified capacity claims.

## Context

The existing Huawei RDS runbook and ADR 0040 require zero unexpected responses, transport errors, and generator drops before promoting to a higher rate. On 20 September 2026, the four-API topology served all dispatched requests at 750 RPS on Cloud SSD and at 800 RPS on Extreme SSD, with exact hold durability, zero overlapping seat intervals, and drained queues. The 30-minute generators nevertheless dropped 2 of 1,350,000 scheduled requests at 750 and 7 of 1,440,000 at 800 because they started more than 50 ms late. These were generator scheduling misses, not observed API failures. A production service availability SLO and an offered-load fidelity gate measure different things.

## Decision

Retain the strict, no-retry, zero-generator-drop gate for any certified sustained-RPS claim. Add a separately labeled diagnostic assessment of a candidate 99.99% scheduled-request delivery objective and service-side availability. Report scheduled, dispatched, responded, generator-dropped, HTTP-error, transport-error, admission-rejected, and durable-hold counts separately. Expected 304 and seat-conflict 409 are not unexpected API errors. Do not treat generator misses as API errors, and do not hide them with retries.

An operator may run a higher-rate diagnostic after a lower stage misses only the generator-drop gate, provided the lower stage met latency, admission, durability, overlap, queue, rollback, and unexpected API/transport-error gates; the generator accounted for every scheduled request; and the miss fraction is below 0.01%. The higher stage must use the same preflight, bounded admission, no-retry workload, post-TTL audit, and rollback. It cannot establish a certified capacity point if any strict gate fails. Stop escalation on any correctness failure, unexpected service/transport error, latency breach, unaccounted load, failed cleanup, or backlog growth. This is not a production SLA commitment.

## Alternatives considered

- Relax ADR 0040's certified-capacity gate to 99.99%: rejected because generator drops prevent a precise offered-RPS claim.
- Retry missed requests inside the primary load: rejected because it changes arrival timing and masks overload.
- Never run a higher diagnostic after a minor generator scheduling miss: rejected because it prevents distinguishing generator limits from service limits despite complete accounting and intact safety gates.

## Consequences

Reports must present two verdicts: strict capacity certification and candidate SLO diagnostic. A diagnostic pass is evidence about the observed workload only, not a production capacity guarantee or a monthly availability SLO. No reservation, payment, Kafka, or persistence behavior changes. The existing 95% conditional-read/5% unique-hold fixture still cannot be extrapolated to an all-hold workload.

## Failure and recovery behavior

The unattended stage retains its fail-closed strict result and guaranteed rollback. A diagnostic review occurs only after full evidence collection. On any non-generator safety-gate failure, do not launch the next rate. If the generator drops requests, preserve worker timing examples and count them as offered-load misses. Failed audits or rollback require restoring services before any further traffic. Never issue a duplicate hold with a new idempotency key to recover an uncertain outcome.

## Validation evidence

The 20 September 2026 local stage results are `tmp/capacity/cloudssd-return-750-confirm-20260920/stage-result.json` and `tmp/capacity/image-matched-pool500-800-confirm-20260920/stage-result.json`; both failed strict generator gates while passing recorded service and correctness gates. This ADR authorizes a prospective Cloud SSD diagnostic; its outcome must be recorded separately and must not be marked as a passed certified stage unless every original gate passes.

Retrospective candidate-SLO evaluation on 21 September 2026 is retained privately at `tmp/capacity/slo-evaluation-20260921.json`. It passes the 99.99% scheduled-delivery diagnostic for the measured Cloud SSD 750 RPS and Extreme SSD 800 RPS stages, while both remain strict-capacity failures. No new Cloud SSD 800 RPS stage was executed because SSH authentication to the ECSs failed before deployment or load start.

The prospective Cloud SSD diagnostic completed on 21 September 2026. The 800 RPS ten-minute stage passed every strict gate. The 30-minute stage scheduled 1,440,000 requests, dropped eight late on the generator, dispatched and received 1,439,992, and passed the candidate 99.99% delivery diagnostic plus all recorded latency, API/transport-error, admission, durability, overlap, queue, and rollback gates. Its strict-capacity verdict remains fail. Private evidence is retained at `tmp/capacity/cloudssd-slo-800-20260921-report.md`.
