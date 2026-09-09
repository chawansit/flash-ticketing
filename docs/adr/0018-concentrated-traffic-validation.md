# ADR 0018: Concentrated traffic validation

Status: Accepted; measured validation recorded below.

## Context
400 HTTP RPS passed a 30-minute uniform 95/5 read/hold test. This does not establish concentrated read or seat-contention capacity. One 300-seat show cannot supply 20 new holds per second for a 120-second hold lifetime.

## Decision
Retain the same backend, 800 shows, private network and limits from ADR 0014. Concentrate 90% of reads on one shared show; keep holds uniformly distributed with disjoint worker seat allocations. Run 200 then 400 RPS for five minutes, stopping escalation on a failed gate. Separately run synchronized waves of 100 and 1,000 distinct viewers on one fresh seat per wave, without retries. Report exactly-one HTTP winner and durable ownership separately from expected seat conflicts, rate limiting, admission rejection and other failures. A client release barrier does not prove simultaneous arrival at the server.

Run a uniform 100 RPS baseline for 60 seconds, 400 RPS for 120 seconds and 100 RPS recovery for 60 seconds with a continuous arrival schedule. Report each phase as well as total results. Preserve bounded in-flight generation and explicit scheduling drops. Retain 800-show reconciliation throughout. No runtime persistence, messaging, idempotency, TTL or scaling policy changes; no accepted runtime decision is superseded.

## Alternatives
Concentrating all writes would primarily measure sold-out inventory. A one-show fixture would remove background reconciliation. Retries would obscure offered traffic and admission rejection. Starting separate processes per burst phase would introduce bootstrap gaps.

## Consequences
Read skew and write contention have separate conclusions. Existing admission limit eight may reject requests during contention even while zero-double-booking correctness passes. These short scenarios are not production maximum or payment-capacity certification.

## Failure/recovery
Keep failed results; stop rate escalation after a failed stage. Use fresh seats for contention and drain expired holds before reuse. Check acknowledged holds against PostgreSQL. Stop benchmark services, remove the temporary network guard and delete private manifests after capture. Preserve all result evidence without credentials.

## Validation evidence
Generator implemented; seven local generator tests passed. Gates: no unexpected responses/drops, read p95 <=150ms and hold p95 <=300ms for throughput phases; exactly one acknowledged and persisted hold/order per contention wave, with all rejection categories reported independently.


Executed cloud evidence: hot reads passed 200 and 400 RPS for five minutes each, zero errors/drops; burst passed all three phases. The 100/1,000 contender waves each had exactly one HTTP and durable winner, but had 10/42 admission rejections and failed-response p95 317/3,636ms respectively. Correctness passed; contention availability/performance did not. All 12,002 acknowledgements matched durable records and expired; queues drained to zero. The initial observer was corrected during the first stage, so its direct-DB coverage is partial. See [results, limitations and raw evidence](../capacity/concentrated-traffic/README.md). Benchmark services were stopped and temporary credentials/network guard removed. No application optimization or production maximum is claimed.
