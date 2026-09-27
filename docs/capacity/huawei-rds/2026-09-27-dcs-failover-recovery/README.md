# Huawei DCS switchover recovery drill: writer recovery passed, activation gate failed

## Result

The bounded reservation-writer recovery implemented by ADR 0060 removed the 129-second persistence stall seen in the first managed-DCS drill. During this repeat drill, the DCS endpoint produced one failed 250 ms probe and a 500.153 ms gap between successful probes. All 45 hold attempts were subsequently present as durable PostgreSQL reservation commands, all holds expired cleanly, no booking intervals overlapped and every observed queue drained to zero.

Redis-first production activation remains **blocked** because the client contract was ambiguous for two holds. The API returned HTTP 503 for those requests, but both commands were accepted by Redis and later committed to PostgreSQL. The strict audit therefore found 45 durable commands for only 43 HTTP 202 provisional acknowledgements. A client that did not resolve the original idempotency key could incorrectly treat a reserved seat as unreserved.

The drill intentionally used only 5 RPS for three minutes. Its purpose was failover correctness, not capacity certification.

| Gate | Result |
|---|---:|
| Scheduled requests delivered | 900 / 900 |
| Generator drops / transport errors | 0 / 0 |
| Hold responses | 43 HTTP 202, 2 HTTP 503 |
| Read responses | 850 successful/cache responses, 5 HTTP 503 |
| Durable PostgreSQL reservation commands | 45 |
| HTTP 202 acknowledgements missing durability | 0 |
| Durable commands with an HTTP 503 response | **2** |
| Booking overlap | 0 |
| Active holds / pending orders after TTL | 0 / 0 |
| Final reservation stream entries / pending | 0 / 0 |
| Final refresh / outbox / dead-letter queues | 0 / 0 / 0 |
| DCS probe failures / samples | 1 / 1,440 |
| DCS observed successful-probe gap | 500.153 ms |
| Admission-limit rejections | 0 |
| Rollback to PostgreSQL mode | passed |

Worst-worker read and hold p95 were 8.708 ms and 105.332 ms. The hold percentile includes the failover-time 503 responses. The seven final service errors represent 0.778% of all requests during this deliberately faulted, low-volume window: two hold `ADMISSION_UNAVAILABLE` responses, four `SEATMAP_UNAVAILABLE` responses and one `SEATMAP_WARMING` response.

## Interpretation

ADR 0060 achieved its narrow objective: the writers recovered within the hold lifetime and persisted every accepted Redis command. The remaining failure is at the request outcome boundary. `EVAL` can succeed and the following replica acknowledgement or response path can fail during a switchover. The server cannot safely infer from that connection failure whether the command exists, so a terminal-looking 503 is insufficient unless the client resolves the same idempotency key.

The next change must define an explicit ambiguous-outcome recovery contract before another high-rate run. The expected design is a bounded idempotency-status lookup or a retry of the same request and idempotency key, returning the already-created result when it exists. That is an idempotency/API contract change and requires a new ADR before implementation.

## Evidence

This directory contains compact, credential-safe evidence for run `20260927T143129Z-dcs-low`: the load summary, deployment topology, API error classifications, DCS probe summary, background-pipeline summaries, admission result and exact post-TTL durability audit. Raw probe and host telemetry remain outside version control.
