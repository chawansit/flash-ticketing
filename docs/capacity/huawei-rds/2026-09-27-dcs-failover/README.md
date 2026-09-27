# Huawei DCS switchover drill: failed durability gate

## Result

The first live managed-DCS master/standby switchover drill **failed the Redis-first activation gate**. PostgreSQL mode was restored after the run and is the approved production default.

The ten-minute stage delivered all 300,000 scheduled requests at 500 RPS with no generator drops or transport errors. Worst-worker read and hold p95 were 13.143 ms and 22.399 ms. A 250 ms independent DCS probe saw one connection error and a 500.154 ms interval between the last success before the switchover and the first success after it.

Application recovery was materially slower. Reservation persistence had a roughly 129-second gap. The API returned 14,612 HTTP 202 provisional acknowledgements, but only 12,632 commands became durable in PostgreSQL. The remaining 1,980 commands reached persistence after their two-minute holds had expired and were compensated. Every one of the eight load partitions had a durability mismatch.

| Gate | Result |
|---|---:|
| Scheduled requests delivered | 300,000 / 300,000 |
| Generator drops / transport errors | 0 / 0 |
| HTTP 202 provisional acknowledgements | 14,612 |
| Durable PostgreSQL commands | 12,632 |
| Missing durable acknowledgements | **1,980** |
| Booking overlap | 0 |
| Final reservation stream entries / pending | 0 / 0 |
| DCS endpoint observed recovery window | 500.154 ms |
| Reservation-writer persistence gap | about 129 s |
| Rollback to PostgreSQL mode | passed |

The 388 hold 503 responses and 10 seat-map 503 responses occurred around the switchover. Hold failures were classified as `ADMISSION_UNAVAILABLE` or `RESERVATION_DURABILITY_UNKNOWN`. These unavailable responses were preferable to false success. The separate durability mismatch is unacceptable because the 1,980 affected requests had already received HTTP 202.

## Diagnosis and next gate

The managed endpoint itself recovered quickly. The reservation writers scanned hundreds of per-event streams serially, so individually short Redis operations composed into a stall longer than the hold TTL. ADR 0060 bounds stream work per poll and requires stale connection-pool reset on Redis errors.

Redis-first reservation intake stays blocked for production until a repeat drill shows exact durability for every HTTP 202, zero double-booking, drained queues and a bounded persistence gap. Raising RPS is not authorized by this result.

## Evidence

The directory retains compact, credential-safe evidence from run `20260927T064519Z-b685d936`: the stage verdict, load summary, deployment and rollback state, DCS probe summary, API error classifications and exact durability audit. Raw host telemetry remains outside version control.
