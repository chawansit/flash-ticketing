# Bounded seat-map delta validation

## Result

The corrected bounded-delta implementation passed the three-minute Huawei control
at 1,000 offered HTTP requests per second. This is a short control result for the
measured workload, not a maximum production-capacity claim. ADR 0071 remains
proposed until the same topology passes the required 15-minute stage.

The control used commit `39c5b67`, four API replicas, eight generator workers,
800 shows with 300 seats each, Redis-first reservations, three persistence
writers with batch size four, and a 94% read / 6% hold mix. Client retries and the
late-delivery window were disabled.

| Metric | Broken continuity | Corrected continuity |
|---|---:|---:|
| Scheduled requests | 180,000 | 180,000 |
| Physical HTTP attempts | 137,400 | 180,000 |
| Generator drops | 42,600 | 0 |
| Transport errors | 84 | 0 |
| Worst-worker read p95 | 921.095 ms | 14.616 ms |
| Worst-worker hold p95 | 1,283.302 ms | 26.601 ms |
| Read responses | 129,099 | 169,200 |
| Total measured response bodies | 954,518,611 bytes | 34,717,504 bytes |
| Approximate body bytes per read | 7,394 | 205 |
| Audited durable holds | 8,185 | 10,800 |
| Overlapping seat intervals | 0 | 0 |

The corrected run completed every scheduled request without retries. All 10,800
HTTP 202 reservation commands had matching idempotency, hold, order and durable
command records after expiry. Broken links, active/overdue holds, pending orders,
unpublished outbox rows, pending refresh work, dead letters, reservation stream
entries and reservation stream pending counts were all zero.

The observer sampled the new `ticketing_seat_delta_outcomes_total` metric 246
times. It observed both empty and changed-delta outcomes and no reset series.
Because the metrics URL is served through the API load balancer, these samples are
diagnostic rather than an exact aggregate. Generator body accounting is complete:
the response volume fell by 96.36% overall despite the corrected run completing
31,800 more reads; approximate bytes per read fell by 97.23%.

## Root cause and correction

The initial implementation recorded delta history when the PostgreSQL projector
patched Redis. Redis-first provisional holds and compensation releases also
changed the seat-map version directly, but did not append a matching delta entry.
The continuity checker therefore detected a safe history gap and returned a full
300-seat reset snapshot to active viewers. Repeating that fallback saturated the
request path.

The correction appends and trims the bounded delta entry inside the same Redis Lua
transaction that changes provisional seat ownership. It also inherits the
seat-map TTL, so recreated delta histories do not become permanent keys. A
real-Redis regression test covers both `AVAILABLE -> HELD` and compensation back
to `AVAILABLE` without reset.

## Verification

- Focused Redis/PostgreSQL integration: 25 passed.
- Complete unit and integration suite: 254 passed with two dependency deprecation
  warnings.
- Ruff passed for all changed Python files, excluding the documented Windows
  executable-bit mount artifact.
- Final TTL and continuity regression: 1 passed.
- Cloud run ID: `20260928T093221Z-b647bc36`.
- Every unattended runner gate passed, including rollback and cleanup.

The next capacity step is the required 15-minute 1,000 RPS run on the same
topology. A higher offered rate must wait until that sustained stage passes.

