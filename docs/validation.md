# Validation results — 2026-09-05

## Environment

Windows host, Docker Desktop Linux engine, 8 Docker CPUs and 8,308,518,912 bytes RAM.
The machine also runs unrelated containers; these are local development measurements, not
dedicated-host or production capacity results. One API process, reservation admission cap 8,
PostgreSQL 17.6, Redis 7.4.5, Kafka 3.9.1, PgBouncer 1.24.1. Python dependencies are captured
in `requirements.lock` and used by the Docker build.

## Automated tests

**31 passed, zero skipped** in the final full real-service run (6.87 seconds).

- 16 unit/API cases: payment state transitions, invalid reservations, authentication/OpenAPI,
  signed callbacks, immediate overload admission.
- 14 PostgreSQL/Redis integration tests: concurrent connections, 100 attempts across four
  processes, all-or-nothing seat sets, idempotency, late payments, expiry ownership, callback
  duplication, unique constraints, Redis shielding/versioned snapshots, outbox acknowledgement
  loss, consumer transaction rollback, and simultaneous payment/reclamation races.
- One HTTP end-to-end test through the real simulator, Kafka and ticket consumer.

The final test invocation used the built tests image with the current tests directory mounted
read-only. `docker compose run --rm --build tests` rebuilds that suite directly from the repository.
Ruff passes. Two upstream Starlette test-client deprecation warnings remain; no test failures.

## Real Kafka outage drill

`python scripts/recovery_drill.py` stopped only this project's Kafka container, initiated a
simulated payment, and observed PAID with zero tickets while Kafka was unavailable. It restarted
Kafka and observed FULFILLED with exactly one ticket. The drill passed and left Kafka running.

## Hot-seat stress measurements

Every run below used 1,000 requests, 200 maximum concurrent requests, distinct user tokens,
and a fresh seat. Every run produced **exactly one successful reservation**. No transport
errors or unexpected status codes occurred. 503 means explicit admission rejection, not success.

| Configuration | 201 | 409 | 503 | Server p95 ms | Client p95 ms | Client conflict p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| Initial, before API admission cap, one pool | 1 | 999 | 0 | 500.85 | 6392.06 | 6392.06 |
| Admission cap 8, one pool | 1 | 799 | 200 | 65.34 | 9022.74 | 9719.56 |
| Admission cap 8, 20 pools | 1 | 344 | 655 | 101.93 | 3427.34 | 3993.74 |
| Admission cap 8, 200 pools | 1 | 135 | 864 | 83.99 | 1052.62 | 1326.25 |

Server time comes from request instrumentation (`Server-Timing` for the revised runs). It
excludes time before middleware begins. Client time includes the HTTP client and network path;
client setup and the load generator's explicit concurrency gate are outside the timed interval.
Changing connection-pool layout materially changes the measurement. The generator runs in one
process, so these results do not isolate server capacity from generator overhead.

**The reference deck's end-to-end latency targets are not yet met at this stress level.** The
final run's server conflict p95 was also 233.09 ms, above the 100–200 ms target. The admission
cap bounds work and protects ownership, but rejects substantial excess traffic. Do not interpret
84 ms aggregate server p95 as 1,000 successful bookings or as proof of production readiness.

The next performance step is a distributed load generator on a separate host, followed by API
replica/admission tuning against a declared capacity target. Browse latency, payment-initiation
latency and 5–10x connection/lock-wait scaling have not been independently benchmarked here.

## Scope limits

Payment and refund effects are simulated. No real gateway, payment UX, frontend, external
notification provider or HA deployment is included. Docker services remain running locally.

## Update: background optimization validation, 2026-09-07

The optimized container suite passed 46 tests with no skips, including 100 synchronized HTTP contenders with exactly one durable hold. A subsequent refresh-age refinement passed all 12 targeted background integration cases and the two-minute real checkout confirmation. The actual Kafka outage drill passed. See [the complete comparison and validation evidence](capacity/optimized/README.md). Earlier figures above remain historical results for the prior implementation.
