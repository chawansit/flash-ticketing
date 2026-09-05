# Load-test conditions and results

Test date: **2026-09-05**. This report documents existing measurements; no new load test was run for this report.

**Outcome:** each of four hot-seat runs produced exactly one successful hold from 1,000 attempts. The end-to-end latency target was not met at 200 concurrent requests. Overload rejection is reported separately from seat conflicts and successful reservations.

## Environment and application conditions

| Condition | Value |
|---|---|
| Host | Windows; Docker Desktop Linux engine |
| Docker resources | 8 CPUs; 8,308,518,912 bytes RAM (about 7.74 GiB) |
| Isolation | Shared development machine with unrelated containers running |
| Network | Load generator and API in the same Compose network; HTTP, not an external internet path |
| API | One FastAPI/Uvicorn process |
| Database | PostgreSQL 17.6; primary is the ownership authority |
| Database pooling | PgBouncer 1.24.1 transaction pooling; 40 default backend connections, no reserve pool; application pool maximum 12 per process |
| Database limits | Maximum 100 PostgreSQL connections; application pool acquisition timeout 150 ms; lock timeout 75 ms; statement timeout 1,500 ms |
| Seat locking | Ordered PostgreSQL NOWAIT row locks; unique final booking constraint |
| Redis | 7.4.5; per-seat contention shield with a 2,000 ms lease |
| Admission | Initial run: no application admission cap. Revised runs: at most 8 reservation requests per API process; excess requests immediately return 503 |
| Hold lifetime | 120 seconds |
| Kafka | 3.9.1; running with publisher, consumer, maintenance and payment-simulator workers |
| Dependency versions | [requirements.lock](../requirements.lock) |
| Configuration | [compose.yaml](../compose.yaml), [application settings](../src/ticketing/config.py) |

These are configured resource limits, not measured peak connection counts or lock waits. No dedicated-host capacity claim is made.

## Workload

- Endpoint: `POST /v1/holds`.
- One event and one initially available seat per run; all attempts in that run targeted the same seat.
- 1,000 total attempts with a maximum of 200 concurrent requests. This is not 1,000 simultaneous requests.
- A distinct user token and idempotency key for every attempt.
- One Python/asyncio load-generator process. The number of independent HTTP client connection pools varied.
- The client concurrency semaphore bounds outstanding calls. This is a closed-loop batch, not a specified requests-per-second arrival rate.
- The hold request creates a pending order. The load script does **not** initiate payments or confirm 1,000 bookings.
- No automatic application-level retries. HTTP timeout configured to 10 seconds.
- The per-user rate limit is 20 requests/second; distinct users each make one attempt, so this does not exercise repeated-user rate limiting.
- Each configuration was measured once. There was no dedicated warm-up phase, repeated statistical sampling or confidence interval.

## Recorded results

All timings are milliseconds, rounded to two decimal places. CSV: [load-test-results.csv](load-test-results.csv).

| Run | Seat | Client pools | Admission cap | Holds: 201 | Conflicts: 409 | Overload: 503 | Server p95 | Client p95 | Client conflict p95 |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|
| R1 | A002 | 1 | None | 1 | 999 | 0 | 500.85 | 6392.06 | 6392.06 |
| R2 | A003 | 1 | 8 | 1 | 799 | 200 | 65.34 | 9022.74 | 9719.56 |
| R3 | A004 | 20 | 8 | 1 | 344 | 655 | 101.93 | 3427.34 | 3993.74 |
| R4 | A005 | 200 | 8 | 1 | 135 | 864 | 83.99 | 1052.62 | 1326.25 |

| Run | Server conflict p95 | Overload rejection rate | Transport errors | Unexpected HTTP responses |
|---|---:|---:|---:|---:|
| R1 | Not separately recorded | 0.0% | 0 | 0 |
| R2 | 75.94 | 20.0% | 0 | 0 |
| R3 | 137.35 | 65.5% | 0 | 0 |
| R4 | 233.09 | 86.4% | 0 | 0 |

No 429 responses occurred. The script accepts 201, 409, 429 and 503 as expected response classes and asserts that exactly one request returned 201. Those assertions passed in all four runs.

**Interpretation:** admission capped work inside the API and reduced aggregate server time, while rejecting a substantial share of attempts. The final run admitted one hold, returned 135 conflicts and rejected 864 attempts. Its 83.99 ms aggregate server p95 is dominated partly by quick overload responses; the conflict-only server p95 was 233.09 ms.

Changing the load client's pooling also changed observed latency and rejection distribution. These evolving runs are not a controlled comparison isolating one variable. The remaining difference between client and server timing has not been fully attributed.

## Measurement definitions

- **Client latency:** measured immediately before `client.post()` until it returns. Includes HTTP-client scheduling/pooling, network time and server work. Excludes client construction, token generation and waiting at the explicit concurrency semaphore.
- **Server latency:** application middleware timing, excluding time before middleware starts. R1 used structured request logs; later runs used the `Server-Timing` response header.
- **Conflict p95:** only responses with status 409. It excludes 503 overload rejections and the successful request.
- **Percentile implementation:** sorted sample at zero-based index `min(n - 1, int(n * 0.95))`, as implemented in the [load generator](../scripts/load_test.py); no interpolation.
- R1 server p95 was extracted from 1,001 hold log entries, including one earlier hold. It is an approximate baseline for the 1,000-request run, not an exactly isolated server sample.
- The historical values were transcribed from observed command output and the existing [validation report](validation.md). Full per-request samples, exact durations, timestamps and container-image digests were not archived. CSV values are a summary, not raw telemetry.
- These runs preceded the initial Git commit. The final implementation was first published as `46f4290`; there are no separate historical commits identifying every intermediate benchmark configuration.

## Commands used

The first two commands ran earlier revisions of the generator. R1 also ran the API before its application admission cap was added. Running the same commands on current code does not recreate those historical versions.

### R1 — initial implementation

```sh
docker compose run --rm --no-deps tests python scripts/load_test.py --attempts 1000 --concurrency 200 --seat A002
```

### R2 — admission cap, one client pool

```sh
docker compose run --rm --no-deps --volume './scripts:/app/scripts:ro' tests python scripts/load_test.py --attempts 1000 --concurrency 200 --seat A003
```

### R3 — 20 independent client pools

```sh
docker compose run --rm --no-deps --volume './scripts:/app/scripts:ro' tests python scripts/load_test.py --attempts 1000 --concurrency 200 --client-pools 20 --seat A004
```

### R4 — 200 independent client pools

```sh
docker compose run --rm --no-deps --volume './scripts:/app/scripts:ro' tests python scripts/load_test.py --attempts 1000 --concurrency 200 --client-pools 200 --seat A005
```

## Re-run the current implementation

```sh
docker compose up -d --build
docker compose run --rm seed
docker compose run --rm --build tests
# Choose an AVAILABLE seat from the seat-map endpoint; A006 is only an example.
docker compose run --rm tests python scripts/load_test.py --attempts 1000 --concurrency 200 --client-pools 200 --seat A006
```

Check availability through `GET /v1/events/00000000-0000-0000-0000-000000000001/seats` before the run. Each test leaves a real expiring hold. The batch must finish before its hold can expire and be legitimately reacquired. Use a fresh seat for each comparison. Record the commit, resource allocation, environment variables, client-pool setting, elapsed duration and output when collecting new results.

## Acceptance assessment

| Criterion | Evidence / status |
|---|---|
| One winner under hot-seat contention | Passed in all four runs: one successful hold each |
| Zero double-booking | No duplicates observed; unique database constraint plus integration tests. Final read-only query found zero duplicated booked seats |
| Reservation client p95 below 200–300 ms | **Not met**; final aggregate client p95 1,052.62 ms |
| Failed-seat response below 100–200 ms | **Not met**; final client conflict p95 1,326.25 ms and server conflict p95 233.09 ms |
| Browse p95 below 100–150 ms | Not independently measured |
| Transaction time below 100–200 ms | Not independently reported |
| Lock-wait p95 below 300–500 ms | Not independently reported; configured timeout is not a measurement |
| Payment initialization below 500 ms | Not independently measured |
| Connections/lock waits remain bounded under 5–10x traffic | Not independently measured |
| Production throughput / HA readiness | Not established by these shared-host runs |

## Separate correctness and recovery checks

These are supporting checks, not additional hot-seat load results:

- **31 tests passed, zero skipped** in the final full suite, including four-process contention, payment/reclamation races, concurrent duplicate callbacks, atomic multi-seat holds, idempotency, refund behavior and consumer recovery.
- **Kafka outage drill passed:** payment reached PAID with zero tickets while Kafka was stopped; after restart, it reached FULFILLED with exactly one ticket.
- Payment and refund effects were simulated. No real payment gateway or external notification provider was involved.

Next: use a distributed generator on a separate host, repeat runs, collect per-request samples and database/pool metrics, then tune replica and admission settings against an explicit capacity target.
