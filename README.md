# Flash-sale ticketing MVP

FastAPI backend for assigned-seat flash sales. PostgreSQL is the seat-ownership authority;
Redis protects the reservation path and serves pre-warmed availability; Kafka delivers
post-payment work. No frontend and no waiting room.

## Start locally

Requires Docker Desktop with Linux containers (or Docker Engine + Compose), and free local
ports 8000, 5432, 6379 and 9090. The stack is for local development, not a highly available deployment.

```sh
docker compose up -d --build
docker compose run --rm seed
docker compose exec api python -m ticketing.cli token --subject alice
```

Open [Swagger UI](http://localhost:8000/docs), paste the token into **Authorize**, and use:

1. `POST /v1/holds`, header `Idempotency-Key: alice-hold-1`:
   `{"event_id":"00000000-0000-0000-0000-000000000001","seat_ids":["A001"]}`.
   This atomically creates the hold **and pending order** and returns both IDs.
2. Optional `POST /v1/orders`, with the hold ID and a new idempotency key. It returns the
   existing order; it never creates another order for the hold.
3. `POST /v1/orders/{order_id}/payments`, another idempotency key:
   `{"outcome":"SUCCEEDED","delay_seconds":1,"duplicates":3}`.
4. `GET /v1/orders/{order_id}` until `FULFILLED`. One ticket is issued per booked seat.

The simulator intentionally delivers the same signed callback repeatedly. Different callback
IDs for the same payment are also safe. Set `delay_seconds` longer than `HOLD_SECONDS` to
exercise refund processing. `FAILED` simulates a declined payment. A failed attempt is terminal
for this MVP's checkout; start a fresh reservation to pay again.

Useful endpoints: [OpenAPI JSON](http://localhost:8000/openapi.json),
[metrics](http://localhost:8000/metrics), [Prometheus](http://localhost:9090),
`/health/live`, `/health/ready`.

## Load-test results

See the [load-test conditions and results](docs/load-test-report.md) and [CSV summary](docs/load-test-results.csv). The report includes all four measured configurations, reproduction commands, and unmet latency targets.

## Tests

```sh
docker compose run --rm seed
docker compose run --rm --build tests
docker compose run --rm tests python scripts/load_test.py --attempts 1000 --concurrency 200 --client-pools 200 --seat A002
```

Run the load test against an available seat; it leaves a real expiring hold. HTTP 409 is normal
contention; 429 and 503 are bounded admission/overload rejection. Transport errors and unexpected
HTTP responses fail the test. A single-host load result is not a production throughput guarantee.

For Python-only development (Python 3.12+):

```sh
python -m venv .venv
# Activate .venv using the command appropriate to your shell.
python -m pip install -e '.[test]'
python -m pytest -q
python -m ruff check src tests scripts
```

Integration tests skip unless `TEST_DATABASE_URL` is provided; cache tests also need
`TEST_REDIS_URL`. Integration fixtures create isolated random schemas and remove only those
schemas. E2E tests need `E2E_API_URL`, a matching JWT secret, the seed event and running workers.
E2E tests consume one seat per run. Compose supplies these settings.

## Architecture

- [Application flow](docs/application-flow.md): reservation, payment, expiry and failure paths.
- [Technology stack](docs/tech-stack.md): versions, responsibilities and runtime limits.
- [System diagrams](docs/system-diagrams.md): runtime topology and payment event sequence.

```text
src/ticketing/
  domain.py                 Pure payment decisions and business failures
  application/              Use cases and atomic persistence/cache ports
  infrastructure/           PostgreSQL reservation adapter, pools, Redis Lua scripts
  api.py                    HTTP schemas, authentication, webhook signature validation
  workers.py                Outbox publisher, Kafka consumer, expiry/cache, simulator
  cli.py                    Checksummed migrations, demo seed, token, dead-letter replay
  observability.py          JSON logging and Prometheus metrics
migrations/                 Transactional, checksummed SQL migrations
tests/                      Unit, real-service integration, HTTP/Kafka end-to-end tests
scripts/                    Contention load generator
docs/                       Design, failure behavior, operational runbook
```

The application depends on ports. SQL and transaction details remain in the PostgreSQL
adapter. Every seat mutation goes through that one adapter, including expiry and callbacks.
Workers are separate processes, not separate seat-owning microservices.

See [architecture](docs/architecture.md), [failure scenarios](docs/failure-modes.md),
[runbook](docs/runbook.md), and [acceptance criteria](docs/acceptance.md).

## Deliberate scope and deployment limits

- Simulation only: no real money, payment token storage, email provider, promotion engine,
  cancellations, resale, general-admission inventory or customer account management.
- Local HS256 tokens stand in for an identity provider. Production startup rejects the default
  secrets and disables token issuance and simulated payment initiation.
- PostgreSQL, Redis and Kafka are single-node local services. TLS, external secret management,
  backups, replicated Kafka, edge bot filtering, autoscaling and HA are deployment work.
- Reservation conflicts fail fast; clients must not blindly retry. Idempotent operations may
  return a transient busy response while the original transaction commits; retry the **same** key
  after that request completes to obtain its original result.
- Cache snapshots are advisory. `reserved_until` lets a future client display the hold deadline;
  a cached HELD seat may already be reclaimable. PostgreSQL alone decides reservation success.
- A real payment adapter must use provider idempotency and reconciliation; the simulated refund
  is a durable database state transition, not an external banking operation.

Stop with `docker compose down`. Named volumes are retained. Do not remove volumes if you need
to retain bookings or Kafka history.

Architecture decisions: [ADR index](docs/adr/README.md).
