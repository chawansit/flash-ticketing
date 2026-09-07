# Technology stack

Versions below come from the repository's [dependency lock](../requirements.lock), [Dockerfile](../Dockerfile) and [Compose configuration](../compose.yaml). This documents the current local MVP.

## Backend and persistence

| Technology | Version / configuration | Responsibility |
|---|---|---|
| Python | Docker runtime 3.12-slim; project requires 3.12+ | API, business rules, workers and test tools |
| FastAPI | 0.141.1 | HTTP endpoints, dependency injection and OpenAPI |
| Uvicorn | 0.52.4 | ASGI server; one process in the default Compose deployment |
| Pydantic | 2.13.5 | Request validation and API models |
| PostgreSQL | 17.6 | Authoritative seat ownership, orders, payment state, bookings, tickets and reliable event records |
| psycopg / psycopg-binary | 3.3.5 | Parameterized SQL and explicit transactions |
| psycopg-pool | 3.3.1 | Bounded per-process connection pools |
| PgBouncer | Container tag v1.24.1-p1 | Transaction pooling in front of PostgreSQL |
| Redis | Server 7.4.5-alpine; Python client 6.4.0 | Seat-map cache, per-seat contention shield and per-user rate limiting |
| Apache Kafka | 3.9.1; single broker/controller in KRaft mode | Asynchronous event delivery |
| kafka-python | 2.3.2 | Outbox publisher and consumer |
| PyJWT | 2.13.0 | Local HS256 bearer-token verification |
| Python hmac/hashlib | Standard library | HMAC-SHA256 payment webhook signatures and request hashes |

The persistence adapter uses explicit SQL rather than an ORM. Redis leases optimize admission; PostgreSQL locks and constraints enforce booking correctness. Kafka delivery is at least once, with idempotent database effects.

## Architecture and code organization

| Layer | Location | Dependency rule |
|---|---|---|
| Domain | [domain.py](../src/ticketing/domain.py) | Pure payment decisions and business failures |
| Application | [application](../src/ticketing/application) | Use cases depend on persistence/cache ports and domain rules |
| Infrastructure | [infrastructure](../src/ticketing/infrastructure) | Implements ports using PostgreSQL and Redis |
| HTTP interface / composition | [api.py](../src/ticketing/api.py) | Validates/authenticates requests and wires concrete adapters |
| Background processing | [workers.py](../src/ticketing/workers.py) | Runs publisher, consumer, maintenance and simulator roles |
| Administrative CLI | [cli.py](../src/ticketing/cli.py) | Checksummed migrations, development seed/token and dead-letter replay |

API and worker containers use the same Python package. Seat writes share the PostgresReservations adapter; there is no separately deployed reservation microservice.

## Reliability mechanisms

| Mechanism | Implementation |
|---|---|
| Atomic multi-seat holds | Ordered NOWAIT locks, post-lock database-time check and one transaction |
| Final booking protection | Unique event/seat constraint in bookings |
| Idempotent operations | Actor/operation/key uniqueness plus canonical request hash and stored result |
| Callback deduplication | Callback ID/hash and terminal successful payment state |
| Expiry safety | Logical deadlines; ownership-checked cleanup |
| Transactional outbox | Business changes and event records share a PostgreSQL commit |
| Consumer inbox | Consumer/event uniqueness committed with fulfillment effects |
| Dispatch recovery | Reclaimable leases for publisher and simulator |
| Poison-event handling | Five processing attempts, durable PostgreSQL dead letter and manual replay |
| Cache convergence | Durable coalesced refresh generations, version-checked Redis updates and bounded scheduled reconciliation |

Schema: [001_initial.sql](../migrations/001_initial.sql).

## Development, tests and operations

| Technology | Version / configuration | Responsibility |
|---|---|---|
| Docker Compose | Host-installed | Local service network, health dependencies and named database/Kafka volumes |
| Prometheus | v3.5.0 image | Scrapes API and four worker processes every 5 seconds |
| prometheus-client | 0.26.0 | Request/transaction latency, outcomes, database/worker errors and outbox age |
| Python logging | JSON formatter | Structured application and worker logs |
| pytest | 8.4.2 | Unit, real-service integration and end-to-end tests |
| HTTPX | 0.28.1 | HTTP tests and asyncio load generator |
| Ruff | 0.16.6 | Lint and formatting checks |
| GitHub Actions | [ci.yml](../.github/workflows/ci.yml) | Builds Compose services and runs the test suite |
| Mermaid | Rendered by GitHub | Source-controlled application and system diagrams |

## Default runtime limits

| Setting | Default |
|---|---|
| Reservation admission | 8 concurrent requests per API process, then immediate 503 |
| Uvicorn concurrency limit | 256 |
| Hold lifetime | 120 seconds |
| Per-user rate limit | 20 reservation attempts per second |
| Redis contention lease | 2 seconds |
| Application DB pool | Maximum 12 connections per process; acquisition timeout 150 ms |
| PostgreSQL lock timeout | 75 ms, with NOWAIT used on critical seat locks |
| Statement timeout | 1,500 ms |
| PgBouncer | 40 default backend connections, 160 client connections, no reserve pool |
| Outbox publication lease | 30 seconds |
| Simulator dispatch lease | 15 seconds |
| Seat-map cache TTL / reconciliation interval | 30 seconds / 20-second per-event target for events inside their sale window |
| Kafka topic / consumer group | ticketing.events / ticketing-fulfillment-v1 |

These limits are configuration choices, not throughput guarantees. See [load-test conditions and results](load-test-report.md).

## Deployment scope

The implemented runtime is a local development stack with one PostgreSQL server, one Redis server and one Kafka broker. Payment, refund and notification behavior is simulated; notification delivery is a development log entry.

A production deployment would additionally need managed identity/secrets, TLS, edge abuse protection, provider payment reconciliation, backups, highly available data services and measured replica/admission sizing. A product frontend, CDN, external payment provider and read replicas are not part of the current diagram.

Related: [application flow](application-flow.md), [system diagrams](system-diagrams.md), [runbook](runbook.md).

Background defaults added by [ADR 0008](adr/0008-bounded-background-processing.md): PUBLISHER_BATCH_SIZE=32, SIMULATOR_CONCURRENCY=4 (must fit DB_POOL_MAX), REFRESH_COOLDOWN_MS=250. Refresh leases last 30 seconds. Metrics include ticketing_cache_refresh_pending and ticketing_cache_refresh_oldest_seconds.
