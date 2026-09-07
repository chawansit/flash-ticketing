# System diagrams

These Mermaid diagrams show the implemented local MVP. GitHub renders them directly from Markdown. Component labels describe logical roles; all Python services use the same application package.

## Runtime topology

```mermaid
flowchart TB
    Client["API client, Swagger UI or load generator"]
    subgraph Local["Docker Compose network"]
        API["FastAPI and Uvicorn<br/>Admission, authentication, use cases and reservation adapter"]
        Workers["Four worker processes<br/>Publisher, consumer, maintenance and payment simulator"]
        Pool["PgBouncer<br/>Transaction pooling"]
        DB[("PostgreSQL<br/>Seat ownership, orders, payments, bookings,<br/>outbox, inbox, refresh requests,<br/>tickets and dead letters")]
        Cache[("Redis<br/>Seat maps, contention shield and rate limits")]
        Broker[("Kafka<br/>ticketing.events")]
        Metrics["Prometheus"]
        Migrate["One-shot migration CLI"]
    end
    Client -->|"HTTP :8000"| API
    API -->|"Parameterized SQL"| Pool
    Workers -->|"SQL for leases, fulfillment and cleanup"| Pool
    Pool --> DB
    API -->|"Cache reads and admission"| Cache
    Workers -->|"Refresh seat snapshots"| Cache
    Workers -->|"Publish and consume events"| Broker
    Workers -->|"Simulator signed payment callback"| API
    Migrate -->|"Schema migration"| DB
    Metrics -.->|"Scrape /metrics"| API
    Metrics -.->|"Scrape worker :9101"| Workers
```

### Read and write responsibilities

| Component | Responsibility |
|---|---|
| API | Event browsing, cached seat reads, holds, order lookup, payment initiation and callback handling |
| PostgreSQL | Final authority for seat ownership and payment/order effects |
| Redis | Fast advisory reads and request admission; never grants a confirmed booking |
| PgBouncer | Bounds backend database connections shared by application processes |
| Publisher worker | Leases outbox rows, sends to Kafka and marks publication after acknowledgement |
| Consumer worker | Issues tickets, settles simulated refunds, queues durable cache refreshes and deduplicates effects |
| Maintenance worker | Expires old holds without waiting on busy locks leases coalesced refresh work and periodically rebuilds seat maps |
| Simulator worker | Uses four bounded threads to lease attempts and send signed duplicate HTTP callbacks |
| Prometheus | Scrapes metrics; it does not participate in reservation decisions |
| Migration CLI | Connects directly to PostgreSQL before application startup |

The worker box represents four separate Compose services. They do not all perform every arrowed operation; the table identifies each role. Migrations and isolated integration tests can access PostgreSQL directly; normal application database access goes through PgBouncer.

Event metadata reads use PostgreSQL. Seat-map reads use Redis, including the polling delta endpoint. A cache miss or outage returns 503 rather than causing a seat-map query stampede into PostgreSQL.

## Payment event delivery and recovery

```mermaid
sequenceDiagram
    autonumber
    participant Sim as Payment simulator
    participant API as FastAPI callback
    participant DB as PostgreSQL
    participant Pub as Outbox publisher
    participant K as Kafka
    participant Con as Consumer
    Sim->>API: Signed payment success callback
    API->>DB: Begin and lock order, payment, hold and seats
    API->>DB: Validate ownership and post-lock deadline
    API->>DB: Save booking, SOLD state, PAID order and OrderPaid event
    API->>DB: Commit
    API-->>Sim: Acknowledge callback
    Sim->>API: Repeat callback
    API->>DB: Check callback and successful payment state
    API-->>Sim: Duplicate - no new booking
    Pub->>DB: Lease unpublished outbox row and commit
    Pub->>K: Publish stable event ID, keyed by aggregate ID
    K-->>Pub: Broker acknowledgement
    Pub->>DB: Mark published if lease token still matches
    K->>Con: Deliver OrderPaid
    Con->>DB: Begin and insert inbox entry, tickets and TicketsIssued event
    Con->>DB: Mark order FULFILLED and commit
    Con->>K: Commit consumer offset
    Note over Pub,Con: Crashes can repeat delivery - inbox and unique constraints protect effects
```

This sequence assumes a valid, unexpired hold and a successful payment. SQL connections pass through PgBouncer; it is omitted here for readability.

- A late successful payment writes RefundRequested instead of creating bookings. Its consumer updates the simulated refund and order to REFUNDED.
- SeatsChanged commits a durable refresh generation together with its inbox entry. Maintenance
  coalesces requests, leases a generation, rebuilds Redis, and acknowledges that generation only
  with the matching lease token. Inbox completion is not proof of cache freshness; monitor the
  refresh queue and cache version. See [ADR 0008](adr/0008-bounded-background-processing.md).
- TicketsIssued follows the same outbox path and produces a development notification log.
- A publication crash after broker acknowledgement can repeat the event ID.
- A consumer crash after its database commit can redeliver the event; its inbox record prevents repeated transactional effects.
- After five failed processing attempts, the consumer persists a dead letter before advancing its Kafka offset. If that persistence fails, it seeks back to the message.
- Aggregate partition keys do not imply global ordering or guarantee publication order across multiple publishers. Handlers are designed to tolerate replay and relevant reordering.

## Operational endpoints and storage

| Service | Local endpoint / storage |
|---|---|
| API | 127.0.0.1:8000; /docs, /openapi.json, /health/live, /health/ready, /metrics |
| PostgreSQL | 127.0.0.1:5432; postgres-data named volume |
| Redis | 127.0.0.1:6379; cache is recoverable from PostgreSQL |
| Kafka | Internal kafka:9092; kafka-data named volume |
| Worker metrics | Internal port 9101 in each worker container |
| Prometheus | 127.0.0.1:9090 |

The API's readiness check uses PostgreSQL and Redis. Kafka outages can delay fulfillment while valid payment callbacks still commit safely. Compose waits for Kafka during initial application startup; that startup dependency is distinct from runtime booking correctness.

## Diagram scope and evidence

These diagrams do not claim high availability, a production capacity level or end-to-end exactly-once delivery. The measured stress-test latency limits and rejection rates are documented in the [load-test report](load-test-report.md).

Sources in this repository: [Compose](../compose.yaml), [API](../src/ticketing/api.py), [workers](../src/ticketing/workers.py), [database adapter](../src/ticketing/infrastructure/postgres.py), [reservation transactions](../src/ticketing/infrastructure/reservations.py), and [schema](../migrations/001_initial.sql).

Related: [application flow](application-flow.md), [technology stack](tech-stack.md), [concurrency contract](architecture.md).
