# ADR 0055: Build and verify measured worker images

Status: Accepted.

## Context

Capacity deployment recorded matching Git revisions on the API and generator hosts, but the backend helper built only the `api` and `migrate` services. It then scaled `maintenance` and `consumer` without rebuilding or force-recreating them. Docker could therefore retain worker images created from an older checkout.

The batch-size-ten and batch-size-two refresh stages at revisions `53dc656` and `b8597aa` changed `src/ticketing/workers.py`, but the deployment evidence verified only `src/ticketing/infrastructure/postgres.py` inside API containers. Those cloud stages cannot validate the batching implementation. Their HTTP, RDS and queue traces remain evidence of the environment at those times, but attribution to refresh batching is invalid.

## Decision

Every capacity deploy builds `api`, `migrate`, `publisher`, `consumer` and `maintenance` from the matched checkout. Force-recreate publisher, candidate maintenance replicas and candidate consumer replicas before traffic. Verify the source hash of the API database instrumentation and verify the source hash of `src/ticketing/workers.py` inside every publisher, maintenance and consumer container.

Fail deployment before fixture preparation or load if any expected replica is missing, unhealthy where health checks exist, or has a source hash different from the checkout. Record worker-source verification in public deployment evidence.

## Alternatives considered

- Trust Git revision alone: rejected because running containers can retain images from another checkout.
- Build all Compose services: rejected because PostgreSQL, Redis, Kafka and PgBouncer use external images, while simulator and reconciler are outside this capacity experiment.
- Verify only image tags: rejected because mutable Compose service tags do not prove source content.
- Restart workers without rebuilding: rejected because it can recreate the same stale image.

## Consequences

Deploy preparation takes longer and briefly restarts asynchronous workers. The fixture preflight still prevents traffic until services and queues are ready. Publisher, consumer and maintenance now share a verifiable source revision with the measured API.

## Failure and recovery behavior

A build, recreation, replica-count or hash failure stops before load. Existing durable outbox, inbox, refresh leases and expiry rows remain in PostgreSQL and are replayed by the restarted workers. Rollback restores replica counts and admission; source rollback requires explicitly deploying the prior Git revision through the same verified process.

## Validation evidence

Unit tests must cover the expanded deployment command and evidence fields. Shell syntax and the full unit suite must pass. Cloud preflight must show the expected replicas and matching hashes before any corrected batching result is accepted.
