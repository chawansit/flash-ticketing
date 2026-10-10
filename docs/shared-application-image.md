# Shared application image and separate services

ADR0258 packages the accepted recovery API and accepted writer/simulator capabilities into one image. The source snapshot is deliberately pinned to those tested inputs; it is not an unqualified rebuild of the current checkout. There is one image build, with a different command per service. Database, Redis, Kafka and PgBouncer continue to use their own infrastructure images.

The selected SWR image is:

```text
swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@sha256:11596e03f629846cb4b1c145b11b45e66eb4e4f9b6703314c67da65df1f02742
```

Its complete source and dependency hashes are in [the build receipt](capacity/cce/shared-application-image-2026-10-10.json) and `/app/shared-image.json` inside the image. An earlier packaging candidate failed the writer compatibility tests and is explicitly excluded in the receipt. Use the selected digest, never a tag or an excluded candidate.

| Service | Container command | Suggested CCE test replicas | Client DB pool per process |
| --- | --- | ---: | ---: |
| API | `uvicorn ticketing.api:app ...` | 4 | 4 (2 general + 2 payment) |
| Reservation writer | `python -m ticketing.workers reservation-writer` | 3 | 12 |
| Outbox publisher | `python -m ticketing.workers publisher` | 1 | 12 |
| Ticket consumer | `python -m ticketing.workers consumer` | 6 | 8 |
| Maintenance: refresh + expiry | `python -m ticketing.workers maintenance` | 1 | 12 |
| Reconciler | `python -m ticketing.workers reconciler` | 1 | 12 |
| Payment simulator, test only | `python -m ticketing.workers simulator` | 1 | 10, with 12 independent HTTP deliveries |
| Optional confirmation worker | `python -m ticketing.workers confirmation` | 0 | 2 |

The CCE preview starts **every deployment at zero replicas**. The suggested layout is 17 active pods, 7.25 vCPU and 14.5 GiB total; worker sizing still needs measurement. Its 146 possible application client connections share the existing PgBouncer physical server budget of 24. API pending acquisition capacity is 20 per process, not 20 extra database connections. No HPA or new external load balancer is included. A full migration is not yet deployed or financially authorized by the consumed four-pod experiment scope.

## Local development

```powershell
docker compose -f compose.yaml -f compose.shared.yaml config --quiet
docker compose -f compose.yaml -f compose.shared.yaml up -d
```

The override removes builds from application services and uses the selected digest. Local PostgreSQL, Redis, Kafka and PgBouncer configuration remains the development configuration; these limits are not the cloud benchmark budgets. Refresh and expiry stay optional; do not run them alongside maintenance. The confirmation profile is optional; activating it alone does not enable asynchronous callbacks in the API. Set `PAYMENT_CONFIRMATION_ASYNC=1` explicitly for the API when using that profile; the default remains off. No gateway simulator belongs in production. The existing tests service is a development test runner, not an application service, and is not replaced by this override.

To rebuild the candidate from its exact public source inputs:

```powershell
python scripts/prepare_shared_application_image.py --build --receipt tmp/shared-image.json
```

The immutable parent must first be available locally. Build context is limited to the verified package and dependency inputs; it excludes workspace credentials and fixture files. New image bytes can have a new manifest digest even with identical source hashes. Verify and publish the resulting digest before rendering new manifests.

## CCE preparation and rollout

Regenerate the inactive preview using:

```powershell
python scripts/cce_shared_application.py --receipt docs/capacity/cce/shared-application-image-2026-10-10.json --output kubernetes/cce-shared-application.preview.json
```

Before applying or activating it, supply a namespace and existing reviewed configuration/credentials separately. The renderer creates no Namespace, Secret or ConfigMap:

- `ticketing-runtime` ConfigMap: copy the complete accepted non-secret workload configuration, including environment, reservation mode, hold TTL, Redis replica acknowledgements, queue limits, Kafka bootstrap and batch/reconciliation limits. Compare its canonical hash to the recorded runtime. Explicit per-role settings in the manifest override this ConfigMap; inspect their differences before migration.
- `ticketing-runtime` Secret: `DATABASE_URL_POOLER`, `REDIS_URL`, `JWT_SECRET`, `WEBHOOK_SECRET`. The database URL must resolve to the same PgBouncer pool and database/user for every role; it must not bypass PgBouncer. Check its server budget and client ceiling before activation.
- `ticketing-rds-ca` Secret: existing CA files, mounted read-only at `/etc/ticketing/rds-ca`. Any certificate path in the connection URL must match this mount; retain the accepted TLS verification mode.
- `swr-pull` Secret: registry access. Verify image pull by digest before shifting traffic; authentication renewal must not change image selection.

Test private network reachability and Kafka **advertised broker addresses**, not just the bootstrap address. Confirm the consumer group/topic partitions and every role's real queue progress. Verify existing generator/Nginx routing to the selected API pods; ClusterIP service creation alone does not establish an ECS-to-CCE traffic route. Worker metrics are not business readiness.

Use a bounded rollout: pause dispatch, record image/configuration/counts, perform payment/durability/queue checks, stop one old ECS role and start its matching CCE deployment. Do not accidentally run old and new replica sets together. Check that role's progress before migrating the next. SIGTERM allows 60 seconds for existing graceful shutdown; durable leases and idempotent replay recover forced termination. On failure, stop the CCE role and restore its recorded ECS image/configuration/count. RDS, DCS, Kafka, PgBouncer and the generator remain in their current locations.

Acceptance after migration still requires successful paid-and-issued tickets, first-attempt and recovered errors, latency, CPU, database waits, zero double-booking/payment loss and complete queue drain. Packaging validation is not a capacity measurement or a new hourly qualification.
