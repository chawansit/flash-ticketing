# ADR 0012: Separate fixture preparation from load generation

Status: Accepted for implementation; external execution pending access.

## Context
Local load generation shares CPU with API/database/workers. Generator drops invalidate
server-capacity claims. An independently hosted generator is needed for larger-show sizing.

## Decision
Keep fixture seeding and database/Redis observation on the application-side machine.
Export an ephemeral development manifest with show IDs and expiring viewer tokens into
ignored tmp/. A portable HTTP-only generator consumes it and needs no database, Redis,
Kafka or signing-secret access. Record generator identity, CPU time, offered/completed
rate, scheduling lag, errors, latency and response bytes. Do not label a Docker container
on the same physical machine as independent capacity evidence. Existing application
ownership, idempotency and cache patterns are unchanged; this extends ADR 0007's method.

## Alternatives
Copy the existing harness with database credentials (unnecessary access and observer
coupling); run another container locally (still resource contention); purchase cloud
infrastructure automatically (not requested and no deployment target supplied).

## Consequences and failure/recovery
Manifest tokens expire and must not be committed. Each invocation validates its origin
against the manifest, uses unique idempotency keys and a reserved seat offset. New runs
need a fresh unused allocation or manifest to avoid measuring intentional seat conflicts.
Failed runs are retained; late arrivals are dropped instead of queued without bound.
External access remains required to verify a separate-generator run. Local larger-show
tests are diagnostics only. No infrastructure services are exposed publicly by this work.

## Validation
Executed 800-show/180-second same-host diagnostic: 50 RPS, read p95 29.76 ms,
450 accepted holds, no drops/unexpected responses. Minimum sampled cache TTL 4 seconds.
HTTP-only same-host smoke: 100 requests, no errors/drops, read p95 23.08 ms.
Ruff formatting/lint and git diff whitespace checks passed. Full application tests were
not rerun for these measurement-only scripts. See [report](../capacity/distributed/README.md).
External execution remains pending a supplied generator host and reachable test API.

## External execution topology (2026-09-08)

The user supplied a Huawei ECS generator. Use an SSH reverse tunnel bound only to ECS
loopback to reach the local API, keeping development API ports private. This avoids
public ingress changes; measured latency includes WAN and SSH overhead and cannot be
used as same-region production service latency. ECS has 4 vCPUs and about 7.4 GiB RAM.
Generator dependencies use an isolated virtual environment; credentials remain temporary
and outside version control. Close the tunnel and remove the remote token manifest after
measurement. Tunnel failure invalidates the run and is reported, not retried invisibly.
