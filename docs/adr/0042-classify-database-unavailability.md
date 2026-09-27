# ADR 0042: Classify database-unavailable responses during capacity stages

Status: Accepted for implementation

## Context

The 2026-09-19 Huawei 750 RPS safety stage returned one unexpected
`DATABASE_UNAVAILABLE` hold response. The API maps pool acquisition timeout,
pool waiting-limit rejection, PostgreSQL operational failure and query
cancellation to the same public 503. Sub-second pool and PgBouncer samples did
not establish which happened. Changing capacity or connection budgets without
identifying the cause would obscure the availability failure.

## Decision

Keep the public 503 contract and existing transaction behavior. At the existing
exception-handler boundary, classify only the four handled exception types into
a fixed, low-cardinality cause. Increment a Prometheus counter and put that
cause in the structured request log. The capacity-stage extractor may retain
the cause alongside its existing bounded error excerpt, but must allowlist the
cause and continue excluding exception messages, SQL, DSNs, request bodies and
authorization headers. The strict stage gate remains zero unexpected errors;
diagnostics cannot turn a failed stage into a pass.

## Alternatives considered

- Infer the cause from sampled pool gauges. Rejected because one failure can
  occur and clear between samples.
- Log the exception text or stack trace for every 503. Rejected because database
  exceptions can contain connection or query details, and raw logs are large.
- Increase pool timeouts or retry 503s. Rejected because the cause is unknown and
  retrying would alter the measured workload.

## Consequences

There are four fixed metric label values and one optional structured-log field
per request. Public responses and reservation semantics do not change. The
next measured stage must run a build containing this diagnostic to benefit.

## Failure and recovery behavior

If the error extractor cannot read logs, the stage remains failed under the
existing evidence/cleanup gate. An unknown or malformed cause in a log is
discarded from compact evidence. Rollback, durability auditing and private
manifest cleanup continue as specified by ADRs 0040 and 0041.

## Validation evidence

Unit tests must verify all four classifications, unchanged public response,
fixed counter labels and extractor allowlisting. The failed 2026-09-19 stage
is motivating evidence, not validation of this new diagnostic. Live attribution
requires a later measured stage with the same committed revision on the API
ECS and operator workspace.

Implementation validation: 22 focused tests and 108 complete unit tests passed.
Ruff passed for the changed Python files. Live cause attribution remains unverified.

