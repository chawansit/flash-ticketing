# ADR 0007: Offered-load capacity validation

Date: 2026-09-06
Status: Accepted

## Context
The previous finite concurrent burst does not measure sustained arrival rate. Fast overload rejection can hide low useful throughput.

## Decision
Extend the existing Python/httpx tooling with a bounded constant-arrival-rate development harness. Run hot-seat, independent-seat and checkout profiles separately. Record scheduled, dispatched and generator-dropped journeys, request status/latency per operation, successful holds, payment acceptance and durable fulfillment after a bounded drain. Use fresh isolated event IDs for every run. Checkout triggers three simulated callbacks per accepted payment.

## Alternatives
A closed-loop benchmark alone hides demand when the server slows. Immediately driving 100k RPS from the shared development host would primarily benchmark the generator and host. Distributed k6 or equivalent remains required for the eventual target; this local harness is not a distributed generator.

## Consequences and recovery
Report generator drops and scheduling lag, and distinguish journeys/sec from HTTP requests/sec. Do not extrapolate the local baseline. Preserve fixtures/results for inspection; do not delete business data automatically. Limit hot-seat duration plus request timeout below the configured hold TTL. Failure drills run separately from capacity measurements.

## Validation
Run all three profiles at a bounded local rate, review correctness queries and record actual results in docs/capacity. A successful baseline validates the harness only, not 100k RPS.


Executed evidence: all three local profiles produced successful baseline results; final checkout drained all three configured callbacks per payment. 32 unit/integration tests passed with no skips. See [capacity report](../capacity/README.md) for raw results, startup failure observations and remaining distributed-validation gaps.
