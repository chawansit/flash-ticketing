# ADR 0046: Build the measured API image from the matched source revision

Status: Accepted for controlled validation; amends ADR 0040

## Context

The operator and both ECS checkouts matched revision e9370d4 during the 20 September 2026 benchmark, and rendered Compose configuration included `DB_POOL_WAIT_MS=500`. However, the live API container still contained the prior `Postgres` implementation with a fixed 150-ms checkout timeout. The backend stage used `docker compose up --force-recreate` without `--build`, so recreating containers reused the old image. The 750-RPS ten-minute pass did not validate the proposed timeout change; a 30-minute run later returned one 153-ms `PoolTimeout`. Source-revision matching without image-revision matching is insufficient for a capacity experiment.

## Decision

Build the API image from the checked-out source before each benchmark deployment, then recreate all four API containers. Build the migrate/observer image from the same source so observer code changes are included. Before load, verify inside a live API container that the expected pool-timeout implementation is present and that rendered/live environment and health match the candidate. Do not infer a running image's code from its Compose environment alone. Keep the fixed connection budget and strict no-retry gates.

## Alternatives considered

- Recreate containers without rebuilding: rejected because it caused the measured configuration drift.
- Rebuild manually outside the stage: rejected as error-prone and non-repeatable.
- Retag an existing image: rejected because a tag does not prove code contents.
- Add a full image-attestation pipeline: useful later, but unnecessary for this local controlled repair.

## Consequences

Deployment will take longer due to two image builds, generally using cached layers. Existing containers are recreated only after a successful build. This corrects experiment provenance but does not itself change database performance or guarantee a passing capacity stage.

## Failure and recovery behavior

A build or readiness failure blocks traffic. The orchestrator must still restore admission, stop observers and clean private manifests. If live image inspection disagrees with the intended code, abort before load and restore the prior known-good image. Preserve the failed 20 September results as invalid evidence for the 500-ms candidate, rather than relabeling them as a successful test.

## Validation evidence

Before this decision, the live container file showed `timeout=0.15` and `Postgres.__init__(url, maximum)` despite the e9370d4 checkout and `DB_POOL_WAIT_MS=500` environment. The corresponding API failure took 152.9 ms. Shell syntax, rebuilt image contents, healthy replicas and a fresh load stage remain to be verified after implementation.
