# ADR 0044: Restore the intended API keep-alive margin

Status: Accepted for a controlled Huawei benchmark; ADR 0032 and ADR 0038 remain accepted

## Context

ADR 0032 selected a 10-second Uvicorn idle timeout for the benchmark while clients retire idle connections at 5 seconds. ADR 0038 added a 5-second Nginx upstream cache timeout, intending Nginx to retire its cached socket before Uvicorn does. The `compose.keepalive10.yaml` override sets `SERVER_KEEPALIVE_SECONDS=10`, but the deployed API container actually runs the Dockerfile's fixed `uvicorn ... --timeout-keep-alive 5` command. That command does not read the environment variable. The intended Nginx-to-API timeout margin is therefore absent in the current deployment.

On 20 September 2026, the post-Extreme-SSD 750-RPS, 10-minute strict safety stage returned one seat-map HTTP 502 among 450,000 responses. Nginx logged `recv() failed (104: Connection reset by peer) while reading response header from upstream` for that request. The selected API container had zero restarts, and application error summaries were empty. The exact reason for the reset is not proven; equal five-second idle expiries create a plausible race. The stage failed strict availability even though latency and post-TTL correctness gates passed. RDS WAL-path commit spikes also persisted, so the disk change is not a reason to relax availability criteria.

## Decision

In the opt-in benchmark override, replace the ineffective environment-only server setting with an explicit Uvicorn command using `--timeout-keep-alive 10`. Keep the Dockerfile's general five-second default, Nginx upstream `keepalive_timeout 5s`, five-second generator client expiry, and `proxy_next_upstream off`. Keep workload retries disabled. This restores the already accepted 5-second server margin only in the measured topology.

Before traffic, inspect the rendered Compose command and the live container command, then validate Nginx configuration and API readiness. Run a fresh no-retry 750-RPS 10-minute safety stage with the existing fixture, post-TTL durability audit, strict zero-unexpected-response gate, and admission rollback. Start a 30-minute confirmation only if the 10-minute stage passes every gate. Do not promote to 800 RPS on a failed stage.

## Alternatives considered

- Shorten Nginx upstream keep-alive below five seconds. This would also create a margin against the current five-second server, but may increase backend TCP churn and would depart from the previously validated topology.
- Retry failed reads at Nginx or the generator. Rejected because it would hide the upstream failure from the capacity gate.
- Disable upstream keep-alive. Rejected because per-request backend connections materially change the load and latency profile.
- Change the global Dockerfile default. Deferred because this correction is specifically for the opt-in benchmark topology.

## Consequences

The API may hold idle Nginx connections for up to five seconds longer, increasing its idle socket count. The existing Nginx cache still retires those connections after five seconds. Active request semantics, seat ownership, persistence, idempotency, hold expiry, Kafka delivery, and payment processing do not change. The corrected command must be kept aligned with any future Uvicorn launch changes.

## Failure and recovery behavior

A bad Compose command or failed readiness stops the stage before traffic; restore the prior override. Any HTTP 502, transport error, generator drop, latency breach, durability mismatch, overlapping seat interval, undrained queue, or failed rollback makes the stage fail and blocks capacity promotion. Do not rerun the failed rate merely to select a passing sample. Preserve error logs and compare the upstream reset time with API and RDS observations before choosing another change.

## Validation evidence

Pre-change evidence: the deployed API command exposed `--timeout-keep-alive 5` even though `compose.keepalive10.yaml` set `SERVER_KEEPALIVE_SECONDS=10`; Nginx exposed `keepalive_timeout 5s`. The 20 September 2026 stage recorded the single upstream reset and failed strict availability. Post-change rendered-command, live-command, health, 10-minute safety, and possible 30-minute confirmation results will be recorded separately after execution; they have not passed at ADR creation time.
