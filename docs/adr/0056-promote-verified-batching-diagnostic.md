# ADR 0056: Promote verified batching under the recovery-SLO diagnostic

Status: Accepted for a diagnostic stage only; strict capacity certification remains unchanged.

## Context

The corrected five-minute 1,000 RPS stage at revision `dd2721d` verified the source hash in every measured worker before traffic. It delivered all 300,000 scheduled requests with no generator drops, transport errors or late deliveries. Nine first-attempt `ADMISSION_FULL` responses produced eight recovered requests and one exhausted retry, a final unexpected-response percentage of 0.000333%. Read/hold worst-worker p95 were 14.150/76.641 ms. All 14,999 acknowledged holds were durable, no hold intervals overlapped, every queue drained, and rollback succeeded.

ADR 0051 permits a 15-minute recovery-SLO diagnostic when the final unexpected-response percentage is below 0.01% and all fidelity, latency, correctness, drain and rollback gates pass. ADR 0054 later prohibited a longer batching stage until every safety gate passed, without distinguishing the strict zero-error certification verdict from the already accepted diagnostic availability budget. The decisions need an explicit precedence rule before another run.

## Decision

Supersede only ADR 0054's blanket longer-stage prohibition. Permit one 15-minute 1,000 RPS batching diagnostic using the exact verified topology and workload from the corrected safety stage: batch size two, one maintenance worker, two Kafka consumers, four API replicas, admission five per API, eight generator workers, two same-key attempts, and a 25 ms base delay.

Apply ADR 0051's diagnostic gate: final unexpected HTTP or transport outcomes must remain below 0.01% of scheduled requests, with zero generator drops and complete accounting. No correctness or cleanup budget is allowed. Durability, broken-link, overlap, queue-drain, readiness and rollback gates must all pass. Keep the automation's strict verdict unchanged, so any unexpected response still prevents certified-capacity status. Do not add a third attempt, increase admission, or alter the RDS configuration in this experiment.

## Alternatives considered

- Repeat five-minute stages until one has zero final errors: rejected because selecting a lucky interval does not establish sustained behavior.
- Require the strict gate before any diagnostic: rejected because it makes ADR 0051's separately labeled recovery-SLO path unusable despite all non-availability safety gates passing.
- Add a third retry: rejected because it changes the recovery workload and can hide overload.
- Raise API admission above five: rejected because RDS observation still shows `WALWrite` and `WalSync` contention.
- Declare 1,000 RPS certified from the five-minute result: rejected because the strict zero-error gate failed and the duration is insufficient.

## Consequences

The 15-minute stage can measure whether the bounded availability error and zero-queue result persist. Its result is diagnostic evidence only. A pass may authorize a fresh 30-minute diagnostic under ADR 0051, but it cannot certify production capacity under ADRs 0040 and 0048.

## Failure and recovery behavior

Stop promotion if final unexpected outcomes reach 0.01%, any request is unaccounted, latency breaches its gate, or any correctness, drain, readiness or rollback check fails. Preserve the evidence and restore admission four, one maintenance worker and one consumer. Idempotency keys remain stable across the single bounded retry.

## Validation evidence

The authorizing five-minute evidence is retained privately at `tmp/capacity/cloudssd-refreshbatch2-dd2721d-1000-safety-20260926` and summarized in ADRs 0054 and 0055. The 15-minute outcome must be appended here before any further promotion.

The 15-minute diagnostic at revision `96fdfc0` scheduled and delivered all 900,000 requests with no generator drops, transport errors or late deliveries. Eight first-attempt `ADMISSION_FULL` responses exhausted their bounded retries, a final unexpected-response percentage of 0.000889%, below the 0.01% diagnostic budget. Worst-worker read/hold p95 were 10.113/66.751 ms. All 44,992 acknowledged holds were durable, no intervals overlapped, all queues drained, worker source verification passed and rollback succeeded.

Refresh generation and completion both increased by 89,984. Pending refresh peaked at 800 and drained 145.847 seconds after its peak; oldest refresh age peaked at 820.727 seconds before returning to zero. Kafka total lag peaked at 22 and ended at zero. The RDS observer recorded 1.234 GB of WAL, zero `wal_buffers_full` increments, a maximum of 16 interesting waiters and a timed checkpoint with 719,739 ms of accumulated write time. The automation's strict verdict is fail because eight retries exhausted; the separately defined diagnostic verdict is pass and authorizes one fresh 30-minute diagnostic. Private evidence is retained at `tmp/capacity/cloudssd-refreshbatch2-96fdfc0-1000-diagnostic15-20260926`.
