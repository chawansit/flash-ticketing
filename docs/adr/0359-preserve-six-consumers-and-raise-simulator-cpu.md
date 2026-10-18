# ADR0359: Preserve six consumers and redistribute simulator CPU

## Status
Accepted for preparation and fresh user-preapproved qualification through clean200/s. Supersedes ADR0357 allocation for future execution; retains its strict identity and historical five-member contracts for evidence validation.

## Context
Allocated180d failed:54000scheduled,36842dispatched/fulfilled,17158drops,140initial/recovered,0final. Journey p9519033.55ms; ticket waiting17835.67ms versus durable1471.27ms and payment HTTP187.89ms. Kafka's single consumer owning partitions0/1 reached lag1474/1429; other partitions peaked55/31/50/60. Consumer partition work means for0/1 were about532/537ms per polled batch versus21–25ms elsewhere. Writer batch/command-age means improved to53.14/815.52ms. API status polling increased, CPU3.49cores and140acquisition errors. These observations support restoring one consumer per partition; they do not isolate every cause. Original six-consumer/four-writer180 reached177.27/s with only2drops and p955696.82ms. Simulator had approached its250mCPU quota.

## Decision
Use the exact unchanged combined application image, six consumers and four writers. Keep15workers/3750mrequests-and-limits/7680Mi, four API1CPU/2Gi,24physical backends/no reserve, admission20, all client pools, generator, TTL/cache/poll, correctness and latency gates unchanged. Allocate simulator500m, maintenance/reconciler125m each, other workers250m; every worker512Mi. Select distinct ADR0359 identity and exact per-role quotas. Reject old five-consumer receipts for this new mode. Require six stable Kafka members, each owning exactly one of six partitions, and projection one member owning six partitions. Restore original normal runtime after every attempt.

## Alternatives
Repeat five-consumer layout: contradicted by partition lag. Add workers/CPU/backends: unnecessary before using observed idle budget and outside this fixed-total experiment. Relax workload/latency/financial gates: rejected. Rewrite application consumer batch processing: wider change than restoring the previously stronger topology.

## Consequences
Removes the observed two-partition consumer imbalance and tests simulator CPU headroom while keeping aggregate spend/capacity unchanged. No gain is assumed. Maintenance/reconciliation get reduced CPU; monitor expiry/refund/drain and reject failure. Compare evidence as a configuration qualification, not an isolated matched causal test.200requires actual clean fully passed/restored180 with this same allocation.

## Persistence, locking, messaging, idempotency, TTL and scaling
No application SQL/transactions/locks/messages/idempotency/TTL change. Kafka rebalances to six consumers, one per partition. Writer count returns to four. Only existing CPU quota distribution changes from the original strongest topology. Historical ADR0357 remains hash-sealed and auditable.

## Failure and recovery
Fail closed on source/role/resource/ownership mismatch. No upward progression on failed quality. Reconcile actual dispatched cohort independently if target count differs, drain and restore. Preserve all consumed attempts; new rounds use fresh identities under existing human preapproval, no main merge or aggregate resize.

## Validation evidence
104 distinct focused cases passed across original and final corrected-source runs. One case initially observed source identity changing while overlay hashes were finalized; reexecuted with final immutable sources and passed; full17 reproduction cases also passed. Actual unchanged candidate-image rehearsal separately passed with networking disabled:22 helpers and22 runtime hashes, strict transferred six-consumer/four-writer identity; refreshed inventory timestamp explicitly synthetic. Ruff passed. Actual four-API/all15worker manifest preparation and real isolated reservation passed. Source/goal hashes and exact allocation are in docs/capacity/cce/simulator-cpu-allocation-180-readiness-2026-10-19.json. No new cloud performance pass claimed.

## First preparation stop

Fresh0359-180 adr0151-d07bec703e74 stopped before performance traffic: actual native financial/post-TTL and zero-global-queue/six-member drain passed, but safety_result caller still mapped every selected fixed allocation to five members. Runner fully restored, zero paid runs. Derive its exact membership from the same explicit native drain contract and exercise the complete Inputs.safety caller for both0357 five and0359 six layouts. Reseal source and use fresh0359-180b; do not reset consumed attempt.

Safety-result correction validation:29 focused cases passed, including the actual Inputs.safety caller for both exact membership layouts, local/transferred new allocation identity and all17 reproduction cases. Ruff passed. Sourceb74e31fe3a449f9478b6addff7b9de33eb99013ef1ff8b277fc31da3fa96de32; fresh180b actual all15-worker preparation and isolated reservation passed, immutable registry verified.

## Measured clean180 validation

Fresh180b adr0151-6668a7e46ed3 PASSED_RESTORED:54000 scheduled/dispatched/fulfilled, zero drops/initial/recovered/final failures,53418 conservative=178.06/s, worst-shard journey p952634.74ms. Full existing database financial/post-TTL/relationships, zero queues, live worker/observer and all restoration/cleanup gates passed. Kafka total lag peak124, each partition peak at most58. Retained evidence audit passed separately, not a second database reconciliation.200 actual same-allocation manifests and isolated reservation passed against this genuine lower-rate result; no200 performance pass yet.
