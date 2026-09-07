# Integrated scheduler validation on the user's machine

Date: 2026-09-08. Claude's patch based on 4503d97 was integrated with the conditional
seat-map APIs. External benchmark results remain in README.md and are not local capacity.

Corrections made before integration:
- Lock pruning candidates with SKIP LOCKED and recheck the lease before deletion.
- Check the deadline before each snapshot; release unstarted leases with their token.
  One started snapshot and SQL bookkeeping can still overrun; this is not preemptive I/O.
- Separate successful acknowledgement, stale token and acknowledgement-error metrics.
  Count actual attempts separately from successful reconciliations in the harness.
- Pass all seven configuration values through Compose. Reject intervals >=30 seconds
  against the existing cache TTL.
- Use stable statement-time comparisons against indexed sale-window columns. LIMIT
  bounds output, not metadata scan cost; no constant-time discovery claim is made.
- Remove the interpretation of throughput x TTL as a safe capacity limit.

Validation executed here:
- 15 scheduler integration cases passed in 4.13 seconds.
- Combined container suite: **71 passed, zero skipped**, 14.29 seconds; two existing
  dependency deprecation warnings. This includes conditional reads, PostgreSQL ownership,
  payment/worker recovery and the 100-request exactly-one-winner HTTP test.
- Ruff and diff whitespace checks passed.
- Migration 004 applied successfully; the previous migrations were not edited.

Mixed HTTP load ran after the tests, with 100 fixed shows x 300 seats, 1,000 independent
viewer validators, 95% reads and 5% unique-seat holds. Two 30-second stages at 50 total
requests/sec used the integrated scheduler, same host and one API process:

| Endpoint | Read p95 | Read 200 / 304 | Holds | Generator drops | Read body bytes |
|---|---:|---:|---:|---:|---:|
| Legacy | 74.7 ms | 1,425 / 0 | 75 | 0 | 44,848,375 |
| Conditional | 23.6 ms | 287 / 1,138 | 75 | 0 | 5,350,481 |

Both stages passed the local read gate and had no HTTP/transport failures. Database
verification found exactly 150 durable holds and 150 orders for successful responses.
This confirms integration at one local operating point, not 200-location capacity.
It is not a controlled causal estimate of scheduler speedup: requests use different
seats, the host is shared, and only one pair was run. No separate offered expiry flood
or production failover load was measured. Deadline-release behavior is tested directly.

[Raw mixed-load results](local-read-integration.json), [post-run health](local-health.json).
Reproduce with scripts/seatmap_load.py at --rates 50 --seconds 30 --shows 100 --viewers 1000.
The isolated Claude scheduler benchmark was reviewed and corrected but not rerun locally;
its original numerical evidence must not be attributed to this integrated implementation.

Follow-up: [three-minute read and expiry validation](sustained-validation.md).
