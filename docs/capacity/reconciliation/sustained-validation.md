# Sustained read and expiry validation

Date: 2026-09-08 Asia/Bangkok. Application commit **160d63b**, unchanged during this run.
This extends the short integrated check with traffic lasting beyond the 120-second hold TTL.
Only the measurement harness and documentation changed; no application tuning or architecture
change was made. The previously executed 71-test regression suite was not repeated for this
measurement-only change. Ruff and diff checks passed.

## Workload and results

One **180-second** stage at **50 total requests/sec**, with **100 shows x 300 seats**,
1,000 simulated viewer validators, **95% conditional reads / 5% unique-seat holds**.
Maps and validators were prewarmed outside the measured phase.

| Measurement | Result |
|---|---:|
| Completed requests | 9,000 |
| Reads | 8,550: 5,491 HTTP 304 and 3,059 HTTP 200 |
| Successful holds | 450 / 450 |
| Generator drops / transport or HTTP errors | 0 / 0 |
| Read p95 | **42.25 ms** |
| Hold p95 | **68.71 ms** |
| Conditional-read body volume | 57.00 MB, headers excluded |
| Observer samples / errors | 90 / 0 |
| Missing maps in samples | **0** |
| Minimum sampled Redis TTL | **10 seconds** |
| Maximum sampled reconciliation age | 20.24 seconds |
| Expired holds cleaned by HTTP-phase end | **150** |
| Maximum sampled overdue ACTIVE holds | 2 |
| Oldest sampled overdue ACTIVE hold | **0.478 seconds** |
| Maximum sampled dirty events | 4 |

One hold was 17 ms overdue in the last sample; expiry is asynchronous. Remaining unexpired
holds at phase end are expected. This run does not claim every hold had expired by then.
Database verification subsequently confirmed exactly 450 durable holds and 450 orders.

## Interpretation

This provides local evidence that conditional browsing, reservation writes, periodic
reconciliation and hold expiry can progress together at the tested operating point.
The HTTP gate passed, and no sampled cache gaps or sustained expiry accumulation appeared.
The gate is not a production capacity certificate. A 3-minute run is not a production soak,
and 100 shows is not the proposed deployment's 800-2,000 screens or multiple showtimes.

The generator, PostgreSQL, Redis, Kafka and workers share the same development computer.
Earlier fixtures remain and are scheduled according to their sale windows. The observer
adds SQL queries and 100 pipelined Redis TTL checks roughly every two seconds. Samples
can miss shorter gaps or spikes; the actual HTTP read responses supply complementary
availability evidence. Reconciliation age is SQL acknowledgement age, not direct cache
age. The TTL values were measured from Redis, not inferred from scheduler timestamps.
No paid checkout workload, infrastructure outage or separate expiry flood was included.

[Raw results and samples](sustained-read.json), [post-run verification](sustained-verification.json).

## Reproduce

Set development TEST_DATABASE_URL and TEST_REDIS_URL as in the runbook, then:

```powershell
.venv/Scripts/python.exe scripts/seatmap_load.py --rates 50 --seconds 180 --shows 100 --viewers 1000 --modes conditional --observe --output new-sustained-result.json
```

Fixtures are retained. `--modes` selects one endpoint mode; omitting it preserves the
alternating legacy/conditional comparisons. `--observe` enables the additional samples.
The final helper also explicitly binds observer variables per stage; the recorded run
used the equivalent single-stage behavior before that lint-only adjustment.
