# Read-only durability audit comparison

Date: 2026-09-29. Both variants audited the same completed Huawei RDS/DCS 1,000 RPS / 30-second fixture from stage `20260928T165459Z-0c0dcd09`. No new load was generated. The dataset contained approximately 2.41 million idempotency records, 677,000 reservation commands and 32,311 historical Redis reservation-stream keys.

| Phase | Grouped SQL, sequential Redis | Grouped SQL, pipelined Redis |
| --- | ---: | ---: |
| Idempotency/hold/order links | 2.168 s | 2.259 s |
| Durable reservation commands | 0.676 s | 0.650 s |
| Seat-ownership overlap | 2.104 s | 2.028 s |
| Redis stream entries/pending | 155.037 s | 3.652 s |
| **Total** | **160.015 s** | **8.619 s** |

The resulting per-run counts, 1,800 audited holds, zero overlap and every queue count matched the original passed stage exactly. The optimized verifier still scans the full Redis keyspace and checks both `XLEN` and `XPENDING` for each stream; it batches up to 256 keys per pipeline. A nonempty stream without its consumer group fails the audit. The change affects only read-only validation; it does not change booking or payment behavior. [ADR 0080](../../../adr/0080-batch-read-only-durability-audit.md) records the decision and failure behavior.

Next: use this verifier in a controlled sustained stage. The historical read-only timing alone does not establish production throughput.
