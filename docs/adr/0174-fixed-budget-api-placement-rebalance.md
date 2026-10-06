# ADR0174: Fixed-budget API placement rebalance

## Status
Accepted for implementation and bounded comparison under ADR0172. Cloud benefit is unmeasured.

## Context
ADR0173 completed 18,000 customer-confirmed paid-and-issued tickets at 60 journeys/s for 300 seconds, with zero customer errors, payment loss or duplicate booking. Its diagnostic coverage gate failed. Primary CPU averaged 71.382% while secondary CPU averaged 17.613%. Shared WAL waits remain unresolved. Additional resources and resizing are outside the standing spending envelope.

## Decision
Compare the existing two-plus-two API placement against one primary API plus three secondary APIs. Reuse the exact frozen ADR0163 control images and settings in both arms, including reclamation off and synchronous payment intake. Keep four APIs, 16 aggregate API pool slots (including eight payment slots), one primary PgBouncer with 24 server connections, all background service counts/settings/placement, database, Redis, generator, load balancer policy and 60 journeys/s for 300 seconds unchanged. Both arms retain ADR0173 database-wait and slow-commit diagnostics.

This explicitly supersedes only the fixed two-plus-two placement requirement of ADR0147 and its descendants for this registered comparison. It does not supersede financial authority, locking, idempotency, expiry, source identity, connection budgets, queue gates or restoration requirements.

Register a distinct bounded profile with one safety pair followed by one paid pair, at most two measured stages and four safety tickets within the existing 3,600-second experiment ceiling. Every scope is fresh and append-only. Failed control stops the candidate. Qualify each emitted placement, all four immutable APIs, background inventory, traffic distribution, CPU observations and cross-host safety before dispatch. Restore the exact original four-primary runtime after each arm. Use the existing staging-directory ownership factory if image staging becomes necessary.

## Alternatives
- Move background workers: potentially larger relief, but introduces broker routing and worker observation changes; evaluate separately after attributing remaining CPU by role.
- Add or resize machines: requires a spending exception and would confound this comparison.
- Optimize database/WAL first: remains necessary if it limits completion, but does not use the measured idle compute now available.
- Treat existing two-plus-two placement as balanced: observed host utilization does not support that assumption.

## Consequences
No extra infrastructure spending or database connections. Existing service charges continue. Moving one API may provide limited primary headroom because background workers remain on the primary; a 40-50% throughput increase is not promised. More remote traffic may add latency. No higher offered load or permanent adoption follows without passing evidence.

## Failure and recovery behavior
Reject unknown placement, wrong host counts, source/configuration drift, exhausted scope, missing diagnostics and changed financial gates before progression. Stop on failed control or customer/correctness gates. Cleanup and exact restoration run even after experiment deadline or interruption. Ambiguous ownership or incomplete financial recovery blocks new load. Never rewrite failed evidence or replay a consumed identity.

## Validation evidence
Implementation and local qualification completed: 402 tests passed across nine suites in 70.00 seconds; Ruff passed after correcting the imported pytest fixture suppression; repository names passed with 1,493 documents. The initial pass had three outdated orchestration mocks, which were corrected before the final complete pass. These checks cover exact control settings/images, 2+2 and 1+3 inventory/CPU contracts, drift rejection, financial/diagnostic gates, reservation budgets, replay prevention and failure restoration. A subsequent 198-test focused pass in 65.06 seconds verified the complete installed observer, its historical compatibility view, actual 1+3 routes and confirmation metrics after an adapter correction. Registration checks passed 70 tests in 42.92 seconds. Cloud execution is recorded below; synthetic checks do not establish capacity. Acceptance includes current latency/error gates, zero double-booking/payment loss, post-TTL durability, complete global queues/Kafka drain, complete diagnostics and exact restoration for both arms. Compare paid-and-issued throughput, customer outcomes, latency, CPU and database waits rather than HTTP RPS alone.

Cloud safety pair `adr0151-c0536c9a9740` passed both 2+2 and 1+3 placements and exact restoration. Paid control `adr0151-196061bb2077` completed 18,000 customer-confirmed paid-and-issued tickets at 60 journeys/s for 300 seconds, with zero customer errors, drops, payment loss or duplicate booking; post-TTL audits and complete global queues/Kafka drain passed. Original runtime was restored. Four diagnostic gates failed, so the paid candidate was skipped. Primary CPU averaged 70.859%, secondary 17.367%; worst-shard hold-to-ticket p95 was 1,696.22 ms. No placement benefit or hourly capacity is established.

Two profile omissions caused absent admission counters and skipped slow-log collection. Both were corrected locally; four-replica admission counters are now required before dispatch. Unavailable backend classification is retained without relaxing full activity visibility. The final corrective pass executed 201 tests in 74.69 seconds, final standing-envelope checks passed 53 tests in 5.29 seconds, repository naming checks passed with 1,494 documents and Ruff passed; these corrections have not been revalidated in cloud. Six activity visibility failures under load remain unresolved. A 75-second read-only idle probe observed no restricted samples and confirmed no full-statistics usage, superuser or grant admin option for the application role; this does not establish the failed sessions' cause. See [sanitized cloud checkpoint](../capacity/flash-sale-opening/api-placement-rebalance-control-2026-10-06.json). Preserve the failed evidence and use a fresh scope after diagnosis.
