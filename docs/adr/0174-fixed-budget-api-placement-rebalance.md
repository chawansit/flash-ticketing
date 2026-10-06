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
Implementation and local qualification completed: 402 tests passed across nine suites in 70.00 seconds; Ruff passed after correcting the imported pytest fixture suppression; repository names passed with 1,493 documents. The initial pass had three outdated orchestration mocks, which were corrected before the final complete pass. These checks cover exact control settings/images, 2+2 and 1+3 inventory/CPU contracts, drift rejection, financial/diagnostic gates, reservation budgets, replay prevention and failure restoration. A subsequent 198-test focused pass in 65.06 seconds verified the complete installed observer, its historical compatibility view, actual 1+3 routes and confirmation metrics after an adapter correction. Registration checks passed 70 tests in 42.92 seconds. Cloud comparison is pending; synthetic checks do not establish capacity. Acceptance includes current latency/error gates, zero double-booking/payment loss, post-TTL durability, complete global queues/Kafka drain, complete diagnostics and exact restoration for both arms. Compare paid-and-issued throughput, customer outcomes, latency, CPU and database waits rather than HTTP RPS alone.
