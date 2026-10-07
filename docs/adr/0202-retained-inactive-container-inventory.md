# ADR0202: Retained inactive container inventory

## Status
Accepted for implementation under ADR0201. Supersedes the all-container rejection rule in ADR0187/ADR0188 only for explicitly captured, unchanged inactive historical containers. Customer, financial, ownership and restoration gates remain unchanged.

## Context
Fresh read-only cloud preparation found the expected 12 running primary containers, an empty secondary and an idle generator. The primary also retains 16 exited containers: four unused local dependency/monitoring roles in flash-ticketing and twelve in the historical flash-cloud-bench project. The worker runner rejects these artifacts before load. Deleting them would unnecessarily change historical resources.

## Decision
Observe all containers, then capture a bounded exact identity receipt for eligible exited historical containers. Eligible containers are exited flash-cloud-bench resources or exited flash-ticketing roles outside every managed API/infrastructure/worker role. Store immutable container IDs and full inspection hashes with canonical mount ordering in the protected original snapshot. Recheck the entire retained set at staging, runtime observation, mutation preconditions, audits and restoration. Return managed rows only after retained identities are independently verified. Never exclude stopped duplicates of managed flash-ticketing roles, running foreign resources, unowned resources or newly appearing historical containers. Secondary inventory remains strict and empty before staging/restoration.

No historical container is started, removed or adopted. Missing, changed or activated retained containers block progression and successful restoration. The retained receipt participates in the existing exact saved-runtime hash and fresh experiment binding.

## Alternatives
Deleting inactive containers loses retained history and requires unrelated cleanup. Ignoring all stopped containers could conceal duplicate managed resources or activation. Filtering only running resources without a captured receipt would weaken restoration evidence.

## Consequences
Preparation can use an unchanged historical host while preserving strict active topology and exclusivity. Docker inspection may return the same mounts in a different order; sort the complete mount objects before hashing, without discarding any field. Inspection hashes fail closed on actual retained metadata changes. Existing snapshots without a retained receipt retain the previous strict behavior.

## Failure and recovery behavior
Receipt mismatch or unknown inactive resources blocks forward actions. Cleanup never targets historical containers; mandatory restoration and financial checks continue for exactly owned experimental resources. A failed retained check cannot certify restoration or authorize another load stage.

## Validation evidence
Executed: [runtime compatibility qualification](../capacity/flash-sale-opening/background-service-separation-runtime-compatibility-2026-10-07.json). The final affected suite passed 539 tests in 276.54 seconds, including three existing isolated Linux checks. The earlier broad suite passed 859 tests, including 23 real PostgreSQL financial cases, and removed its owned database. Initial fixture/setup failures remain recorded separately. A fresh read-only cloud snapshot verified 12 managed containers, 16 exact retained inactive containers, three Kafka volumes, an empty secondary and idle generator. The immutable comparison package passed local preparation. No cloud load or capacity improvement is claimed by this checkpoint.
