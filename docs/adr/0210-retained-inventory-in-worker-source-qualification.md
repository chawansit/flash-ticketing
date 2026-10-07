# ADR0210: Retained inventory in worker source qualification

## Status
Accepted for implementation. Extends ADR0202 exact retained inventory recognition to ADR0184 host-aware source qualification. No resources are deleted or adopted.

## Context
The corrected ADR0209 run passed private readiness on both hosts and started workers, then failed host-aware inventory before any customer dispatch. Restoration, duplicate booking, full queues, idle generator and configuration cleanup all passed automatically. The runtime observer recognizes the 16 exactly bound inactive historical containers. The separate inventory collector reads all containers and sends those historical rows directly to a strict active-layout check, which rejects stopped or foreign resources. This code-path discrepancy is consistent with the observed inventory rejection; source checks must retain the same exact recognition contract.

## Decision
Pass the scope-authenticated saved retained-inventory receipt from the bootstrap execution into the source collector. Apply the existing verify_retained function to both initial and final primary observations before validating active layout and identity. Secondary observations retain an empty historical allowance. Bind the retained receipt digest in the inventory report. Never derive allowances from current observations, ignore unknown containers or modify historical resources.

## Alternatives
Deleting historical containers exceeds ownership. Filtering by stopped status or project hides unexpected resources. Dropping inventory checks weakens version and source guarantees. Reuse the exact saved receipt already authenticated by the scope.

## Consequences
The runtime and inventory paths recognize the same preserved historical rows. All active source, import, image, metrics, host, budget and before/after identity checks remain strict. This is a harness correction with no claimed performance benefit.

## Failure and recovery behavior
Missing, altered or activated retained containers, new historical containers and any historical secondary resource remain failures. Do not authorize customers from partial inventory. Preserve failed evidence and consumed counters; use a fresh scope only after restored correctness and queues pass.

## Validation evidence
Cloud run adr0153-parents-6e300e94f5da: both-host dependency readiness passed; failure at verify_host_aware_inventory; zero paid/customer dispatch. All bootstrap recovery gates passed automatically and status FAILED_RESTORED. Offline rejection reproduced for all 16 saved historical rows; existing verify_retained selected the exact 12 original managed rows. Initial affected suite: 145 passed, 8 failed in 139.61s; failures caught a bootstrap attribute reference before any cloud use. Corrected authenticated execution binding: all 75 runner/profile checks passed in 150.21s. Initial observer/retained checks passed. Ruff, names and diff checks passed. Corrected cloud inventory and paid throughput remain unqualified. Sanitized evidence: docs/capacity/flash-sale-opening/background-service-separation-startup-and-inventory-2026-10-07.json.
