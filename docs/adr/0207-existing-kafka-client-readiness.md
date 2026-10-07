# ADR0207: Existing Kafka client for dependency readiness

## Status
Accepted for implementation. Supersedes ADR0196's aiokafka metadata-client choice only. Application messaging, frozen backend images, topology, budgets and customer gates remain unchanged.

## Context
The ADR0206 fixes passed 627 affected checks. The next fresh cloud scope passed staging and common runtime validation, then aborted before any paid stage. A protected read-only diagnostic reproduced ModuleNotFoundError for aiokafka. The project declares kafka-python; the frozen images contain that client, while readiness tests substituted an unavailable aiokafka module. Automatic runtime restoration succeeded; independent queues, duplicate booking, generator idle and sealed configuration cleanup passed.

## Decision
Use the already installed kafka-python KafkaAdminClient for one read-only describe_cluster metadata request. Preserve three-second request and version-discovery limits, exact single private advertised broker verification, guaranteed close and the existing shared probe deadline. Do not subscribe, join a consumer group, publish, commit offsets or install a new runtime dependency.

Qualify the readiness probe's actual direct imports inside the immutable frozen image with no network, mounts or credentials, in addition to synthetic error-path tests. Bind readiness source in the runner entry identity so fresh scopes authenticate this change. Preserve the aborted result and consumed counters; use a fresh scope for any next cloud experiment.

## Alternatives
Adding aiokafka would change frozen image contents solely for a diagnostic and require rebuilding/requalifying them. Ignoring Kafka readiness weakens a required gate. Reuse the deployed client's supported read-only metadata API instead.

## Consequences
The probe uses the dependencies actually present in deployed images. This corrects readiness compatibility and does not improve or measure ticket throughput. Mocked client tests alone are insufficient proof of image compatibility.

## Failure and recovery behavior
Missing imports, connection errors, wrong broker metadata or deadline failures remain failed readiness gates. Always close created clients; mandatory owned restoration and independent audits remain required. No candidate or paid dispatch after a failed bootstrap.

## Validation evidence
Protected import diagnosis: tmp/adr0153-parents-20b6dfaf9be9/dependency-diagnostic.private.json. Aborted scope: adr0153-parents-543471440d66, zero paid/protocol counters. Independent recovery: tmp/adr0153-parents-0f5bc6456337/recovery-closure.json. Implemented using the existing KafkaAdminClient. Final affected readiness/recovery/profile suite executed: 156 passed, zero failed/skipped in 78.10 seconds, including actual probe imports inside the immutable frozen consumer image without network, mounts or credentials. Recovery closure extension separately executed: 54 passed in 1.53 seconds. Naming, lint and diff checks passed. Sanitized evidence: docs/capacity/flash-sale-opening/background-service-separation-dependency-recovery-2026-10-07.json. Corrected live cloud readiness and paid capacity remain unverified; no paid stage or capacity improvement was measured.
