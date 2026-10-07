# ADR0203: Retain Kafka image-declared volumes

## Status
Accepted for implementation. Supersedes the single-mount broker assumption in ADR0184/ADR0186/ADR0188 for bounded, explicitly observed Kafka image-declared auxiliary volumes. No messaging delivery, data durability or customer gate changes.

## Context
Read-only preparation observed Kafka's existing data volume and two anonymous volumes mounted at /etc/kafka/secrets and /mnt/shared/config. The current snapshot and execution contracts assume one mount and cannot preserve this actual deployment during recreation. Silently replacing auxiliary volumes or deleting historical resources is unacceptable.

## Decision
Identify the data volume using KAFKA_LOG_DIRS, inspect every broker volume and capture exact existing identities. Accept at most the data volume plus the two known image-declared auxiliary destinations, each backed by a distinct local Docker volume with no custom driver options. Require the auxiliary destinations in the actual broker image volume declarations. Restore and deploy with all those existing volumes declared external; never create or replace them. Bind auxiliary identities into the original snapshot and fresh experiment binding. Inspect and compare the complete volume set in staging/execution/audits/restoration.

Treat an empty Docker mount Mode as its equivalent effective read/write mode, using the independent RW flag; explicit external Compose mounts may render rw instead of the inherited empty string. Preserve source, destination, volume name, propagation and RW exactly. No other semantic difference is normalized.

## Alternatives
Ignoring additional mounts loses restoration evidence. Deleting auxiliary volumes can lose broker configuration/secrets. Continuing with anonymous replacement volumes changes a factor besides worker placement. Rejecting the real baseline indefinitely prevents the authorized comparison without addressing the incompatibility.

## Consequences
The snapshot also preserves the observed load-balancer nofile soft/hard limits of 65535. Accept only that exact existing setting on the load-balancer, render it explicitly in Compose and compare it in planned runtime/restoration checks; do not raise or generalize operating-system limits. Other unsupported resource options still fail closed.

Original and candidate Kafka configurations retain all existing broker volumes. Unsupported destinations, missing image declarations, changed volume identities or nonlocal drivers block progression. Legacy single-volume snapshots retain their existing behavior.

## Failure and recovery behavior
Missing or changed volumes block mutation and certification. Cleanup targets only owned experimental files and containers, never volume data. Financial audits and exact restoration remain mandatory. No automatic repair or volume adoption occurs.

## Validation evidence
Executed: [runtime compatibility qualification](../capacity/flash-sale-opening/background-service-separation-runtime-compatibility-2026-10-07.json). The final affected suite passed 539 tests in 276.54 seconds, including three existing isolated Linux checks. The earlier broad suite passed 859 tests, including 23 real PostgreSQL financial cases, and removed its owned database. Initial fixture/setup failures remain recorded separately. A fresh read-only cloud snapshot verified 12 managed containers, 16 exact retained inactive containers, three Kafka volumes, an empty secondary and idle generator. The immutable comparison package passed local preparation. No cloud load or capacity improvement is claimed by this checkpoint.
