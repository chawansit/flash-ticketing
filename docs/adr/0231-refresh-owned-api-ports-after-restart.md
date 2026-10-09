# ADR0231: Refresh owned API ports after restart

Status: Accepted for bounded restoration correction and unchanged short control.

## Context

ADR0230 delivered all 25,200 paid tickets, but intermediate ECS restoration timed out. ECS APIs publish container port 8000 through the host range 8101–8104. Docker can reassign actual host ports on restart while container identity and HostConfig remain unchanged. A local three-container reproduction changed the mappings after stopping and restarting the exact same containers; every API was ready at its actual new port. This explains how the old routes can fail without a backend failure. It is a reproduced mechanism, not proof of the historical mapping, which was not retained.

## Decision

After starting each captured owned API, retain its actual published port receipt from the same verified Docker inspection. Require the exact captured container IDs, image/configuration identity, private host address, container port 8000, allowed host range and unique endpoints. Rebuild only the upstream endpoints using the existing Nginx policy, then check readiness against those endpoints. Preserve original route restoration when no restart was attempted. Persist the route hash and sanitized port changes before readiness. This supersedes ADR0228's assumption that pre-stop published port mappings are stable; ownership, budgets, Nginx settings and final original topology restoration remain unchanged.

## Alternatives

Increase the readiness timeout, suppress the intermediate check, or assign fixed ports by recreating APIs. Waiting cannot correct stale endpoints; suppressing the gate conceals restoration failure; recreation changes captured execution identities. Fresh verified port receipts preserve the same processes and policy.

## Consequences

A restored route can differ in upstream ports from its captured text. No customer retry, backend change, extra connection or compute capacity is introduced. Fail closed on foreign, missing, duplicate or out-of-range endpoints; never discover arbitrary services by name.

## Failure and recovery

Attempt restart on both owned hosts independently. Restore routing only with complete exact receipts; retain CCE routing and failed evidence if ownership is ambiguous, allowing the existing final restoration path to run. Preserve financial, post-TTL, queue and resource gates. Do not replay prior experiments or alter their original reports. A fresh 84/s, five-minute control must pass all measurement and restoration gates before hourly progression.

## Validation evidence

Executed local reproduction: tmp/adr0153-parents-7cd563535554/local-port-reallocation.json; three temporary containers removed, actual-port readiness passed, no cloud or customer load. Generated Docker inspection and route tests must execute before cloud mutation. A new bounded reservation is authorized by the user's Proceed to the stated short-control then hourly plan. The consumed prior scope remains unchanged.

Cloud verification: adr0151-c5edbd09ed73 exercised reassigned published ports and successfully restored the candidate and original ECS services, with no cleanup failures. Its customer/financial qualification failed and remains failed. The later ADR0234 candidate passed all measurement, integrity and restoration gates. Port restoration changes did not add database connections or alter application behavior.
