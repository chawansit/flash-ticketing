# ADR 0023: Bounded read transport diagnostics

Status: Accepted — diagnostics implemented; two cloud runs completed without reproducing the failure. Root cause unresolved.

## Context
Both ADR 0022 uniform runs recorded one fast read ReadError in worker 3. Existing evidence records only the exception class, not request timing or transport phase. It cannot identify the cause.

## Decision
Add opt-in HTTPX trace collection to the existing generator, preserving requests, pooling, timeouts, arrival plan and failure gates. Retain at most 20 failure examples per worker and 32 phase events per request. Record UTC/request index/operation, elapsed time, trace phase names and bounded exception class/errno chains. No exception message, headers, credentials, bodies or URLs are retained. No retries or connection-policy change. Diagnostic runs retain unsuccessful results.

## Alternatives
Suppressing the error or retrying would hide the failed gate. Changing keep-alive before observation could confound diagnosis. Packet capture can follow if the trace cannot distinguish transport phases; it is not needed for the initial bounded probe.

## Consequences
Tracing adds overhead and remains opt-in. Absence of a TCP-connect trace is evidence that this request did not open a new connection, not proof of the precise reuse failure cause. No booking, persistence, TTL, delivery or scaling policy changes and no accepted ADR is superseded.

## Failure and recovery
Preserve failed-request accounting and no-retry policy. Validate with a real socket reset and synthetic secret-bearing exceptions to ensure diagnostics cannot log credentials. Keep ordinary deployment unchanged; stop cloud services and delete manifests/firewall rules after testing.

## Validation evidence
Local reset/redaction and harness tests passed (4 tests, 5.53s); complete unit suite passed (45 tests, 14.23s). The first 400 RPS five-minute baseline diagnostic run passed without reproducing the error. A second identical-rate run with fresh seats was started because the failure remained unresolved. Both outcomes will be retained. A passing run cannot establish that an intermittent failure is fixed.


## Executed outcome
Both 400 RPS five-minute baseline diagnostic runs passed, 240,000 requests total, with no transport errors or generator drops. All 12,000 acknowledged holds persisted and expired correctly across eight worker IDs. Prior ReadErrors remain unexplained and their failed gates remain intact. Namespace-level TCP resets occurred despite successful measured traffic; they are not per-request causal evidence. No retry, keep-alive, booking or candidate-adoption change was made. Services/manifests/firewall were cleaned up. See [report and evidence](../capacity/read-transport/README.md).
