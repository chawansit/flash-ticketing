# ADR0262: Retain startup proof with live runtime verification

## Status
Accepted for a verification correction before another bounded paid run. Supersedes ADR0228's requirement to reread the initial startup proof from the latest bounded log at every observation. All ownership, source, specification, image and process checks remain required.

## Context
ADR0261's control issued 24,410 unique tickets but failed final verification because its latest 64 KiB log lacked the startup proof. Log rotation is a plausible cause; the original failed gate is not retroactively passed. Repeated unbounded log retrieval would increase overhead without proving process continuity.

## Decision
Capture and deep-copy the initial startup proof inside the deployment object. At later observation require the caller's previous receipts to equal that captured admission. Reuse the captured proof only when the bounded log contains no proof line. A present malformed, conflicting or duplicate proof must fail. Revalidate the live pod UID, ownership, full declared container specification, immutable image, container ID, zero restart count, container start time, private IP, readiness and fresh process-start metric. Require every resulting receipt to equal the admitted receipt. Initial admission always requires a valid startup log; no fallback after a missing admission or restart.

## Alternatives
Unbounded log retrieval risks cost and still loses rotated logs. Kubernetes exec or a new sidecar introduces unnecessary permissions and workload. Ignoring startup verification would weaken source and configuration guarantees.

## Consequences
Bounded log retention no longer causes a false failure for a verified unchanged runtime. This corrects measurement only; it cannot improve application capacity or qualify past failed evidence. Receipts remain owned by one deployment instance.

## Failure and recovery behavior
Unknown or replaced runtime, changed proof, forged previous receipt, process restart, container identity change or unready application stops progression. Owned cleanup remains available. Preserve original failed reports.

## Validation evidence
Pending execution: initial missing proof, rotated logs, changed container/process/specification/image, mutated previous receipts and present conflicting proofs. No cloud capacity claim.

Executed 74 adapter checks passed; the broader targeted runner set passed 212 with one skip. Initial admission remains mandatory and rotated logs reuse only the same deployment instance's immutable receipt; container ID, start time and fresh process-start metrics are revalidated. No prior failed report is changed.

Fresh cloud run adr0151-692aefc90917 passed unchanged_native_pods after paid load and complete observers. The retained startup proof was checked against live container ID/start time, pod specification/image and process start. No earlier failed report is reclassified.
