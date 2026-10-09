# ADR0243: Use managed CCE SWR pull credentials

## Status
Accepted for the ADR0242 matched short comparison. Cloud validation pending.

## Context
Control adr0151-dc3f10fd2b89 dispatched no paid journeys. All four pods were scheduled but blocked in ImagePullBackOff; retained pod events show SWR OAuth 401. The same supplied credentials subsequently passed direct authenticated manifest reads with both single and containerd-style dual scopes. Credential expiry, application startup and insufficient compute are therefore not established causes. The custom-only CCE pull path failed; its precise credential resolution cause remains unresolved.

## Decision
For the explicit ADR0242 profile, reference namespace-local CCE-managed default-secret first and retain the owned swr-pull secret as fallback. Apply the identical ordered references to both arms. Huawei documents default-secret as the SWR pull credential automatically created in each Autopilot namespace and periodically refreshed. Reference it directly; never retrieve, copy, overwrite or delete its contents separately. The owned namespace lifecycle remains unchanged.

Keep immutable image digests, API source admission, four 1-vCPU/1-GiB pods, connection budget, readiness deadline, paid workload and correctness gates unchanged. Preserve the historical profile's custom-only path. This partially supersedes the custom-only native pull-secret choice for ADR0242; no production architecture, IAM grant or financial behavior changes.

## Alternatives
Blindly replace a credential that still authenticates: unsupported expiry diagnosis. Increase readiness time: does not repair 401. Make the repository public or relax source checks: weakens protection. Read cluster-managed secret contents: unnecessary exposure. Use managed credentials only: removes the existing explicit fallback and expands the manifest/lifecycle change.

## Consequences
CCE can use its supported rotating SWR identity. Actual pull authorization still requires live validation; no successful startup or capacity increase is claimed from documentation alone. A missing or unauthorized managed secret fails closed within the existing deadline. Baseline and candidate remain comparable because both have the same pull configuration.

## Failure and recovery behavior
Retain bounded pod states before cleanup. Do not dispatch until all four immutable image/source/startup checks pass. On failure preserve evidence, remove only the owned namespace/helpers, drain queues and verify exact normal restoration. Rollback returns ADR0242 to custom-only references under a fresh scope; never reuse an ambiguous run. Do not change IAM or repository visibility without separate authorization.

## Validation evidence
The failed run restored completely, queues drained and temporary credentials were removed. Read-only registry variants returned HTTP200. The affected profile/adapter/entry/transition suite passed 181; Ruff, naming and reproduction checks passed (535 historical inputs, six current overlays, 78 frozen generator files). Both selected image arms require the same ordered secret references; historical profile references remain unchanged. Fresh control adr0151-f1d128f9546a validated all four native immutable images, 22-module sources and startup proofs using these secret references. The separate observer parser gate stopped paid dispatch; full restoration passed. This proves native startup, not throughput. [Retained failed-run evidence](../capacity/cce/transaction-control-pull-failure-2026-10-09.json).

Reference: [Huawei Autopilot cluster secrets](https://support.huaweicloud.com/usermanual-cce-autopilot/cce_11_0388.html).
