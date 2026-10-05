# Event refresh comparison runner

ADR0151 compares event refresh off/on with two APIs per host, a 1000 ms advisory cache in both arms, the same images and fixed connection/workload budgets. It does not execute the older one-host/two-host placement comparison or certify 300,000 tickets/hour.

## Local preparation

From the repository root with its Python environment and Git available:

```powershell
.venv/Scripts/python.exe scripts/prepare_status_refresh_artifacts.py --output tmp/adr0152-qualified-artifacts
.venv/Scripts/python.exe scripts/run_status_refresh_comparison.py --output tmp/status-refresh-preparation.json
```

Use a fresh output filename. This command verifies the complete 19-module isolated source tree and creates a receipt template; it does not connect to cloud services, modify approval state, build images or dispatch customers. ADR0152 now recreates `tmp/adr0152-qualified-artifacts/source` from the frozen Git revision and the pinned eight-file patch. It verifies all 207 exported files, including the 19 runtime modules, without reading the older temporary candidate. The runner deliberately rejects missing or changed source rather than copying unrelated current-branch optimizations.

The actual artifact receipt must contain exactly `images`, `parent_images` and `source_manifest_sha256`. Each image/parent map needs API plus consumer, writer, maintenance, publisher, reconciler and simulator roles. Use immutable image IDs. All images must preserve their recorded parent layers and inherited Env/Cmd/Entrypoint/User/WorkingDir, with the required source-manifest label. Verify both `/app` and installed import sources; retained cached code must match the source. No image build/pull/export is performed by this runner.

## Retrieve exact originals

With authorized artifact-only scope, the original-image helper uses the protected password terminal and existing pinned host identities:

```powershell
.venv/Scripts/python.exe scripts/fetch_status_refresh_parents.py --config tmp/adr0147-live-config.json --ssh-runtime tmp/adr0126-ssh-runtime --private-fallback
```

It checks the frozen normal topology and idle generator, exports only the inspected immutable original images, verifies the transferred archive, removes the sealed owned remote archive and compares running container identities before local image loading. `--private-fallback` uses the pinned secondary identity through the primary after direct transport timeout. No deployment or customer request is made. Failed or ambiguous exports are not automatically replayed; inspect their owned evidence before recovery.

## Offline image build

The exact immutable original role images must already be present in the local Docker daemon. Supply a JSON object mapping API, consumer, reservation-writer, maintenance, publisher, reconciler and simulator to their original immutable IDs. The API parent must be the frozen cloud image; a local test parent is rejected.

```powershell
.venv/Scripts/python.exe scripts/prepare_status_refresh_artifacts.py --output tmp/status-refresh-image-build --build --parents tmp/original-role-parents.json
```

Use a fresh output directory. The command preflights every parent before building any candidate, preserves dependency inputs and runtime configuration, copies qualified bytes into both source roots, updates installed package records and checks imports/bytecode. Builds and verification containers use no network; it does not pull, transfer, deploy or dispatch customers. A complete successful build writes `artifact-receipt.json` for the comparison runner. Partial failures retain logs and never emit a deployment receipt. Local tags and images are retained; no automatic pruning is performed.

ADR0153 retrieved the six real immutable originals over pinned SSH, verified archive size/SHA and removed the owned remote archive. All six real candidate images have now been built locally and verified. The private artifact receipt is `tmp/adr0153-real-candidate-build/artifact-receipt.json`. They have not been staged or deployed on cloud hosts; fresh live qualification and performance remain pending. The earlier Docker fixture remains synthetic historical evidence.

## Cloud release

No allowance is active at this checkpoint. A new `bounded_status_refresh` entry in CURRENT_STATE must record explicit user authorization and an exact `binding_for(config, artifact, sources)` value. Preparation does not grant that authorization. A dry-only allowance is 1 qualification pair, 0 paid runs and 2 safety tickets. An approved dry-plus-paid pair allows 1 qualification pair, 2 paid runs and 4 safety tickets total. All consumption counters start at integer 0. Previous ADR0147/0148 allowances cannot be reused.

Use separate `--qualify` and `--execute` invocations, each with `--config`, `--artifact` and `--ssh-runtime`. They require the protected password terminal supported by the existing runner. Paid execution requires the same exact configuration/artifact/source/adapter binding and a successful dry comparison finished within the previous hour. There is no automatic qualify-then-pay transition.

Each invocation reserves its protocol/safety allowance before SSH and takes an exclusive owned lock. A paid launch is separately persisted before calling the generator, including ambiguous dispatch. Every arm restores the original primary runtime, removes secondary resources and credential snapshots, and verifies generator idle/full queue drain before any next arm. Failed control stops the pair. Failed or ambiguous recovery retains the active marker and lock for explicitly scoped recovery; rerunning is not a substitute for recovery.

## Result interpretation

A paid arm schedules 60 buyers/s for 300 seconds using 60 fresh 300-seat shows, 500 active journeys across two fixed shards, 1-second status polls, one callback target and no customer retries. It retains the original latency/customer/financial/zero-double-booking/full queue/Kafka/observer/restoration gates. Only `cache_disabled` is renamed `bounded_equal_cache_age` and validated against the explicit 1000 ms contract.

The comparison report records paid/customer outcomes, error/drop counts, p95 timings, CPU, financial and queue audits, status checks per dispatched journey, consumer database work, callback backlog and cache outcome deltas. Cache totals cover the full observer lifetime including the completion tail. Missing samples, resets or invalid counters produce an explicit unavailable result. `performance_measurement_complete` is separate from correctness gates; incomplete measurement does not establish an optimization benefit. Neither total HTTP attempts nor the offered buyer rate is automatically a tickets/hour capacity measurement.

Local real-image source/configuration checks have executed. Cloud image staging, dry safety/payment checks and performance improvement remain pending. The local validation report records only tests that ran.
