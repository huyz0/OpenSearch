# Serverless shell pilot readiness

The pilot serves small, intermittently active indices inside one trust domain. Use a disposable local RustFS 1.0.5 store for object-store verification. Record the RustFS version, disk layout, Java version, seed, workload and commit with each result.

## Build and durability

Run the required smoke from a clean Linux checkout with the supported Java toolchain.

```bash
bash serverless/dev-tools/rustfs-gate.sh smoke --no-daemon --max-workers=4
```

The script verifies and runs the pinned RustFS binary on loopback. It fails if RustFS is unavailable or a selected object-store test is skipped. The filesystem tests retain their deterministic request budgets. The GCS fixtures compile in an isolated source set and can run against `fake-gcs-server` with the existing `emulatorTest` task.

Keep live `nodelog/` objects out of age-based lifecycle rules. Reclaim from durable commit coverage and published replay/trim state. The smoke checks a hard stop of every serving process followed by a restart on the same bucket, logical advancement beyond a week, a missing required record, and a conditional PUT whose response is lost. The logical clock test advances the shell's lease and replay clock. The hard-stop test uses actual child processes and fresh homes.

## Recovery and failure isolation

Use the pre-release RustFS workflow or run the fleet gate locally.

```bash
bash serverless/dev-tools/rustfs-gate.sh fleet --no-daemon --max-workers=4
```

The kill scenario compares a fixed window against a no-fault window at the same offered rate. It selects writes by request start and drains in-flight writes before evaluating them. The initial acceptance bounds are at most five additional percentage points of healthy-write errors and at most twice the healthy-write p99. A run that shed load or had no healthy acknowledgements cannot pass. The affected cohort must acknowledge on every covered shard by the recovery deadline and remain writable before the killed process comes back. Use routing-aware attribution before generalizing the current one-shard-per-index fixture to multi-shard indices.

Record queue age, open phases, and RustFS traffic before changing ownership or recovery concurrency. Report failed and timed-out requests alongside p99.

In the local three-node shakedown on RustFS 1.0.5, 15 of 15 affected shards acknowledged by 7.1 seconds with an eight-second lease, and all 15 passed the subsequent write check. The healthy cohort acknowledged 594 of 594 writes at 277 ms p99 against 610 of 610 at 280 ms in the control. The affected cohort had 416 unknown outcomes during the 48-second observation window. The end-of-run ledger checked 1,969 acknowledged writes and found zero lost. These figures apply to 48 indices, a 24-index active set and 20 offered writes a second.

## Matched RustFS comparison

Start two throwaway deployments from the same OpenSearch revision, one using the shell and one with stock remote-backed storage. Give each an exclusive RustFS bucket, identical resources and the same publish/refresh and durability targets. Configure stock with segment and translog repositories backed by its bucket. Point each deployment's repository endpoint at its own wire meter, forwarding to the same local RustFS instance. Run each meter in its own terminal.

```bash
./gradlew :serverless:testkit:rustfsWireMeter --args='19200'
```

Use the two printed meter addresses and the Java process IDs with `python3 serverless/tools/compare_rustfs.py --help`. The tool submits the same seeded schedule and records offered writes, acknowledgements, observed freshness, CPU, RSS, and HTTP attempts during dormant, active and held-idle windows. SDK-tagged retries are also reported. Use fresh buckets for repeat runs. Sweep catalog population with a fixed active set, then vary active fraction and hot-index share. Keep the deterministic dormant-catalog request gate as the merge check. Use the wire measurements to choose where to optimize held-idle polling.

A workload comparison qualifies for a pilot decision when both deployments meet the same live targets and pass an equivalent loss and recovery check. Record recovery evidence alongside the comparison JSON. Check reader-cache turnover and cross-zone resource costs separately before applying local measurements to a deployment plan.

A local shakedown on RustFS 1.0.5 used one node per deployment, 16 indices, four eligible for writes, 24 writes at five per second, a one-second refresh interval, and five-second idle windows.

| Deployment | Acknowledged and searchable within 2 s | Ack p99 | Observed freshness p99 | Active HTTP attempts | Active PUTs |
|---|---:|---:|---:|---:|---:|
| Shell | 24/24 | 640 ms | 887 ms | 124 | 41 |
| Stock remote-backed | 24/24 | 657 ms | 920 ms | 70 | 70 |

Two follow-up runs grew the catalog to 64 indices. With four eligible indices and the original hot-index share, both deployments again acknowledged and made 24 of 24 writes searchable within two seconds. The shell made zero dormant HTTP attempts in both five-second catalog windows. At 64 indices and a 50% hot-index share over eight eligible indices, the shell made 176 active HTTP attempts including 57 PUTs. Stock made 106 active attempts, all PUTs. Both met the same live targets.

After hard-killing both processes in the latter run, each restarted from its original home on the same RustFS store. The shell returned all 24 acknowledged documents in 28.8 seconds, and stock returned all 24 in 17.7 seconds. Both met the 90-second process-restart target. These small runs exercise the comparison method. Use longer idle windows and representative workloads to judge deployment economics.

## Client and tenant contract

| Capability | Pilot contract | Evidence |
|---|---|---|
| Document writes and reads | Named indices, explicit IDs, and verified refresh semantics. Exercise conditional updates before admitting clients that depend on them. | REST write, refresh, and replay tests |
| Search | Named-index queries. Check response semantics, including partial results, with the pilot's clients. | REST response tests and the matched workload |
| Access | Trusted callers authenticate at ingress. Restrict access to transport and the object store to the service. | Authentication tests and deployment validation |
| Authorization | Exercise allowed and denied reads with an action filter. Shared untrusted deployment requires enforced identity, negative tests across aliases, wildcard expansion, snapshots and forwarded actions, and measured resource isolation. | Scoped reader filter tests and deployment-specific authorization tests |
| Tenant cost | Use observe mode for the pilot. Enable enforcement after validating complete comparable collector samples and tenant grouping. | Collector self-test for gaps, restarts, and membership changes |

Use the existing REST spec walk for routing coverage. Use semantic tests for client behavior. A storage grouping or cost label alone grants no access to a tenant's data.
