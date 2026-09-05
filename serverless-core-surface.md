# What the serverless shell costs core

Branch: `feature/serverlessnode`. Base: `main` (`fae98a3a5f3`), which is the fork point, so
every number here is this branch's own doing.

This document exists because the question "can we minimise core change" had been answered by
argument rather than by measurement. It is now measured.

## The answer

Core's delta against main went from **24,313 lines across 344 files** to **8,681 across 226**,
and none of the remainder is the shell's.

| | files | lines | why it is still here |
| --- | --- | --- | --- |
| blobstore CAS register | 4 | 313 | **the shell needs it** |
| data-format engine + mapper capability hooks | ~49 | ~2,700 | `sandbox/` consumes it (174 files) |
| native memory / allocator / admission control | ~13 | ~600 | `sandbox/` (36 files) |
| tiering | ~11 | ~850 | `sandbox/` |
| assorted fork features | rest | rest | batching, replica defaults, WLM, null-safety fixes |

## What the shell actually needs from core

Two things, and they are the whole list.

**1. The object-store compare-and-swap register** — `BlobRegister`, `BlobRegisterCasResult`,
three methods on `BlobContainer` (`readRegister`, `compareAndSwapRegister`,
`createRegisterIfAbsent`) and the filesystem implementation in `FsBlobContainer`. 313 lines.
The shell reaches them from 161 call sites; the whole safety argument -- ownership, fencing,
descriptor writes -- rests on this primitive. Implementations for S3, Azure and GCS live in the
repository plugins, outside core.

**2. `Engine#engineRecoveryOperations()`** — `ServerlessWriterEngine` overrides it to replay an
object-store log during recovery, and `IndexShard` calls it. Easy to miss: it is a new method on
a class main already had, so a survey that compares imported *classes* does not see it. That is
exactly how an earlier version of this analysis got it wrong.

Everything else the shell touches in core is API main already had. Checked and found unused:
`RestHandler#apiAvailabilityScope`, `Plugin#getAdditionalHealthPaths`, `PluginNodeStats`,
`BlockCache#tieredStats`, `MappedFieldType#searchCapability` -- zero references in `serverless/`.

There is one coupling worth naming honestly rather than counting as a dependency: `ServerlessNode`
constructs `IndicesService` and `SearchService` directly, so it tracks their constructor
signatures. Those signatures differ from main because of fork features (the data-format store
directory factories, the workload-group service), not because of anything serverless. Against
stock main the shell would pass main's arguments.

## What was removed, and what nearly went with it

Two seam groups came out. In-place shard merge, the shard recovery strategy and engine-native
snapshot went first; the metadata plane -- descriptors, index catalogs, creation strategies,
claimed-index lifecycle, absent-index suppliers, gated residency, computed routing, the
resolver-aware accessors -- and remote cluster state's manifest sharding went second.

The two original pluggability commits interleaved genuine fork features with the seams inside
the same files, so restoring a file to main silently dropped features that had nothing to do
with serverless. Six were found, every one by a failing test rather than by reading:

- create-index batching (`submitStateUpdateTasks`, so concurrent creations collapse into one
  cluster-state update)
- the empty-node-set fix in `validateIndexTotalPrimaryShardsPerNodeSetting` -- `allMatch` is
  vacuously true on no nodes, so an empty cluster counted as remote-store enabled
- two null-safety fixes on the bulk adaptive-shard path
- `MergeDrainTimeoutException`'s exception id, which tiering still needs
- the split-into-zero guard, kept and relaxed to reject only zero because main permits one child
- the rollover test's `AliasValidator`, which is what makes an explicit-write-index rollover
  regression visible at all

All six are kept. The lesson for anyone continuing this: **restore-to-main is not a safe default
for these files; cut by hand and let the tests find what you took.** `MetadataCreateIndexService`
is cut by hand for precisely this reason.

## Verification

- `:server:test` -- 20,088 tests, 1 failure: `NativeAllocatorPoolStatsTests#testToXContent`,
  which fails identically before any of this work and is native-memory work unrelated to it.
- `:serverless:testkit:test` -- 549 tests, 0 failures.
- `:serverless:testkit:processTest` (forked JVMs), `pluginTest` (a real plugin installed from its
  assembled zip), `tlsTest` (a real handshake) -- 18 tests, 0 failures.
- `:serverless:shell:checkServerDoesNotReferenceServerless` and
  `:serverless:auth:checkAuthDoesNotReferenceTheShell` both pass: no file under `server/` names
  `org.opensearch.serverless`, and the auth plugin still reaches only the plugin API.
- `s3Test` needs a live MinIO/SeaweedFS endpoint and was not run here; it assume-skips without one.

## What is left to decide

The remaining ~8,400 lines are the fork's other feature lines, not serverless. They are
separable onto their own branches whenever someone wants to; nothing in `serverless/` references
any of them. Removing them would mean removing `sandbox/` too, which is a product decision rather
than a cleanup.
