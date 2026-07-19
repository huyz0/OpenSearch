---
title: Plugin Fixes
description: Correctness fixes made to the plugin during review, and the deliberate scope boundaries.
---

For changes to OpenSearch **core** (outside `plugins/serverless-storage`) that were needed to make this plugin possible, see [Core Changes](/core-changes/). This page covers plugin-internal design decisions and the concurrency/correctness fixes made during review.

This plugin was built against a 40-task plan to move it from a fixed shard count decided at index creation toward unbounded, dynamic horizontal partitioning (`dynamic-partitioning-plan.md`). It was followed by many rounds of broadening-scope correctness review across the whole plugin, tracked in `dynamic-partitioning-progress.md`.

## Key design decisions

**Manifest identity for split children.** Rather than inventing a synthetic sub-`ShardId` or a new blob-path convention, split children get an ordinary integer `ShardId` from core's existing (previously unwired) unbounded hash-range split model (`SplitShardsMetadata`/`ShardRange`). That meant the existing flat `<indexUuid>/<shardId>` manifest path worked unmodified — the only new problem was making a child's first manifest reference a hash-range subset of the parent's files without copying bytes, solved by retargeting `ShardCloner`'s existing clone recipe from cross-index to same-index sibling shards.

**WAL batching reuses core's group-commit primitive.** An earlier bespoke per-record future/buffering scheme had real concurrency bugs (orphaned-future deadlock, unbounded blocking `future.get()`). It was replaced with `AsyncIOProcessor`/`BufferedAsyncIOProcessor` — the same primitive `RemoteFsTranslog` already uses — plus the existing `index.translog.durability` setting, instead of inventing new machinery.

**`leaseTerm` on `ShardHead`, distinct from `primaryTerm`.** `primaryTerm` only advances on a real publish. That left a split-brain gap: a partitioned former primary could keep publishing under a stale term because nothing else had advanced yet. `leaseTerm` advances the moment any node acquires or renews the write lease, and fencing checks now compare against it instead of `primaryTerm`.

**PITR retention runs from the reader engine too**, mirroring the writer-side `GcSchedulerTask`. Without this, a shard that scales its writer to zero silently freezes its pin set, defeating the retention window for any shard that isn't actively being written.

## Deliberately out of scope

- **Resharding-by-copy auto-trigger** — investigated, not built.
- **General N-way shard merge** — only a split's own two children merging back is supported; a merge across unrelated shards would need different bookkeeping and wasn't justified.
- **Full snapshot-repository integration** — assessed, deferred.
- **WAL batching enabled by default** — the group-commit fix made it safe, but the default stays off; the risk/benefit for flipping it didn't clear the bar. Dedicated-WAL-stream shards deliberately stay on the legacy synchronous path as a known higher-cost opt-in, not a gap.

## Correctness review: concurrency fixes

A recurring pattern across review rounds: any fix touching shared mutable state under concurrency needed its own adversarial pass, since earlier fixes in this same effort introduced regressions later rounds caught. The fixes below are grouped by theme rather than listed chronologically — see `dynamic-partitioning-progress.md` for the full round-by-round record.

### Writer lease / fencing

- **Split-brain lease fencing gap.** `ObjectStoreCommitHeadPublisher`'s fencing checks compared against `primaryTerm`, which doesn't advance on lease acquisition alone. A stale writer that had lost the lease but not yet attempted a publish could still slip a commit through. Fixed by introducing `leaseTerm` (see above) and fencing on it instead.
- **`TransportEnableWritePartitionRoutingAction` false-ack race** and **`TransportSnapshotPinAction` concurrent-pin race** — two separate transport actions where a concurrent second caller could observe a misleading success acknowledgment; both fixed to re-verify state immediately before acting rather than trusting a pre-check performed earlier in the request.

### Cache and lock safety

- **`LocalDiskCachingBundleStore` unbounded lock-map leak**, then a **self-caught regression**: the first fix (removing a per-key lock entry on eviction) was itself added without holding that key's own lock, which could let a concurrent in-flight writer's lock be silently dropped and re-minted, breaking mutual exclusion. Found by a dedicated adversarial self-review pass targeting the plugin's own recent diffs, not general review. Fixed by synchronizing the delete-and-remove on the entry's own lock.
- **`InMemoryPlaintextBundleCache` double-counted bytes.** Two concurrent misses for the same key could both reach the cache's `put()`, and the byte-accounting unconditionally added the new entry's size — double-counting when the second `put()` overwrote the first. Fixed by subtracting the previous entry's size (from `Map.put`'s own return value) before adding the new one.
- **`WalBatchingProcessor.put` backlog-bytes leak** on an interrupted queue put, and a **widened try/finally** elsewhere in the WAL path with the same shape — a byte counter incremented before a blocking call that could throw, without a matching decrement on the exceptional path.

### Scheduler and resource lifecycle

- **`ServerlessStoragePlugin#close()` leaked five node-level schedulers.** Only the WAL GC scheduler was being cancelled on plugin shutdown; the scale-to-zero, scale-up, data-stream shard-count advisor, in-place split, and in-place merge scheduler tasks were not, leaking their background threads past node shutdown.
- **PITR reconciliation scheduled from the reader engine too**, closing the freeze-on-scale-to-zero gap described above.
- **Stale comments** referencing a removed `CompactionSchedulerTask.isLeaseHeldAt` guard, cleaned up as part of the same pass.

### Validation and request handling

- Rejecting self-referential and conflicting resharding requests: `EnableWritePartitionRouting` alias-equals-target, alias reassignment to a target already owned by a different alias or a fenced source, duplicate targets/sources, negative thresholds.
- `ShardCloneRequest` self-clone (source == target) rejection; `ShardCloner` cross-source-retry pin leak fix; lineage-chain walking fixed to traverse the full chain rather than one hop for clone/split fallback reads.
- Migration commit pinning switched from a deprecated `acquireLastIndexCommit` call to `acquireLastIndexCommitAndRefresh`, and from raw `Store` ref-counting to a `GatedCloseable<IndexCommit>` try-with-resources, closing a window where the commit being migrated could be deleted out from under the migration.
- A write-block-before-migration check, to prevent a real data-loss path where migration could read a shard that was still quiescing.
- `PitrRetentionPolicy` given a deterministic tie-break, and `retentionWindowMillis` bounded to prevent overflow.

### REST/transport hardening

- `shard_id` parsing in REST handlers guarded against malformed input instead of throwing an unguarded `parseInt`.
- `WalGcSchedulerTask` changed to skip a single bad shard rather than aborting its entire sweep on one failure.
- `WalChunkReader` record count bounded before allocation, closing a potential unbounded-allocation path from a malformed chunk.

## Verification discipline

Every fix in this list went through: implement → compile → add a regression test → **break the fix and confirm the regression test fails with the predicted symptom** → restore the fix and confirm green → run the full `:plugins:serverless-storage:test` suite. Concurrency fixes additionally used tests built to force the actual race (e.g. two-thread `CountDownLatch`-gated tests), not just a happy-path assertion.
