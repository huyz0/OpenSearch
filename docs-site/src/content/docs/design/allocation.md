---
title: Allocation & Placement
description: Custom allocation deciders, the ExistingShardsAllocator, and reader cache-affinity placement.
---

Shard placement here has to answer two questions core's own defaults don't: which node is even allowed to hold this particular role, writer or reader, and, once a shard has no durable on-disk copy for the default logic to reason about, how allocation works at all.

## Two stateless deciders

**`ReaderShardPlacementAllocationDecider`** (`NAME = "serverless_storage_reader_placement"`) enforces a symmetric node-role split for serverless-storage indices only (a no-op `YES` for every other index): a search-only shard requires `node.attr.serverless_storage_reader: "true"`; a non-reader (writer) shard is forbidden from a reader-designated node. Stateless — a pure function of `ShardRouting`/`RoutingNode`/`RoutingAllocation`, nothing cached.

**`SuspendedShardAllocationDecider`** (`NAME = "serverless_storage_suspended_shard"`) looks up `SuspendedShardsMetadata.isSuspended`/`isReaderSuspended` by role and returns `NO` from both `canAllocate` (keeps an unassigned suspended shard unassigned) and `canRemain` (evicts a started shard on the next reroute pass). As covered on [Scale-to-Zero & Scale-Up](/design/scale-to-zero-scale-up/), `canRemain=NO` alone is not sufficient to force eviction — `ShardSuspensionCoordinator` issues an explicit `CancelAllocationCommand` rather than relying on the balancer alone.

## Why the default gateway allocator can't work here

Core's default gateway allocator answers "which node has an in-sync on-disk copy?" — a question that's meaningless for an object-store-backed shard with no durable local copy at all. Left on the default allocator, every activation would resolve to `NO_VALID_SHARD_COPY` and stay permanently unassigned (confirmed by reading `PrimaryShardAllocator` directly, not inferred).

**`ServerlessStorageExistingShardsAllocator`** implements core's `ExistingShardsAllocator` SPI (`NAME = "serverless_storage"`), selected automatically per-index via `ServerlessStorageIndexSettingProvider`. `allocateUnassigned` skips the gateway fetch entirely: it picks the first decider-approved node and initializes it directly. Correctness is deferred entirely downstream — to [`ShardHead`'s term-fencing CAS](/design/coordination/). A node picked here that shouldn't actually hold the primary simply loses the fencing race on its first publish attempt; allocation doesn't need to get this "right" in the way the gateway allocator does, only "plausible."

### Cache-locality preference, and why its fast path is load-bearing

A reader shard that lands back on the node where its data is already warm in cache starts serving faster than one that lands anywhere else, so placement tries to honor that when it reasonably can. For reader (search-only) shards specifically, node selection checks `ReaderCacheAffinityMetadata.isAffinityFresh` (a TTL-gated staleness check, never a permanent preference) — if the previously-recorded preferred node is still decider-approved and the affinity record hasn't gone stale, that node is preferred over the first-approved fallback.

This preference check is explicitly documented as an **O(1) fast path that must stay O(1)**: a prior regression, caught and fixed, scanned the full approved-node list looking for the preference before falling back, which turned every unassigned reader placement into an O(n) scan instead of an early-break. The javadoc calls this out directly as a caught regression, not a hypothetical concern — worth preserving the fast-path shape in any future change here.

`applyStartedShards` records, for every started reader shard, a fresh `(indexUuid, shardId, nodeId)` observation via `ReaderCacheAffinityRecorder` — this is the write side that feeds the next allocation's preference check.

## `ReaderCacheAffinityMetadata` / `ReaderCacheAffinityRecorder`

The preference check above reads from somewhere; this is where that somewhere is written, and how it decides when to stop trusting what it wrote. `ReaderCacheAffinityMetadata` is a static utility over `IndexMetadata` custom data (separate keys for the node id and the recorded timestamp). `isAffinityFresh` is the sole gate — staleness always wins over permanence, so a node that's since been drained or removed simply ages out rather than requiring an explicit invalidation path. `withShardCacheAffinity` carries a documented `MIN_REWRITE_INTERVAL_MILLIS = 60s` rewrite-throttle guard (fixing a prior bug where the throttle was dead code and every single search hit rewrote cluster state).

`ReaderCacheAffinityRecorder` is a thin, fire-and-forget `ClusterStateUpdateTask(Priority.LOW)` wrapper around that metadata mutation — deliberately low priority, since a missed or delayed cache-affinity update degrades placement quality, not correctness.
