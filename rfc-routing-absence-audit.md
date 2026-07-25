# A1: which code assumes an index has a routing entry

Prerequisite for making a cold index absent from `RoutingTable` (Phase A of
`rfc-scalable-index-metadata-tasks.md`). Every dereference of `RoutingTable.index(...)`,
`indicesRouting().get(...)` and `shardRoutingTable(...)` in `server/src/main`, classified by what
absence should mean there. 58 call sites.

The headline: **the two call sites the plan named as the hard part are the easy part**, and the real
work is a dozen unrelated NPEs.

## Category 1. Already correct, absence is handled (18 sites)

Nothing to do. Listed so a later pass does not re-examine them.

| site | how |
|---|---|
| `ClusterChangedEvent:119` | `hasIndex` guard on both states |
| `MetadataCreateIndexService:277` | guarded by `routingTable().hasIndex(index)` on the line above |
| `TransportBulkAction:687` | passes the table on; `bulkAdaptiveSelectShard` returns null for a null table |
| `TransportBroadcastReplicationAction:172` | fixed already, treats absence as no shards |
| `ActiveShardCount:173` | explicit null check |
| `ClusterStateHealth:98`, `:189` | explicit null check |
| `MetadataIndexStateService:631`, `:765` | explicit null check |
| `OperationRouting:482` | the throw *is* the guard; see category 3 |
| `SnapshotsService:1971`, `:2038` | explicit null check |
| `ScaleIndexClusterStateBuilder:142`, `:177`, `ScaleIndexShardSyncManager:210` | explicit null check |
| `RemoteClusterStateService:487` | keys come from a routing diff, so present by construction |

## Category 2. Would throw NullPointerException on a cold index (12 sites)

These are the actual work of Phase A, and none of them were in the plan's list. All are the same
shape as the `TransportBroadcastReplicationAction` defect fixed earlier: a lookup whose result is
dereferenced without a null check.

**Request paths, reachable by an ordinary user request against a cold index:**

| site | expression | what should happen |
|---|---|---|
| `TransportAnalyzeAction:142` | `.index(concreteIndex).randomAllActiveShardsIt()` | no shard available, not NPE |
| `TransportGetFieldMappingsIndexAction:115` | `.index(concreteIndex).randomAllActiveShardsIt()` | same |
| `TransportUpdateAction:214` | `.index(...).shard(id).primaryShardIt()` | same |
| `TransportUpgradeAction:202` | `indexRoutingTable.allPrimaryShardsActive()` | treat as not-active, skip the index |

**Snapshot paths:**

| site | expression |
|---|---|
| `SnapshotsService:3443` | `indexRoutingTable.shard(i).shardId()` |
| `SnapshotsService:3511` | `indexRoutingTable.shard(i).shardId()` |

Snapshotting a cold index is a real scenario, so these need a decision, not just a guard: is a cold
index snapshotted from its remote state, or skipped?

**Health, via a shared constructor:**

`TieringRequestValidator:141` and `TieringServiceValidator:189` both build
`new ClusterIndexHealth(indexMetadata, indexRoutingTable)`, and that constructor iterates the routing
table directly at `ClusterIndexHealth:157`. So the NPE is one level down and shared. Fixing
`ClusterIndexHealth` to treat null as "no shards, status RED" fixes both call sites and any future one.

**Resize, merge and allocation paths:**

`MetadataCreateIndexService:1991`, `DiskThresholdDecider:668`, `MetadataInPlaceMergeShardService:232`,
`MetadataInPlaceSplitShardService:193`. A cold index arguably should not reach any of these, but they
should reject it rather than NPE. Lower priority than the request paths.

**Clusterless mode:**

`LocalShardStateAction:53` iterates the result directly. Only reachable in clusterless mode, where a
routing entry is being updated for a shard that is starting, so absence may be impossible by
construction. Confirm before touching.

## Category 3. Throws by contract, and that is mostly right (21 sites)

`RoutingTable.shardRoutingTable(...)` throws `IndexNotFoundException` or `ShardNotFoundException` when
absent. Most callers want exactly that: the allocation commands
(`AllocateStalePrimary`, `AllocateEmptyPrimary`, `AllocateReplica`),
`TransportClusterAllocationExplainAction`, `IndexMetadataUpdater`, `SegmentReplication*`,
`IndicesClusterStateService`, `RetentionLeaseActions`, `FailAwareWeightedRouting`. Leave them.

**The two that matter are already almost right.** `TransportReplicationAction:1041` and
`TransportBulkAction:741` both do:

```java
final ShardRouting primary = state.getRoutingTable().shardRoutingTable(request.shardId()).primaryShard();
if (primary == null || primary.active() == false) {
    // retry, wait for allocation
}
```

The retry-and-wait branch the plan wanted them to take **already exists** and is already reached when
`primaryShard()` returns null. The only problem is that `shardRoutingTable(...)` throws before they get
there.

So A3 does not need to change the contract of `shardRoutingTable(ShardId)`, which would ripple across
all 21 callers. It needs a null-returning variant used by exactly these two, whose existing null
branches then do the right thing. That is a much smaller and much safer change than the plan assumed.

## What this changes about Phase A

1. **A3 shrinks.** Add `shardRoutingTableOrNull(ShardId)`, migrate two callers. No contract change.
2. **A2 stands as written**, and is genuinely a behaviour change: `OperationRouting.indexRoutingTable`
   throwing is load-bearing for the 404 that search returns for an unknown index.
3. **A new task is needed** for category 2: twelve NPEs that have nothing to do with the two call
   sites the plan named. Several are plain defects today, in the same way the broadcast one was,
   reachable whenever metadata and routing disagree. They can be fixed independently of whether cold
   indices ever ship.
4. **`ClusterIndexHealth` is a chokepoint** worth fixing once rather than at each call site.
