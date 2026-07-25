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

**Request paths, reachable by an ordinary user request against a cold index (ALL FOUR FIXED):**

| site | expression | what should happen |
|---|---|---|
| `TransportAnalyzeAction:142` | `.index(concreteIndex).randomAllActiveShardsIt()` | no shard available, not NPE |
| `TransportGetFieldMappingsIndexAction:115` | `.index(concreteIndex).randomAllActiveShardsIt()` | same |
| `TransportUpdateAction:214` | `.index(...).shard(id).primaryShardIt()` | same |
| `TransportUpgradeAction:202` | `indexRoutingTable.allPrimaryShardsActive()` | treat as not-active, skip the index |

All four now guard. The first three return an empty iterator rather than null: an empty one becomes a
`NoShardAvailableActionException` in `TransportSingleShardAction`, or a retry in
`TransportInstanceSingleOperationAction` which already has that branch for "between index gateway
recovery and shardIt initialization"; null would instead mean "execute locally", which is wrong here.

**Test coverage is uneven, deliberately.** `TransportUpgradeAction.indicesWithMissingPrimaries` is
covered by a unit test that reproduces the original NullPointerException, after making the method
package-private and static -- it reads nothing from the instance, so that is honest rather than a
testability hack.

The other three are covered by inspection only. Each is a `shards()` override, and reaching one from a
test needs more scaffolding than the guard deserves:

- `TransportAnalyzeAction` and `TransportGetFieldMappingsIndexAction` take
  `TransportSingleShardAction.InternalRequest`, a protected inner class whose constructor is
  package-private, so only a test inside `org.opensearch.action.support.single.shard` can build one.
- `TransportUpdateAction.shards` takes a public `UpdateRequest`, but `shardId` has no public setter, so
  a test cannot put the request into the state the guarded branch requires.

Left uncovered rather than either faking coverage or restructuring three production classes for it.
The natural place to catch these is an integration test that stands up a node, which is where the
cold-index work will need one anyway.

**Snapshot paths:**

| site | expression |
|---|---|
| `SnapshotsService:3443` | `indexRoutingTable.shard(i).shardId()` |
| `SnapshotsService:3511` | `indexRoutingTable.shard(i).shardId()` |

Snapshotting a cold index is a real scenario, so these need a decision, not just a guard: is a cold
index snapshotted from its remote state, or skipped?

**Health, via a shared constructor (FIXED):**

`TieringRequestValidator:141` and `TieringServiceValidator:189` both build
`new ClusterIndexHealth(indexMetadata, indexRoutingTable)`, and that constructor iterates the routing
table directly at `ClusterIndexHealth:157`. So the NPE is one level down and shared.

Fixed at the two callers rather than in the constructor. `ClusterIndexHealth` is `@PublicApi`, and
`ClusterStateHealth:98-102` -- the primary consumer, and the thing that defines what health means --
already skips an index whose routing entry is null instead of reporting on it. Teaching the
constructor to accept null would have invented a health status for a state core deliberately excludes
from health. The two validators now follow the existing convention: not healthy enough to tier.

**Resize, merge and allocation paths (ALL FOUR FIXED):**

`MetadataCreateIndexService:1991`, `DiskThresholdDecider:668`, `MetadataInPlaceMergeShardService:232`,
`MetadataInPlaceSplitShardService:193`.

Shrink validation and the disk decider now fall through to their existing behaviour: no routing entry
means no started shards, which is the condition shrink's "must have all shards allocated on the same
node" error already describes, and means the decider contributes no shard size and uses its default.
The two in-place resharding services reject with a clear message, since splitting or merging an index
with nothing to split or merge from is not a state worth continuing into.

**Not reachable today, unlike the request paths.** These are only reached for a source index that has
been closed or write-blocked, and closing goes through
`RoutingTable.Builder#addAsFromOpenToClose`, which keeps the routing entry. What does *not* keep it is
`addAsNew`, which skips an index whose state is `CLOSE` -- so the state is straightforward to
construct, and is what the tests use, but no live path was found that produces it. Recorded as a
latent guard rather than a live defect.

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
