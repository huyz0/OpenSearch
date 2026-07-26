# Area C task plan: computed placement

Parent: `plan-100m-index-implementation.md` Part 5, Area C. Evidence: S6 (allocator ceiling) and S12
(rendezvous placement measured at theoretical optimum) in `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`.

## Why this area

S6 measured the allocator binding at the order of 100k active shards, with cold allocation superlinear.
The target is 100M indices at up to 100 shards each, so up to 10B shards. Five orders of magnitude is not
a tuning problem, so the global pass has to stop happening.

S12 measured the replacement: rendezvous hashing with K=3 candidates moves exactly `added/(N+added)` of
primaries at every fleet size, distributes within 5% of even, and costs 80 to 359 ns per lookup. The
design risk is retired. What is left is integration, and integration is where the risk now lives.

## C0: surface audit, done first because the plan depends on it

Counted in `server/src/main/java`:

| surface | count |
|---|---|
| files that **write** a `RoutingTable` | 20 |
| files that **read** `routingTable()` | 70 |
| files that use `RoutingNodes` | 26 |
| files touching shard state transitions | 8 |

The writers, grouped, because the grouping is what decides the approach:

- **Index lifecycle**: `MetadataCreateIndexService`, `MetadataDeleteIndexService`,
  `MetadataIndexStateService`, `MetadataUpdateSettingsService`.
- **Allocation**: `AllocationService`. The one that matters.
- **Resharding**: `MetadataInPlaceSplitShardService`, `MetadataInPlaceSplitShardCommitService`,
  `MetadataInPlaceMergeShardService`, `MetadataInPlaceMergeShardCommitService`.
- **Recovery and restore**: `RestoreService`, `RemoteStoreRestoreService`, `LocalAllocateDangledIndices`,
  `ClusterStateUpdaters`.
- **Shard state**: `LocalShardStateAction`.
- **Other**: `ScaleIndexClusterStateBuilder`, `TieringService`, `TransportClusterStateAction` (filtering),
  `ClusterManagerService`, `NoopRemoteRoutingTableService`, `OperationRouting` (the A2 degradation path).

**What the audit changes about the approach.** 70 readers against 20 writers means replacing the
*representation* is far cheaper than replacing the *interface*. Keep `RoutingTable` and
`IndexRoutingTable` as the types every reader already uses, and change where an entry comes from. That is
the same trade C5 made for `IndexMetadata`, and it is why the holder pattern is the right precedent
rather than a convenient one.

**The transition observers are concentrated, which is the good news.** `initializeShard`, `startShard` and
`relocateShard` are reached from `RoutingNodes`, `AllocationService`, the two balancers and
`MoveAllocationCommand` -- all allocator-internal, none of which run for a serverless index under computed
placement. Only `SnapshotsService` and `IndexService` observe transitions from outside the allocator, and
those two need reading closely rather than assuming.

## Tasks

Each is done when it compiles, has tests, **each test has been shown to fail without its production
change**, and the affected suites pass.

### Phase 1: the placement function

**C1. `RendezvousShardPlacement`.** Promote the algorithm from `ComputedPlacementSpike` into production.
- Input: an ordered node list and `(indexUuid, shardId)`. Output: K ordered candidates.
- The hash must be pinned and a test must assert its output does not change, because two coordinators
  disagreeing about placement is a split view of the cluster, not a performance problem.
- Keep the spike's partial-selection loop rather than sorting; N is in the hundreds and K is small.

**C2. Node set snapshot.** Define "eligible" (role, health, not draining) and where the snapshot comes
from. Two coordinators computing against different node lists produce different placement, so this is a
correctness input, not a convenience.

### Phase 2: the seam

**C3. `IndexRoutingTableHolder`.** Mirror C5 exactly: an interface the concrete type implements, so no
existing caller changes, plus a lazy implementation that computes on first access per index.

**C4. Computed `IndexRoutingTable`.** Build shard routing entries from C1 rather than from allocator
output. Writers get top-1 as primary, search nodes get the rest as search-only replicas.

**The known trap, from A5.** `addAsRecovery` picks its recovery source from `inSyncAllocationIds`, and
under computed placement those are absent, so it silently falls back to `EmptyStoreRecoverySource` and
resurrects a live index as blank. Build the entry explicitly with `ExistingStoreRecoverySource`, and write
the test that fails when it is not.

**C5. Gate on index type.** Only serverless indices take this path; everything else keeps the allocator.
A setting, defaulted off, exactly as C5 and C6 shipped.

**C6. Bypass the allocator.** A serverless index must not enter `AllocationService.reroute` at all. Check
what `SuspendedShardAllocationDecider` and `ServerlessStorageExistingShardsAllocator` still need to do and
remove what becomes dead.

### Phase 3: the request paths

**C7. Search.** Feed the K candidates to adaptive replica selection instead of the routing table's replica
list. Verify `OperationRouting` accepts hash-derived candidates unmodified -- this is the assumption the
area rests on and it has not been checked.

**C8. Write.** Top-1 as a hint, `ShardHead` CAS decides. A writer that is not the lease holder must fail
cleanly and the request retry against the new owner.

### Phase 4: what the audit says needs reading rather than assuming

**C9. `SnapshotsService` and `IndexService` transition observers.** The only two outside the allocator.
Read them, decide what a computed-placement index means for each, and test it.

**C10. The other nineteen writers.** For each, decide: no-op for serverless, unchanged, or needs work.
Resharding is the group most likely to need real work, since split and merge manipulate routing directly.

### Phase 5: measurement

**C11. Re-measure the ceiling.** S6's numbers were taken against the allocator. Re-run the same harness
with computed placement on and confirm cold allocation becomes flat rather than superlinear. This is the
claim the whole area exists to make, and it should be a number rather than an argument.

## Acceptance

- A cluster with serverless indices performs no global reroute.
- Placement for a given shard is identical on every coordinator.
- A search request reaches one of the K candidates.
- A writer that is not the lease holder is rejected rather than permitted to write.
- Recovery source is never `EmptyStoreRecoverySource` for an index with data, asserted directly.
- Cold-start time is flat in index count, measured.

## Risks

- **Blast radius.** 70 readers. Keeping the interface is what makes this tractable, and any change that
  forces readers to adapt should be treated as a design failure rather than as work.
- **The A5 trap.** It has bitten once, silently, and this area recreates the exact condition.
- **Hot-tenant skew.** A hash cannot know one tenant takes a thousand times the traffic. K=3 gives room to
  choose but does not solve it. An override list for known-hot indices may be needed.
