# Dynamic Horizontal Partitioning — Execution Progress

Tracks execution of the 40-task breakdown of `dynamic-partitioning-plan.md`. One
section per completed task. Numbering matches the task list handed to the user
(1-40), cross-referenced to the plan's own `0.x`/`1.x`/... item numbers where
applicable.

## Task 1 — Read `MetadataInPlaceSplitShardService` et al., document the real transition sequence

Status: **done** (research only, no code changes).

### Files read in full

- `server/src/main/java/org/opensearch/cluster/metadata/MetadataInPlaceSplitShardService.java`
- `server/src/main/java/org/opensearch/action/admin/indices/split/InPlaceSplitShardClusterStateUpdateRequest.java`
- `server/src/main/java/org/opensearch/cluster/metadata/SplitShardsMetadata.java`
- `server/src/main/java/org/opensearch/cluster/metadata/ShardRange.java`
- `server/src/test/java/org/opensearch/cluster/metadata/MetadataInPlaceSplitShardServiceTests.java`
- `server/src/test/java/org/opensearch/cluster/metadata/SplitShardsMetadataTests.java`
- `server/src/test/java/org/opensearch/cluster/metadata/ShardRangeTests.java`

### The transition sequence today (the whole thing is one step)

1. A caller (currently **only test code** — no REST/transport action exists)
   builds an `InPlaceSplitShardClusterStateUpdateRequest(cause, index, shardId,
   splitInto)` and calls `MetadataInPlaceSplitShardService.split(request,
   listener)`.
2. That submits an `AckedClusterStateUpdateTask` at `Priority.URGENT`,
   throttled by the `ClusterManagerTask.IN_PLACE_SPLIT_SHARD` key.
3. `execute()` calls the static `applySplitShardRequest(currentState, request,
   allocationService::reroute)`, which:
   - validates: index exists; `numberOfVirtualShards == -1` (incompatible
     with virtual shards); all nodes on the same version `>= V_3_7_0`; split
     not already in progress/already committed; primary shard is `started()`
     and not `relocating()`.
   - builds a new `SplitShardsMetadata` via `Builder.splitShard(shardId,
     splitInto)` — pure metadata bookkeeping: computes child `ShardRange`s,
     assigns child shard IDs (reusing holes from cancelled splits), validates
     contiguous non-overlapping ranges, marks the parent as
     "in-progress-split".
   - builds a **routing table that is an unmutated copy of the current one**
     (`RoutingTable.builder(currentState.routingTable()).build()` — no shard
     added, removed, or touched).
   - calls `AllocationService::reroute` on that unchanged routing table (a
     no-op for split purposes, since there's nothing new to allocate).
4. Task completes; listener acknowledges once cluster-manager publishes.

There is no second phase in production code. `SplitShardsMetadata.Builder`
also has `updateSplitMetadataForChildShards` (in-progress → committed) and
`cancelSplit` (abort), but **nothing outside unit tests calls either** — no
service watches for child-shard recovery completion and finalizes or rolls
back a split.

### Parent / in-progress-child / committed-child states (all inside `SplitShardsMetadata`)

- **Never split**: shard ID in `activeShardIds`, no entry in
  `parentToChildShards`.
- **Parent, split in progress**: shard ID added to `inProgressSplitShardIds`
  *and* still present in `activeShardIds` (not removed until commit);
  `parentToChildShards.get(id)` holds the new `ShardRange[]`.
  `isSplitParent(id)` is `activeShardIds.contains(id) == false &&
  parentToChildShards.containsKey(id)` — **false** while in progress, since
  the parent is still active.
- **In-progress child**: its `ShardRange` lives in `parentToChildShards`,
  but it is **not** in `activeShardIds` and **not** in
  `rootShardsToAllChildren` yet (that array only updates on commit).
  `SplitShardsMetadata.isRecoveringChild(childId, parentId)` is the query for
  "is this a not-yet-committed child of this in-progress parent."
- **Committed child**: produced by `Builder.updateSplitMetadataForChildShards`
  — validates the child IDs match what was reserved, recomputes the root's
  full `ShardRange[]` into `rootShardsToAllChildren`, adds children to
  `activeShardIds`, removes the parent from `activeShardIds` and from
  `inProgressSplitShardIds`. After this, `isSplitParent(parentId)` is true and
  `getShardIdOfHash` resolves through the committed ranges.
- **Cancelled**: `Builder.cancelSplit(id)` removes the in-progress marker and
  frees the reserved child IDs as reusable holes.

### Implemented vs. missing

Implemented: the `SplitShardsMetadata`/`ShardRange` data model (range math,
ID allocation with hole reuse, contiguous/non-overlap validation,
`MINIMUM_RANGE_LENGTH_THRESHOLD = 1000`, serde, nested splits);
`applySplitShardRequest`'s validation + metadata mutation;
`OperationRouting.calculateShardIdOfChild` → `getShardIdOfHash` for
query-time hash routing (this already understands in-progress children).

Missing/stubbed (confirmed by direct read + grep, zero hits across
`cluster/routing/**`):

- **No REST or transport action.** `action/admin/indices/split` contains only
  the cluster-state-update DTO and a `package-info.java`. This is exactly the
  gap task 17 (Phase 0.6, "operator-only REST/transport action") needs to
  fill — it is not yet present anywhere in core.
- **No `IndexRoutingTable`/`RoutingNodes` wiring at all.** The routing table
  built in step 3 above is byte-for-byte unchanged. Nothing reads
  `SplitShardsMetadata` from `IndexRoutingTable.java`, `RoutingTable.java`,
  or anywhere under `cluster/routing/allocation/`. This is exactly task 6-9's
  target (Phase 0.2).
- **No commit/cancel caller.** Nothing finalizes or aborts an in-progress
  split outside unit tests. This is a **new gap not previously called out in
  the plan** — recorded as a new task (see below).
- **`RecoverySource.Type.IN_PLACE_SPLIT_SHARD` / `InPlaceSplitShardRecoverySource`**
  (`RecoverySource.java`) exist as a forward-declared data shape but nothing
  constructs a `ShardRouting` using them yet — exactly the seam task 11
  (Phase 0.3) targets.
- **Parent shard routing is never touched** during an in-progress split (no
  read-only/relocating-like transition) — worth folding into the Part 3
  read-path-correctness design question already in the plan, not a new risk.

### New task added to the backlog (auto-added per `/goal` authorization)

**Task 8.5 — Design + implement the split commit/cancel driver.** Nothing in
core today watches in-progress splits and calls
`SplitShardsMetadata.Builder.updateSplitMetadataForChildShards` (commit) or
`cancelSplit` (abort). This must exist before Phase 0 is usable end-to-end:
once children recover (task 8's new `ShardRouting`s reach `STARTED`), *something*
has to commit the split in cluster state, and if a child recovery fails,
*something* has to cancel it and free the reserved IDs. Inserted into the
task list between the current task 8 (`AllocationService`/`RoutingTable`
wiring) and task 10 (`RecoverySource` dispatch seam), since the commit driver
and the recovery-source hook are two halves of the same completion signal and
should be designed together.

## Task 2-3 — Audit `SplitShardsMetadataTests`/`ShardRangeTests` coverage

Status: **done** (research only, no code changes).

Enumerated every `test*` method in both suites (`ShardRangeTests`: 22 tests;
`SplitShardsMetadataTests`: 68 tests). Coverage is thorough for the data
model itself: range comparison/containment/serde (`ShardRangeTests`),
`getShardIdOfHash` for no-children/in-progress/completed/consecutive splits,
`splitShard` including nested splits and hole-reuse after a cancelled split,
`updateSplitMetadataForChildShards` (commit) success and every validation
failure mode, `cancelSplit`, active-shard iteration, and full
stream/XContent serde round-trips for every state combination
(`SplitShardsMetadataTests`).

**No test in either suite touches `IndexRoutingTable`, `RoutingTable`,
`AllocationService`, or `ShardRouting`** — confirms this is purely a
metadata-model test suite with zero coverage of (and therefore zero
implicit design guidance for) the routing/allocation side. This matches
Task 1's finding exactly: the gap is entirely in the routing layer, not
hidden test debt in the metadata model. No new tasks added by this pass.

### Answer to the key open question

**Does `IndexRoutingTable`/`RoutingNodes` gain `ShardRouting` entries for
children during a split today? No — entirely absent.** This confirms tasks
6-9 (Phase 0.2) are exactly the right next unit of work, not already partly
done. Proceeding to task 2/3 next (finish reading the test suites in
isolation was folded into this pass) and then to the `AllocationService`
design (tasks 5-6).
