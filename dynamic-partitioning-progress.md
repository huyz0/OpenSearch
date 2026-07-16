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

## Task 12 — Manifest-identity design spike (§0.4)

Status: **done** — design decision only, per the plan's own instruction not
to guess this in isolation. No code changes in this entry; implementation
is Task 13-14, scoped separately below.

### What was investigated

- **Today's manifest identity convention**: `ServerlessStoragePlugin.blobContainerFor(indexUuid,
  shardId)` builds the container path as literally `BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId))`
  — flat `<indexUuid>/<shardId>`. `CommitManifest`'s identity fields are
  exactly `(indexUuid, shardId, primaryTerm, generation)`; `ShardHead` is a
  single scalar `(primaryTerm, leaseHolderNodeId, leaseExpiryMillis,
  latestManifestGeneration)` pointer — one linear generation sequence per
  shard, no branching/lineage field anywhere.
- **`ShardCloner.clone`/`ShardSplitter.split`** (this plugin's existing,
  unrelated zero-copy machinery, built for producing a brand-new *index* from
  a source shard): both give the target a **whole new `(indexUuid, shardId)`
  identity** with a full manifest at generation 1, whose `files()` simply
  point at the *same* bundle names the source manifest already references —
  zero-copy is achieved by not copying bytes, not by any stored reference
  count. GC-safety comes from `DurablePinRegistry` pinning the exact source
  `(primaryTerm, generation)` before the clone's manifest is even read, plus
  a tiny `CloneLineage` record (`(sourceIndexUuid, sourceShardId)`) written
  into the target's own container purely so `deleteClone` can find its way
  back to release that pin later.
- **`ShardPartitionDescriptor`**: a write-once `(partitionIndex, numPartitions)`
  record, explicitly documented as lifetime-immutable ("a shard's partition
  assignment never changes across its own lifetime... a further split would
  create new target shards of its own"). Confirmed this matches the plan's
  own claim exactly — no discrepancy found.
- **No existing precedent anywhere** (`WalChunkService.ShardKey`,
  `ManifestRetentionPolicy`, `ShardHead`) for multiple concurrent manifest
  lineages, sub-shard IDs, or range/generation tags under one core `ShardId`
  — confirmed via exhaustive grep across `shardstate/`, `manifest/`, `wal/`,
  `gc/`, `clone/`, `resharding/`.

### The decision

**Reject both candidate shapes 0.4 originally proposed** ("synthetic
sub-`ShardId`" and "new blob-path segment"). Neither is needed, because
Task 5-9's `AllocationService`/`RoutingTable` wiring already gives every
child shard a **real, ordinary core `ShardId`** — a genuinely new integer
shard ID reserved by `SplitShardsMetadata.Builder.splitShard` (via
`maxShardId`), not a synthetic or derived one. The child is, from the
storage layer's point of view, just another shard of the *same index*
(same `indexUuid`) that happens to have a new integer ID. That means the
existing flat `<indexUuid>/<shardId>` convention **already accommodates
it with zero changes** — `blobContainerFor`, `CommitManifest`, `ShardHead`,
`WalChunkService.ShardKey`, and `ManifestRetentionPolicy` all continue to
work completely unmodified, because none of them assumed anything about
how a `shardId` integer came to exist.

The genuinely new design problem was never "where do we put the child's
manifest" — it was mis-scoped in the original plan wording. It's actually:
**how does the child's first manifest reference a hash-range subset of the
parent's bundle files without physically copying them or waiting for a
real merge**. That problem already has a solved precedent one layer up:
`ShardSplitter.split`'s exact recipe (clone the manifest's `files()`
verbatim via `ShardCloner.clone`, protect them with a `DurablePinRegistry`
pin on the source generation, attach a small write-once partition
descriptor) — just retargeted from "new index, new shardId" to "same
index, new shardId, source is a sibling shard of the same index instead of
a cross-index source."

**Concrete shape for Task 13-14's implementation** (not built in this
entry):

1. `Engine#recoverFromInPlaceSplit(ShardId childShardId)` (Task 10-11's
   hook), on the plugin's `ObjectStoreWriterEngine`, resolves the parent
   shard ID from `IndexMetadata.getSplitShardsMetadata()` (findable via a
   reverse lookup — `getChildShardsOfParent` iterated over in-progress
   parents, or a new small helper; exact lookup mechanics are Task 13's
   detail, not blocked on anything found in this spike).
2. Reuses `DurablePinRegistry` to pin the parent's current `(primaryTerm,
   generation)`, exactly as `ShardCloner.clone` does today for a
   cross-index source.
3. Writes a full `CommitManifest` at `(indexUuid, childShardId,
   primaryTerm=1, generation=1)` whose `files()` are the parent's current
   manifest's `files()` verbatim — no bytes copied, same mechanism
   `ShardCloner`/`ShardSplitter` already use.
4. Writes a partition-descriptor-equivalent scoped to the child's
   `ShardRange` (not `ShardPartitionDescriptor`'s `(partitionIndex,
   numPartitions)` shape, since core's hash-range split model is a
   different partitioning scheme than this plugin's existing equal-count
   partition split — this is a genuinely new record type, small, following
   `ShardPartitionDescriptor`'s own "write-once, separate from
   `CommitManifest`" precedent).
5. A `CloneLineage`-equivalent (or a straightforward reuse of the same
   class, since its `(sourceIndexUuid, sourceShardId)` shape already fits —
   `sourceIndexUuid` just happens to equal the child's own `indexUuid`) for
   later pin release once the split fully commits or is cancelled (Task
   8.5's driver already has the commit/cancel signal; wiring pin release
   into it is Task 13-14 scope).
6. `ShardPartitionDescriptor`'s own lifetime-immutable contract (flagged by
   the plan as needing an update, Phase 0.5 / task in this breakdown) is
   untouched by this decision — it's a separate, existing record for this
   plugin's *own* equal-count split mechanism, not reused here.

### Why this matters for Part 2d's reconciliation question

This closes a real ambiguity Part 2d of the plan already flagged: whether
`OrchestrateShardSplitAction` (this plugin's own operator-triggered
split-and-clone action, built earlier this session) and the new in-place
split mechanism are the same thing wearing two names, or two different
things. They are now confirmed **architecturally sibling, not identical**:
`OrchestrateShardSplitAction` produces a brand-new index (new `indexUuid`)
via `ShardSplitter`/`ShardCloner`, while in-place split keeps the same
`indexUuid` and only grows the shard count within it, via core's
`SplitShardsMetadata`/routing machinery from Task 5-9 plus a manifest
strategy that borrows `ShardCloner`'s zero-copy recipe but retargets it.
Both are legitimate, coexisting tools for different scaling shapes (new
index vs. more shards of the same index) — this was already Part 2d's own
conclusion; this spike confirms the storage-layer mechanics support that
conclusion without contradiction.

## Task 15 — `ShardPartitionDescriptor` lifetime-immutable contract

Status: **resolved as not-applicable, no code change** — superseded by
Task 12's finding.

The original task list (written before Task 12's design spike) assumed
`ShardPartitionDescriptor`'s "never re-split" contract would need updating
for the new in-place split feature. Task 12 found this doesn't apply:
`ShardPartitionDescriptor`'s `(partitionIndex, numPartitions)` shape is
specific to *this plugin's own, pre-existing* equal-count split mechanism
(`ShardSplitter`/`ShardCloner`, which produces a brand-new index), and
cannot represent core's hash-range `ShardRange` model at all. Task 12's
decision was that in-place split needs its own new, separate range-scoped
record — it does not read, write, or touch `ShardPartitionDescriptor`.

Re-examined the original task's premise directly: does *this plugin's own*
split mechanism actually need to support re-splitting a target in place?
No — `ShardSplitter`/`ShardCloner`'s whole design is "produce a brand-new
index/shard identity," and a target needing further splitting would, under
that same design, just get split again into further brand-new targets
(exactly what the existing javadoc already says: *"a further split would
create new target shards of its own"*). That's not a limitation to fix —
it's consistent with how that mechanism already works, and changing it
would be unrelated scope creep relative to the actual goal (in-place
growing shard count via core's new mechanism), not a real gap. Closing
this task without a code change; flagging it so it isn't silently dropped.

## Task 13 (started) — `SplitShardsMetadata.getParentAndRangeOfChild`

Status: **first increment done and committed**; the remaining plugin-side
engine wiring is a large, separately-scoped unit of work, tracked below as
still-open.

Task 12's implementation shape needs, as its first step, a way for a
plugin engine to find "who is my parent, and what's my `ShardRange`" given
only its own child shard ID — and Task 12's research pass found this was a
genuine, previously-unnoticed gap: `SplitShardsMetadata` only exposed
parent→children lookups (`getChildShardsOfParent`/`getChildShardIdsOfParent`),
never the reverse. Added `SplitShardsMetadata.getParentAndRangeOfChild(int
childShardId)` (`server/src/main/java/org/opensearch/cluster/metadata/SplitShardsMetadata.java`),
returning `Tuple<Integer, ShardRange>` or `null`, scoped deliberately to
only the in-progress-split window (matches the only case that actually
needs it: `Engine#recoverFromInPlaceSplit` fires during initial child
recovery, before the split commits).

4 new tests in `SplitShardsMetadataTests`: resolves correctly during an
in-progress split, returns `null` for a shard that isn't a child of
anything, returns `null` once the split has committed (deliberately scoped
— a committed child's lineage lives in `rootShardsToAllChildren`, a
different, already-existing lookup path this method doesn't duplicate),
and returns `null` after a cancelled split. Verified meaningfulness: forced
the method to always return `null`, confirmed 1/4 test failed as expected
(the two already-`null`-expecting tests trivially "passed" even when
broken, which is why the positive-case test's independent failure is the
real signal here), restored, confirmed clean.

**Still open (the substantial remaining part of Task 13-14)**: wiring
`ObjectStoreWriterEngine#recoverFromInPlaceSplit` to actually use this
lookup plus `ShardCloner`'s zero-copy recipe. The research pass for this
found real, non-trivial gaps that need their own follow-up design/implementation
pass, not a quick addition:
- `ObjectStoreWriterEngine` currently has no way to reach a `BlobContainer`/
  `BlobContainerManifestStore`/`ShardStateStore`/`DurablePinRegistry` for
  an arbitrary sibling shard ID within the same index — only its own. This
  needs new constructor plumbing (e.g. an `IntFunction<BlobContainer>`
  resolver, mirroring `ServerlessStoragePlugin#resolveBlobContainer`'s
  existing shape) threaded through `WriterEngineFactory`.
- Whether to call `ShardCloner.clone` directly (which unconditionally
  writes a cross-index-shaped `CloneLineage` record) or replicate its
  manifest-write body without the lineage write, since this is a
  same-index case — an explicit design call Task 12 flagged but did not
  make, correctly deferred to whoever implements the actual write path
  since it depends on whether downstream lazy-directory fallback-read code
  needs lineage presence.
- `ShardPartitionDescriptor`'s `(partitionIndex, numPartitions)` shape
  cannot represent a `ShardRange`; a new small write-once record type is
  needed, following its "separate from `CommitManifest`, write-once"
  precedent.

Given the size of this remaining piece (new constructor surface across two
classes, a new record type, a real design call on lineage semantics, plus
its own test-and-verify pass), it is being left as explicitly open rather
than rushed — consistent with the plan's own "Scale expectations, stated
plainly" paragraph, which specifically calls out Phase 0 as deserving more
caution and time than this session's usual pace, not less.

## Task 13-14 — `ObjectStoreWriterEngine#recoverFromInPlaceSplit` implementation

Status: **done**, commit follows this entry. Closes the "still open" item flagged at the end of
Task 13's earlier increment.

### What was built

1. [`InPlaceSplitRangeDescriptor`](plugins/serverless-storage/src/main/java/org/opensearch/serverless/storage/resharding/InPlaceSplitRangeDescriptor.java) +
   [`BlobContainerInPlaceSplitRangeStore`](plugins/serverless-storage/src/main/java/org/opensearch/serverless/storage/resharding/BlobContainerInPlaceSplitRangeStore.java) —
   the new, separate write-once record Task 12 concluded was needed (a
   `(parentShardId, start, end)` triple), mirroring `ShardPartitionDescriptor`/
   `BlobContainerShardPartitionStore`'s shape exactly without reusing them,
   since the older record can't represent a hash range at all.
2. `ObjectStoreWriterEngine` gained a new field, `siblingShardBlobContainerResolver`
   (`IntFunction<BlobContainer>`, nullable = feature disabled, same shape as
   every other optional feature in this class), threaded through one new
   telescoping constructor overload, and a real `recoverFromInPlaceSplit`
   override:
   - reads `engineConfig.getIndexSettings().getIndexMetadata().getSplitShardsMetadata()`
     and calls Task 13's `getParentAndRangeOfChild` to find the parent shard
     ID and this child's `ShardRange`. `null` (not a recognized in-progress
     child) logs a warning and no-ops rather than throwing — the hook can
     legitimately fire for a shard whose split already committed by the time
     recovery runs.
   - resolves both the parent's and this shard's own manifest/head/pin/lineage
     stores via the resolver, then calls **`ShardCloner.clone` directly,
     unmodified** — this is the design call Task 12 flagged as open: since
     `CloneLineage`'s `(sourceIndexUuid, sourceShardId)` shape already
     accommodates `sourceIndexUuid == targetIndexUuid` (a same-index source
     is not a case `ShardCloner` forbids or mishandles), reusing it verbatim
     needed no new manifest-writing code and keeps the exact same
     formally-verified pin-before-read ordering (`formal/CloneGc.tla`)
     `ShardCloner`'s own javadoc documents — retargeting was purely a
     parameter change, not new logic.
   - passes a `beforeActivation` closure writing the new
     `InPlaceSplitRangeDescriptor`, following `ShardSplitter.split`'s own
     established "descriptor durable before the target becomes visible"
     ordering precedent exactly.
3. `WriterEngineFactory` gained the matching new field + telescoping
   constructor overload, threading the resolver into
   `ObjectStoreWriterEngine`'s construction.
4. `ServerlessStoragePlugin.getEngineFactory` now passes a real resolver —
   `siblingShardId -> resolveBlobContainer(indexUuid, siblingShardId)` — so
   the feature is actually reachable in production, not just plumbed and
   left disabled.

### Tests + verification discipline

New `testRecoverFromInPlaceSplitAttachesChildToParentsData` in
`ObjectStoreWriterEngineTests`: publishes a real parent manifest/head
directly via the stores (no need for a full parent engine), builds a real
`SplitShardsMetadata` via the actual `Builder.splitShard` API (not
hand-constructed), opens a real child engine with the resolver wired in,
calls `recoverFromInPlaceSplit`, and asserts: the child gets a published
head at generation 1, the child's manifest references the exact same
`files()`/`segmentsFileName()` as the parent's (not a copy — the core
zero-copy property under test), and the range descriptor is durably
written with the correct parent ID and hash bounds.

Verified meaningfulness by forcing the resolver-null-check to always
return early (`if (true) { return; }`), confirming the test failed exactly
where expected (no published head), then restoring and confirming clean.

Full plugin quality gate (`:plugins:serverless-storage:check -x
internalClusterTest`) — found and fixed 2 real missing-javadoc gate
failures (the new telescoping constructor's `@param` list, and the new
record's accessor-method javadoc) before it passed clean. Full
`internalClusterTest` sweep also clean.

### Scope note

This closes Phase 0's core storage-layer gap (0.3/0.4). Not yet done, and
explicitly out of scope for this task: 0.5 (n/a per Task 15's resolution),
0.6 (operator-facing REST/transport trigger for an in-place split), 0.7
(the full end-to-end IT: real cluster, real writer index, trigger a split
via the REST action, assert reads/writes survive and route correctly).
Those remain the next concrete units of Phase 0 work.

## Tasks 10-11 — `RecoverySource` dispatch seam for `InPlaceSplitShardRecoverySource`

Status: **done**, commit follows this entry.

### Design grounding

Investigated where core actually dispatches on `RecoverySource.Type` to decide
how a shard recovers: `IndexShard.startRecovery`'s `switch` (the single
dispatch point) had cases for `EMPTY_STORE`/`EXISTING_STORE`, `REMOTE_STORE`,
`PEER`, `SNAPSHOT`, `LOCAL_SHARDS` — and no case for `IN_PLACE_SPLIT_SHARD`,
which fell into `default:` and threw `IllegalArgumentException("Unknown
recovery source ...")`. Any child shard created by Task 5-9's routing wiring
would have failed recovery immediately.

Studied `LOCAL_SHARDS` (classic resize's recovery path) as the closest
precedent: `IndexShard.recoverFromLocalShards` → `StoreRecovery.recoverFromLocalShards`
→ real Lucene `IndexWriter.addIndexes`-based segment merge from sibling
shards' directories, entirely local (no `RecoveryTarget`/`PeerRecoveryTargetService`
involvement — those are `PEER`-only). Confirmed `RecoveryTarget`/
`PeerRecoveryTargetService` are irrelevant here for the same reason.

### Implementation

Followed the `Engine#onPrimaryTermBumped` seam shape exactly (per the
research recommendation) rather than doing real segment-level work in core:

- [`Engine.java`](server/src/main/java/org/opensearch/index/engine/Engine.java):
  new no-op-default `recoverFromInPlaceSplit(ShardId shardId)`, documented as
  firing once, after the engine is opened exactly as an `EMPTY_STORE`
  recovery would (empty local Lucene index) — core does no data copying of
  its own for this recovery source, giving a plugin engine the chance to
  attach the child to its share of the parent's data however it understands
  that (e.g. manifest-level partition filter, not a physical copy).
- [`Indexer.java`](server/src/main/java/org/opensearch/index/engine/exec/Indexer.java) /
  [`EngineBackedIndexer.java`](server/src/main/java/org/opensearch/index/engine/EngineBackedIndexer.java):
  matching default method + delegating override, mirroring the
  `onPrimaryTermBumped` triplet exactly.
- [`IndexShard.java`](server/src/main/java/org/opensearch/index/shard/IndexShard.java):
  new `case IN_PLACE_SPLIT_SHARD:` in `startRecovery`'s switch, dispatching to
  a new `recoverFromInPlaceSplit(ActionListener<Boolean>)` method that calls
  the existing `recoverFromStore(...)` (reusing its exact machinery, same as
  `EMPTY_STORE` does) and then, on success, invokes the new
  `Indexer#recoverFromInPlaceSplit` hook via the same null-safe
  `getIndexerOrNull()` pattern `onPrimaryTermBumped`'s call site uses.

### A real bug found and fixed while writing the first real test

Reusing `recoverFromStore(...)` as-is doesn't work: `StoreRecovery.internalRecoverFromStore`
gates its entire "should this shard's local store already contain data"
decision on a single `indexShouldExists` boolean, computed as `recoverySource().getType()
!= EMPTY_STORE` (`StoreRecovery.java:709`) — every other recovery source type,
including our new `IN_PLACE_SPLIT_SHARD`, was treated as "should already have
segments on disk," which fails immediately since a freshly-allocated child
shard's local directory is empty. Fixed by widening that condition to also
treat `IN_PLACE_SPLIT_SHARD` as not-should-exist, matching the design intent
("opened exactly like `EMPTY_STORE`"). This is exactly the kind of gap the
session's "implement → real test → verify meaningfulness by breaking it"
discipline exists to catch — it was found by the first real test actually
exercising the new recovery path, not by inspection.

### Tests + verification discipline

New [`InPlaceSplitShardRecoveryTests`](server/src/test/java/org/opensearch/index/shard/InPlaceSplitShardRecoveryTests.java):
builds a real primary `IndexShard` with an initializing `ShardRouting` whose
recovery source is `InPlaceSplitShardRecoverySource.INSTANCE`, calls the new
`recoverFromInPlaceSplit` directly, and asserts recovery succeeds and the
shard reaches `STARTED` — a scenario that, before this task, would have
either thrown "Unknown recovery source" (if reached via the switch) or the
`indexShouldExists` `IndexShardRecoveryException` found above (once the
switch case was added but before the `StoreRecovery` fix).

Verified meaningfulness twice: (1) removed the new `StoreRecovery` fix,
confirmed the test fails with exactly the `IndexShardRecoveryException`
described above, restored, confirmed clean. (2) Confirmed via the test's own
prior failing run (before the `StoreRecovery` fix was written) that the test
genuinely exercises the new code path end-to-end, not a mocked shortcut.

Ran the required broader regression sweep (core recovery-path change):
`:server:test --tests "org.opensearch.index.shard.*" --tests
"org.opensearch.index.engine.*"` — `BUILD SUCCESSFUL`, zero
`failures="[1-9]`/`errors="[1-9]` across result XML. Also confirmed the
plugin (`serverless-storage`) still compiles clean against the widened
`Engine`/`Indexer` surface (both new methods are additive, no-op-default).

### Known scope limits (flagged honestly, not silently resolved)

- No test exercises the actual `switch` case in `IndexShard.startRecovery`
  end-to-end (that requires full `PeerRecoveryTargetService`/`RepositoriesService`/
  `IndicesService` wiring, heavier than this task's scope) — the new test
  calls `recoverFromInPlaceSplit` directly. The switch-case addition itself
  is a single trivial line, low-risk relative to the two behaviors that
  *were* verified (the `StoreRecovery` gate and the `Engine` hook delegation).
- `Engine#recoverFromInPlaceSplit` is still a no-op in every engine,
  including this plugin's `ObjectStoreWriterEngine` — that's deliberately
  Task 12-14's scope (the manifest-identity design spike), not this task's.
  A real in-place split is not yet end-to-end functional; this task only
  ensures core no longer *rejects* the recovery source.

## Task 8.5 — Split commit/cancel driver

Status: **done**, commit follows this entry.

### Design grounding

Investigated the codebase for an existing "watch cluster state, react by
submitting a follow-up cluster-state-update task, cluster-manager-only"
precedent to copy instead of inventing a new shape. `PersistentTasksClusterService`
(`server/src/main/java/org/opensearch/persistent/PersistentTasksClusterService.java`)
is exactly this pattern: gated on `DiscoveryNode.isClusterManagerNode(settings)`
at construction, re-checks `event.localNodeClusterManager()` per event, and
submits a plain `ClusterStateUpdateTask` when a condition is met. Confirmed
there is no more specific "shard reached STARTED → do X on manager" hook to
reuse instead — neither `AllocationService` nor `GatewayAllocator` expose
one; both only participate during reroute computation.

For the cancel signal, reused `MaxRetryAllocationDecider`'s own existing
"give up permanently" condition (`UnassignedInfo.getNumFailedAllocations() >=
SETTING_ALLOCATION_MAX_RETRY`) rather than inventing a new retry-budget
concept — a child shard that decider has already given up on is exactly
the case a split needs to detect and cancel from.

### Implementation

New [`MetadataInPlaceSplitShardCommitService`](server/src/main/java/org/opensearch/cluster/metadata/MetadataInPlaceSplitShardCommitService.java),
a cluster-manager-only `ClusterStateListener`:

- On every cluster state change, for every in-progress split in every
  index's `SplitShardsMetadata`, evaluates each child shard's primary
  `ShardRouting`:
  - all children `STARTED` → submits a commit task calling
    `SplitShardsMetadata.Builder.updateSplitMetadataForChildShards`.
  - any child's primary has exhausted its allocation-retry budget →
    submits a cancel task calling `SplitShardsMetadata.Builder.cancelSplit`
    **and** removes the abandoned children's `ShardRouting` entries from
    the routing table — necessary because `SplitShardsMetadata`'s own
    hole-reuse logic (found reading `findHoles` in Task 1) assumes a freed
    child ID isn't still referenced by a live routing entry; leaving them
    in place would let a future split collide with this attempt's orphaned
    routing rows.
  - otherwise, no-ops (still converging).
- Both `applyCommit`/`applyCancel` are static, package-visible, and
  re-validate the completion condition against the state they're actually
  applied to (not just the state that triggered the listener) — cluster
  state can advance between `clusterChanged` firing and the task executing.
- Registered in `Node.java` next to `PersistentTasksClusterService`'s own
  construction site, following the same "construct directly in `Node`,
  don't try to route it through Guice" wiring.

### Tests + verification discipline

6 new unit tests in `MetadataInPlaceSplitShardCommitServiceTests`: commit
no-ops while children are still unassigned, commit promotes children once
all `STARTED`, commit is idempotent once already committed, cancel frees
child IDs and removes their routing entries (parent's own entry
untouched), and cancel triggers correctly off a simulated exhausted
allocation-retry `UnassignedInfo`.

Verified meaningfulness: temporarily forced `evaluateSplitCompletion` to
always return `STILL_IN_PROGRESS` (never commit); 2 of 6 tests failed
exactly as expected (`AssertionError` on the "split committed" assertions).
Restored the real logic, re-ran clean.

Ran the required broader regression sweep (core change: new class wired
into `Node.java`, touches cluster metadata/routing):
`:server:test --tests "org.opensearch.cluster.metadata.*" --tests
"org.opensearch.cluster.routing.*" --tests "org.opensearch.node.*"` —
`BUILD SUCCESSFUL`, zero `failures="[1-9]`/`errors="[1-9]` across result XML.

### Known scope limits (not addressed by this task, flagged honestly)

- The parent shard's own `ShardRouting` is left untouched by both commit
  and cancel — a committed split still has the parent shard `STARTED` and
  serving. This matches the existing test-only semantics of
  `updateSplitMetadataForChildShards` (it never touched routing either) and
  is exactly the read-path-correctness question the plan's Part 3 already
  flags as needing an explicit design decision before Phase 0 is
  usable end-to-end — not silently resolved here, deliberately deferred to
  where the plan already scoped it.
- `clusterChanged` iterates every index on every cluster-state-changed
  event — fine at the scale this plugin operates at today (splits are rare,
  operator-triggered), but worth revisiting for cost if in-progress splits
  become common under Phase 1's automatic triggering.

## Tasks 5-9 — `AllocationService`/`RoutingTable` wiring for in-progress split children

Status: **done**, commit follows this entry.

### Design grounding (research pass before implementing)

Investigated classic core `_split`/`_shrink` (RESIZE) as the closest precedent:

- `MetadataCreateIndexService.clusterStateCreateIndex` builds the new index's
  metadata and `IndexRoutingTable` **in the same cluster-state-update task**
  (`RoutingTable.builder(...).addAsNew(indexMetadata)` →
  `IndexRoutingTable.initializeAsNew` → `initializeEmpty`), then calls
  `AllocationService.reroute()` afterward. Reroute is what turns the
  `UNASSIGNED` entries created there into real allocations — it does not
  itself create routing entries.
- For a resize target, `initializeEmpty` assigns
  `RecoverySource.LocalShardsRecoverySource.INSTANCE` to each new primary
  when `IndexMetadata.getResizeSourceIndex() != null`.
- `IndexRoutingTable.validate(Metadata)` is the only place that checks
  `IndexMetadata.getNumberOfShards() == routingTable.shards().size()`, and
  nothing in the production commit path actually calls it — so nothing
  strictly *requires* metadata and routing to update atomically, but every
  real precedent (resize, and every other index-lifecycle op) does it that
  way regardless, and other code generally assumes the two stay in lockstep.
  Followed that precedent rather than relying on the unenforced invariant.
- `RecoverySource.Type.IN_PLACE_SPLIT_SHARD` /
  `RecoverySource.InPlaceSplitShardRecoverySource` already exist in this
  fork (shaped like `LocalShardsRecoverySource`: singleton `INSTANCE`, no
  extra fields) and are already wired into `RecoverySource`'s deserialization
  switch and `ShardRouting`'s primary/replica-recovery-source assertion —
  but nothing constructs a `ShardRouting` with it yet. This is exactly the
  recovery source to use for the child primary.

### Implementation

[`MetadataInPlaceSplitShardService.applySplitShardRequest`](server/src/main/java/org/opensearch/cluster/metadata/MetadataInPlaceSplitShardService.java)
now, in the same cluster-state-update task that records the split in
`SplitShardsMetadata`:

1. Copies every existing `IndexShardRoutingTable` entry for the index
   unchanged into a fresh `IndexRoutingTable.Builder`.
2. For each child `ShardRange` reserved by `SplitShardsMetadata.Builder.splitShard(...)`
   (read back via `getChildShardsOfParent(shardId)`), adds:
   - a primary `ShardRouting`, `UNASSIGNED`, recovery source
     `InPlaceSplitShardRecoverySource.INSTANCE`.
   - `numberOfReplicas` replica `ShardRouting`s, `UNASSIGNED`, recovery
     source `PeerRecoverySource.INSTANCE` (ordinary peer recovery from the
     new primary once it's up, same as any other replica).
3. Attaches the resulting `IndexRoutingTable` via `RoutingTable.Builder.add(...)`
   before calling `rerouteRoutingTable.apply(...)` — so the very next reroute
   pass (already invoked at the end of this method) has real unassigned
   shards to allocate, exactly like the resize precedent.

This closes the gap Task 1 found: before this change, a split only ever
touched `IndexMetadata`; the routing table was a byte-for-byte copy. Now a
split produces real, schedulable child shards.

### Tests + verification discipline

Added 3 new tests to `MetadataInPlaceSplitShardServiceTests`:
`testApplySplitShardRequestCreatesUnassignedChildShardRouting` (primary/replica
recovery-source + state assertions), `testApplySplitShardRequestPreservesExistingShardRoutingEntries`
(parent/siblings untouched), `testApplySplitShardRequestChildRoutingHasCorrectParentIndex`.

Verified meaningfulness by commenting out the `routingTableBuilder.add(...)`
call and re-running: 2 of the 3 new tests failed with a
`NullPointerException`/`AssertionError` exactly where expected, confirming
they'd catch a regression. Restored the real code, re-ran clean.

Ran the required broader regression sweep (core change, not plugin-only):
`:server:test --tests "org.opensearch.cluster.metadata.*" --tests
"org.opensearch.cluster.routing.*"` — `BUILD SUCCESSFUL`, zero
`failures="[1-9]` / `errors="[1-9]` across the result XML.

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
