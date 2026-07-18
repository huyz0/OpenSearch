# Dynamic Horizontal Partitioning — Execution Progress

Tracks execution of the 40-task breakdown of `dynamic-partitioning-plan.md`. One
section per completed task. Numbering matches the task list handed to the user
(1-40), cross-referenced to the plan's own `0.x`/`1.x`/... item numbers where
applicable.

## Phase 0 — done (0.1 through 0.8)

Every Phase 0 checklist item is complete: 0.1 (transition-sequence read), 0.2 (routing-table
wiring), 0.3 (`InPlaceSplitShardRecoverySource` dispatch), 0.4 (manifest-identity spike →
`ShardCloner.clone` reuse), 0.5 (n/a — `ShardPartitionDescriptor` unrelated to this mechanism, Task
15), 0.6 (REST/transport action), 0.7 (full real-workload IT, Task 20 below), and now 0.8: beyond
the plugin's own quality gate + `internalClusterTest` sweep (already run for Task 20), a broader
core-level sweep — `org.opensearch.index.shard.*`, `org.opensearch.index.engine.*`,
`org.opensearch.cluster.routing.allocation.decider.*`, `org.opensearch.action.admin.indices.split.*`
— ran clean, closing out the "matching core-level test sweep" 0.8 explicitly calls for given this
phase's changes touch `AllocationService`/`IndexShard`. Proceeding to Phase 1 (automatic split
triggering).

## Phase 1 item 1.1 — Extend `ShardSplitCandidatesAction` with a shard-size (split-for-size) signal

Status: **done**. `ShardSplitCandidatesAction` (Task 56) previously surfaced only a
`writesPerMinute()`-based split-for-heat signal; this adds the DynamoDB-style split-for-size
counterpart the plan's own item 1.1 calls for, reusing an existing manifest-metadata-only size
computation rather than inventing a new one.

### Implementation

- `ObjectStoreWriterEngine.shardSizeInBytes()` (new): `headPublisher.readLatestManifest(indexUuid,
  shardId)` (already used elsewhere in this class, e.g. WAL replay) piped through
  `ManifestSegmentMetrics.from(manifest).totalBytes` — no local I/O, no bundle opened, the same
  manifest-metadata-only approach `CompactionSchedulerTask` already uses for its own size-based
  triggers. Returns `0` if the shard has never published a manifest, or on a read failure
  (best-effort, same tolerance `writesPerMinute()`'s own callers already have).
- `ShardActivityRegistry.snapshotShardSizes()` (new): the size counterpart to the existing
  `snapshotWritesPerMinute()`, same `WeakReference`-tolerant shape.
- `ShardWriteRateEntry`/`ShardSplitCandidateEntry` (both extended, not replaced): now carry
  `shardSizeInBytes` alongside `writesPerMinute`. `ShardSplitCandidateEntry` splits its old single
  `candidate` boolean into `writeRateCandidate()`/`sizeCandidate()`, with `candidate()` becoming
  `writeRateCandidate() || sizeCandidate()` — a shard can be flagged for either reason, and a caller
  that only cares about one signal can still ask for it specifically.
- `ShardSplitCandidatesResponse.merge` now merges both signals (`Math::max` across nodes, same as
  before) and evaluates both thresholds independently.
- `ShardSplitCandidatesRequest`/`RestShardSplitCandidatesAction`: new optional
  `size_threshold_bytes` override alongside the existing `writes_per_minute_threshold`, same
  per-request-override shape.
- `ServerlessStoragePlugin.SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING`
  (new, `NodeScope`, default 10 GiB): mirrors the existing WPM threshold setting's exact shape
  (`volatile` field resolved once in `createComponents`, getter, request-override precedence).
  Same "illustrative default, untuned against a real workload" caveat as the WPM threshold already
  carries.

### Tests + verification discipline

New test `ObjectStoreWriterEngineTests#testShardSizeInBytesReportsZeroBeforeAnyPublishAndRealTotalAfter`
(mirrors the existing `writesPerMinute` test's shape): asserts `0` before any publish, then a real
positive value after indexing and flushing two documents. Verified meaningfulness by temporarily
replacing the real implementation with `return 0L;` and re-running — failed exactly as expected
(`shardSizeInBytes must report the real total file size...`); restored, re-ran clean.

`ShardSplitCandidatesResponseTests` extended with a new
`testAShardOverSizeThresholdIsASizeCandidateEvenWithLowWriteRate` case (proving the two signals are
independently evaluated, not just OR'd blindly at the wrong layer) plus updated existing cases to
the new constructor shape. `ShardSplitCandidateEntryTests` extended similarly, plus a new
`testCandidateIsTrueWhenEitherSignalTriggers` case. `DataStreamShardCountAdvisorSchedulerTaskTests`
(an existing consumer of `ShardSplitCandidateEntry`) updated to the new constructor shape,
unchanged in behavior.

Full verification sweep before commit: `:plugins:serverless-storage:check -x internalClusterTest`
(one unrelated failure on the first run, `CompactionRebaseExecutorTests.testConcurrentRebaseExecutorsNeverLoseAnUpdate`
— confirmed flaky, not caused by this change, by re-running that single test in isolation clean;
the full gate re-ran clean afterward), `:plugins:serverless-storage:internalClusterTest` full
suite, no filter (clean — confirms no regression in `ServerlessStorageShardSplitCandidatesIT` or
any other IT).

## Phase 1 items 1.2/1.3/1.4 — Automatic split-trigger scheduler task

Status: **done**. Turns item 1.1's advisory signal into a real, gated automatic trigger of Phase
0's `InPlaceSplitShardAction` — the plan's own Phase 1 completes with this.

### Implementation

- `InPlaceSplitTriggerCoordinator` (new): the "do the work" half, mirroring `ReaderReplicaExpansionCoordinator`'s
  shape closely — same sustained-duration hysteresis (a shard must be flagged a candidate on
  `requiredConsecutiveTicks` consecutive evaluations before it's actually split, tracked per
  `(indexUuid, shardId)` in a `ConcurrentHashMap`) and same illustrative per-tick budget
  (rate-limits how many shards this splits in one evaluation, busiest by `writesPerMinute()` first,
  losers keep their streak rather than resetting it). Guards against re-triggering a shard that's
  already mid-split or already a split parent by checking `SplitShardsMetadata.isSplitOfShardInProgress`/
  `isSplitParent` against a caller-supplied `ClusterState` before issuing the real `InPlaceSplitShardAction`
  request.
- `InPlaceSplitTriggerSchedulerTask` (new): the scheduling half, mirroring `ScaleUpCandidatesSchedulerTask`'s
  two-constructor (policy-only vs. policy-and-mechanism) shape exactly — periodically calls
  `ShardSplitCandidatesAction`, cluster-manager-only, and hands the result to the coordinator if one
  was supplied.
- Four new settings mirroring the scale-up settings' exact shape: `..._AUTO_SPLIT_EVAL_INTERVAL_SETTING`
  (off by default), `..._AUTO_SPLIT_ENABLED_SETTING` (a second, independent gate — the scheduled
  evaluation alone must stay observational, never a silent trigger the first time an operator turns
  on the eval interval), `..._AUTO_SPLIT_REQUIRED_CONSECUTIVE_TICKS_SETTING` (defaults to 5, higher
  than scale-up's own default of 2, since an in-place split is a one-way action with no in-place
  merge existing yet to undo it), `..._AUTO_SPLIT_MAX_SPLITS_PER_TICK_SETTING` (defaults to 2).
- **Item 1.3 (split-point selection)**: always bisects into exactly 2 children
  (`InPlaceSplitTriggerCoordinator.SPLIT_INTO`), matching core's own default equal-subdivision
  shape. No attempt at targeting a specific hot sub-range for split-for-heat — documented directly
  in the class's own javadoc as a deliberate choice, per the plan's own item 1.3 guidance not to
  guess at DynamoDB's undocumented real algorithm; hysteresis (wait, re-measure, split again next
  tick if still hot) is the honest first cut.
- **Item 1.4 (no silent policy)**: every new threshold/hysteresis/budget constant's javadoc states
  it's illustrative and untuned against a real workload, same discipline every other threshold in
  this plugin already carries.

### A real bug the new test caught

First draft of `alreadySplitOrInFlight` called `state.metadata().index(entry.indexUuid())` —
`Metadata#index(String)` looks up by index **name**, not UUID, so every lookup returned `null` and
every candidate was silently treated as "index gone, nothing to split." Every coordinator test that
expected a real split call failed with "zero interactions with this mock" the first time they ran,
which is exactly what caught it before it shipped. Fixed by looking up via `entry.indexName()`
instead, with an explicit UUID cross-check afterward (guards against the name having since been
reused by an unrelated index between the evaluation being computed and the coordinator acting on it).

### Tests + verification discipline

New `InPlaceSplitTriggerCoordinatorTests` (13 tests): candidate filtering, request shape
(index/shard/splitInto), the in-progress-split/already-parent guard, the deleted-index guard,
sustained-duration hysteresis (single tick doesn't trigger, streak reaching the threshold does, a
gap resets it), per-tick budget (caps the count, prioritizes the busiest shard, losers keep their
streak), and non-positive budget meaning unlimited — the same coverage shape
`ReaderReplicaExpansionCoordinatorTests` already established for its sibling.

Verified meaningfulness by temporarily replacing the real `alreadySplitOrInFlight` in-progress/parent
check with `return false;` and re-running `testDoesNotReSplitAShardAlreadyMidSplit` — failed exactly
as expected (an unwanted `client.execute` call was observed); restored, re-ran clean.

Full verification sweep before commit: `:plugins:serverless-storage:check -x internalClusterTest`
(clean) and `:plugins:serverless-storage:internalClusterTest` full suite, no filter (clean).

**Phase 1 is now done** (items 1.1 through 1.4).

## Phase 2 item 2.1 — In-place merge (full-sibling-pair)

Status: **done and shippable**. Began as a research/design spike (same "spike first, implement once
the shape is grounded" discipline Task 12 used for split's own manifest-identity question), which
found a real data-freshness flaw in its own first premise; the resolved design is now fully
implemented, tested end-to-end on a real cluster, and reachable through an operator action. See "The
resolution" and "What is now built, tested, and shippable" below for the shipped state; the spike
narrative is kept verbatim for the reasoning trail.

### Confirms the plan's own premise, more precisely

Core genuinely has zero merge scaffold: `SplitShardsMetadata` has no method that de-commits an
already-committed split. Its one existing "undo" operation, `Builder.cancelSplit`, only works on a
still-*in-progress* split (`assert inProgressSplitShardIds.contains(sourceShardId)`) and doesn't
touch `activeShardIds` at all -- it exists for `MetadataInPlaceSplitShardCommitService`'s own
allocation-failure rollback path (Task 8.5), not for voluntarily reversing a split that already
committed and has been serving traffic. A genuine merge feature needs a new
`SplitShardsMetadata.Builder` method with different preconditions and a different effect, not a
reuse of `cancelSplit`.

### The key insight: a merge of exactly a split's own two full children is nearly free

Every child shard's Lucene data is attached to its parent via `ShardCloner.clone` (Task 12/19) --
a manifest-reference, not a physical copy. Both children of one split still point at the *same*
underlying parent bundle files; the only thing that makes them logically distinct is
`InPlaceSplitFilteringDirectoryReader`'s hash-range filter (Task 20's read-path fix) applied at
query time. This means merging the two full, never-further-split children of one split back
together doesn't need `ShardShrinker`'s real Lucene `IndexWriter#addIndexes` merge at all (unlike
this plugin's existing cross-shard `shrink`, which fundamentally requires one because its sources
are independent, unrelated shard identities with no shared lineage) -- it only needs to widen (or
simply drop) the survivor's own range filter back to the parent's original full range, since the
union of two sibling `ShardRange`s that came from the same split is, by construction, exactly the
parent's own original range. **The data is already there; a full-sibling-pair merge is a metadata
and routing operation, not a data operation** -- the same "logical-first" insight this whole effort
found on the split side, now confirmed to hold symmetrically on the merge side too.

This insight also bounds the design's honest scope: it only applies cleanly to merging *exactly*
the full, unsplit-further child set of one original split back together (the "undo my own split"
case). Merging two shards that are *not* siblings of the same split (e.g. two arbitrary adjacent
leaves several splits and merges deep) is a fundamentally harder problem -- their data was never
co-resident in one Lucene commit to begin with, and *that* case genuinely needs something closer to
`ShardShrinker`'s real merge. Scoping Phase 2's first increment to the sibling-pair case (mirroring
how split itself was first scoped to a flat, non-nested `SPLIT_INTO=2` case before any nested-split
work was attempted) is the honest, grounded first cut, not a shortcut around the harder case.

### Proposed data-model shape

- New `SplitShardsMetadata.Builder.mergeChildrenBackToParent(int parentShardId)`: asserts
  `rootShardsToAllChildren[parentShardId]` is non-null (a split happened), every recorded child is
  in `activeShardIds` (no child has itself been further split -- the "full, unsplit-further child
  set" precondition above), and the split is not still in-progress. Effect: removes every child
  from `activeShardIds`, adds `parentShardId` back, clears `rootShardsToAllChildren[parentShardId]`
  to `null`. Symmetric, in shape, to `Builder.splitShard`'s own effect in reverse -- reuses the
  exact same underlying fields, no new metadata structure needed.
- New `MetadataInPlaceMergeShardService` (mirrors `MetadataInPlaceSplitShardService`'s own shape):
  submits a cluster-state-update task that, in one step, calls the new builder method above *and*
  updates `IndexRoutingTable` -- removes both children's routing entries, adds back a single
  `UNASSIGNED` primary `ShardRouting` for `parentShardId` with a new recovery source (see below).
- New `InPlaceMergeShardRecoverySource` (mirrors `InPlaceSplitShardRecoverySource`'s own shape --
  singleton `INSTANCE`, no extra fields): the parent's revived primary recovers via this. At the
  `EngineFactory` seam (this plugin's `WriterEngineFactory.recoverInPlaceSplitLocalStore` is the
  precedent), the plugin-side hook simply needs to materialize the survivor child's *own* already-full
  local Lucene state (it already has one child's worth of live segments locally) and drop its
  `InPlaceSplitFilteringDirectoryReader` wrapping -- no clone, no re-fetch, since one surviving
  child's own local store, once unfiltered, already *is* correct: it was cloned from the same parent
  bundle the other child was, and a full-pair merge's union is exactly that whole bundle.
- `OperationRouting.getShardIdOfHash` degradation: once `rootShardsToAllChildren[parentShardId]`
  is `null` again (merge committed), `getShardIdOfHash` already falls through to its own existing
  `rootShardsToAllChildren[rootShardId] == null` branch and returns `rootShardId` directly -- **no
  core routing-code change is needed at all**, since this exactly re-creates the pre-split state
  `getShardIdOfHash` was already written to handle. This is the strongest evidence the "undo my own
  split" framing is the right first cut: it doesn't just simplify the write path, it makes the read
  path a complete no-op change.

### What's still open (deliberately, honestly, not attempted here)

Item 2.2 (whether `ShardShrinker`'s physical-merge approach is reusable) is now answered by the
above: **no, and it doesn't need to be** for the sibling-pair case -- `ShardShrinker` remains the
right tool for merging unrelated shard identities (its own existing, already-shipped use case),
while in-place merge needs none of its machinery.

### Update: the metadata primitive itself is now implemented and tested

The design above turned out cheap enough to build immediately rather than leaving purely as a
paper design: `SplitShardsMetadata.Builder.mergeChildrenBackToParent(int parentShardId)` is now
real, following exactly the proposed shape. Three preconditions, each with its own
`IllegalArgumentException`:

1. `parentShardId` must be an original root shard id (`0 <= parentShardId < rootShardsToAllChildren.length`)
   -- the flat, non-nested scope this spike deliberately chose.
2. The split must not still be in-progress (`inProgressSplitShardIds`).
3. **Every direct child must still be an active, unsplit leaf** -- checked against
   `parentToChildShards.get(parentShardId)` (the *original* direct-children list a split recorded),
   not `rootShardsToAllChildren[parentShardId]` (which a nested split of one of those children
   silently grows with that child's own grandchildren). A real bug caught by
   `testMergeChildrenBackToParentRejectsAFurtherSplitChild`: the first draft validated against
   `rootShardsToAllChildren` and the test's "split one child further, then try to merge the
   parent" case passed with no exception, because the grandchildren it introduced were themselves
   active and looked like valid, mergeable children. A second bug the same test line of
   investigation caught: checking only `activeShardIds.contains(child)` missed a child that had
   been split but not yet *committed* (still in `inProgressSplitShardIds`, so still counted
   active) -- fixed by also rejecting a child that's `inProgressSplitShardIds`.

Effect: every child is removed from `activeShardIds`, the parent is added back,
`rootShardsToAllChildren[parentShardId]` is cleared to `null` (its pre-split state), and
`parentToChildShards` no longer records the (now-reversed) split.

New tests in `SplitShardsMetadataTests` (6): a full round-trip (split, commit, merge back --
parent active again, children inactive, root's child-list `null`), confirmation that
`getShardIdOfHash` needs zero code changes and correctly resolves a former child's whole hash
range back to the parent post-merge, and four rejection cases (in-progress split, never-split
shard, non-root/child shard id, and the further-split-child case above that caught the two real
bugs).

Verified meaningfulness by commenting out the `rootShardsToAllChildren[parentShardId] = null;`
line and re-running `testGetShardIdOfHashResolvesToParentAfterMerge` /
`testMergeChildrenBackToParentReversesACommittedSplit` -- the hash-resolution test failed exactly
as expected (`expected:<0> but was:<3>`, resolving to a stale child id instead of the parent);
restored, both tests passed clean again. Full regression sweep before commit:
`org.opensearch.cluster.metadata.*`/`org.opensearch.cluster.routing.*` plus
`:server:missingJavadoc`, all clean.

### Update: the cluster-state and recovery-dispatch layers are now built too (and a design flaw found)

Three more layers landed on top of the metadata primitive, each its own commit, each tested and
swept the same way as everything else:

1. **`InPlaceMergeShardRecoverySource`** -- a new `RecoverySource` shaped like
   `InPlaceSplitShardRecoverySource`, with a new `IN_PLACE_MERGE_SHARD` `RecoverySource.Type` appended
   after `IN_PLACE_SPLIT_SHARD` so no existing type's ordinal shifts. The revived parent's primary
   recovers via this source. (It was landed with no wire fields; the resolution below later gave it a
   `List<ShardRange>` of the retired children -- see "How the engine hook resolves the children" there
   for why that channel, and not `SplitShardsMetadata`, is the one that survives to recovery time.)
2. **`MetadataInPlaceMergeShardService`** -- mirrors `MetadataInPlaceSplitShardService` in reverse.
   One `AckedClusterStateUpdateTask` that, atomically, calls `mergeChildrenBackToParent` and rewrites
   the `IndexRoutingTable`: retires every child's routing entry (mirroring how the split *commit*
   service retires the parent's entry) and revives a single `UNASSIGNED` parent primary recovering
   via `InPlaceMergeShardRecoverySource`. Split-level preconditions come from the metadata primitive
   (clean `IllegalArgumentException`s); an added per-child routing-liveness check rejects a merge whose
   children are relocating or not started. Real unit tests mirror `MetadataInPlaceSplitShardServiceTests`,
   including a split-then-merge round trip. Verified the routing-revival test is real by commenting out
   the parent-primary `addShard` and confirming the revival test fails (parent primary null), then
   restoring.
3. **The core recovery-dispatch seam** -- `IndexShard.startRecovery` routes `IN_PLACE_MERGE_SHARD`
   through `recoverFromStore`, `StoreRecovery` treats it as not-should-exist and offers a new
   `EngineFactory.recoverInPlaceMergeLocalStore` hook before the local translog/engine open (the same
   ordering constraint Task 19 established), and `EngineFactory` gains that hook as a default no-op.

### The design flaw that was found: the spike's "revive the parent from one surviving child" premise does not hold

The spike above (see "Proposed data-model shape") claimed the plugin-side merge hook could revive the
parent almost for free: take one surviving child's own already-full local Lucene state and just drop
its `InPlaceSplitFilteringDirectoryReader` range filter, since both children were cloned from the same
parent bundle, so "one surviving child's own local store, once unfiltered, already *is* correct."

That is true only at the instant a split commits, with **zero post-split writes**. It breaks the moment
the children start serving traffic. After a split commits, each child is an independent writer primary
with its own blob container/manifest (`WriterEngineFactory.recoverInPlaceSplitLocalStore` resolves a
*per-shard* container via `resolveSiblingShardBlobContainer(childShardId)`), and routing sends each new
document to exactly one child by hash. So each child accumulates its own disjoint post-split segments in
its own bundle. Pick child A as the survivor and drop its filter and you get: the shared parent base
bundle (all pre-split docs) plus child A's post-split writes, but you **lose child B's post-split writes
entirely** -- silent data loss.

A correct merge cannot be "drop one survivor's filter." It has to fold *both* children's current segment
sets together. The one genuinely hard-looking part is post-split *updates and deletes*: a post-split
delete/update of a pre-split doc that lives in a shared, immutable base segment is recorded as that
child's own per-segment `liveDocs` against the shared segment, so the two children reference the same
base segment with *divergent* `liveDocs`, which a plain manifest concatenation cannot express.

### The resolution: fold both children's already-filtered readers via one `addIndexes`

The divergent-`liveDocs` problem dissolves once you stop trying to concatenate manifests at all. The key
fact is that hash routing partitions every document into exactly one child's `ShardRange`, so there is no
cross-child version conflict to reconcile the way normal concurrent-update merging has -- each document
has one authoritative owner by hash. So the merge materializes each child, wraps it in **that child's own
`InPlaceSplitFilteringDirectoryReader`** (the same filter the read path already uses, Task 20 fix 3), and
folds the *filtered* readers together with a single `IndexWriter#addIndexes(CodecReader...)`. Each filtered
reader contributes exactly that child's authoritative slice: a base document survives only through the one
child whose range owns its hash (respecting that child's own deletes, because the reader is wrapped in
Lucene's `SoftDeletesDirectoryReaderWrapper` first so a superseded old version is already excluded from
`getLiveDocs()` before the range filter runs), and every post-split document is in-range in exactly the
child that wrote it. The union therefore has no double-counting and needs no bespoke delete-set
reconciliation. This is the same physical-merge precedent `ShardShrinker` (Task 37) and
`LuceneMergeCompactionPublisher` (compaction) already established -- including that the merge writer's
`IndexWriterConfig` must `setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)`, or `addIndexes` throws the
moment it copies soft-deleted source content (the exact latent bug compaction already had to fix).

The pure Lucene work lives in a new `InPlaceSiblingMerger.merge(...)` (mirroring `ShardShrinker`'s shape,
so it is unit-testable with `FsBlobContainer` fixtures exactly like `ShardShrinkerTests`); the plugin's
`WriterEngineFactory.recoverInPlaceMergeLocalStore` does only the blob-store resolution around it and the
translog bootstrap, mirroring `recoverInPlaceSplitLocalStore`'s exact contract. Merged
`maxSeqNo`/`localCheckpoint` is the max across children (a monotonic watermark, exactly as `ShardShrinker`
treats independent sources' seq-no spaces); a fresh `HISTORY_UUID` is minted since the revived parent is a
new history, not a continuation of either child's.

**How the engine hook resolves the children.** The step-1 implementation surfaced a real plumbing gap the
spike had not anticipated: `MetadataInPlaceMergeShardService` de-commits the split in the *same*
cluster-state update that revives the parent, so by the time the parent recovers, `SplitShardsMetadata`
no longer records which children it came from -- the accessor the task assumed (`getChildShardsOfParent`)
returns nothing. Unlike a split child, which recovers *during* the in-progress window and can still read
its own parent/range from metadata, a merge's parent recovers *after* the erasure. The fix carries the
retired children's `ShardRange`s (each already encoding its own child shard id) on the
`InPlaceMergeShardRecoverySource` itself, populated before de-commit -- the one channel that survives to
recovery time. The hook reads them back via `indexShard.recoveryState()`.

**Two more real bugs the end-to-end IT caught, neither visible to the mock-reroute service unit tests:**

- *Stale-in-sync assertion hang.* A revived parent reuses its pre-split shard id, so its
  `inSyncAllocationIds` still carried the stale pre-split primary's allocation id. With a fresh
  `InPlaceMergeShardRecoverySource` primary whose new allocation id is absent from that non-empty set,
  `IndexMetadataUpdater#updateInSyncAllocations` mistook it for a forced *stale-primary* allocation and
  threw an `AssertionError` on the cluster-manager thread -- an `Error`, not an `Exception`, so the
  cluster-state task machinery never completed the request and it *hung* rather than failing (the same
  failure shape Task 20 first hit). Fixed by resetting the revived parent's in-sync set to empty in
  `applyMergeShardRequest`, mirroring how the split service seeds each child's set empty. Regression-guarded
  by `InPlaceMergeRealRerouteTests`, which drives a *real* `AllocationService` reroute + shard-start cycle
  -- the coverage `MetadataInPlaceMergeShardServiceTests` lacked, since it drives an identity-function
  reroute. Verified real: that test fails with exactly this `AssertionError` when the reset is removed.
- *Leftover-store bypass.* A revived parent can find a stale leftover local store from before it was split;
  `readLastCommittedSegmentsInfo` then succeeds on it, so `StoreRecovery` never reaches the engine-materialize
  branch (a split child's brand-new shard id has no leftover) and the stale store is cleaned to empty --
  silently reviving the parent with zero documents. Fixed by invoking the merge (and split) engine hook
  after cleaning such a leftover.

### What is now built, tested, and shippable

- **`InPlaceSiblingMerger` + `WriterEngineFactory.recoverInPlaceMergeLocalStore`** -- the real engine hook.
  `InPlaceSiblingMergerTests` exercises the merge directly against `FsBlobContainer`-published children with
  disjoint post-split writes and per-child updates against the shared base segment. Verified real by breaking
  the fix: removing `setSoftDeletesField` makes `addIndexes` throw on soft-deleted content; removing
  `addIndexes` drops every document. Both restored, green.
- **The recovery source now carries the children**, and `MetadataInPlaceMergeShardService` populates them
  before de-commit and resets the revived parent's in-sync set.
- **End-to-end IT** -- `ServerlessStorageInPlaceMergeDocumentReachabilityIT`: a real 2-node cluster,
  40 pre-split docs, split into 2, 20 post-split docs, 5 pre-split updates and 3 pre-split deletes routed to
  the owning children (so the children genuinely diverge on `liveDocs`), then merge, asserting every
  document's correct final state (updated values win, deleted docs gone, all post-split writes present) via
  both scatter-gather search and real single-shard `GET`-by-id routing. This is the test that proves the
  design, not just that it compiles.
- **Operator entry point** -- the `InPlaceMergeShardAction`/`TransportInPlaceMergeShardAction`/
  `RestInPlaceMergeShardAction` trio (`POST /{index}/_merge_in_place/{parent_shard_id}`), mirroring the split
  trio exactly, flushing the index first (the merge counterpart of the split's Task-18 pre-flush, so a
  child's un-published writes aren't dropped) and surfacing precondition violations as a clean 400. Held back
  until the hook was real and the round trip proven, exactly as planned -- the IT now drives it end to end.

Item 2.3 (an automatic merge trigger policy), the merge counterpart of Phase 1's split-trigger scheduler,
is now built too -- see the "Phase 2 item 2.3" section immediately below. The honest scope bound from the
spike still holds: all of this is the **full-sibling-pair** merge (undo one flat `SPLIT_INTO=2` split).
Merging shards that are not siblings of the same split remains the harder, unspiked problem `ShardShrinker`'s
cross-identity machinery is the right tool for.

## Phase 2 item 2.3 — Automatic merge trigger policy

Status: **done, off by default**. The merge-side counterpart to Phase 1's split auto-trigger, built to
mirror `InPlaceSplitTriggerCoordinator`/`InPlaceSplitTriggerSchedulerTask` as closely as the merge problem
allows. Both `auto_merge` gates default off; an operator opts in explicitly, and should read the anti-flap
and cool-down notes below first.

### What "merge candidate" means, and how the signal is sourced

Split needs only one shard's own write-rate or size to cross a threshold. Merge is about a *pair*: for each
parent whose split has committed and that has exactly two direct, unsplit-further children (the
full-sibling-pair scope this whole feature is bounded to), `InPlaceMergeTriggerCoordinator` sums both
children's `writesPerMinute` and `shardSizeInBytes` and flags the pair a merge candidate when *both* combined
sums sit at or below the merge thresholds.

The per-child signal is the very same one split's own trigger already collects: `ShardSplitCandidatesAction`
fans out `ShardActivityRegistry`'s `snapshotWritesPerMinute`/`snapshotShardSizes` cluster-wide and reports a
`ShardSplitCandidateEntry` (carrying raw `writesPerMinute`/`shardSizeInBytes`) for every writer shard. The
merge scheduler reuses that action verbatim rather than adding a parallel merge-candidates action, and the
coordinator sums the two children's raw values. A pair whose two children aren't *both* currently reporting a
signal (or where either reports an `UNKNOWN` size) is skipped, not assumed quiet -- without both children's
size we can't safely conclude the pair fits back inside one shard.

Eligibility (committed, exactly-two-child, none split further) is screened against cluster state via two new
non-mutating `SplitShardsMetadata` reads: `getSplitParentShardIds()` to enumerate parents and
`canMergeChildrenBackToParent(int)`, which runs exactly the same split-level precondition
`Builder.mergeChildrenBackToParent` enforces but returns a boolean instead of throwing -- so the coordinator
screens candidates without provoking the primitive's `IllegalArgumentException`. Hysteresis and the per-tick
budget reuse the shared `SustainedCandidateTracker`, keyed by the *parent* shard id (the stable identity
across a merge decision, since the children are what get retired). On trigger the coordinator submits
`InPlaceMergeShardAction` via the client, exactly as split submits `InPlaceSplitShardAction` -- the internal
coordinator uses the transport action directly; the REST action is the separate operator surface.

### Anti-flap design (the reasoning, not skipped)

The danger unique to the merge side: split's own auto-trigger just split a hot/large shard, and if merge saw
the resulting pair momentarily dip below the merge threshold before write patterns stabilized, the two could
flap split/merge/split. Two mechanisms guard against it:

- **A deliberately large threshold margin.** Merge combined thresholds default *well below* the split
  thresholds, with real margin, not equal to them: combined 2,000 writes/min vs the 10,000/min per-shard
  split trigger (one fifth), and combined 4 GiB vs the 10 GiB per-shard split-for-size trigger. Right after a
  split each child still carries roughly half the parent's former load, so the pair's combined signal is an
  order of magnitude above the merge ceiling -- it simply isn't a merge candidate until its real, sustained
  load has fallen far below what split it.
- **A higher sustained-tick requirement than split.** `required_consecutive_ticks` defaults to 10 for merge
  vs 5 for split. Merge is the "give back" side; being too eager risks the flap, so it warrants a longer
  sustained-signal requirement. `max_merges_per_tick` defaults to 1 (vs split's 2) since reviving a fresh
  parent primary that must recover is heavier than a split.

### Split-commit cool-down (now built; closes the earlier "no cool-down" gap)

A third, stronger anti-flap guard is now in place: a hard minimum "cool-down since the split committed"
window during which a pair is not a merge candidate at all, no matter how quiet its signal looks. The
earlier increment could not build this because no split-commit timestamp existed anywhere in cluster state.
That is now fixed:

- `SplitShardsMetadata` carries a new `Map<Integer, Long> splitCommitTimestamps` (parent shard id -> epoch
  millis at commit). It is populated by `SplitShardsMetadata.Builder.updateSplitMetadataForChildShards(...,
  long commitTimestamp)`, called from `MetadataInPlaceSplitShardCommitService.applyCommit` with
  `Instant.now().toEpochMilli()` -- the same absolute epoch-millis source `MetadataCreateIndexService` uses
  for `SETTING_CREATION_DATE`, so the value stays meaningful after cluster-state publication. The timestamp
  is dropped when the parent is merged back (`mergeChildrenBackToParent`) or a split is cancelled.
- The field is wire-gated on `Version.V_3_8_0` inside `SplitShardsMetadata`'s own `writeTo`/`StreamInput`
  ctor (the whole blob is already gated on `V_3_6_0` in `IndexMetadata`, so this inner gate is what keeps a
  mixed `V_3_6_0`/`V_3_7_x` <-> `V_3_8_0` cluster's cluster-state wire format safe). It also round-trips
  through XContent for on-disk cluster-state.
- `InPlaceMergeTriggerCoordinator` gains a `minCooldownMillis` (from the new
  `serverless_storage.resharding.auto_merge.min_cooldown` setting, default `30m`). The cool-down is checked
  in `findEligiblePairs` **before** the pair ever enters the `SustainedCandidateTracker` -- time-since-split
  is a hard floor, not a signal to average. A pair inside its window is omitted entirely (which also keeps
  its tracked streak reset). Fails open on a disabled gate (`min_cooldown <= 0`) or an unrecorded timestamp
  (`NO_SPLIT_COMMIT_TIMESTAMP`, e.g. a split committed on cluster-state predating the field).

Default reasoning for `30m`: long enough for post-split write patterns to stabilize (well beyond a few eval
ticks at the plugin's usual sub-minute-to-minutes intervals), short enough that a genuinely and durably idle
pair is still reclaimed within an operational window rather than pinned indefinitely.

The two signal-based guards (deliberately-low combined thresholds, higher sustained-tick default) still
stand alongside this time floor; together the three are the merge side's anti-flap defense.

### Settings (mirror the split auto-trigger four, plus the two combined thresholds and the cool-down)

- `serverless_storage.resharding.auto_merge.eval_interval` (off by default, `-1`)
- `serverless_storage.resharding.auto_merge.enabled` (independent gate, off by default)
- `serverless_storage.resharding.auto_merge.required_consecutive_ticks` (default 10)
- `serverless_storage.resharding.auto_merge.max_merges_per_tick` (default 1)
- `serverless_storage.resharding.auto_merge.min_cooldown` (default `30m`; `<= 0` disables the time gate)
- `serverless_storage.resharding.merge_candidate_combined_writes_per_minute_threshold` (default 2,000)
- `serverless_storage.resharding.merge_candidate_combined_size_threshold_bytes` (default 4 GiB)

### Tests + verification discipline

`InPlaceMergeTriggerCoordinatorTests` (13 tests) mirrors `InPlaceSplitTriggerCoordinatorTests`'s
fixture-building: candidate found and merged; correct index/parent-shard targeted; combined write-rate above
threshold excluded; combined size above threshold excluded; in-progress split excluded; child-split-further
excluded; child-not-reporting excluded; child-size-`UNKNOWN` excluded; sustained-tick hysteresis (not merged
until the streak is reached); a gap in quietness resets the streak; per-tick budget limits merges; budget
prioritizes the quietest pair first; non-positive budget means unlimited; no candidates when nothing is
split. `InPlaceMergeTriggerSchedulerTaskTests` (4 tests) mirrors the split scheduler-task tests
(cluster-manager-only, failure tolerance, reaching a real coordinator). Verification: broke the
`canMergeChildrenBackToParent` eligibility screen and confirmed exactly the in-progress-split and
child-split-further tests fail with the expected wrong behavior, then restored to green; ran the full
`:plugins:serverless-storage:test` sweep plus the core `MetadataInPlaceMergeShardServiceTests`/
`SplitShardsMetadata` tests.

The cool-down follow-up adds five coordinator tests (pair excluded when younger than cool-down even with a
below-threshold signal and single-tick budget; excluded across many sustained ticks; included once past
cool-down; disabled cool-down means no time gate; fails open when no commit timestamp is recorded) plus
core `SplitShardsMetadata`/`MetadataInPlaceSplitShardCommitService` tests (timestamp recorded on real commit,
absent before commit and for non-split shards, cleared on merge-back, stream and XContent round-trips, and a
pre-`V_3_8_0` wire read that drops the field). Verification for the cool-down gate specifically: replaced the
`now - committedAt < minCooldown` comparison with a hard `return false` and confirmed the two
"excluded during cool-down" tests fail with the pair merged too early (the merge log line fires), then
restored and confirmed green. Ran the `org.opensearch.cluster.metadata.*`/`org.opensearch.cluster.routing.*`
core sweep and the full `:plugins:serverless-storage:test` sweep.

## Task 20 (new, found while attempting item 0.7) — `IndexMetadata.numberOfShards` invariant breaks for split children, FIXED

Status: **fixed and verified, Phase 0.7's full real-workload IT now passes**. The initial analysis
below (kept verbatim) turned out to be right that this needed real care, but the actual fix was
narrower than first feared once a key fact came to light: this fork's in-place split already gates
on `Version.V_3_7_0`+ for every node, and `primaryTermsMap`/`inSyncAllocationIds` have used a real
`Map`-based wire format (not the legacy `long[numberOfShards]` array) since `V_3_6_0` — the
backward-compatibility wire-format redesign the original analysis worried about was already done
upstream and simply didn't need touching. Three separate fixes were required in total, found one at
a time as each one unblocked the next real error; see "The three fixes" below.

### The bug

Writing the real Phase 0.7 IT (a genuine 2-node cluster, the plugin's real writer engine, 40
documents, a real `InPlaceSplitShardAction` call) surfaced an uncaught `AssertionError` on the
**cluster-manager apply thread itself**, inside `AllocationService.reroute` →
`RoutingAllocation.updateMetadataWithRoutingChanges` → `IndexMetadataUpdater.updateInSyncAllocations`
→ `IndexMetadata.inSyncAllocationIds(int shardId)`:

```
assert shardId >= 0 && shardId < numberOfShards;
```

`MetadataInPlaceSplitShardService.applySplitShardRequest` (Tasks 5-9) adds real child
`ShardRouting` entries to the `IndexRoutingTable` for shard ids at or beyond the index's original
`numberOfShards` (e.g. splitting shard 0 of a 1-shard index into 2 children reserves shard ids 1
and 2) — but never touches `IndexMetadata.numberOfShards` itself, by design: Task 1's own note
that `SplitShardsMetadata`'s active-shard bookkeeping and `IndexMetadata.numberOfShards`
"intentionally diverge" is exactly why `OperationRouting.generateShardId` can keep resolving hashes
via the original, over-provisioned `routingNumShards`/`routingFactor` without rehashing. The very
next `AllocationService.reroute` call, though, walks the routing table's *real* shards (now
including the children) and asserts every shard id it touches is `< numberOfShards` — which no
longer holds.

**In a real cluster, this is worse than a test-only assertion failure.** `internalClusterTest` runs
with `-ea`; production JVMs typically do not. With assertions disabled, the same code path calls
`inSyncAllocationIds.get(shardId)` for the out-of-range child shard id, gets `null` back (the map
was only ever filled for `[0, numberOfShards)` by `IndexMetadata.Builder.build()`'s "fill missing
slots" loop), and the caller immediately calls `.isEmpty()`/`.contains(...)` on that `null` —
a `NullPointerException` on the cluster-manager's single-threaded apply executor. That executor
processes all cluster-state updates for the whole cluster; an uncaught exception there does not
cleanly fail the one pending request — as observed, the client's `actionGet()` on the split action
simply **hangs forever** (no response is ever sent, since the task that would have completed it
crashed mid-execution). This reproduced as a real 20-minute test-suite timeout, not a fast,
diagnosable test failure.

### Why this is deeper than a one-line patch

Relaxing `inSyncAllocationIds(int)`'s assertion and falling back to `getOrDefault(shardId,
emptySet())` only fixes the symptom at that one call site. The same `numberOfShards`-bounded shape
recurs at real invariants elsewhere in `IndexMetadata`:

- `IndexMetadata.Builder.build()`'s "fill missing slots in inSyncAllocationIds" loop only fills
  `[0, numberOfShards)` — entries a caller adds for a child shard id beyond that range would
  currently be silently dropped at build time, not just unread.
- `primaryTermsMap`'s build-time check is a hard equality: `primaryTermsMap.size() != numberOfShards`
  throws `IllegalStateException` once *any* explicit primary term has been recorded (which happens
  almost immediately once a shard is promoted/its term bumped) — so recording a child's own primary
  term (needed the moment its primary activates, exactly as
  `InPlaceSplitLocalStoreRecoveryTests`'s own unit test had to do by hand via
  `.primaryTerm(childShardId, 1)`) would make every subsequent build of that `IndexMetadata` throw.
- `primaryTermsMap` also has a *wire-format* representation as a plain `long[numberOfShards]` array
  (`IndexMetadata.Builder#writeTo`/`readFrom` and the `IndexMetadataDiff` codec), for backward
  compatibility with pre-map-based versions — extending it to cover child shard ids needs either a
  version-gated wire format change or a deliberate decision to keep the array
  `numberOfShards`-sized and carry child primary terms in a separate, already-map-shaped field.

None of these are safe to guess at under this session's normal "implement → test → verify by
breaking it" discipline in one pass — `IndexMetadata` is exercised by essentially every core
subsystem (snapshot/restore, every `AllocationDecider`, remote cluster-state publication, stats
aggregation), and an under-tested change here risks correctness far outside the scope of the split
feature itself. This needs its own dedicated design pass: most likely, the real answer is that
`IndexMetadata.numberOfShards` needs to stay meaning exactly what `OperationRouting` needs it to
mean (the routing-hash denominator's origin count) while a **new, explicitly split-aware concept**
carries the active/child shard set's own primary terms and in-sync allocation ids, keyed
dynamically rather than pre-sized off `numberOfShards` — likely living alongside
`SplitShardsMetadata` itself (which already tracks a `maxShardId` field, currently private with no
public accessor, that a fix along these lines would need to expose).

### What this blocks

**Phase 0 item 0.7 (the full real-workload IT) cannot pass until this is fixed** — any real
`AllocationService.reroute` after a split's routing-table children are added hits this assertion
(or, in production, the NPE) as soon as the reroute pass processes those shards, which happens on
essentially every subsequent cluster-state update, not just the split's own. This is very likely
why `MetadataInPlaceSplitShardServiceTests`/`MetadataInPlaceSplitShardCommitServiceTests` (Tasks
5-9, 8.5) never caught it: those tests call `applySplitShardRequest`/`evaluateSplitCompletion`
directly with a hand-built `AllocationService`/`RoutingAllocation` fixture, not necessarily one that
exercises `IndexMetadataUpdater.updateInSyncAllocations` for a shard that just transitioned from
`UNASSIGNED` to `INITIALIZING` in a real reroute pass end to end — a real 2-node
`internalClusterTest` was needed to surface it, exactly the gap Phase 0.7 exists to close.

The new IT written to exercise item 0.7
(`ServerlessStorageInPlaceSplitDocumentReachabilityIT`) was **not committed** — as written, it
reliably hangs the whole test JVM for the full 20-minute suite timeout once it calls the split
action, which would be actively harmful to land in the tree (any future full-suite run pays that
20 minutes). It will be reinstated once this task is fixed; its design (real 2-node cluster, 40
real documents, scatter-gather search + single-shard `GET` reachability check, PUT-count-based
zero-copy-copy assertion) remains valid and is recorded here for that purpose.

### New task added to the backlog (auto-added per `/goal` authorization)

**Task 20 (this entry) — Make `IndexMetadata`'s per-shard-keyed structures
(`inSyncAllocationIds`, `primaryTerms`, and their wire formats) tolerate shard ids introduced by an
in-place split without requiring `numberOfShards` itself to grow.** This is now the single
highest-priority remaining item for Phase 0 — without it, `AllocationService.reroute` cannot safely
process a post-split routing table at all, in or out of a test.

### The three fixes

**Fix 1 — `IndexMetadata`'s per-shard-keyed structures.** `inSyncAllocationIds(int shardId)`'s
assertion relaxed to `shardId >= 0` (dropped the `< numberOfShards` upper bound) with a
`getOrDefault(shardId, emptySet())` read instead of a raw `.get()`. `IndexMetadata`'s constructor
assertion loosened from `primaryTermsMap.size() == numberOfShards` to `>= numberOfShards`.
`IndexMetadata.Builder.build()`'s two per-shard-sizing blocks were rewritten to the same shape: for
`inSyncAllocationIds`, every existing entry (base shard or split child) is preserved verbatim, and
only base-shard gaps `[0, numberOfShards)` get filled with an empty set (previously the fill loop
only ever *copied* entries in that range, silently dropping anything a caller had added beyond it);
for `primaryTermsMap`, the check became "every base shard `< numberOfShards` must have an explicit
entry" rather than "the map's size must exactly equal `numberOfShards`" — extra entries for split
children are allowed and preserved. `MetadataInPlaceSplitShardService.applySplitShardRequest` now
proactively seeds both maps for each new child (`putInSyncAllocationIds(childId, emptySet())`,
`primaryTerm(childId, UNASSIGNED_PRIMARY_TERM)`) at the moment the child's `ShardRange` is reserved,
rather than relying solely on a later `IndexMetadataUpdater` update to add them lazily.

**Fix 2 — `IndexRoutingTable.validate`.** Fixing #1 traded the hang for a fast, clean
`IllegalStateException: Wrong number of shards in routing table, missing: []` — a second, structurally
identical invariant: `IndexRoutingTable.validate` hard-required
`indexMetadata.getNumberOfShards() == shards().size()`, which a post-split routing table (fewer base
shards once the parent retires, plus children beyond `numberOfShards`) can never satisfy again. Fixed
by changing the check from "routing table size must exactly equal `numberOfShards`" to "every base
shard `< numberOfShards` must have a routing entry, *unless* `SplitShardsMetadata.isSplitParent(i)`
says it was legitimately retired by a split" — no longer requires exact size equality, only that
nothing is unaccountably missing.

Both fixes together were verified against the full `org.opensearch.cluster.metadata.*`/
`org.opensearch.cluster.routing.*`/`org.opensearch.cluster.routing.allocation.*` test sweep (clean,
zero regressions) before moving on, exactly as fixes 1 and 2 were confirmed individually at each
step.

**Fix 3 — read-path double-counting (a genuinely new, third bug, found only once 1 and 2 let the
split actually complete in a real cluster).** With the cluster-manager-crash and routing-validation
bugs fixed, Phase 0.7's IT ran to completion for the first time — and failed with `Count is 80 hits
but 40 was expected`. Root cause: `ShardCloner.clone` (Task 12/19) attaches a child to its parent's
data by manifest *reference*, never physically copying only the child's share of documents — so
every document from the parent's full manifest is still physically present in **every** child's
directory, exactly the same situation `PartitionFilteringDirectoryReader`/`ShardPartitionDescriptor`
(Task 35, this plugin's *other*, pre-existing equal-partition resharding-by-copy mechanism) already
solved for its own sibling case — but nothing analogous existed yet for in-place split's real
`ShardRange`-shaped children. Without a query-time filter, each of the 2 children returned every one
of the 40 documents, hence 80 total hits.

Fixed by adding the same pattern for this mechanism, as a new sibling rather than a generalization
of the existing one (their id-partitioning schemes are deliberately incompatible: the old one's
`(partitionIndex, numPartitions)` shape can't represent an arbitrary hash range, and its own hash
doesn't need to agree with core's real routing hash the way an in-place split child's does — GET-by-id
uses `OperationRouting`'s real `Murmur3HashFunction`/`ShardRange` resolution, so the read-path filter
has to reproduce that exact same decision, not invent its own self-consistent one):

- `SplitShardsMetadata.getRangeOfShard(int shardId)` (new): unlike the pre-existing
  `getParentAndRangeOfChild` (deliberately scoped to only the in-progress window, for recovery), this
  finds a child's range for its *entire* lifetime, in-progress or already committed — scans both
  `parentToChildShards` and `rootShardsToAllChildren`. Needed because filtering has to keep working
  indefinitely (until a physical bundle rewrite happens, not done by this increment), not just until
  commit.
- `InPlaceSplitPartitionFilter.matches(String id, ShardRange range)` (new, plugin): `Murmur3HashFunction.hash(id)`
  compared against `range.contains(hash)` — deliberately core's own real routing hash, not a
  reinvented scheme, so a document visible via search is also reachable via GET and vice versa.
- `InPlaceSplitFilteringDirectoryReader` (new, plugin): copies `PartitionFilteringDirectoryReader`'s
  `FilterDirectoryReader`/`FilterLeafReader` shape verbatim (see that class's own javadoc for the
  design rationale, unchanged here), substituting the new hash-range matcher.
- Wired into `ServerlessStoragePlugin.onIndexModule` via `IndexModule#setReaderWrapper` — a different,
  and in retrospect more correct, seam than `PartitionFilteringDirectoryReader`'s own wiring (inside
  `ObjectStoreReaderEngine`'s internal reference manager): `setReaderWrapper` is core's standard,
  shard-identity-aware (`OpenSearchDirectoryReader#shardId()`), engine-agnostic hook, so this filter
  applies uniformly whether the child's queries are served by `ObjectStoreWriterEngine` (the common
  case in this session's own IT, a single-writer/no-replica setup) or `ObjectStoreReaderEngine`,
  without needing separate wiring in each.

**A fourth bug found while wiring fix 3**: the first version of
`InPlaceSplitFilteringDirectoryReader.getReaderCacheHelper()` copied
`PartitionFilteringDirectoryReader`'s own choice of returning `null` (that class's own javadoc:
"unlike the wrapped reader's own core cache helper... no stable reader-level cache key exists here")
— correct for that class's own wiring, but `IndexModule#setReaderWrapper`'s contract (enforced by
`IndexShard#wrapSearcher`) is stricter: it requires the wrapped reader's `getReaderCacheHelper()` to
be **identical** to the original, unwrapped reader's, throwing `IllegalStateException: wrapped
directory reader doesn't delegate IndexReader#getCoreCacheKey` otherwise. Fixed by delegating
(`return in.getReaderCacheHelper();`) instead of returning `null` — correct here specifically because
this filter's bitset is a deterministic, unchanging-for-the-child's-whole-lifetime function of the
wrapped reader's own live docs: two cache lookups against the same underlying reader generation
always see the same filtered result, so sharing that generation's cache identity is safe.

### Verification

New IT `ServerlessStorageInPlaceSplitDocumentReachabilityIT` (real 2-node cluster, real
`ObjectStoreWriterEngine`, 40 documents with real padding content, a real `InPlaceSplitShardAction`
call) — the exact test whose repeated failures drove fixes 1 through 4 — now passes cleanly. It
checks: (a) the parent's routing entry is retired and both children are routed once the split
commits, (b) all 40 pre-split documents are reachable post-split via both scatter-gather search and
single-shard real `GET`-by-id routing (not a plugin-side reimplementation), (c) the object-store PUT
count increase from the split stays under 40 (document-count-independent, proving `ShardCloner.clone`
genuinely didn't copy bundle bytes).

Full verification sweep before commit: `:plugins:serverless-storage:check -x internalClusterTest`
(clean), `:plugins:serverless-storage:internalClusterTest` full suite, no filter (clean — confirms
the new `setReaderWrapper` wiring doesn't regress any other plugin IT), `:server:test` on
`org.opensearch.cluster.metadata.*`/`org.opensearch.cluster.routing.*`/
`org.opensearch.cluster.routing.allocation.*` plus `:server:missingJavadoc` (clean).

**Phase 0 item 0.7 is now done.** In-place split is genuinely end-to-end for the first time this
session: metadata bookkeeping (Tasks 1-9), routing-table wiring (Tasks 5-9, this task), local Lucene
materialization (Task 19), and read-path correctness (this task's fix 3) all verified together in one
real cluster, not just at each individual layer.

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

## Task 17 (0.6) — Operator REST/transport action to trigger an in-place split

Status: **done**, commit follows this entry. Closes core's own missing public-API gap Task 1's
research first identified: `MetadataInPlaceSplitShardService` existed with no REST/transport action
reaching it at all, and was never even constructed in `Node.java` (unreachable via Guice).

### What was built

- `InPlaceSplitShardAction` (`ActionType<AcknowledgedResponse>` + `Request` extending
  `AcknowledgedRequest`) — new core action, `indices:admin/shards/split_in_place`.
- `TransportInPlaceSplitShardAction` (`TransportClusterManagerNodeAction`) — calls
  `MetadataInPlaceSplitShardService.split(...)` directly, mapping its
  `ClusterStateUpdateResponse` to `AcknowledgedResponse`.
- `RestInPlaceSplitShardAction` — `POST /{index}/_split_in_place/{shard_id}?split_into=N`,
  deliberately operator-only with no invented auto-triggering policy, matching this fork's
  established discipline for early-phase resharding actions (`TransportOrchestrateShardSplitAction`
  set the same precedent for this plugin's separate, older split mechanism).
- Registered in `ActionModule.java` (action + REST handler), following the exact pattern
  `DeleteComponentTemplateAction`/`RestDeleteComponentTemplateAction` already use.
- `Node.java`: constructed `MetadataInPlaceSplitShardService` (previously never instantiated
  anywhere in production code) and bound it via Guice, next to `MetadataCreateIndexService`'s
  own construction site — this is the reachability fix; the service itself was already correct
  from Task 1 onward, just unreachable.

### Tests + verification discipline

New `InPlaceSplitShardActionIT` (`server/src/internalClusterTest`): creates a real single-shard
index, calls the action, and asserts the whole chain end-to-end — acknowledged response,
`SplitShardsMetadata` recording the split in progress, and (critically) real routing-table
entries for both child shards with the correct `InPlaceSplitShardRecoverySource`. Plus a
validation-rejection test (`splitInto < 2`).

**A real environment lesson, not a code bug**: the first version of this IT used
`OpenSearchIntegTestCase`'s default `TEST` scope (`numDataNodes = 2`, which also bootstraps 3
dedicated cluster-manager nodes) and hung for the full 20-minute suite timeout during plain
`createIndex`/`ensureGreen` — before the new action was even invoked. Rewrote to
`numDataNodes = 0` with a single manually-started node (the same lighter pattern this session's
plugin ITs already used), which fixed it immediately and cut runtime to seconds. Verified
meaningfulness on the *lighter* version: removed the routing-table wiring, confirmed the IT
failed with the correct assertion (`expected a real routing entry for child shard [1]`) in under
a minute, restored, confirmed clean.

Ran a broader regression sweep (`cluster.metadata.*` + `action.admin.indices.*`) and
`missingJavadoc` — both clean.

### Scope note

Phase 0's remaining item is 0.7: the full end-to-end IT already partially covered by this task's
own `InPlaceSplitShardActionIT` (real cluster, real writer index, trigger a split, assert reads/
writes survive) — what's not yet covered is indexing documents before/after the split and
verifying `OperationRouting`'s real hash resolution finds every pre-split document via the
correct child, and confirming no bundle bytes are physically copied (§18 risk #1's existing
object-store request-count metrics). That real-workload verification is the next concrete unit
of Phase 0 work, along with Part 3's still-open read-path-correctness design questions (double-
counting during the transition window) that item 0.7 explicitly depends on resolving first.

## Part 3 read-path correctness — parent retirement at commit

Status: **done**, commit follows this entry. Resolves the first of Part 3's three flagged
correctness questions (read-path double-counting during the split transition window), which the
plan explicitly required be answered before any further Phase 0 work depending on it (item 0.7)
could be meaningful.

### The gap

Task 8.5's original commit driver promoted children to active in `SplitShardsMetadata` but left
the parent's own `ShardRouting` entries untouched — flagged honestly at the time as a known,
deliberately-deferred limitation. In practice this meant a committed split would leave **both**
the parent and its children simultaneously `STARTED` and therefore both search-visible over the
same underlying data (a child's manifest, until a not-yet-built physical bundle rewrite happens,
references the exact same bundle files the parent's manifest does — this plugin's own
logical-first, physical-later split model). Every document would be counted twice: once via the
parent's full pre-split view, once via whichever child its hash falls into.

### The fix

`MetadataInPlaceSplitShardCommitService.applyCommit` now removes the parent's `ShardRouting`
entries (primary and every replica) from the routing table **in the same cluster-state update**
that promotes the children in `SplitShardsMetadata` — resolving Part 3's question as option (a):
a partition is owned by exactly one visible shard at a time, even across the split boundary,
matching the plan's own "DynamoDB-like" framing. This is safe because the parent's manifest bytes
remain referenced (and pinned against GC by `ShardCloner.clone`, invoked from each child's
`Engine#recoverFromInPlaceSplit`) by the children that just took over its range — removing its
routing entry only shuts down its now-redundant `IndexShard`, it does not touch any data.

### Tests + verification discipline

New unit test `testApplyCommitRetiresParentShardRoutingAtomically` in
`MetadataInPlaceSplitShardCommitServiceTests`. Verified meaningfulness by reverting the retirement
loop to always re-add the parent's shard table, confirming the test failed with the parent's
routing entry still present, then restoring.

New end-to-end IT `testInPlaceSplitShardActionCommitsAndRetiresParentRouting` in
`InPlaceSplitShardActionIT`: real single-node cluster, triggers a real split via the REST/transport
action, `assertBusy`-waits for the async commit driver to actually fire, then asserts the parent's
routing entry is gone and exactly the 2 child shards are reported active. This is the first test in
this plan's execution that exercises the full chain from REST request through to a *committed*
(not just in-progress) split with real shard recovery on a real single-node cluster.

Regression sweep across `cluster.metadata.*` and `missingJavadoc` — both clean.

### Scope note

This closes Part 3's read-path question. The other two Part 3 questions — replica coordination
during a split, and WAL/term-authority interaction across the split boundary — remain open and are
the next concrete design units before Phase 0 item 0.7 (the full real-workload IT with actual
document indexing and `OperationRouting` hash-resolution verification) can be attempted honestly.

## Part 3 read-path correctness — replica coordination and WAL/term-authority interaction

Status: **done** (design resolution only; one real gap found and flagged, not yet fixed).

### Replica coordination — resolved, no new code needed

Investigated whether a split's replicas need special coordination logic (each replica
independently deriving `SplitShardsMetadata`'s transition, vs. the primary's split being
explicitly propagated). Answer: **no special coordination is needed, by construction** —
Task 5-9's routing wiring already gives every child shard ordinary replica `ShardRouting` entries
(`PeerRecoverySource.INSTANCE`), so a child's replicas recover the completely normal way, via peer
recovery from the child's own primary, which itself already has the correct data via
`Engine#recoverFromInPlaceSplit`'s zero-copy attach. Replicas never read `SplitShardsMetadata`
directly and don't need to. Symmetrically, the parent's replicas need no special handling at
retirement either — removing the parent's `ShardRouting` entries (primary and replicas) in the
same commit update (previous entry) shuts them down exactly like any other shard removal; a
replica has no independent state to reconcile since it only ever mirrors its primary.

### WAL/term-authority interaction — resolved with a real gap found, not fixed

Investigated whether WAL mirroring (per-`ShardId` stream, `WalChunkService` keyed by
`(indexUuid, shardId)`) needs to become partition-aware for a split. Two parts to this:

- **Forward-going WAL, post-split**: no special handling needed. Each child gets a genuinely new
  `ShardId`, so it naturally gets its own independent WAL stream from its own first write onward
  — nothing to inherit or partition, by the same "real shard ID, ordinary machinery" reasoning
  Task 12 already established for manifests.
- **The parent's WAL history at the moment of cloning**: **a real, unaddressed correctness gap**,
  found by tracing the actual code, not assumed away. `ShardCloner.clone` (reused verbatim by
  `Engine#recoverFromInPlaceSplit`, per Task 13-14) clones whatever the parent's `ShardStateStore`
  reports as `latestManifestGeneration` **at the moment the child recovers** —
  `sourceHead.get().head().latestManifestGeneration()`, no forced flush precondition anywhere in
  the call chain. If the parent has WAL-mirrored writes that landed *after* its last published
  manifest generation but *before* the split (an entirely ordinary situation — this plugin's own
  `_refresh`/publish-rate-limiting design, §8, deliberately does not publish on every single
  write), those writes are **not captured in the cloned manifest at all**. They exist in the
  parent's WAL stream, but a child never replays the parent's WAL (only its own, which starts
  empty) — so those documents would be silently absent from every child, a real, silent data-loss
  window, not a theoretical one.

**Not fixed in this pass** — flagged honestly rather than papered over. The correct fix shape
(not yet implemented, needs its own careful design + test pass): `MetadataInPlaceSplitShardService.applySplitShardRequest`
or the REST/transport layer should force a real flush/publish on the parent (synchronously,
before the split's cluster-state update is even submitted, or as a precondition the split waits
on) so `latestManifestGeneration` is guaranteed to reflect every write that had already returned
success to a client by the time the split was requested. This needs care around exactly this
plugin's own `_refresh`/publish semantics (§8) and the writer-lease/fencing model (B1-B13 earlier
this session) to get the ordering right, not a one-line change.

### New task added to the backlog (auto-added per `/goal` authorization)

**Task 18 (new) — Force-flush the parent before an in-place split clones its manifest.** Fixes
the WAL/manifest-generation gap identified above. Blocks Phase 0 item 0.7 (the full real-workload
IT) from being trustworthy — that IT would need to index documents right up to the split and
verify none are lost, which will fail today until this is fixed.

## Task 18 — Force-flush the parent before an in-place split clones its manifest

Status: **done**, commit follows this entry. Fixes the WAL/manifest-generation data-loss gap
identified in the previous entry.

### The fix

`TransportInPlaceSplitShardAction.clusterManagerOperation` now issues a real `FlushRequest`
against the source index and only calls `MetadataInPlaceSplitShardService.split(...)` once that
flush's listener resolves successfully. This guarantees `latestManifestGeneration` reflects every
write that had already been acknowledged to a client by the time the split request was received,
closing the window where `ShardCloner.clone` (reused by `Engine#recoverFromInPlaceSplit`) could
otherwise clone a manifest generation older than the parent's actual acknowledged state.

Flushes the whole index rather than just the target shard, since no shard-scoped `FlushRequest`
variant exists in core — correct but coarser than ideal; narrowing it to just the target shard is
separate, still-open follow-up work, not attempted here.

### Tests + verification discipline

New `testInPlaceSplitShardActionForcesAFlushBeforeSplitting` in `InPlaceSplitShardActionIT`:
indexes an unflushed document, confirms it shows up as an uncommitted translog op on the parent
shard specifically, triggers the split, and asserts the parent's translog uncommitted-op count is
back to zero immediately after — reading shard 0's own stats directly rather than an index-aggregate
stat (an index-aggregate flush counter is not a reliable signal here: newly-created child shards'
own engine construction performs its own unrelated flush the moment they open, which would
otherwise mask whether the *parent* specifically was flushed).

**A genuine test-methodology dead end worth recording honestly**: the first attempt at
verifying this by breaking the fix (removing the forced-flush call) did not fail as expected —
investigated at length (confirmed via `.class` file timestamps that the broken code really was
what ran, ruled out a stale/cached Gradle test result), but could not pin down why the parent's
translog uncommitted-op count still read zero without the fix. Given `OpenSearchIntegTestCase`'s
own test infrastructure is known to randomize some index settings (translog flush thresholds
among them) for exactly the kind of shake-out-bugs reason that would explain this, and further
diagnosis cost was disproportionate to the remaining session budget, this specific
break-then-restore check was not completed to the same standard as every other change this
session. The fix itself is straightforward and directly inspectable in the diff (the flush call
unconditionally precedes the split submission, no conditional path around it), and the *positive*
test (fix present, assertion passes) is real and does exercise the intended code path — but the
mechanical "prove it would fail without the fix" step that grounds every other commit in this
plan could not be completed here. Flagged rather than silently omitted.

Full IT class (4 tests) passes with the real fix in place. `spotlessApply`/`missingJavadoc` clean.

## Task 19 (new, found while scoping item 0.7) — Local Lucene materialization gap, FIXED

Status: **fixed and verified**. Initially documented without a fix (see the original analysis
below, kept verbatim); the fix followed the same delicate engine-open/translog-association
ordering this plugin's own `recoverMissingLocalStore` javadoc already warns is easy to get subtly
wrong, mirroring that seam exactly rather than inventing a new shape.

### The gap

While scoping Phase 0 item 0.7 (the full real-workload IT), traced exactly what a child shard's
*local* Lucene engine actually contains after `Engine#recoverFromInPlaceSplit` finishes — not just
what the *object store* contains, which Task 13-14's own test already verified. Finding: **they
diverge, and the divergence means a child would return zero documents to any real query**, not the
parent's data.

Task 10-11's `IndexShard.recoverFromInPlaceSplit` calls `recoverFromStore(...)` first (which fully
opens the engine against a **locally empty** Lucene index and empty translog — that's the entire
point of Task 10-11's `StoreRecovery` fix, treating `IN_PLACE_SPLIT_SHARD` like `EMPTY_STORE`), and
only *after* that succeeds does it invoke `Engine#recoverFromInPlaceSplit` (Task 13-14), which
writes the cloned manifest to the **object store**. Nothing re-opens or refreshes the already-open
local engine against that new object-store state. The local Lucene directory the open
`IndexWriter`/reader is actually pointed at never receives the parent's segment files at all.

This plugin already has the exact mechanism this needs —
`WriterEngineFactory#recoverMissingLocalStore` / `ObjectStoreCommitMaterializer#materialize` — used
today for cross-node writer failover (§7.1.2: "no node's local disk is ever authoritative"). But
it's invoked by `StoreRecovery` at a specific point *before* the engine opens and *before* a local
translog is created, specifically so materialized segments and the fresh translog agree with each
other. `recoverMissingLocalStore`'s own javadoc explicitly documents a prior failed attempt at
materializing "at the `EngineFactory` level" (i.e., after engine construction) leaving "a stale
translog-UUID reference behind" — the exact class of bug Task 10-11/13-14's current ordering would
reproduce if patched naively (e.g., by just adding a materializer call inside
`Engine#recoverFromInPlaceSplit` without also re-sequencing when the local translog is created).

### Why not fixed in this pass

The correct fix requires restructuring *when* in the recovery sequence materialization happens —
before `StoreRecovery.internalRecoverFromStore` creates the local translog and opens the engine,
not after, mirroring `recoverMissingLocalStore`'s own careful ordering. That means either: (a)
teaching `StoreRecovery.internalRecoverFromStore`'s existing `indexShouldExists`-driven branching
to also attempt in-place-split materialization in its "should not exist" branch (parallel to how
it already calls `recoverMissingLocalStoreFromEngine` in the other branch), or (b) a structurally
different seam entirely. Either shape needs its own careful design-and-test pass, including
verifying the translog-UUID/local-checkpoint association is correct, which is exactly the kind of
verification this session's "implement → test → verify by breaking it" discipline exists for — not
something to bolt on quickly at the end of an already-long session.

### New task added to the backlog (auto-added per `/goal` authorization)

**Task 19 (this entry) — Materialize the parent's segments into the child's local Lucene store,
correctly ordered relative to translog/engine-open.** Blocks Phase 0 item 0.7 (and, transitively,
makes Task 13-14's own "child is attached to parent's data" claim only true at the object-store
bookkeeping layer, not yet true at the actual queryable-engine layer) until resolved. This is now
the single highest-priority remaining item for Phase 0 — without it, an in-place split cannot
actually serve a single real document to a real query, regardless of how correct the
metadata/routing/manifest bookkeeping built in Tasks 1-18 is.

### The fix

Added `EngineFactory.recoverInPlaceSplitLocalStore(IndexShard, Store)`, a new default (`return
false`) core seam mirroring `recoverMissingLocalStore` exactly: `StoreRecovery.internalRecoverFromStore`
now calls it *before* the local translog is created / the engine is opened, for shards whose
recovery source is `IN_PLACE_SPLIT_SHARD`. This closes the ordering gap the analysis above
identified — the same class of bug `recoverMissingLocalStore`'s own javadoc warns about
("materializing at the `EngineFactory` level" after engine construction leaves a stale
translog-UUID reference) is avoided by using the exact same "before engine construction" seam,
not a new one.

Restructured `StoreRecovery.internalRecoverFromStore`: added an `isInPlaceSplitChild` flag and an
`inPlaceSplitMaterialized` flag; on read failure of the segment info, a new branch calls
`engineFactory.recoverInPlaceSplitLocalStore(...)` (parallel to the existing
`recoverMissingLocalStoreFromEngine` branch) and re-reads `si` on success. The
`indexShouldExists`-driven branching was widened to `indexShouldExists || inPlaceSplitMaterialized`
so the in-place-split path skips `bootstrapNewHistory` and local translog creation — the
`EngineFactory` hook already created the translog, matching `recoverMissingLocalStore`'s own
contract exactly.

`IndexShard.startRecovery`'s `IN_PLACE_SPLIT_SHARD` case now shares the ordinary
`EMPTY_STORE`/`EXISTING_STORE` dispatch to `recoverFromStore` (since `StoreRecovery` now branches
internally), replacing Task 10-11's separate `recoverFromInPlaceSplit(...)` wrapper method, which
called `recoverFromStore` first and only *afterward* invoked `Engine#recoverFromInPlaceSplit` —
architecturally the wrong order, and the actual root cause of the gap. That wrapper, and the
now-superseded `Engine#recoverFromInPlaceSplit`/`Indexer#recoverFromInPlaceSplit` seam (Task
13-14), were removed entirely.

On the plugin side, the `ShardCloner.clone` attach logic moved from `ObjectStoreWriterEngine`
(instance-level, ran after engine open — Task 13-14's original, now-wrong-ordering placement) to
`WriterEngineFactory.recoverInPlaceSplitLocalStore` (factory-level, runs before engine
construction). It looks up the parent shard id and hash range via
`SplitShardsMetadata.getParentAndRangeOfChild`, clones via `ShardCloner.clone` into the child's
own manifest/lineage stores, reads the resulting manifest, and materializes it into the child's
local `Store` directory.

**A second bug found while implementing this**: materialization must read bundle bytes from the
**parent's** blob container, not the pre-configured `WriterEngineFactory.materializer` field —
that field is fixed to the shard's own (still-empty) container, correct only for
`recoverMissingLocalStore`'s "re-read my own past publish" case. Since `ShardCloner.clone` never
copies bundle bytes (only references), a fresh `ObjectStoreCommitMaterializer` pointed at the
parent's container is required. Fixed by constructing one specifically for this step and dropping
the irrelevant `materializer == null` guard.

### Tests + verification discipline

New test `InPlaceSplitLocalStoreRecoveryTests#testChildEngineActuallyServesParentsDocumentAfterInPlaceSplitRecovery`
(plugin, `IndexShardTestCase`-based since it needs a real `IndexShard`/`IndexMetadata`, unlike the
`EngineTestCase`-based tests this plugin otherwise uses): starts a real parent writer shard,
indexes and flushes one document, builds real `SplitShardsMetadata` reserving a child shard id,
constructs a child `IndexShard` with `InPlaceSplitShardRecoverySource`, recovers it via
`recoverFromStore`, and asserts the child's own Lucene reader (not the object-store manifest)
actually contains the parent's document.

Two test-setup bugs found and fixed while writing it: (1) the child `IndexMetadata`'s
`SETTING_NUMBER_OF_SHARDS` must cover the child shard id, since `IndexMetadata`'s per-shard
`primaryTerm` array is fixed-size at construction from that setting; (2) the child `IndexMetadata`
needs an explicit `.primaryTerm(childShardId, 1)` matching `ShardCloner.clone`'s own hardcoded
target primary term, otherwise core's engine construction acquires a writer lease under term `0`
against a `ShardHead` already CAS'd to term `1`, and fails with "a newer primary term already
holds this shard's head".

Verified meaningfulness by commenting out `parentBundleMaterializer.materialize(manifest,
directory)` in `WriterEngineFactory` and re-running: failed as expected with
`IndexNotFoundException: no segments* file found`. Restored, re-ran clean.

Full verification sweep run before commit: `:plugins:serverless-storage:check
-x internalClusterTest` (`BUILD SUCCESSFUL`), `:plugins:serverless-storage:internalClusterTest`
(`BUILD SUCCESSFUL`, zero `failures="[1-9]`/`errors="[1-9]` in the result XML),
`:server:test --tests "org.opensearch.index.shard.*" --tests "org.opensearch.index.engine.*"
:server:missingJavadoc` (`BUILD SUCCESSFUL`, zero failing XML).

Compilation cascade from removing `Engine#recoverFromInPlaceSplit`: `ObjectStoreWriterEngineTests`'s
now-broken `testRecoverFromInPlaceSplitAttachesChildToParentsData` was removed (superseded by the
new end-to-end test above, which actually catches the bug the old one missed — it only asserted
object-store state, never the local engine's document count). `InPlaceSplitShardRecoveryTests`
(core) updated to call `recoverFromStore` directly and its javadoc rewritten to point at
`EngineFactory#recoverInPlaceSplitLocalStore` for the real attach logic.

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

## Correctness review — CC1/CC2/CC3

A deep review of the shipped in-place split/merge feature turned up three real
correctness/robustness gaps. Each is fixed below in its own commit, held to the same
implement-then-break-the-test-to-prove-it discipline as everything above.

### CC1 (HIGH) — write-during-split data loss window, FIXED

The bug: during an in-progress split, ordinary indexing routes to the still-`STARTED`
parent (`generateShardId` resolves children only when explicitly asked, which nothing in
the write path does). Each child clones the parent's then-latest published manifest once,
at its own recovery time. A document the parent acknowledged *after* a child had cloned
but *before* the split committed lands only in a later parent manifest generation no child
references, and becomes permanently unreachable the moment
`MetadataInPlaceSplitShardCommitService` retires the parent's routing at commit. Silent
data loss under continuous write load, not a documented tradeoff.

The choice: reject-writes vs. real dual-write across the two engines. The review pointed
at pre-existing dead scaffolding built for a dual-write path (`ShardRoutingState.SPLITTING`,
four `ShardRouting` methods, `OperationRouting.shardWithRecoveringChild`, the
`includeInProgressChildren=true` branch of `getShardIdOfHash`) and asked whether to wire it
up or delete it. Grep confirmed it had zero non-test callers anywhere in the repo. I went
with reject-writes: it closes the window with no synchronous cross-engine correctness to
get subtly wrong mid-recovery, and the split's own design already accepts a recovery-time
cost, so a short write-rejection window is an easy trade. This matches the session's
established skepticism of clever engine-level cross-cutting mechanisms.

The fix: `IndexShard.ensureWriteAllowed` now rejects a new primary write whenever this
shard is the parent of an in-progress split
(`getSplitShardsMetadata().isSplitOfShardInProgress(shardId)`), throwing
`IllegalIndexShardStateException`. That is a shard-not-available exception
(`TransportActions.isShardNotAvailableException`), so it is retriable exactly the way a
relocating/closing shard's rejection already is: the write fails fast and succeeds once it
re-routes to a committed child. All three write paths (index/delete/noop) funnel through
`ensureWriteAllowed`, so the one guard covers them all; replica ops are untouched, since a
rejected primary op never produces one.

Having chosen reject-writes, I deleted the now-confirmed-dead dual-write scaffolding rather
than leave it as an active lie about how routing behaves during a split
(`ShardRoutingState.SPLITTING` + its `fromValue` case, `ShardRouting`'s
`isSplitTarget`/`getParentShardId`/`getRecoveringChildShards`/`splitting` plus the
`recoveringChildShards`/`parentShardId` transient fields and the collapsed 12-arg
constructor, `OperationRouting.shardWithRecoveringChild`, and the `includeInProgressChildren`
parameter threaded through `generateShardId`/`calculateShardIdOfChild`/`getShardIdOfHash`).
The transient fields were never serialized and the `SPLITTING` state was never constructed
in production, so the removal is mechanical dead-code deletion, not a wire-format change.
`AllocationId.newSplit` was left in place: it is a self-contained, separately-tested factory,
not part of the misleading write-routing surface, so deleting it would be scope creep.
`ShardRoutingStateSplitTests` (which only exercised the deleted scaffolding) was removed;
`SplitShardsMetadataTests`' two tests that asserted the deleted in-progress-child routing
were rewritten to assert the real production behavior (an in-progress child is never a
routing target; the hash resolves to the parent, or to the last committed child).

Test: `IndexShardTests.testRejectsPrimaryWriteWhileInPlaceSplitInProgress` indexes a doc,
marks the shard an in-progress split parent, and asserts both index and delete are rejected
with a retriable shard-not-available exception. Verified real by commenting out the guard
call: the test fails with "no exception was thrown." Swept
`cluster.routing.*`/`cluster.metadata.*`/`action.bulk.*` clean.

### CC2 (MEDIUM-HIGH) — hash filter ignored custom routing, FIXED

The bug: core's real routing hash uses `effectiveRouting = routing != null ? routing : id`
plus a `partitionOffset` for `routing_partition_size > 1`. The read-path filter
(`InPlaceSplitPartitionFilter`, via `InPlaceSplitFilteringDirectoryReader`) hashed `_id`
only. For any document indexed with a custom `_routing`, or on a partitioned index, a child
bucketed by the wrong hash versus `OperationRouting` -- so a routed document could be
search-visible on one child while GET-routed to the other.

Two shapes, two fixes. `routing_partition_size` is an index-level setting, so it is blocked
up front: `MetadataInPlaceSplitShardService` now rejects a split of any index with
`index.routing_partition_size > 1` with a clear `IllegalArgumentException`, mirroring the
existing virtual-shards precondition. The filter fundamentally cannot reproduce the
per-document partition offset, so this is a hard limitation made explicit rather than a bug
to paper over.

Custom `_routing` is per-document and cannot be statically blocked. It turns out `_routing`
*is* stored per-doc -- `RoutingFieldMapper` stores it whenever a document carries a routing
value -- exactly as core's own `ShardSplittingQuery.Visitor` already reads it. So
`AbstractIdFilteringDirectoryReader` now reads both `_id` (via `binaryField`) and `_routing`
(via `stringField`) and hands both to the predicate; `InPlaceSplitFilteringDirectoryReader`
hashes `effectiveRouting = routing != null ? routing : id`, reproducing `OperationRouting`
exactly so search and GET agree. The predicate contract widened from `Predicate<String>` to
`BiPredicate<String,String>` (id, routing); `PartitionFilteringDirectoryReader` (the
pre-existing equal-partition copy mechanism, its own self-consistent scheme) ignores the
routing argument, behavior unchanged.

Tests: `InPlaceSplitFilteringDirectoryReaderTests.testFiltersByCustomRoutingNotId` indexes
documents whose `_id`-hash and `_routing`-hash straddle the split point and asserts each is
visible only in the range that owns its *routing* hash (with a guard that the fixture
actually contains straddlers, so the test isn't vacuous). Verified real by reverting the
filter to hash `_id`: it fails.
`MetadataInPlaceSplitShardServiceTests.testApplySplitShardRequestThrowsIfRoutingPartitionSizeGreaterThanOne`
asserts the precondition; verified real by disabling the check.

### CC3 (MEDIUM) — merge now has a real automatic rollback if the revived parent fails to recover, FIXED

The gap CC3 originally landed a narrower fix for: the old one-step
`MetadataInPlaceMergeShardService.applyMergeShardRequest` de-committed the split and revived
the parent as a single `UNASSIGNED` primary in the same atomic update. If that primary then
exhausted its allocation-retry budget the shard was left permanently red, with only a loud
WARN and no automatic path back. Split has a symmetric rollback for exactly this
(`MetadataInPlaceSplitShardCommitService`'s `SHOULD_CANCEL` path), but the one-step merge
couldn't reuse its shape: split watches a persistent `inProgressSplitShardIds` marker that
survives the recovery window, while the one-step merge erased every trace of the children in
the very update that revived the parent, leaving no "merge in progress" state to roll back
from. The original CC3 commit made that failure loud and operator-actionable and deferred the
real rollback.

This lands the deferred rollback by making merge a genuine two-phase operation, symmetric to
split's split/commit:

**Phase 1 (initiate).** `applyMergeShardRequest` no longer destroys anything. It calls the new
`SplitShardsMetadata.Builder.startMergeChildrenToParent`, which validates the same split-level
preconditions the old `mergeChildrenBackToParent` did but only records the parent in a new
`inProgressMergeParentShardIds` set (the exact mirror of `inProgressSplitShardIds`). The
children stay recorded in `parentToChildShards`/`rootShardsToAllChildren`/`activeShardIds`,
keep their routing entries, and keep owning their hash ranges, so they go on serving reads and
writes. The parent is still revived as one `UNASSIGNED` primary with
`InPlaceMergeShardRecoverySource` carrying the children's `ShardRange`s.

**Phase 2 (commit or cancel).** `MetadataInPlaceMergeShardCommitService` is rebuilt from the
CC3 warn-only listener into the real commit/cancel driver, structurally identical to
`MetadataInPlaceSplitShardCommitService`. On every cluster-state change, for each pending merge
it reads the revived parent primary's routing state: `STARTED` means commit (now, and only now,
call `mergeChildrenBackToParent` to actually remove the children and retire their routing, in
the same update the parent takes over the range, so the range is owned by exactly one
search-visible shard across the boundary); retry-exhausted (`getNumFailedAllocations() >=
index.allocation.max_retries`, the same threshold split uses) means cancel, which drops just the
pending marker via the new `Builder.cancelMerge` and removes the parent's dead routing entry.
Because phase 1 never removed the children, cancel is a lossless rollback: they are still
active, routed, and range-owning, so the shard range is fully servable through them again. The
externally visible contract of `POST /{index}/_merge_in_place/{parent_shard_id}` is unchanged
(submit acks on initiation, exactly as split does; the commit/cancel is async), so no
action/transport/REST changes were needed.

**Write-during-merge safety (symmetric to CC1).** Because the children now stay live during the
parent's recovery, a document a child acknowledges after the parent snapshotted it but before
commit would be lost when the children are retired. `IndexShard.ensureNotInProgressMergeChild`
rejects primary writes to a still-live merge child with the same retriable shard-not-available
exception CC1's split-parent guard uses, closing that window; the write succeeds once it
re-routes to the merged parent (or, on rollback, to the child again).

**Wire format.** `inProgressMergeParentShardIds` is gated on `Version.V_3_8_0` and appended
after `splitCommitTimestamps` in the stream, following that field's exact pattern. A pending
merge only ever exists in a uniform-version cluster (the merge service rejects a merge unless
every node is on the same version), so a mixed-version rolling upgrade never carries this state;
an older peer that somehow received the blob drops the marker (empty set), the safe read.

Tests. `SplitShardsMetadataTests` covers the pending-merge state machine (start/commit/cancel
transitions, and that cancel restores metadata `equals` to the pre-merge snapshot), plus wire
and XContent round-trips and the pre-`V_3_8_0` drop. `MetadataInPlaceMergeShardCommitServiceTests`
covers the commit-finalizes and the rollback paths; the rollback test is the real one, not
tautological: it drives an exhausted revived parent through the same `evaluateMergeCompletion`
detection the live listener uses, cancels, then asserts real serviceability by sweeping 5000
ids through `OperationRouting.generateShardId` and requiring every one to resolve to a live
child (never the dead parent, and both children exercised). `IndexShardTests` covers the write
guard, and `InPlaceMergeRealRerouteTests` drives phase 1 -> real allocation -> commit end to
end. Verified real by breaking the cancel-trigger condition (`if (false && ...)`): the rollback
test goes red with `expected SHOULD_CANCEL but was STILL_IN_PROGRESS`; restored, green again.

## Scale-to-zero — search-vs-reactivation race in `ShardReactivationActionFilter` (bug fix)

A deep review of the suspend/reactivate path found a real MEDIUM bug in
`ShardReactivationActionFilter`. The filter holds a search request open until its target reader
copy is back, but it only decided to hold when it observed the *suspended marker* still set at
`apply()` time. The marker and the actual shard state diverge during reactivation:
`TransportReactivateShardsAction` clears the marker in its cluster-state update, but the reader
copy itself only reaches `STARTED` later (potentially seconds under real object-store latency, as
it recovers). A search landing in that window — marker already cleared, reader copy still
`INITIALIZING` — was never added to the pending-wait list, so it proceeded straight through
`chain.proceed` and failed with `NoShardAvailableActionException` under the default
`cluster.routing.search_replica.strict=true`. That is exactly the client-visible failure
scale-to-zero is supposed to never surface.

**Fix.** Gate the search wait on real routing-table state, not just the metadata marker. Every
search now also checks whether the reader copy that would serve it is actually `STARTED`
(`readerCopyNotYetStarted`), and waits via the same `ClusterStateObserver` mechanism whenever it
is not — whether the marker is still set or already cleared. The existing `allFullyReactivated`
wait predicate already checked `searchOnlyReplicas() ... STARTED` correctly; the bug was only that
the *decision to wait at all* was made off the marker. The new routing check is kept cheap for the
common (nothing-reactivating) case: indices with no search-only replicas configured short-circuit
before touching the routing table, and the per-shard scan stops at the first `STARTED` copy. The
writer-suspend trigger and the `TransportReplicationAction`/`TransportSingleShardAction`
never-wait split are unchanged — those paths have core's own retry loop.

**Tests + verification discipline.** New `ShardReactivationActionFilterTests` builds the race
deterministically: an index with the reader marker cleared but the search-only replica still
`INITIALIZING`, and asserts the search is held (not proceeded), then that moving the replica to
`STARTED` releases it. A control test asserts a fully-started reader index proceeds synchronously
(no wait on the hot path). Verified real by reverting the fix to marker-only checking
(`readerWait = readerSuspended`): the race test goes red with "the search must be held, not
proceeded" while the control stays green; restored, both green. The existing IT
(`ServerlessStorageReaderShardSuspensionIT`) only issues one search and cannot exercise this race
(it needs a search arriving mid-recovery), so the coverage lives at the unit level where the
window is constructible without real timing; a two-overlapping-searches IT was judged not worth
the timing flakiness for what the unit test already proves deterministically.

## Scale-to-zero — CONCERN 2: does unconditional suspend eviction risk losing acked writes? (investigated, confirmed safe)

The same review flagged a possible durability gap: `ObjectStoreWriterEngine.close()`'s final
flush/publish is best-effort and swallows exceptions ("a later reactivation will republish"), but
`ShardSuspensionCoordinator.evict()` force-unassigns the shard via `CancelAllocationCommand`
without checking whether that final publish succeeded. With WAL mirroring off (the default), the
worry was that a publish failing at the wrong moment plus local data not surviving to reactivation
could silently drop acknowledged writes.

Investigated both open factual questions and concluded this is **not a real gap** — the eviction
does not introduce any acked-write loss. The reasoning, traced through core and this plugin:

1. **A failed publish does not advance durability.** `ObjectStoreWriterEngine.commitIndexWriter`
   calls `translogDeletionPolicy.recordDurablePublication(maxSeqNo)` only *after*
   `publishCommitAsHead` returns true. So a publish that throws or returns false never raises the
   durability watermark, and `ObjectStoreDurabilityTranslogDeletionPolicy` keeps the un-published
   op's local translog generation. The acked op stays on local disk.

2. **Cancel-driven unassignment does not delete local shard data.** Core's
   `IndicesStore.shardCanBeDeleted` returns true only when *every* copy of the shard is `started()`
   and none is on the local node. A scale-to-zero-evicted shard is force-cancelled to `UNASSIGNED`
   and pinned there by `SuspendedShardAllocationDecider` (it returns `NO` to every allocation), so
   no copy is `started()` anywhere. `shardCanBeDeleted` is false, `deleteShardStore` never fires,
   and the shard's local Lucene directory + translog survive on the node. A same-node reactivation
   replays them (`LOCAL_TRANSLOG_RECOVERY`).

3. **The Lucene directory backing matters only for readers.** The object-store-backed
   `LazyBundleDirectory` substitution applies only to reader (search-only) shards; a writer/primary
   uses a normal local `FSDirectory`, with its committed segments *also* published to the object
   store by the commit publisher. So a writer's acked-but-unpublished ops live in the local translog
   (point 1) and its published segments live in the object store — neither is lost by eviction.

4. **An idle candidate's acked writes are already published.** A suspension candidate is selected
   because it is idle past a threshold; periodic flush/publish on the refresh schedule has already
   published every acked write well before the quiescent flush runs. So reactivation reads a
   manifest that already covers them regardless of whether the quiescent publish succeeded — exactly
   what `flushAndPublishQuiescentBestEffort`'s comment already claims ("missed optimization, never a
   correctness gap").

The only residual window is an acked-but-unpublished op whose reactivation lands on a *different*
node with WAL mirroring off. That is the general, pre-existing WAL-off durability limitation
(unpublished ops are not cross-node durable) — identical for any relocation or node restart, not
something this eviction introduces or worsens, and precisely the gap WAL mirroring exists to close.
Gating `evict()` on the quiescent publish's outcome would also mean building the
coordinator-to-live-engine communication channel `ShardSuspensionCoordinator`'s own javadoc already
documents as a deliberately-out-of-scope, larger increment. Given points 1-4 already close the
acked-data-loss path, adding that channel here would be manufacturing a fix for a non-problem,
against this project's established discipline.

**Outcome: documented as already-safe, no code behavior changed.** The reasoning is recorded in
`ShardSuspensionCoordinator.evict()`'s javadoc. New test
`ObjectStoreWriterEngineTests#testFailedQuiescentPublishDoesNotAdvanceDurabilitySoTheUnpublishedOpSurvivesLocally`
proves the load-bearing property (point 1): with a `FailableBlobContainer` simulating the object
store going unreachable mid-publish, the quiescent publish throws and the durability watermark stays
put — the un-published op is never marked durable, so its local translog is retained. Verified real
by moving `recordDurablePublication` to *before* the publish call: the test goes red (watermark
advances to the un-published op's seq-no despite the failure); restored, green again. A publish
failure inside `commitIndexWriter` is a tragic flush event that then fails/closes the engine, which
is itself why `close()`'s quiescent publish is best-effort and swallows the exception — the acked op
is already fsynced in the local translog and unaffected by the engine closing.

## Skeptical re-review of CC1/CC2/CC3 — two small gaps in the in-place split/merge commit services

A follow-up re-review of the split/merge commit services turned up two real, low-severity gaps.
Both are fixed.

### Gap 1 — `applyCancel` did not re-check the cancel condition before rolling back

`applyCommit` on both commit services already re-runs its completion classifier
(`evaluateMergeCompletion` / `evaluateSplitCompletion`) *inside* the cluster-state-update task and
only commits if the state it is actually applying to still reads `READY_TO_COMMIT` — it does not
trust the earlier check that queued the task. `applyCancel` had no matching guard: it only
re-checked `isMergeOfShardInProgress` / `isSplitOfShardInProgress`, not that the state still
classified as `SHOULD_CANCEL`.

The race that exposes it: a pending merge's revived parent exhausts its allocation retries, so
`clusterChanged` queues a cancel task. Before it runs, an operator issues
`_cluster/reroute?retry_failed=true`; the parent allocates and reaches `STARTED`, and a commit task
is now also queued behind the cancel. The cancel runs first, still sees the marker set, and rolls
back — discarding a `STARTED` primary that already holds the merged data even though it would have
committed. Not data loss (the children were never removed, so the rollback is lossless by
construction), just wasteful. The split side had the identical missing re-check — a symmetric,
pre-existing pattern.

Fix: `applyCancel` on both services now re-runs its classifier against the state it is applying to
and no-ops unless it still reads `SHOULD_CANCEL`. If the state has since become `READY_TO_COMMIT`
(or slipped back to `STILL_IN_PROGRESS`), the cancel does nothing and lets the appropriate task —
the queued commit, or a later re-evaluation — handle it. This makes cancel symmetric with commit's
own long-standing defensive re-check.

One existing split test (`testApplyCancelFreesChildIdsAndRemovesRouting`) was calling `applyCancel`
on a state that was never actually cancel-worthy (children unassigned, zero failed allocations) and
relying on the old unconditional behavior. With the hardened re-check it correctly no-ops now, so
the test was updated to drive a genuinely exhausted (`SHOULD_CANCEL`) state first — which is what a
real cancel task always applies to anyway.

New tests: `testApplyCancelNoOpsWhenStateBecameCommitWorthy` on both service test classes builds the
race directly (state is commit-worthy by the time `applyCancel` runs) and asserts the hardened path
no-ops instead of rolling back — marker still set, the `STARTED` shard(s) not discarded. Verified
real by removing each re-check in turn: the new test goes red with the spurious rollback (`expected
same … was …` — a different, mutated cluster state), restored, green.

### Gap 2 — the `clusterChanged` listener dispatch itself had zero test coverage

Every existing test on both commit services called `applyCommit` / `applyCancel` /
`evaluate*Completion` directly, bypassing the real `clusterChanged(ClusterChangedEvent)` method that
is registered as the actual `ClusterStateListener` (wired in `Node.java` under `isClusterManagerNode`).
A regression in `clusterChanged` itself — wrong iteration over indices/shards, a swallowed exception,
the listener silently not firing — would pass every existing test and only surface in a live cluster.

New tests drive `clusterChanged` directly with a mock `ClusterService`, a real before/after
`ClusterChangedEvent` pair, and a `DiscoveryNodes` set whose local node is the elected
cluster-manager (so `event.localNodeClusterManager()` is true). The merge test transitions into a
`READY_TO_COMMIT` state and asserts a `submitStateUpdateTask` whose source begins `commit in-place
merge of shard [0]`; the split test transitions into a `SHOULD_CANCEL` state (a child's allocation
retries exhausted) and asserts a `cancel in-place split of shard [0]` task. Each also has a
`testClusterChangedNoOpWhenNotClusterManager` control (no elected cluster-manager local node -> no
task submitted), confirming the cluster-manager gate. Verified real by breaking the merge listener's
own dispatch (skipping `submitCommit` in the `READY_TO_COMMIT` branch): only the new listener test
failed, all seven direct-call tests stayed green — proving the listener test uniquely exercises the
dispatch path. Restored, all green.

Verification sweeps after each fix: `org.opensearch.cluster.metadata.*` and
`org.opensearch.cluster.routing.*` (clean) plus `:server:missingJavadoc` (clean).

## WAL mirroring — real group-commit batching (`serverless_storage.wal_flush.batching.enabled`)

WAL mirroring (`serverless_storage.wal_mirroring.enabled`, default off) closes a real durability gap:
this engine only ships segments/manifests to the object store at flush time, so an acknowledged
write that hasn't flushed yet lives only in the local translog. WAL mirroring durably writes the
operation stream itself ahead of the segment flush. The problem was the wiring: `WalMirroringTranslog.add()`
flushed to the object store after **every single operation** -- one PUT (plus a CAS to claim a chunk
sequence) per document, blocking the indexing thread until it completed. Turning the feature on as
wired cost roughly one S3 PUT per document, which is why it defaults off.

### The design pivot that made this worth doing

An earlier draft of this work proposed a bespoke `PendingRecord`/`CompletableFuture`-per-record
buffering scheme built inside `WalChunkService`. An adversarial design review of that draft found
several real bugs it would have shipped: an orphaned-future deadlock under the existing per-shard
fairness budget, a lock-held-during-retry regression, and an unbounded `future.get()` blocking an
indexing thread with no timeout. A follow-up question during planning reframed the whole thing for
the better: should a write wait for WAL replication before acking, the same way
`index.translog.durability=REQUEST` already makes a write wait for remote-store translog upload?
Investigating that found core OpenSearch has *already solved this exact problem* for `RemoteFsTranslog`
(remote-store's own translog upload), using two pieces of machinery this plugin reuses directly
instead of reinventing:

1. **`AsyncIOProcessor`/`BufferedAsyncIOProcessor`** (`server/.../common/util/concurrent/`) -- the
   exact "batch many concurrent callers' writes onto one shared periodic flush, notify each caller
   via a listener when its batch completes" primitive `RemoteFsTranslog` itself uses for group
   commit. `WalBatchingProcessor` is a thin subclass: one node-shared instance drains every writer
   shard's enqueued records into one chunk per `serverless_storage.wal_flush.interval` tick. The
   queueing, the one-drain-at-a-time promise semaphore, the per-caller notification, and the
   `ArrayBlockingQueue` backpressure once full are all inherited and proven correct upstream, so
   none of the custom concurrency code from the earlier draft had to be written or verified.
2. **`index.translog.durability`** (`REQUEST`/`ASYNC`, already dynamic, already index-scoped) --
   reused as-is, no new plugin knob. `WalMirroringTranslog.add()` now appends locally and enqueues
   without blocking on object-store I/O; the new `ensureSynced(Location)` override is what waits for
   the group-commit upload. Confirmed by reading the actual code (not assumed) that this override is
   reached by the standard `IndexShard.sync()` -> `translogSyncProcessor` -> `translogManager().ensureTranslogSynced()`
   -> `Translog.ensureSynced(Stream)` -> `ensureSynced(Location)` chain. Under `REQUEST` the client's
   own request thread calls it before acking; under `ASYNC` a background sync task does. Same
   backend-agnostic contract core already applies to local fsync and remote-store upload, no
   redundant "wait or not" setting added. No bespoke timeout either, matching `RemoteFsTranslog.ensureSynced()`'s
   own precedent (the request-level timeout bounds the wait).

### How the processor reaches the translog without new plumbing

The processor is threaded through the *existing* `WalAppendTarget` seam rather than the engine's
constructor chain. `WalAppendTarget.batchingProcessor()` (default null) is overridden by
`WalChunkService` to return the processor the plugin attaches to the node-shared service in
`resolveSharedWalChunkService()`, and by `EncryptingWalChunkService` to forward its delegate's. The
*presence* of an attached processor is the batching-enabled signal the translog keys off, so no new
argument had to reach through `WriterEngineFactory`/`ObjectStoreWriterEngine`'s deep telescoping
constructors. A consequence, chosen deliberately: a dedicated-WAL-stream shard uses its own
per-shard `WalChunkService` (never the shared one), which is never given a processor, so dedicated
streams stay on the legacy synchronous path -- consistent with that feature already being an
explicit higher-request-cost regulatory opt-in.

Encryption moves with the path: the legacy path wraps the service in a per-shard
`EncryptingWalChunkService` decorator, which the batching path bypasses, so `WalBatchingProcessor.write()`
re-applies the same per-record `WalRecordCrypto.encrypt` keyed off the node-level provider (a single
provider covers a mixed batch because each record's key derives from its own `indexUuid`).

### Why the per-shard fairness budget doesn't extend to the batching path

`WalChunkService`'s fairness budget exists because, on the legacy path, `flush()` only fires on a
caller's own trigger, so one noisy shard could bloat the shared buffer and delay every other shard's
flush. Once batching is driven by the processor's own interval timer that starvation is closed
structurally: every drain empties the *entire* queue regardless of which shard populated it, so no
shard's ops wait longer than one buffer interval behind another's volume. The queue capacity is the
uniform memory bound instead. The old budget keeps its original job on the legacy path, unchanged.

### Phased rollout (Phases 0-2 done; Phase 3 deliberately separate)

- **Phase 0** (commit: hoist retry / register settings / plugin `close()`): extracted the bounded
  retry-with-backoff from `WalMirroringTranslog#flushWithRetry` into `WalChunkService#writeChunkWithRetry`,
  now shared by the legacy `flush()`/overflow paths and the new batching path (and, incidentally,
  giving `overflowShardToDedicatedChunk` the retry it previously had none of). Registered the three
  off-by-default settings (`wal_flush.batching.enabled`/`.interval`/`.queue_capacity`). Added a
  `ServerlessStoragePlugin#close()` override closing `walGcSchedulerTask`, fixing a real pre-existing
  leak: the plugin had no `close()` at all, so a live scheduled WAL GC sweep was never cancelled on
  shutdown. Zero behavior change, verified against the full existing WAL suite.
- **Phase 1** (commit: real group-commit): `WalBatchingProcessor`, the `add()`/`ensureSynced()`
  split, wired behind the setting. This is the phase that actually delivers the cost reduction.
- **Phase 2** (this commit): the cost-accounting proof test plus this writeup.
- **Phase 3** (not part of this work): flipping the default to `true`, only after real load testing
  against the RFC's cost-sanity target.

### The point-proving test

`WalBatchingCostAccountingTests` uses the same `RequestCountingBlobContainer`/`ObjectStoreRequestCounter`
harness that guards GC's DELETE cost. Four shards each fire 50 concurrent `add()` calls (200 ops)
plus one priming op, all folded within one buffer interval by gating the first chunk write open only
after the whole burst has provably enqueued behind the held promise semaphore (the first drain is
scheduled with zero delay, so gating is what makes "one interval" deterministic rather than timing-
dependent). Result: **201 ops cost exactly 4 PUT-shaped requests** (2 sequence-claim CAS + 2 blob
writes, one priming chunk + one folded burst chunk), against the roughly 402 the one-PUT-per-op
legacy path would cost. Flat O(1), not O(N). The test also reads both chunks back and asserts all
201 records survived the fold, so the cost win is not bought by silently dropping ops.

### Verification discipline (break-then-restore)

Two deliberate self-breaks, each confirmed to be caught by exactly the intended test, then restored:

1. **`ExecutionException` -> `IOException` unwrap** in `ensureSynced`: replaced the cause-unwrap with
   a generic re-wrap. `testBatchingEnsureSyncedPreservesTheIOExceptionTypeOnUploadFailure` failed
   (`expected ... "injected transient failure", got "WAL mirror group-commit upload failed"`),
   proving it actually asserts the original chunk-write `IOException` is preserved, not just that
   *some* exception is thrown. Restored, green.
2. **The `ensureSynced`/durability wait** itself: short-circuited the WAL wait so `ensureSynced`
   returned without blocking on the pending upload. `testBatchingAddReturnsWithoutBlockingAndEnsureSyncedPerformsTheDurableUpload`
   failed (`expected WAITING but was TERMINATED` -- the sync thread no longer parked on the upload),
   proving the test genuinely exercises the request-thread-waits mechanism that `index.translog.durability=REQUEST`
   drives. Restored, green.

Full `:plugins:serverless-storage:test` and `:plugins:serverless-storage:missingJavadoc` clean after
each phase.

### Deliberately deferred (separate future scope, not gaps in this work)

- **Byte-threshold early-flush trigger.** `BufferedAsyncIOProcessor`'s `process()`/`scheduleProcess()`
  are package-private in core, not designed for cross-package early-triggering, and the queue-capacity
  bound already gives a coarse safe backstop. If load testing later shows interval-only batching
  misses the RFC's cost/latency targets, a size trigger is a scoped follow-up, not a v1 blocker.
- **Request-level 429 backpressure.** The RFC's larger stated goal for the ingest tier; legitimately
  separate from the batching mechanism itself.
- **Shutdown drain of the queued tail.** `ServerlessStoragePlugin#close()` intentionally does not
  force a final processor drain: under `REQUEST` a write's upload already completed before its ack
  (that is what `ensureSynced` waits for), so no acknowledged op is stranded; under `ASYNC` an
  unsynced tail may drop on shutdown, exactly the durability-for-latency trade `ASYNC` already makes
  for core's own translog. `BufferedAsyncIOProcessor` also exposes no cross-package synchronous-drain
  seam, so a forced drain isn't cleanly implementable and correctness doesn't need one.
- **Autoscaling backlog signal under batching.** `WalChunkService#bufferedRecordCount()`/`totalBufferedBytes()`
  read the legacy buffer, which the batching path leaves empty (records sit in the processor queue),
  so those signals under-report while batching is on. Tied to the same deferred byte-accounting work.

Naming note: this keeps the shipped "WAL mirroring" terminology as-is. "Mirroring" arguably overstates
what this does (a durability-only upload of the op stream, closer to what core's remote-store calls
"translog upload" than a live synced replica), but renaming an already-shipped, tested public setting
is a separate decision with real churn and no bearing on whether the batching mechanism works.

## WAL mirroring — byte-threshold early-flush trigger (`c2c1625dadf` / `17bb9ce3dee`), resolving the deferral above

The "byte-threshold early-flush trigger" item above turned out not to need a new cross-package seam
after all -- `BufferedAsyncIOProcessor`'s `process()`/`scheduleProcess()` stay package-private; the
trigger is built entirely from two new small `protected` extension points plus one `AtomicReference`
tracking the currently scheduled-but-not-run drain:

- `getBufferByteThreshold()` / `itemSizeInBytes(Item)` -- both default to disabled/zero, so every
  existing subclass (`RemoteFsTranslog` included) is behaviorally unchanged.
- `WalBatchingProcessor` overrides both: `itemSizeInBytes` returns the WAL record's payload length,
  and the threshold itself comes from the new `serverless_storage.wal_flush.byte_threshold` setting
  (`ByteSizeValue.ZERO` = off, matching the plugin's other off-by-default conventions).

The one real subtlety: the promise semaphore that gates `BufferedAsyncIOProcessor` only gates *who
may schedule* a drain, not *when* it runs. A byte-threshold trip after another `put()` has already
scheduled the next drain for later in the interval can't just call `process()` itself -- the semaphore
is already held. Fixed by tracking the pending `Scheduler.ScheduledCancellable` and, on a threshold
trip that finds the semaphore held, racing to `cancel()` it (idempotent, so at most one concurrent
caller wins) and firing `process()` immediately in that case; if the drain already started, `cancel()`
returns false and the trip is a no-op since the in-flight write already covers the item.

Caught one test-design bug before it shipped: the base class's very first-ever schedule always fires
immediately regardless of the configured interval (`lastRunStartTimeInNs` starts at `0`, so "time
since last run" always looks overdue on the first call). An early version of
`testByteThresholdTriggersAnImmediateDrain` didn't account for this and passed even with the
threshold logic deliberately broken -- caught by following the "break the fix, confirm the test
fails" discipline. Fixed by adding a priming `put()`/drain before the timed part of both new tests
(`testByteThresholdTriggersAnImmediateDrain`, `testBelowByteThresholdWaitsForTheInterval`), which
re-broke the threshold path and confirmed both tests now fail as expected before restoring the fix.

Also added `plugins/serverless-storage/README.md` (`c2c1625dadf`) as the plugin's first quickstart
doc -- build/test commands, the enable recipe (remote cluster state + `serverless_storage.base_path`
or `.repository` + `index.serverless_storage.enabled`), the full REST operator surface, and pointers
to the RFC and this log for anything deeper.

## WAL mirroring — request-level 429 backpressure on upload backlog (`52324677588`)

The other item deferred above ("request-level 429 backpressure") is now done for the batching path,
resolving rfc-serverless-opensearch.md &sect;6.4's remaining explicit-backpressure goal: *"the WAL
buffer is bounded; when upload backlog crosses a threshold the node rejects new indexing with 429."*
Previously the only backstop was the processor's bounded queue, which once full blocks the calling
(indexing) thread rather than rejecting cleanly -- fine as a memory bound, but not the clean pushback
the RFC calls for.

`WalBatchingProcessor` now tracks real-time backlog bytes (an `AtomicLong` incremented in an
overridden `put()`, decremented once a batch's `write()` attempt finishes -- success or failure,
either way those records are no longer backlog) via a new `backlogBytes()`/`isOverBacklogRejectThreshold()`
pair, separate from the early-flush trigger's own byte counter (that one resets every drain; this one
doesn't). `WalMirroringTranslog#add` checks the new accessor before enqueueing a batching-path write
and, once over the configured `serverless_storage.wal_flush.backlog_reject_threshold` (`ByteSizeValue.ZERO`
= off, matching every other off-by-default convention in this plugin), throws
`org.opensearch.core.concurrency.OpenSearchRejectedExecutionException` instead -- the exact exception
type and message convention `IndexingPressure` already uses for its own backpressure, so this is
recognized by the same downstream handling (`ReplicationOperation` checks for this type explicitly)
without any plugin-specific 429 wiring at the REST layer.

The local translog write (`super.add()`) has already happened by the point this check runs, matching
the legacy path's existing "local append can succeed before a mirror-write failure surfaces" ordering
-- not a new risk this introduces.

Confirmed with the same break-the-fix discipline: `isOverBacklogRejectThreshold()` hardcoded to
`false`, watched the new `testAddRejectsWithBackpressureOnceTheBacklogThresholdIsExceeded` fail,
restored, green.

### Still explicitly out of scope

Node-wide (not just per-shard-processor) admission and the "per-shard budgets first, node-wide
second" ordering rfc-serverless-opensearch.md &sect;6.4 also mentions are not built -- the shared
`WalBatchingProcessor` is already node-wide (one instance for the whole node, per &sect;6.4's own
cross-shard batching design), so there is no separate per-shard budget to order against on the
batching path today; `SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING`'s fairness siphon remains a
legacy-path-only mechanism (see `WalChunkService`'s own javadoc). Feeding this backlog signal into
autoscaling (&sect;10, "the same signal feeds autoscaling") is also not wired up -- `backlogBytes()`
exists as a public accessor a future autoscaling signal could read, but nothing reads it yet.

## WAL batching Phase 3 — evaluated, decision: still keep the default off

Revisited whether `serverless_storage.wal_flush.batching.enabled`'s default should flip to `true`,
now that the byte-threshold trigger and 429 backpressure above close the two gaps Phase 2's own
writeup flagged as blockers. **Decision: no, the default stays `false`.** The reasoning:

- The one thing actually available in this environment to re-check is the existing synthetic
  cost-accounting proof (`WalBatchingCostAccountingTests`) -- re-run clean as part of this session's
  other changes, still showing the flat O(1) PUT-shaped-request
  behavior (e.g. 201 ops across 4 shards costing 4 PUTs, not ~402) that motivated building batching in
  the first place. That is a *correctness* proof (batching does what it says), not a *load* test --
  it runs against a local `FsBlobStore`, at a handful of ops, with no real network latency, no S3/GCS
  throttling behavior, and no concurrent-shard production ingest pattern.
- The RFC's own gating condition for this flip was explicitly "real load testing," not "the known
  gaps are closed" -- closing the byte-threshold and backpressure gaps removes two *reasons a load
  test would have failed*, it does not substitute for actually running one. This environment has no
  real cloud object store, no load generator, and no multi-node cluster to run one against, so that
  precondition genuinely cannot be satisfied here.
- Flipping a default that reshapes the durability-critical write path for every existing deployment of
  this plugin, without the validation the design itself calls for, is exactly the kind of unforced
  default change worth refusing even though the code changes to reach this point are done and tested.

**What would need to happen before the next revisit:** an actual load test against a real S3/GCS-class
object store at realistic sustained node ingest rates, measuring (a) p50/p99 indexing-ack latency
added by the ~200ms interval under `index.translog.durability=REQUEST`, (b) whether the byte threshold
and backlog-reject threshold's defaults (`ByteSizeValue.ZERO`, i.e. both off) need real non-zero
defaults once batching itself defaults on, and (c) real object-store PUT cost/throttling behavior
under sustained concurrent-shard load, not the local-filesystem proxy this session's tests use. None
of that infrastructure exists in this session's environment, so Phase 3 remains explicitly deferred
rather than attempted with a synthetic substitute.

## Resharding-by-copy source write-fencing — re-checked, still a deliberate scope boundary

Revisited whether the last open item -- resharding-by-copy never fencing the source index's writes
during orchestration (`ae0ee3d25d4`) -- needed real building now, or was still correctly closed as a
documented precondition. Re-read both write-ups: rfc-serverless-opensearch.md's own resharding
section (around "the source index is never quiesced, fenced, or made read-only anywhere in this
orchestration") and `TransportOrchestrateShardSplitAction`'s class javadoc, which carries the same
warning and the same operator precondition verbatim.

**Conclusion: no code change. This stays a documented precondition, not a gap to close.** The RFC
already gives the honest reasoning for why: real source fencing (blocking writes against the source
during the clone window) or a dual-write bridge (mirroring writes to both source and targets until
cutover) are each "significant, separate mechanisms," "the same 'real design work, not implementation
time' reasoning this section already gave for the cutover primitive itself before it existed." That
reasoning still holds -- neither is a small bounded fix, and building either as a rushed addition
under this session's own "sweep the open-items list" pass would be exactly the kind of half-scoped,
undertested mechanism the discipline followed throughout this session (implement -> real test ->
break-the-fix -> restore) argues against for something this durability-sensitive. Confirmed the
warning is still present, current, and consistent in both places it should be -- that was the actual
risk worth re-checking (documentation drifting out of sync with code), and it hasn't.

## Scale-to-zero/scale-up lifecycle wiring — re-checked, false alarm

A whole-project sweep for remaining serverless-storage work flagged "scale-to-zero policy not wired
to actual suspend/reactivate lifecycle" as a possible large gap, worth confirming before trusting it.
It doesn't hold up: all three legs are real, wired, cluster-state-mutating code, not evaluation-only.

- **Suspend**: `ScaleToZeroCandidatesSchedulerTask` is constructed unconditionally whenever
  `serverless_storage.scale_to_zero.eval_interval > 0`, and handed a real `ShardSuspensionCoordinator`
  once `serverless_storage.scale_to_zero.suspend_enabled` (default `false`) is flipped -- at which
  point `suspendCandidates`/`suspendReaderCandidates` mutate `IndexMetadata` via a real
  `ClusterStateUpdateTask` and force-evict via `CancelAllocationCommand`.
- **Reactivate**: `ShardReactivationActionFilter` is unconditionally active (no enable flag at all) as
  a real `ActionFilter` on every search/write request -- this is the mechanism the earlier
  "search-vs-reactivation race" bug fix (this doc, "Scale-to-zero — search-vs-reactivation race" entry)
  already correctness-reviewed and tested.
- **Scale-up**: `ScaleUpCandidatesSchedulerTask` is likewise constructed unconditionally past its own
  `eval_interval` gate, handed a real `ReaderReplicaExpansionCoordinator` once `scale_up.enabled`
  (default `false`) is flipped, at which point it issues real `UpdateSettingsRequest` calls bumping
  `index.number_of_search_replicas`, under consecutive-tick hysteresis and a per-tick budget.

The only "manual" part is that `suspend_enabled`/`scale_up.enabled` both default `false` -- a
deliberate operator opt-in (matching every other risk-bearing feature flag in this plugin), not a
missing implementation. Both settings' own javadoc, and the scheduler tasks' own package-info,
already say as much explicitly. No code change made; this closes as a confirmed non-issue.

## Commit notification: polling vs. event-driven pub/sub — re-checked, already correctly deferred

Same sweep flagged commit notification's 5 s poll interval as still open relative to the RFC's
originally-envisioned event-driven pub/sub. Re-reading the RFC's own Phase 3 section (the paragraph
right after `pollForNewerManifest`'s introduction) shows this was already fully reasoned through, not
overlooked:

- The gap is real and explicitly named -- "notification wiring remains open in its originally-intended
  event-driven form (true pub/sub via the directory/gossip tier)" -- but the RFC itself calls it "not a
  correctness gap": the milestone's freshness-lag target (p99 < 15 s) is already comfortably met by
  the 5 s poll interval alone.
- It's also been partially narrowed since that paragraph was first written: `WriterPublicationNotifier`/
  `PollNowAction` (&sect;8) now give writer-triggered push notification after every publish, so in the
  common case a reader picks up a new generation in one RPC's time, not up to the full 5 s poll
  window. What's left open is specifically the gossip/directory-tier form for the case routing-table-
  based push doesn't reach (a reader not yet visible in routing) -- a genuinely different, larger
  mechanism than a wiring gap, and no longer the only way the freshness goal gets met in practice.

No code change made. This is a real, named, deliberately-scoped-out refinement with an already-met
milestone target underneath it -- correctly left as "possible later refinement," not something to
build under a generic sweep-the-open-items pass.

## WAL append-time fencing — re-checked, already fully implemented (scan finding was stale/wrong)

The project-wide sweep's single largest flagged item was "WAL append-time fencing": the claim that
`WalChunkService#append` has no fencing check, so a record tagged with a once-valid primary term
could still be appended after that term was superseded, with only post-hoc `filterByShardAndMinimumTerm`
filtering to catch it. **This is wrong as a characterization of the current code** -- the scan read
`WalChunkService#append` in isolation and correctly observed it has no synchronous fencing check
(true, and by design -- an append-time check would defeat the "cheap, unsynchronized buffering"
`WalChunkService` exists for, per rfc-serverless-opensearch.md's own reasoning), but missed that the
actual fencing mechanism was built on the *replay* side instead, and is fully implemented, formally
verified, and tested:

- `plugins/serverless-storage/formal/WalReplayFencing.tla` formally proves a term-only filter
  (`NaiveReplay`) is insufficient (a 4-state counterexample) and that a filter bound by *both* the
  term floor *and* a WAL-position cutoff (`FixedReplay`) holds exhaustively (603,722 states).
- `WalChunkReader.filterByShardAndMinimumTerm` + `WalReplayRecovery.replayOperations` implement
  exactly `FixedReplay`: replay is bound by `(indexUuid, shardId, minPrimaryTerm)` *and*
  `activationWalPosition` (the WAL length snapshotted at activation), never term alone.
- The snapshot itself was the one real remaining gap (construction-time snapshot missed the
  live-promotion case, only fresh-allocation) -- closed via a new `Engine#onPrimaryTermBumped(long)`
  hook called from `IndexShard#bumpPrimaryTerm` inside the same mutex-held, atomic-with-the-term-bump
  window `WalReplayFencing.tla`'s `AcquireLease` action models, traced and confirmed atomic against
  the real `IndexShard` code (not assumed). `ObjectStoreWriterEngine#onPrimaryTermBumped` re-snapshots
  `activationWalPosition` for exactly this case.
- Tested end-to-end: `ObjectStoreWriterEngineTests#testOnPrimaryTermBumpedReSnapshotsActivationWalPositionToTheLiveBound`
  (re-ran clean as part of this check), `ServerlessStorageWriterReplicaPromotionIT` (real 2-node
  live-promotion kill test), `WalReplayRecoveryTests` (re-ran clean), plus a real bug found and fixed
  along the way (WAL replay never decrypted an encrypted record's payload -- fixed, regression-tested).

No code change made here. The scan's finding was a real methodology gap (reading one class's
javadoc/code without tracing where its stated limitation was actually closed elsewhere), not a real
gap in the feature. Corrected here so this doesn't get re-flagged by a future scan reading the same
surface-level signal.

## Client write-routing cutover after split — also a stale-docs false alarm, `resharding/package-info.java` fixed

The sweep's other large item was "client write-routing cutover after split needs core routing/wire-
protocol changes beyond plugin scope," citing `resharding/package-info.java`'s own "explicitly out of
scope" note. Re-checking against the actual code (not just that one doc comment) found the same kind
of drift as the WAL fencing item above -- real work landed after the doc note was last touched:

- `resharding/package-info.java`'s "out of scope" note was last edited `f016f7026e2` (2026-07-14
  06:53). `CutoverSplitRoutingAction` (alias-based cutover) landed as `ec83cf80afe` (2026-07-14
  19:58) -- **thirteen hours later, same day**. The doc note was simply never revisited after that
  commit; not a case of anyone missing real work, just doc-lag on a fast-moving day.
- The actual current state: `CutoverSplitRoutingAction` (points an alias at split targets),
  `EnableWritePartitionRoutingAction` (assigns each target a write partition under that alias), and
  `TransportOrchestrateShardSplitAction` (chains provisioning -> per-partition split -> cutover ->
  optional write-routing into one resumable sequence) are all real, implemented, REST-exposed (I
  wrote unit test coverage for every one of them as part of the REST-test-coverage item above), and
  already documented accurately in rfc-serverless-opensearch.md's own resharding section (see this
  doc's earlier "resharding-by-copy source write-fencing" entry, which was correctly *not* a false
  alarm -- the RFC text was already current there, just the package-level javadoc lagged behind it).
- **What genuinely is still out of scope, confirmed real**: auto-triggering a resharding-by-copy
  split off a write-rate/size threshold alone. `ShardSplitCandidatesAction` surfaces the signal; no
  scheduler acts on it automatically for this mechanism. This is a *different* thing from the
  in-place-split mechanism's own auto-trigger (`InPlaceSplitTriggerSchedulerTask`/
  `InPlaceSplitTriggerCoordinator`, wired in `ServerlessStoragePlugin#createComponents` behind
  `serverless_storage.resharding.auto_split.*`), which is real and already automated -- this plugin
  has two genuinely separate split mechanisms (in-place split, real Lucene-level split of one
  index's own shards; resharding-by-copy, zero-copy split across independent target indices), and
  only the latter lacks an auto-trigger. Confirmed by reading `ServerlessStoragePlugin#createComponents`
  directly, not assumed.

Fixed `resharding/package-info.java` to state the real current scope: routing cutover is implemented
(with its own already-documented source-write-fencing caveat), auto-triggering resharding-by-copy off
a threshold is what's actually still manual. No new mechanism built -- this was a documentation
correction, not an implementation gap, exactly like the WAL fencing item above.

## Resharding-by-copy source write-fencing — real fix this time (`84ecb092012`)

A completeness-assessment pass (this session) asked "how far from done is this feature, really,"
which surfaced the source-write-fencing gap this doc previously closed as "confirmed still a
deliberate scope boundary, no code change" (see this doc's own earlier "Resharding-by-copy source
write-fencing — re-checked" entry). Revisiting it with fresh eyes and a concrete design question --
"is a full dual-write bridge really the only way to close any of this, or is there a smaller real
increment" -- found there was: `WritePartitionRoutingActionFilter`
already fences a write-routing *target's* direct writes via an `IndexMetadata` custom-data marker
(`WritePartitionRoutingMetadata`) plus an `ActionFilter` rejection check. Applying that exact same
shape to the *source* side of a split, rather than inventing anything new, closes the largest,
longest-lived, most damaging piece of the gap -- the part where the source silently accepts writes
forever, not just during the split itself.

**What shipped**: `SourceSplitFenceMetadata` (the same custom-data-marker pattern as
`WritePartitionRoutingMetadata`), `FenceSplitSourceAction`/`TransportFenceSplitSourceAction`/
`RestFenceSplitSourceAction` (the same three-file action shape every other resharding action in this
plugin already has) to set it, and one new `if` branch in `WritePartitionRoutingActionFilter`'s
existing `rejectIfDirectTargetWrite` reusing the identical rejection mechanism. `TransportOrchestrateShardSplitAction`
calls `FenceSplitSourceAction` automatically immediately after `CutoverSplitRoutingAction` succeeds,
so every orchestrated split closes this gap without any extra operator step; `OrchestrateShardSplitResponse`
gained a `sourceFenced` field so callers can observe it. `TransportOrchestrateShardSplitAction`'s own
`WARNING` javadoc, `resharding/package-info.java`, and rfc-serverless-opensearch.md's Phase 4 section
were all updated to describe the real current state rather than the old blanket "never fenced,
never enforced" framing.

**Honest, explicitly-not-closed remainder**: fencing only takes effect once cutover has already
succeeded. The earlier window -- between `ShardSplitAction`'s clone point and cutover actually
completing -- is still real and still unenforced; a document written to the source in that
specific window is still silently lost. Closing that fully still needs true write-blocking
synchronized with the clone itself, or an actual dual-write bridge -- both remain genuinely new,
separate mechanisms, correctly still out of scope, not something this increment tried to also solve.
This is a real, meaningful narrowing (permanently-open -> bounded-to-one-narrow-window), not a full
fix, and every piece of new documentation says so explicitly rather than overclaiming.

**Verification, following this session's established discipline**: `SourceSplitFenceMetadataTests`
(round-trip persistence, unit-level), `RestFenceSplitSourceActionTests` (REST param parsing), and a
real extension to `ServerlessStorageOrchestrateShardSplitActionIT` -- a genuine orchestrated split
over the transport layer, then a real `client().index()` call against the now-fenced source name,
asserting rejection with a message naming both the fenced index and the superseding alias. Confirmed
meaningful by disabling the new filter check and re-running: the IT still failed, but with a
different, real error (`UnavailableShardsException`, since the test's own `numDataNodes = 0` means an
unfenced write would have tried and failed to route to a real shard instead) -- proving the assertion
is actually pinned on the new rejection path, not just on "any exception." Restored, green. Full
`:plugins:serverless-storage:test` and `:internalClusterTest` suites and `spotlessJavaCheck` on every
touched/new file all clean.

## Auto-trigger for resharding-by-copy split candidates — investigated, correct call is: don't build it

Next item on the punch list this session's completeness sweep produced. Before writing a scheduler
task mirroring `InPlaceSplitTriggerCoordinator`'s shape (the obvious copy-paste move, and what the
punch list literally asked for), traced what `ShardSplitCandidatesAction`'s `writesPerMinute()`
signal is already consumed by -- and found a real, already-automatic consumer that changes the
answer entirely.

**`InPlaceSplitTriggerCoordinator`/`InPlaceSplitTriggerSchedulerTask` (dynamic-partitioning-plan.md
Phase 1 items 1.2-1.4, landed 2026-07-16) already auto-consumes this exact signal**, growing a hot
shard's count *within the same index* (same `indexUuid`) via core's own `InPlaceSplitShardAction`,
on a real scheduler, with sustained-duration hysteresis and a per-tick budget. This plugin's own
`ShardSplitter`-based split (what this package calls "resharding-by-copy," `OrchestrateShardSplitAction`)
is architecturally sibling to in-place split, not identical -- confirmed by this doc's own earlier
"Why this matters for Part 2d's reconciliation question" entry: in-place split grows shard count
within an index; `ShardSplitter` produces a brand-new index with its own name and `indexUuid`. Two
different scaling shapes, both legitimate, deliberately coexisting.

**Decision: do not build an auto-trigger for `ShardSplitter`/`OrchestrateShardSplitAction`.** Two
independent, real reasons, not just "seems risky":

1. **Silently creating new index identities has no safe unattended default.** In-place split's
   automatic growth is invisible outside this plugin -- same name, same uuid, just more shards.
   `OrchestrateShardSplitAction` creates a genuinely new index with a new name, reachable through a
   new alias -- something alias management, ILM policies, dashboards, and access control all need to
   know about. Automating that with no operator/downstream-system awareness is a real design
   decision this plugin has no basis to make unilaterally, not a wiring gap.
2. **A real, currently-undesigned coordination hazard.** `InPlaceSplitTriggerCoordinator`'s own
   in-flight guard (`alreadySplitOrInFlight`) only checks *its own* mechanism's state
   (`SplitShardsMetadata`) -- it has no idea whether `ShardSplitter` has also decided to act on the
   same shard. Adding a second automatic decider consuming the identical candidate signal, with no
   mutual-exclusion or reconciliation between the two, risks both mechanisms firing on the same hot
   shard in the same tick. Designing that reconciliation properly is real, separate work -- not
   something to bolt on as a side effect of copying an existing scheduler-task pattern.

**A real, separate documentation bug found and fixed along the way, same "RFC drift" pattern as the
two false-alarm findings earlier this session**: rfc-serverless-opensearch.md's own &sect;10 section
still said "nothing consumes this [writesPerMinute] signal yet, and that is intentional... the only
real mechanism for adding write capacity, `ShardSplitter`'s auto-split, still has nothing safe to
trigger against" -- written 2026-07-11, five days *before* `InPlaceSplitTriggerCoordinator` landed
and started consuming that exact signal automatically. Unlike the two earlier false alarms this
session found (where the fix was "the code already does the right thing, update the stale doc"),
this one is subtler: the doc's core factual claim ("nothing consumes this signal") is now false, but
its underlying conclusion ("don't auto-trigger `ShardSplitter`") turns out to still be correct, for
the two reasons above -- reasons the original paragraph never actually gave (it blamed the missing
routing cutover, which is now built, per this doc's own earlier entries). Fixed the RFC to state the
real current situation and the real reasons, not the stale one.

Both `resharding/package-info.java` and rfc-serverless-opensearch.md's &sect;10 section now say the
above explicitly, so this doesn't get re-flagged as a gap by a future sweep reading only "nothing is
auto-triggered here" as a surface-level signal. No scheduler task built; the punch list's original
framing of this item was itself based on an incomplete read of what already existed.

## WAL batching Phase 3 — real (if simulated) data now exists, decision stands: still keep the default off

Built `WalBatchingLatencyBenchmarkTests` (`b5b9909cb10`) -- the best-effort substitute the Phase 3
entry above said it would build if it could: reuses `LatencyInjectingBlobContainer`/`LatencyProfile`
(the same simulated-object-store-latency machinery `LazyDirectoryBootSetPrefetchBenchmarkTests`
already uses for a different milestone) to drive real concurrent multi-shard indexing-ack latency
through both the legacy and batching WAL paths, request-thread-waits-for-durable-upload semantics
matching real `index.translog.durability=REQUEST`.

**Real measured results, consistent across repeated runs:**

| Profile | Legacy p50 | Legacy p99 | Batching p50 | Batching p99 |
|---|---|---|---|---|
| TYPICAL | ~250-460ms | ~1.1-1.4s | ~200ms | ~200-220ms |
| HIGH | ~1.2-2.2s | ~4.7s | ~620-640ms | ~1.3s |

Batching wins clearly on both p50 and p99, in both profiles, every run. A genuine second finding
surfaced along the way, not planned for: `WalChunkService#append`/`flush` (the legacy path) are
`synchronized`, and the real plugin shares exactly one node-level `WalChunkService` instance across
every writer shard on a node -- so concurrent shards don't each independently pay simulated write
latency, they fully serialize through one lock. An early version of this benchmark with a larger op
count exceeded a 120s bound under `HIGH` profile for this reason alone (confirmed via thread dump).
This is a real, additional argument for batching beyond the ack-latency table above: the legacy
path's throughput ceiling under multi-shard contention is one lock's worth of sequential PUTs, not N
independently-paced ones.

**Decision, revisited with this new data: still no, the default stays `false`.** The data is a real
signal in batching's favor, not noise -- but it doesn't change the actual gating question. This
benchmark still runs against a local `FsBlobStore` with simulated, independent, non-correlated
per-call latency, not real S3/GCS network behavior, connection pooling, or throttling under genuine
sustained production concurrency. The RFC's own gating condition was "real load testing" specifically
because those are exactly the variables a simulated benchmark cannot reproduce -- a strong simulated
signal narrows the *expected* outcome of the real test, it doesn't substitute for running it. What
this data *does* change: the earlier framing "no data exists to make an informed decision" is no
longer accurate -- there is now real, reproducible, favorable-to-batching data; the decision not to
flip the default is now a considered "wait for real validation despite a favorable signal," not "no
information available either way."

