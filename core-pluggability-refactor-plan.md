# Core Pluggability Refactor — Implementation Plan

**Branch:** `refactor/core-pluggability` (off `feature/serverless`)
**Origin:** [Core Pluggability Review](https://claude.ai/code/artifact/64634987-24c5-4361-bee9-8138f8f4cede) — a six-pass, seven-sub-pass audit of every core (`server/`) file `feature/serverless` modifies relative to `main`.
**Goal:** shrink and clean up the ~30 inline-coupling findings from that review so `server/` stops naming serverless concepts by name, while keeping `plugins/serverless-storage/` and `sandbox/` as the only place that vocabulary lives. Independent bug fixes found along the way ship separately.

This document is the single source of truth for the refactor while it's in flight. Update task status here as phases land; don't let this drift from `TaskList`.

---

## Ground rules for every phase

1. **No behavior change for a node without `plugins/serverless-storage` installed.** Every new hook defaults to today's behavior. If a phase can't be done without changing default behavior, stop and flag it instead of proceeding.
2. **Old call sites keep compiling.** Where an existing method's contract changes shape (e.g. `Metadata.Builder` constructors), add an overload; don't break the old one in the same commit that also changes behavior.
3. **One phase, one concern.** Don't fold a bug fix into an SPI-migration commit. Category-c fixes (§8) are cherry-picked out separately so they're reviewable (and upstreamable) on their own.
4. **Compile + run the touched module's tests after every phase**, not just at the end. `./gradlew :server:compileJava :server:test --tests "<touched packages>"` at minimum; full `./gradlew check` is too slow to run after every commit but must pass before a phase is considered done.
5. **`plugins/serverless-storage` moves in lockstep.** A core phase isn't done until the plugin is updated to use the new seam and the plugin's own test suite (`./gradlew :plugins:serverless-storage:test :plugins:serverless-storage:internalClusterTest`) passes.

---

## Phase sequencing

```
A (correctness fixes)  ─┐
                         ├─▶ independent, do first, lowest risk
B (NodeStats SPI)       ─┘

C (push resolution down into Metadata/RoutingTable) ─── highest leverage, do before D/E
   │
   ├─▶ D (IndexCreationStrategy SPI, replaces name-prefix convention)
   │
   └─▶ E (move IndicesClusterStateService policy into plugin)

F (mapper capability rollout)   ─┐  independent of C/D/E, can run in parallel
G (fork-codec governance)       ─┤
H (admission-control decoupling)─┘

I (extract category-c fixes to their own branch) ── can run anytime, no dependency
```

C is the load-bearing phase: D and E both consume the resolver interfaces C introduces, so doing C first means D and E are pure call-site migrations instead of also inventing the abstraction.

---

## Phase A — Correctness fixes (do first)

These aren't architecture work; they're bugs the review surfaced that need a decision regardless of how the rest of this plan goes.

### A1. Wire in or remove the dead deadlock guard

**File:** `server/src/main/java/org/opensearch/cluster/metadata/MetadataCreateIndexService.java:2312-2330`

`refuseToWriteAMappingFromTheClusterStateThread(...)` is fully implemented and documented — it exists to stop a specific deadlock in the gated-mapping path — but nothing calls it.

**Steps:**
1. Re-read the method's javadoc and reconstruct the exact call sequence it's meant to guard (the gated put-mapping path: `MetadataMappingService.putMapping` → `recordGatedMapping` → wherever a mapping write could land back on the cluster-state-update thread).
2. Write a test that reproduces the deadlock scenario without the guard (should hang or assert-trip under `-ea`).
3. If the scenario is real: call the guard from the correct point in `MetadataMappingService`/`MetadataCreateIndexService`'s gated path, confirm the test now fails fast instead of deadlocking.
4. If investigation shows the scenario is no longer reachable (e.g. a later refactor closed it a different way): delete the method, and record in the commit message exactly which later change made it unreachable, with a citation.
5. Either way, this ships as its own commit with its own test — don't bundle with anything else.

**Risk:** low (additive guard) if wired in; zero if deleted with justification. **Blocking:** nothing.

### A2. Close the alias/DLS gap for gated indices

**Files:** `MetadataIndexAliasesService.java:166-177`, `Metadata.java:478-491`, `IndexNameExpressionResolver.java:697-712` (`filteringAliases`)

The bug: `MetadataIndexAliasesService` lets a gated index acquire an alias post-creation but stores only the name (`IndexDescriptor.aliases()` is `List<String>`, no filter/routing/write-index). `Metadata`/`IndexNameExpressionResolver` both assume "a gated index has no aliases, so no filtering is needed" and skip filter enforcement for one. A filtered alias against a gated index looks like it worked and silently doesn't filter.

**Steps:**
1. Confirm the failure mode with a test: create a gated index (via the plugin's test harness), add a **filtered** alias to it, run a search through the alias, assert the filter is/isn't applied. This nails down whether it's a silent bypass or something else already catches it (the review couldn't fully verify without running the plugin's test harness).
2. If confirmed as a bypass, the fix is **not** "teach `IndexDescriptor` to carry filter/routing/write-index" (that's a bigger, separate feature) — the fix for now is **reject the operation that creates the inconsistency**: `MetadataIndexAliasesService`'s gated-index branch should refuse (not silently truncate) an `AliasAction.Add` that carries a filter, routing, or `isWriteIndex`, with a clear error telling the caller filtered aliases aren't supported on this index type yet. Name-only aliases keep working.
3. Update the invariant comments in `Metadata.java:478-491` to state the invariant is now *enforced at write time*, not just assumed.
4. Add a regression test asserting the new rejection, plus the original "gated index has no aliases at creation" test stays green.

**Risk:** low — it narrows an already-broken path rather than widening anything. **Blocking:** nothing, but do this before any phase that touches `MetadataIndexAliasesService` again (D touches it).

### A3. Decide the `ShardRoutingState.SPLITTING` removal deliberately

**Files:** `ShardRoutingState.java`, `ShardRouting.java` (recoveringChildShards/parentShardId/splitting()), `OperationRouting.java` (shardWithRecoveringChild)

This was removed as a side effect of the in-place-split rework, not as its own reviewed decision, and it's a `@PublicApi` wire-format enum constant that ships in `main` today.

**Steps:**
1. Grep `main` and this branch for any real caller of `SPLITTING`/`shardWithRecoveringChild`/`getRecoveringChildShards` outside test code, to establish whether removal is actually safe.
2. If nothing depends on it: keep it removed, but add an explicit `@Deprecated` + `CHANGES.md`/release-notes entry documenting the removal as an intentional, reviewed API change, with a migration note (the replacement is `SplitShardsMetadata.isSplitParent()`/`isSplitOfShardInProgress()`).
3. If anything depends on it (including a mixed-version-cluster wire-compat concern): reinstate the enum value and the `ShardRouting` fields, and layer the new `SplitShardsMetadata`-based representation *alongside* it rather than replacing it.
4. Either outcome ships as its own commit, separate from A1/A2.

**Risk:** low to reinstate (additive), needs care either way since it's wire format. **Blocking:** nothing.

---

## Phase B — Restore a generic node-stats SPI

**Files:** `Plugin.java`, new `PluginNodeStats.java` (restore), `NodeStats.java`, `NodesStatsRequest.java`, `TransportNodesStatsAction.java`, `server/src/main/java/org/opensearch/plugin/stats/*` (NativeAllocatorPoolStats, AnalyticsBackendNativeMemoryStats)

**Steps:**
1. Restore `PluginNodeStats` (abstract `NamedWriteable`, per-plugin payload) and `Plugin.nodeStats()` (`@ExperimentalApi`, default `List.of()`) exactly as they existed before deletion — check `git log -p -- server/src/main/java/org/opensearch/plugins/PluginNodeStats.java` on `main` for the last-known-good version. `Plugin.getAdditionalHealthPaths(Settings)` is unrelated and stays as-is; both methods coexist.
2. Restore `NodesStatsRequest.Metric.PLUGIN_STATS` and the `readPluginStats`/`writePluginStats` wire handling in `NodeStats`, tolerant-of-unknown-plugin-entries as it was before.
3. Keep the `NativeAllocatorPoolStats totalNativeAllocatorStats`/`totalEstimatedNativeBytes` fields on `NodeStats` for this phase — don't rip them out yet, that's a breaking wire-format change on its own and out of scope here. Just make sure the generic path exists *alongside* them.
4. In `plugins/serverless-storage` (or wherever the Arrow/analytics-backend plugin lives — confirm exact module first, this may be in `sandbox/` not `plugins/`), add a `PluginNodeStats` subclass wrapping `NativeAllocatorPoolStats`/`AnalyticsBackendNativeMemoryStats` and override `nodeStats()` to return it, as a **second**, additive way to get the same data.
5. Once that's landed and stable (a follow-up phase, not this one): deprecate the hardcoded `NodeStats` fields in favor of the generic path. Don't do this in the same phase — two wire-format changes to `NodeStats` in one commit is asking for trouble.

**Risk:** medium — touches the `_nodes/stats` wire format. Version-gate every new field the same way the branch already does elsewhere (`Version.onOrAfter(...)`). **Blocking:** nothing, independent of C/D/E.

---

## Phase C — Push resolution down into `Metadata` / `RoutingTable`

This is the load-bearing phase. Everything in D and E gets simpler once this lands.

### C1. Define the resolver interfaces (additive, no behavior change yet)

New files:

```java
// server/src/main/java/org/opensearch/cluster/metadata/IndexMetadataResolver.java
public interface IndexMetadataResolver {
    /** Called only on a Metadata.index(name) miss. Return null to mean "genuinely absent." */
    @Nullable IndexMetadata resolve(Metadata metadata, String indexName);
}

// server/src/main/java/org/opensearch/cluster/routing/IndexRoutingResolver.java
public interface IndexRoutingResolver {
    /** Called only on a RoutingTable.index(name) miss. Return null to mean "genuinely absent." */
    @Nullable IndexRoutingTable resolve(ClusterState state, IndexMetadata indexMetadata);

    /** Shards this resolver believes are locally assigned but absent from the published routing table. */
    default Collection<ShardRouting> localShardsFor(ClusterState state, String nodeId) {
        return List.of();
    }

    /** Whether a routing table should be published at all for this index (false ⇒ scale-to-zero-style). */
    default boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return true;
    }
}
```

Add a new `ClusterPlugin` default method:

```java
default Optional<IndexMetadataResolver> getIndexMetadataResolver() { return Optional.empty(); }
default Optional<IndexRoutingResolver> getIndexRoutingResolver() { return Optional.empty(); }
```

This is the exact shape `EnginePlugin.getEngineFactory()` already uses — same pattern, same file, no new precedent to justify.

**Test:** unit tests for the interfaces' default behavior only; nothing wired in yet, so no behavior change is possible at this point. Compile-only phase.

### C2. Wire the resolver into `Metadata`

- `Metadata` gains an optional `@Nullable IndexMetadataResolver resolver` field, set once at construction by whichever code builds the "live" `Metadata` the cluster actually runs (this is `ClusterService`/`Node` startup, where `PluginsService.filterPlugins(ClusterPlugin.class)` already runs for other hooks — reuse that same collection point). `Metadata.Builder` gets a `resolver(IndexMetadataResolver)` setter; every other builder path (tests constructing bare `Metadata` objects) is unaffected because the field defaults to `null`.
- `Metadata.index(String name)`: on a map miss, if `resolver != null`, call `resolver.resolve(this, name)` before returning `null`.
- Exactly one call site changes control flow (`Metadata.index`); every caller elsewhere in core (`OperationRouting`, `TransportBulkAction`, `IndexNameExpressionResolver`, etc.) is untouched at this step, since they already call `metadata.index(name)`.

**Do not yet remove `AbsentIndexDescriptorSuppliers` or its call sites** — this step only proves the plumbing works. Run the full existing gated-index test suite against this new path by having `AbsentIndexDescriptorSuppliers`'s own internals delegate to (or be replaced 1:1 by) the plugin's new `IndexMetadataResolver` implementation, side by side with the old static-registry path still installed, and diff the two for a while under test before deleting the old path in C4.

### C3. Wire the resolver into `RoutingTable` / `RoutingNodes`

Same shape: `RoutingTable.index(String)` and `RoutingTable.allShards()` consult `IndexRoutingResolver.resolve(...)` on a miss; `RoutingNodes.localRoutingNode(...)` calls `resolver.localShardsFor(...)` instead of `AbsentIndexRoutingSuppliers.localShards(...)` directly. `OperationRouting`'s three inlined fallback copies (`searchShards`, `indexMetadata`, `shards`) collapse to nothing extra — they already call the now-resolver-aware accessor.

**This is the step that touches the most call sites, but every one of them is a deletion, not a modification** — remove the manual `if (x == null) { x = AbsentIndexRoutingSuppliers.resolve(...); }` fallback and leave the original one-line accessor call, because the accessor itself now does that job.

### C4. Migrate the ~25 call sites and retire the static registries

Work through the fan-out list from the review one file at a time, each its own small commit:

`OperationRouting.java` · `RoutingNodes.java` · `IndexRoutingTable.java` · `IndicesClusterStateService.java` (6 sites) · `MetadataCreateIndexService.java` · `MetadataDeleteIndexService.java` · `MetadataIndexStateService.java` · `MetadataMappingService.java` · `IndexNameExpressionResolver.java` (6 sites) · `TransportBulkAction.java` · `TransportShardBulkAction.java` · `TransportReplicationAction.java` · `ActiveShardCount.java` · `ScaleIndexOperationValidator.java` · `TransportForceMergeAction.java` · `TransportRecoveryAction.java` · `TransportSegmentReplicationStatsAction.java` · `TransportIndicesSegmentsAction.java` · `TransportIndicesStatsAction.java` · `TransportClearIndicesCacheAction.java` · `TransportUpgradeAction.java` · `TransportUpgradeStatusAction.java` · `TransportRemoteStoreStatsAction.java` · `DataStreamsStatsAction.java` · `TransportGetIngestionStateAction.java` · `TransportUpdateIngestionStateAction.java` · `TransportCatShardsAction.java` · `RestAllocationAction.java` · `TransportGetFieldMappingsIndexAction.java` · `TransportAnalyzeAction.java` · `TransportSingleShardAction.java` · `TransportGetIndexAction.java` · `TransportGetSettingsAction.java` · `TransportIndicesAliasesAction.java` · `MappingStats.java`.

For each: delete the `AbsentIndex*Suppliers` call, confirm the plain accessor now does the job (it does, per C2/C3), delete the now-unused import, run that file's test class.

Once every caller is migrated: delete `AbsentIndexDescriptorSuppliers.java` and `AbsentIndexRoutingSuppliers.java` themselves, and `DescriptorOnlyCreation`'s usages that duplicated the same fallback (D handles the rest of `DescriptorOnlyCreation`).

**`BroadcastEmptiness` stays** — it's now a generic defense-in-depth check (a broadcast touching zero shards for an index it should have touched), not a serverless-specific detector, since the bug class it guards against is no longer specific to the static-registry path.

### C5. Update `plugins/serverless-storage`

The plugin implements `ClusterPlugin.getIndexMetadataResolver()`/`getIndexRoutingResolver()` returning its existing descriptor/gating logic, instead of populating the static `AtomicReference`s. Delete the plugin-side code that reaches into `AbsentIndexDescriptorSuppliers.install(...)`-style static setup.

**Test:** `./gradlew :plugins:serverless-storage:internalClusterTest` full pass, plus every `server` test touched across C2-C4.

**Risk:** this is the highest-risk phase in the whole plan — it changes how every gated/computed index resolves in the hottest paths in the codebase. Land C1-C3 (pure additive plumbing) fully tested and merged before starting C4 (the migration), and do C4 as many small, independently-revertable commits rather than one big one.

---

## Phase D — Replace the name-prefix convention with `IndexCreationStrategy`

**Depends on:** C (uses the same SPI registration point).

### D1. Define the SPI

```java
public interface IndexCreationStrategy {
    /** True if this strategy claims the given index name/request — e.g. a naming convention or a request attribute. */
    boolean claims(String indexName, CreateIndexClusterStateUpdateRequest request);

    /** Handle creation entirely; core's normal clusterStateCreateIndex is never called for a claimed index. */
    CompletionStage<ClusterStateUpdateResponse> createIndex(ClusterState state, CreateIndexClusterStateUpdateRequest request);

    /** Mirror hooks for delete/open/close — same claims()-gated shape. */
    boolean claimsForDeletion(ClusterState state, Index index);
    CompletionStage<AcknowledgedResponse> deleteIndex(ClusterState state, List<Index> claimed);
    // ...open/close equivalents
}
```

Registered via a new `ClusterPlugin.getIndexCreationStrategy()` default-empty hook, same registration point as C1.

### D2. Migrate call sites

- `MetadataCreateIndexService.createIndex()`: replace `DescriptorOnlyCreation.isRegistered() && namesAServerlessIndex(...)` with "does any registered strategy claim this name" — checked once, generically.
- `MetadataCreateIndexService.certainlyGated()` / `TransportCreateIndexAction.localExecute()`: replace with `strategy.canExecuteOffClusterManager()` (new method on the interface, default `false`).
- `MetadataDeleteIndexService.deleteIndex()`, `MetadataIndexStateService.open/closeIndices()`, `MetadataMappingService.putMapping()`: same pattern — one `claims(...)` check at the top, delegate the rest to the strategy.
- **Delete** `MetadataCreateIndexService.validateServerlessNamespace()` — this exact validation belongs in the plugin's own `IndexCreationValidator` implementation (the hook already exists and does exactly this job; see review §5, this was flagged as a hook that existed and wasn't used).
- Move the "serverless" wording, the 50-index multi-close/open cap, and the mixed-request refusal message out of `MetadataIndexStateService` into the plugin's own strategy implementation.
- `CreateIndexClusterStateUpdateRequest.descriptorWrite` field: remove it from the shared DTO; the strategy's `createIndex()` return value (a `CompletionStage`) replaces the side channel — the caller awaits the strategy's own future directly instead of stashing one on the request.

### D3. Update `DescriptorOnlyCreation`

Becomes an implementation detail entirely inside `plugins/serverless-storage`, implementing `IndexCreationStrategy`. Core no longer references it by name anywhere.

**Test:** every gated-index creation/deletion/open/close/mapping test in `plugins/serverless-storage`'s suite, plus `MetadataCreateIndexServiceTests`/`MetadataDeleteIndexServiceTests`/`MetadataIndexStateServiceTests` on the core side (these should need **no changes** if the migration preserved behavior for the non-gated path — that's the acceptance bar).

**Risk:** medium-high, touches five core services' primary entry points. Do this after C is fully landed and soaked.

---

## Phase E — Move `IndicesClusterStateService` gated-index policy into the plugin

**Depends on:** C, D (uses the resolver + strategy machinery both introduce).

### E1. Confirm the three existing hooks are sufficient

`AllocatedIndex.idleMillis()`, `AllocatedIndices.setOnDemandShardOpener(Consumer<Index>)`, `AllocatedIndices.hasExistingShardData(ShardId, String)` — all three are already clean, additive, default-safe (review §5 calls these out as the good part of this file). Confirm no fourth hook is needed once C's resolver handles the "local shards with no routing entry" problem (`RoutingNodes.localRoutingNode` no longer needs its own special case once C3 lands — the resolver's `localShardsFor` is consulted by `RoutingTable`/`RoutingNodes` directly).

### E2. Extract policy logic

Move out of `IndicesClusterStateService` into a new component inside `plugins/serverless-storage`:
- The `indices.gated.*` settings (sweep interval, idle-eviction-after, max-open) — these become plugin-owned settings, not node-scope settings registered by core.
- `sweepDeletedGatedIndices`, `evictColdestOfASample`, `resolveGatedMaxOpen`, the whole eviction subsystem.
- `openComputedShardsOnDemand` and its ~95-line implementation — the plugin implements this against `setOnDemandShardOpener`, taking `IndicesService`'s existing `createShard` as an injected dependency rather than `IndicesClusterStateService` open-coding a second lifecycle inline.
- `startComputedShardLocally`/`startComputedShardAfterRecovery` — this one needs a new, explicit hook rather than the implicit `routingTable().hasIndex()==false` check: add `EngineFactory`/`IndexShard`-level `boolean ownsShardStateTransitions()` (default `false`) that a plugin's engine can set `true` for, checked once instead of re-derived five times across `IndicesClusterStateService`.
- `withNodeLocalRecoverySource`/`computedAwareInSyncIds`/`computedAwareShardRoutingTable`: once C's resolver is wired into `RoutingTable`, these collapse — the "compute a substitute recovery source when there's no published routing" logic becomes the resolver's job (`IndexRoutingResolver.resolve(...)` already returns a full `IndexRoutingTable`, so recovery-source derivation from it is generic, not gated-specific).
- `heldOnDemand`/`DescriptorOnlyCreation.skipsClusterState` guards in `removeIndices`/`removeShards`/`updateIndices`: replace with a single generic predicate, `boolean isExternallyManaged()` on `IndexMetadata` (default `false`, set via the `IndexCreationStrategy` from D), read once per loop instead of four bespoke call sites.

**This is genuinely the largest single extraction in the plan** — expect it to be several weeks of a human team's time in a codebase this size; budget accordingly and don't rush the four call-site consolidation for `isExternallyManaged()` in particular, since `removeIndices`/`removeShards`/`updateIndices` are on the hottest path in the class.

**Risk:** high — this is core shard-lifecycle code every node runs on every cluster-state application. Land behind extensive internalClusterTest coverage, and consider feature-flagging the extraction itself (run old and new code paths side by side, diff results, before deleting the old path) given the blast radius.

---

## Phase F — Finish the mapper capability rollout

**Independent of C/D/E.**

- `ObjectMapper.TypeParser.parseNested`, `RoutingFieldMapper.preParse`, `TextFieldMapper`/`SourceFieldMapper`'s stored-field forcing, `KeywordFieldMapper`'s raw-value-tracking `PARSER`, `DocumentParser.builderSupplierForText`: each currently does `if (indexSettings.isPluggableDataFormatEnabled()) { ... }`. Replace each with a query against the same `DataFormatRegistry`/capability mechanism `MappedFieldType.searchCapability()` already established — e.g. `DataFormat.unsupportedMapperTypes()`, `DataFormat.requiresStoredFields()`, `DataFormat.rawValueTrackingRequired()`, `DataFormat.defaultDynamicFieldBuilder(XContentFieldType)`.
- `UnknownFieldRefresh`: replace the static `AtomicReference<Refresher>` with a `@Nullable Refresher` threaded through `MapperService`/`DocumentMapperParser` construction the same way `DataFormatRegistry` already is (per the review, this one's inconsistent with its own neighboring pattern).

**Test:** existing mapper test suites should be unaffected in behavior; add one test per replaced check confirming identical behavior on/off.

**Risk:** low-medium, mechanical once the target shape is set.

---

## Phase G — Fork-codec governance

**Independent.**

`ClusterMetadataManifest.java:70-85` (`FORK_CODEC_BASE`) and `:337-361` (unconditional, irreversible codec bump).

**Steps:**
1. Add a cluster setting, e.g. `cluster.remote_store.state.allow_fork_codecs` (default `false`), gating whether `MANIFEST_CURRENT_CODEC_VERSION` is allowed to resolve to anything `>= FORK_CODEC_BASE`. Default `false` means: out of the box, this build behaves exactly like it has no fork-only codecs, matching upstream's own staged-rollout discipline.
2. Document the setting clearly as "flip this only if you understand you're taking a one-way door away from stock-OpenSearch-readable repositories" — preserve the existing, good documentation, just make it something an operator opts into rather than something that's true unconditionally the moment a node upgrades.
3. `RemoteClusterStateCleanupManager`'s `unreadableCodec` guard (the good, already-existing safety net) needs no change — it already does the right thing regardless of how the setting resolves.

**Risk:** low — this is strictly a safety net added around an existing mechanism, not a functional change to the mechanism itself.

---

## Phase H — Decouple admission control from the Arrow SPI

**Independent.**

`NativeMemoryBasedAdmissionController.java` imports `org.opensearch.arrow.spi.PoolGroup`/`NativeAllocatorPoolStats` directly; `server/build.gradle` has an unconditional compile dependency on `libs:opensearch-arrow-spi`.

**Steps:**
1. Define `NativeMemoryPressureSignal` in `server`: `OptionalDouble utilizationPercentFor(AdmissionControlActionType type)`.
2. `NativeMemoryBasedAdmissionController` takes a `Supplier<NativeMemoryPressureSignal>` instead of `Supplier<NativeAllocatorPoolStats>`.
3. The Arrow/analytics-backend plugin implements `NativeMemoryPressureSignal` wrapping its own `PoolGroup`-keyed stats.
4. Remove `server`'s compile dependency on `libs:opensearch-arrow-spi` from `server/build.gradle` once nothing in `server/src/main` imports it anymore.
5. `ResourceTrackerSettings`'s 79%→80% native-headroom default change: revert to 79% as the shared default, and have the DataFusion parquet cache request its own budget reservation through whatever headroom-reservation mechanism exists (or add a minimal one: `ResourceTrackerSettings.reserve(String owner, ByteSizeValue amount)`), rather than retuning the shared default for one consumer.

**Risk:** low-medium — mechanical interface substitution, but confirm nothing else in `server/` legitimately needs the Arrow SPI before removing the build dependency.

---

## Phase I — Extract the independent bug fixes

**Independent, no blocking dependency, can run any time.**

Fifteen changes identified in the review that have nothing to do with serverless and are currently only reachable by merging the whole branch. Create a companion branch off `main` (not off `feature/serverless`) — `extract/generic-fixes-from-serverless` — and manually port each (not a straight cherry-pick, since history isn't linear per-fix):

1. `FsRepository.java` — CWE-22 `base_path` path-traversal fix (flag for security backport too).
2. `MetadataCreateIndexService.java` — `DEFAULT_REPLICA_COUNT_SETTING` node/persistent/transient resolution fix.
3. `SplitShardsMetadata.java` — generic in-place merge (undo-split) primitive.
4. `UnassignedInfo.java` / `DelayedAllocationService.java` — cluster-level delayed-allocation default.
5. `BalancedShardsAllocator.java` — balance-factor-sum validators (already has upstream issue #22305).
6. `BalancedShardsAllocator.java` — `weightSpreadAcrossAllNodes` decider-scan skip (re-benchmark before landing).
7. `DiskThresholdDecider.java` — shrink-source NPE guard.
8. `AutoExpandReplicas.java` — `anyIndexHasAutoExpandReplicas` fast path.
9. `MetadataUpdateSettingsService.java` — empty-node-set `allMatch` vacuous-truth fix.
10. `NestedQueryBuilder.java` — `visit()` recursion fix.
11. `Bitmap64IndexQuery.java` / `BitmapIndexQuery.java` — scorer-supplier fixes.
12. `Analysis.java` — `resolveAnalyzerPath` traversal fix.
13. `ShardSplittingQuery.java` — NPE fix.
14. `SubdirectoryAwareDirectory.java` — fsync durability fix.
15. `MetadataIndexTemplateService.java` — validation-overload consistency fix.

Each gets its own commit on the extraction branch with a test, so it's independently reviewable/mergeable to `main` regardless of what happens to the rest of this plan.

---

## Acceptance criteria for calling this plan "done"

- [ ] Every phase A-H item is either landed on `refactor/core-pluggability`, or has an explicit, written reason in this document for why it's deferred (not silently dropped).
- [ ] `AbsentIndexRoutingSuppliers.java` and `AbsentIndexDescriptorSuppliers.java` no longer exist in `server/` (Phase C4).
- [ ] `DescriptorOnlyCreation`'s name-prefix check is not referenced from any file under `server/src/main` (Phase D3).
- [ ] `grep -rl "serverless" server/src/main/java` returns nothing outside genuinely generic, non-product-specific hits (Phase D, E, G).
- [ ] `plugins/serverless-storage` and `server` test suites both pass at each phase boundary.
- [ ] `core-pluggability-review` artifact's five "urgent" flags (§4) are each resolved or explicitly deferred with reasoning (Phase A, B).
- [ ] Phase I's 15 fixes exist as individual, reviewable commits on `extract/generic-fixes-from-serverless`.

## Status log

Update this table as work lands. Don't let it go stale — it's the fastest way for anyone (including a future me) to see where this actually is versus where the plan says it should be.

| Phase | Status | Notes |
|---|---|---|
| A1 | **done** | Wired `refuseToWriteAMappingFromTheClusterStateThread(indexMetadata)` in immediately before `IndexDescriptorPublisher.createGated(...)` in `clusterStateCreateIndex`'s gated branch — exactly where the surrounding comment said "the refusal below" already lived. New `RefuseGatedWriteOnClusterStateThreadTests` (2 tests); 142 pre-existing gated/create-index tests unaffected. |
| A2 | **done** | `MetadataIndexAliasesService`'s gated-index alias branch now refuses `AliasAction.Add` with a filter/routing/write-index flag instead of silently keeping only the name. Updated the two stale "gated index provably has no aliases" comments in `Metadata.java`/`IndexNameExpressionResolver.java` to state the real, enforced invariant. New `GatedIndexAliasFilterSafetyTests` (5 tests); 18+76+46 pre-existing tests unaffected. |
| A3 | **done** | Investigated whether anything depends on the removed `SPLITTING`/`recoveringChildShards`/`parentShardId`/`shardWithRecoveringChild` representation, on this branch **and on `main`**. Found zero real callers on either: `OperationRouting#shardWithRecoveringChild` was never called by anything but its own definition even on `main`, and every other reference lived only in `main`'s dedicated `ShardRoutingStateSplitTests` unit test. It was speculative scaffolding for an earlier design, never wired to a production caller on either branch — not a load-bearing wire-compat concern. Decision: leave the removal as-is (reinstating would resurrect genuinely dead code), documented as a deliberate, reviewed decision via a class-level javadoc note on `ShardRoutingState` pointing at `SplitShardsMetadata` as the current mechanism. No hand-editable release-notes slot exists yet for an unreleased version in this repo's PR-title-driven changelog, so this status-log entry is the durable record of the decision. |
| B | **done** | Restored `PluginNodeStats`/`Plugin.nodeStats()` (from `main`'s last-known-good version) alongside the existing `getAdditionalHealthPaths`. `NodeStats` gained the generic `pluginStats` map back via a **new constructor overload** (old 34-arg constructor now delegates with `null`) rather than changing the existing signature, wire-gated at `V_3_8_0` (this fork's current unreleased version — `pluginStats` never existed on this branch at `V_3_7_0`, so there was no old wire format to stay BWC with). Added `NodesStatsRequest.Metric.PLUGIN_STATS` and threaded a new `pluginStats` boolean through `NodeService#stats(...)` (updated its 4 call sites: `TransportNodesStatsAction`, `TransportClusterStatsAction`, `InternalTestCluster`, and the existing native-memory test). New `NodeService#collectPluginStats()` iterates `PluginsService#filterPlugins(Plugin.class)`. Kept `nativeAllocatorStats`/`totalEstimatedNativeBytes` untouched, coexisting rather than migrating onto the generic path (that migration is deliberately deferred — two `NodeStats` wire-format changes in one commit is unnecessary risk). New `NodeServicePluginStatsTests` (4 tests); 25 pre-existing tests across `NodeServiceNativeMemoryTests`/`NodeStatsTests`/`ClusterStatsResponseTests`/`ClusterStatsNodesTests`/`DiskUsageTests` unaffected. |
| C1-C5 | not started | |
| D1-D3 | not started | |
| E1-E2 | not started | |
| F | not started | |
| G | not started | |
| H | not started | |
| I | not started | |
