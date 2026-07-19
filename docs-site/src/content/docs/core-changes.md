---
title: Core Changes
description: Changes made to OpenSearch core (outside plugins/serverless-storage) to make a pluggable, per-shard-role storage engine possible.
---

Everything on this page is a change to OpenSearch **core** (`server/`, `modules/`, blob-store repository plugins) — not to `plugins/serverless-storage` itself. These are the extension points and seams core needed before an external plugin could implement a writer/reader-split, object-store-native engine at all. For the plugin's own design and its internal correctness fixes, see [Architecture](/design/architecture/) and [Plugin Fixes](/plugin-fixes/).

`rfc-serverless-opensearch.md` and `rfc-serverless-metadata-plane.md` (repo root) are the design-level proposals behind this work — this page is the concrete, as-built list of what actually changed in core to realize them.

## 1. Per-shard-role engine and directory selection

Before this change, a plugin's `EngineFactory` and `Directory` were chosen once per index, at index-service construction — every shard copy of an index got the same engine. That makes a writer/reader split impossible: there was no hook to say "this specific shard copy is a search-only reader, give it a different engine than the primary."

- `EnginePlugin.getEngineFactory(IndexSettings, ShardRouting)` — a new overload taking the shard's routing, defaulting to the old index-only signature so existing plugins are unaffected.
- `IndexStorePlugin.DirectoryFactory.newDirectory(IndexSettings, ShardPath, ShardRouting)` — same pattern for directory selection.
- `IndexModule`/`IndexService` now carry a `BiFunction<IndexSettings, ShardRouting, IndexerFactory> indexerFactoryProvider`, resolved per-shard inside `IndexService#createShard` instead of once at index-service construction.
- `Node.java`'s engine-factory-provider collection changed from `Function<IndexSettings, ...>` to `BiFunction<IndexSettings, ShardRouting, ...>` to carry this through plugin wiring.

This is the seam the whole writer/reader split is built on: the plugin's `getEngineFactory` inspects the incoming `ShardRouting` and returns `WriterEngineFactory` or `ReaderEngineFactory` accordingly.

## 2. Engine abstraction: hooks for a non-local-durable, remote-materialized engine

A classic `Engine` assumes local Lucene commits plus a local translog are the durability source of truth, and that a shard with no local commit is either brand new or corrupt. An object-store-backed engine breaks both assumptions — durability lives in a remote WAL, and a shard can legitimately have no local commit yet still hold valid remote state (a reactivating scaled-to-zero shard, or a freshly cloned split child).

- **`EngineFactory` recovery hooks**, all `default false`, all `(IndexShard, Store) -> boolean`: `recoverMissingLocalStore`, `recoverInPlaceSplitLocalStore`, `recoverInPlaceMergeLocalStore`. `StoreRecovery.internalRecoverFromStore` tries these before falling back to the old hard failure — letting an engine materialize a local commit + translog from a remote manifest instead of the recovery failing.
- **`Engine.engineRecoveryOperations()`** — replays operations from a non-local-translog durability source (a WAL mirrored to remote storage) through the same `applyTranslogOperation` path core already uses for ordinary translog replay. Wired from `IndexShard.recoverAdditionalEngineOperations()`, called right after normal translog recovery.
- **`Engine.onPrimaryTermBumped(long)`** — fired atomically inside `IndexShard.bumpPrimaryTerm`, propagated through `Indexer`/`EngineBackedIndexer`. Gives an engine a guaranteed hook to capture activation-time state (e.g. a WAL replay-fencing position) at the exact moment a term bump happens, rather than racing to observe it separately.
- **`Engine.globalCheckpointSupplierForCombinedDeletionPolicy()`** — lets an engine widen local-commit retention when a remote store, not core's usual global-checkpoint/retention-lease chain, is the actual authority on what must be kept.
- **`Engine.ownsRemoteSegmentDurability()`** — opts a shard out of core's `RemoteStoreRefreshListener` upload path when the engine already publishes its own durable remote copy on commit, avoiding a redundant/conflicting upload.

## 3. Shard-level write guards and recovery-source dispatch

- **`IndexShard.ensureNotInProgressSplitParent()` / `ensureNotInProgressMergeChild()`** — reject primary writes during an in-progress in-place split or merge, closing a window where a write could land on a shard mid-transition and be stranded once the split/merge commits.
- `StoreRecovery`'s dispatch now routes `RecoverySource.Type.IN_PLACE_SPLIT_SHARD` / `IN_PLACE_MERGE_SHARD` through the normal `recoverFromStore` path, treating an empty local store on these recovery sources as "legitimately empty, to be materialized" rather than corrupt.

## 4. Node wiring: activating dormant split/merge services

`Node.java` now constructs and Guice-binds `MetadataInPlaceSplitShardService`, `MetadataInPlaceMergeShardService`, `MetadataInPlaceSplitShardCommitService`, and `MetadataInPlaceMergeShardCommitService`. The split service class already existed in core before this work but was never instantiated — its REST/transport actions were unreachable. This fixes that gap and adds the merge counterpart alongside it.

## 5. Blob-store compare-and-swap primitive

The plugin's `ShardHead` coordination (see [Architecture](/design/architecture/)) needs an atomic, fencable compare-and-swap over a small blob — something no existing `BlobContainer` method provided.

- **`BlobContainer.readRegister(String)`** / **`compareAndSwapRegister(String, long expectedGeneration, BytesReference)`** — new default methods (`UnsupportedOperationException` unless a backend implements them), backed by new `BlobRegister`/`BlobRegisterCasResult` types.
- **`FsBlobContainer`** implements it directly with a generation-tracked file and an intra-JVM `ReentrantLock` map (`FileChannel.lock()` is inter-process only, not sufficient alone).
- S3, Azure, and GCS repository code gained matching support so each backend's native conditional-write primitive (S3 `If-Match`, GCS generation preconditions, Azure ETags) can back the same interface.

This generalizes CAS-over-blob as a repository capability, rather than something specific to this plugin's manifest store.

## 6. REST handler serverless-scope annotation

**`RestHandler.serverlessScope()`**, a new default method returning `AVAILABLE` / `INTERNAL_ONLY` / `UNAVAILABLE` (default `UNAVAILABLE`), with roughly fifteen existing `Rest*Action` classes annotating themselves `AVAILABLE`. Nothing in `RestController` enforces this yet — it's a forward-declaring seam so handlers record their intended availability incrementally, ahead of a future gate that would reject `UNAVAILABLE` handlers in a serverless deployment. Treat this as scaffolding, not an active restriction.

## 7. Routing and recovery-source changes for split/merge

- **`ShardRoutingState.SPLITTING`** was removed. The transient in-progress-split marker on the parent shard is superseded by the two-phase commit-service model in item 4 — a split child is now a first-class routing table entry from the start, not an ephemeral flag.
- **`RecoverySource` gained `InPlaceMergeShardRecoverySource`**, carrying the retired children's `ShardRange`s directly. Unlike the split case, this can't be read back from `SplitShardsMetadata` at parent-recovery time, because the merge's cluster-state update de-commits the split metadata in the same step that revives the parent.
- **`SplitShardsMetadata`** grew substantially to track in-progress merges alongside splits (`isSplitOfShardInProgress`, `isChildOfInProgressMerge`), and `OperationRouting`/`IndexRoutingTable` route based on that instead of the old `includeInProgressChild` parameter.
- **`IndexMetadata`** relaxes `inSyncAllocationIds`/`primaryTermsMap` invariants to allow entries for shard IDs at or above `numberOfShards` (reserved for in-place-split children) without growing `numberOfShards` itself, since that value stays the hash-routing denominator.

## 8. New in-place merge transport actions

`InPlaceMergeShardAction` / `TransportInPlaceMergeShardAction` / `RestInPlaceMergeShardAction`, mirroring the pre-existing split actions. Combined with item 4's wiring fix and item 7's metadata support, this completes what had been a split-only, partially-wired feature into a full two-phase split-and-merge capability in core, which the plugin's resharding feature (see [Flows](/design/flows/)) builds on directly rather than reimplementing.

:::note
This list reflects a diff against the OpenSearch upstream merge-base, filtered to exclude `plugins/serverless-storage` and this docs site. Class and method names are transcribed from that diff — confirm against the current source before relying on exact signatures.
:::
